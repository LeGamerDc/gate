package gate

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

type serverReady struct {
	engine gnet.Engine
	addr   string
	err    error
}

// Server is a running gate server instance.
type Server struct {
	engine gnet.Engine
	addr   string

	stopOnce sync.Once

	done chan struct{}

	errMu sync.Mutex
	err   error
}

// loggerOnce 保证包级 log 只被赋值一次。
//
// 原先每次 StartServer 都会覆盖 log，而事件循环 goroutine 正在读它——同进程
// 起第二个 server 就是一个 data race，并且会把第一个 server 的 logger 换掉。
// gnet 的 SetDefaultLoggerAndFlusher 本身也是进程级全局，所以这里的语义只能是
// "全进程共用第一个 server 的 logger"，把它明确下来而不是留一个竞态。
var loggerOnce sync.Once

// StartServer starts a gate server in the background and returns once the
// listener is ready to accept connections.
//
// 注意：gate 与 gnet 的日志是进程级全局的，同一进程内启动多个 server 时，
// 只有第一个 server 的 Logger 生效。
func StartServer(c *Config) (*Server, error) {
	if err := c.purge(); err != nil {
		return nil, err
	}
	loggerOnce.Do(func() {
		logging.SetDefaultLoggerAndFlusher(c.Logger, nil)
		log = c.Logger
	})

	e := &ev{
		c:         c,
		ready:     make(chan serverReady, 1),
		trackIdle: c.IdleTimeout > 0,
	}
	s := &Server{
		done: make(chan struct{}),
	}

	opts := []gnet.Option{
		gnet.WithNumEventLoop(c.LoopCount),
	}
	if ka := c.keepAlive(); ka > 0 {
		// 没有 SO_KEEPALIVE 时，被 NAT 静默丢弃的半开连接永远不会触发 OnClose，
		// 会一直占着缓冲区和 handler。
		opts = append(opts, gnet.WithTCPKeepAlive(ka))
	}
	if e.trackIdle {
		// OnTick 是空闲连接扫描的驱动，不开 ticker 它根本不会被调用。
		opts = append(opts, gnet.WithTicker(true))
	}

	go func() {
		err := gnet.Run(e, c.protoAddr(), opts...)
		s.setErr(err)
		close(s.done)
	}()

	select {
	case ready := <-e.ready:
		if ready.err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = ready.engine.Stop(ctx)
			<-s.done
			return nil, ready.err
		}
		s.engine = ready.engine
		s.addr = ready.addr
		return s, nil
	case <-s.done:
		return nil, s.errValue()
	}
}

// Addr returns the resolved listening address.
func (s *Server) Addr() string {
	return s.addr
}

// Stop gracefully stops the server. It is safe to call multiple times.
func (s *Server) Stop(ctx context.Context) error {
	var err error
	s.stopOnce.Do(func() {
		err = s.engine.Stop(ctx)
	})
	return err
}

// Wait blocks until the server exits.
func (s *Server) Wait() error {
	<-s.done
	return s.errValue()
}

func (s *Server) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

func (s *Server) errValue() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func engineListenAddr(en gnet.Engine) (string, error) {
	fd, err := en.Dup()
	if err != nil {
		return "", err
	}

	file := os.NewFile(uintptr(fd), "gate-listener")
	defer file.Close()

	ln, err := net.FileListener(file)
	if err != nil {
		return "", err
	}
	defer ln.Close()

	return ln.Addr().String(), nil
}
