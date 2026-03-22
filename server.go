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

// StartServer starts a gate server in the background and returns once the
// listener is ready to accept connections.
func StartServer(c *Config) (*Server, error) {
	if err := c.purge(); err != nil {
		return nil, err
	}
	logging.SetDefaultLoggerAndFlusher(c.Logger, nil)
	log = c.Logger

	e := &ev{
		c:     c,
		ready: make(chan serverReady, 1),
	}
	s := &Server{
		done: make(chan struct{}),
	}

	go func() {
		err := gnet.Run(e, c.protoAddr(), gnet.WithNumEventLoop(c.LoopCount))
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
