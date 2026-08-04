package gate

import (
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

var (
	_ gnet.EventHandler = (*ev)(nil)
	// log 在 StartServer 之前也必须可用：DefaultSenderBuilder 等包级对象可能
	// 在没有启动过 server 的情况下被使用，此前那种情况会直接 nil panic。
	log logging.Logger = logging.GetDefaultLogger()
)

type ev struct {
	c     *Config
	en    gnet.Engine
	ready chan serverReady

	// conns 是活跃连接注册表，只为 IdleTimeout 扫描而存在；未启用时完全不写入，
	// 避免在热路径上白白付出 sync.Map 的开销。
	conns     sync.Map // *Conn -> struct{}
	connCnt   atomic.Int64
	trackIdle bool
}

func StartEventLoop(c *Config) error {
	s, err := StartServer(c)
	if err != nil {
		return err
	}
	return s.Wait()
}

// safeCall 在调用业务代码时兜住 panic。
//
// gnet 核心一个 recover 都没有，任何从事件循环里逃逸出去的 panic 都会直接终止
// 进程，所以每个回调业务代码的边界都必须有这层保护——否则一条能让业务解码
// panic 的畸形消息就能打死整个网关。
func safeCall(what string, f func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("[gate] panic in %s: %v\n%s", what, r, debug.Stack())
			ok = false
		}
	}()
	f()
	return true
}

func (e *ev) OnBoot(en gnet.Engine) (action gnet.Action) {
	e.en = en
	if e.ready != nil {
		addr, err := engineListenAddr(en)
		e.ready <- serverReady{
			engine: en,
			addr:   addr,
			err:    err,
		}
	}
	return gnet.None
}

func (e *ev) OnShutdown(gnet.Engine) {}

func (e *ev) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	// 先原子占位再建连接：check-then-add 在多 event loop 下不是原子的，
	// 并发的 OnOpen 会全部读到低于上限的计数、全部放行，一次连接风暴就能冲破
	// MaxConnections。原子变量只防数据竞争，不会让"检查并占位"变成一步。
	if !e.reserveSlot() {
		log.Warnf("[gate] connection limit %d reached, rejecting %v", e.c.MaxConnections, c.RemoteAddr())
		return nil, gnet.Close
	}

	ip, port := remoteAddrOf(c)
	conn := &Conn{
		conn:       c,
		remoteIp:   ip,
		remotePort: port,
		codec:      serverCodec(e.c.MaxMessageSize),
	}
	conn.init()

	ws := e.c.Transport == ServerTransportWebSocket
	if ws {
		conn.outbound = &wsOutbound{conn: c}
	} else {
		// TCP 没有握手阶段，连接建立即可写。
		conn.markOpen()
	}

	if !safeCall("SenderBuilder.Build", func() { conn.sender = e.c.SB.Build(conn) }) || conn.sender == nil {
		e.releaseSlot()
		return nil, gnet.Close
	}
	if !safeCall("ConnHandlerBuilder.Build", func() { conn.handler = e.c.CHB.Build(conn) }) || conn.handler == nil {
		closeSender(conn)
		e.releaseSlot()
		return nil, gnet.Close
	}

	if e.trackIdle {
		e.conns.Store(conn, struct{}{})
	}

	if ws {
		c.SetContext(newWSConnState(
			conn,
			e.c.WebSocketPath,
			e.c.MaxWebSocketHandshakeBytes,
			e.c.MaxWebSocketBufferedBytes,
			e.c.OnWebSocketUpgrade,
		))
		// WebSocket 要等握手完成才算就绪，markOpen/notifyReady 由 wsConnState 触发。
		return nil, gnet.None
	}

	c.SetContext(conn)
	if !conn.notifyReady() {
		// OnReady panic 说明业务的连接级初始化没有走完，继续把消息喂给一个
		// 半初始化的 handler 只会让问题更隐蔽。其他回调 panic 也都是关连接。
		return nil, gnet.Close
	}
	return nil, gnet.None
}

// reserveSlot 原子地占用一个连接名额，超过上限时返回 false。
func (e *ev) reserveSlot() bool {
	n := e.connCnt.Add(1)
	if limit := e.c.MaxConnections; limit > 0 && n > int64(limit) {
		e.connCnt.Add(-1)
		return false
	}
	return true
}

func (e *ev) releaseSlot() {
	e.connCnt.Add(-1)
}

// closeSender 调用业务可能自定义的 SenderI.Close，并隔离它的 panic。
func closeSender(conn *Conn) {
	if conn.sender == nil {
		return
	}
	safeCall("SenderI.Close", conn.sender.Close)
}

func (e *ev) OnClose(c gnet.Conn, _ error) (action gnet.Action) {
	conn := connOf(c)
	if conn == nil {
		return gnet.None
	}

	// 先落 closed 状态再做其余清理。OnClose 和发送回调都跑在同一个事件循环
	// goroutine 上，因此这一步之后再排队的任何 Wake 回调都会在 writeOutbound
	// 处被 ErrConnClosed 挡下，不会碰到这个马上要被 gnet close 掉、并且可能被
	// 新连接复用的 fd。
	conn.markClosed()

	if e.trackIdle {
		e.conns.Delete(conn)
	}
	e.connCnt.Add(-1)

	if conn.handler != nil {
		safeCall("ConnHandler.Close", conn.handler.Close)
	}
	// SenderI 可以是业务自定义实现，它的 Close 同样是业务代码，必须隔离。
	closeSender(conn)
	return gnet.None
}

func (e *ev) OnTraffic(c gnet.Conn) (action gnet.Action) {
	if e.c.Transport == ServerTransportWebSocket {
		state, ok := c.Context().(*wsConnState)
		if !ok {
			return gnet.Close
		}
		return state.onTraffic()
	}
	conn, ok := c.Context().(*Conn)
	if !ok {
		return gnet.Close
	}
	return conn.onTraffic()
}

func (e *ev) OnTick() (delay time.Duration, action gnet.Action) {
	interval := e.c.idleCheckInterval()
	if interval <= 0 {
		// gnet 的 ticker 会拿返回值直接 Reset 定时器，返回 0 会变成忙等。
		// 正常情况下 ticker 只在 IdleTimeout > 0 时才启用，这里只是兜底。
		return maxIdleCheckInterval, gnet.None
	}

	var (
		now     = time.Now()
		timeout = e.c.IdleTimeout
		closed  int
	)
	e.conns.Range(func(k, _ any) bool {
		conn, ok := k.(*Conn)
		if !ok {
			return true
		}
		if conn.isClosed() {
			e.conns.Delete(conn)
			return true
		}
		if conn.idleFor(now) >= timeout {
			conn.Close()
			closed++
		}
		return true
	})
	if closed > 0 {
		log.Infof("[gate] idle sweep closed %d connection(s)", closed)
	}
	return interval, gnet.None
}

// notifyReady 在连接真正可以发送数据时通知 handler：TCP 是 OnOpen 之后立即，
// WebSocket 是握手完成之后。
//
// 在此之前 handler 在 OnOpen 阶段就被构造出来了，WebSocket 模式下如果它在构造
// 时就发消息，一个 WS 数据帧会被直接写进还没写完的 HTTP 握手响应里，连接作废。
// notifyReady 返回 false 表示业务的 OnReady panic 了，调用方应当关闭连接。
func (c *Conn) notifyReady() bool {
	// 先把握手期间入队的消息放出去，再通知业务。
	//
	// sender 在连接尚不可写时会把消息留在队列里并复位 triggered，此时没有任何
	// Wake 在路上；不主动 flush 的话，这批消息会一直滞留到业务下一次 Send。
	// 顺序也保证了 Build 阶段发的消息排在 OnReady 里发的消息前面。
	if f, ok := c.sender.(flusher); ok {
		f.flush()
	}
	if h, ok := c.handler.(ReadyHandler); ok {
		return safeCall("ConnHandler.OnReady", h.OnReady)
	}
	return true
}

// flusher 是 SenderI 的可选扩展：实现了它的 sender 可以在连接变为可写时
// 被要求立即排空队列。放成可选接口而不是加进 SenderI，是为了不破坏外部实现。
type flusher interface {
	flush()
}

// connOf 从 gnet 连接上下文取回 *Conn，两种传输都适用。
func connOf(c gnet.Conn) *Conn {
	switch v := c.Context().(type) {
	case *Conn:
		return v
	case *wsConnState:
		return v.conn
	default:
		return nil
	}
}

// remoteAddrOf 安全地取出对端地址。
//
// 原先是裸的 .(*net.TCPAddr) 断言：gnet 在 getpeername 失败时会返回 nil，
// 那会在事件循环 goroutine 里 panic，进而终止整个进程。
func remoteAddrOf(c gnet.Conn) (net.IP, int) {
	switch a := c.RemoteAddr().(type) {
	case *net.TCPAddr:
		return a.IP, a.Port
	case *net.UDPAddr:
		return a.IP, a.Port
	default:
		return nil, 0
	}
}
