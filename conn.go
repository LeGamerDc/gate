package gate

import (
	"net/http"
	"net/netip"
	"sync/atomic"
)

// Handler 是业务唯一必须实现的接口。一个实例服务所有连接（连接私有状态放 S），
// 方法会被多个事件循环并发调用，自身字段必须只读或自带同步。
type Handler[S any] interface {
	// OnOpen 在连接真正可以发送数据的时刻调用（TCP：socket 就绪；WS：101 已
	// 写进出站链之后），返回值成为 c.State。返回 error 表示拒绝：立刻关闭，
	// 且不会调用 OnClose。
	OnOpen(c *Conn[S]) (S, error)
	// OnMessage 每收到一条完整业务消息调用一次；msg 生命周期仅限本次调用。
	// 返回非 nil：同一批剩下的消息不再投递，连接关闭（返回前 Send 的仍会送出）。
	OnMessage(c *Conn[S], msg []byte) error
	// OnClose 恰好调用一次；c.State 在期间仍然有效。
	OnClose(c *Conn[S], reason error)
}

// None 用于不需要每连接状态的场景：gate.Options[gate.None]。
type None = struct{}

// Handshake 是 WebSocket 升级请求的只读快照（非 WS 连接为 nil）。
type Handshake struct {
	URI    string
	Header http.Header
}

// Conn 是连接句柄的「壳」（06「Conn 的壳与核」）：id/remote/hs 不可变，
// 任何状态下（含 Closed，OnClose 里打日志）可读；需要活连接的方法经 core，
// 进入 Detached 时 core 置 nil，此后返回 ErrConnClosed / no-op。
//
// *Conn 永不复用（01 D17）：它会逃逸到业务注册表、定时任务、resume 闭包里。
type Conn[S any] struct {
	// State 是业务状态，只允许在串行域内访问：
	// OnOpen / OnMessage / OnClose / Post / AsyncDo 的函数体。
	State S

	id     uint64
	remote netip.AddrPort
	hs     *Handshake
	core   atomic.Pointer[connCore]
}

// bindConn 把泛型壳与非泛型核对接：回调闭包捕获壳与 handler。
//
// 整体赋值 core.cb 是安全的：coreCallbacks 里只有业务回调，传输层的钩子
// （onDrain）是 connCore 的另一个字段，不会被这一句抹掉。
func bindConn[S any](h Handler[S], core *connCore, id uint64, remote netip.AddrPort, hs *Handshake) *Conn[S] {
	shell := &Conn[S]{id: id, remote: remote, hs: hs}
	shell.core.Store(core)
	core.cb = coreCallbacks{
		onOpen: func() error {
			s, err := h.OnOpen(shell)
			if err != nil {
				return err
			}
			shell.State = s
			return nil
		},
		onMessage: func(msg []byte) error { return h.OnMessage(shell, msg) },
		onClose:   func(reason error) { h.OnClose(shell, reason) },
		detach:    func() { shell.core.Store(nil) },
	}
	return shell
}

// ID 进程内唯一且永不复用；适合做注册表 key。任意状态可用。
func (c *Conn[S]) ID() uint64 { return c.id }

// Remote 返回对端地址（PROXY 模式下是真实来源地址）。任意状态可用。
func (c *Conn[S]) Remote() netip.AddrPort { return c.remote }

// Handshake 返回 WS 升级请求快照；非 WebSocket 返回 nil。任意状态可用。
func (c *Conn[S]) Handshake() *Handshake { return c.hs }

// Send 复制 b 入队；允许 gate 压缩、合包、加密。可从任意 goroutine 调用。
func (c *Conn[S]) Send(b []byte) error {
	core := c.core.Load()
	if core == nil {
		return ErrConnClosed
	}
	return core.out.send(b, flagZ|flagC|flagE)
}

// SendAlone 复制 b 入队；独立帧，不压缩不合包，加密照旧——
// 没有任何接口能绕过连接的 Cipher。
func (c *Conn[S]) SendAlone(b []byte) error {
	core := c.core.Load()
	if core == nil {
		return ErrConnClosed
	}
	return core.out.send(b, flagE)
}

// SendFrame 零拷贝发送预编码帧（广播用）。配了 Cipher 的连接返回 ErrCipherConflict。
func (c *Conn[S]) SendFrame(f *Frame) error {
	core := c.core.Load()
	if core == nil {
		return ErrConnClosed
	}
	return core.out.sendFrame(f)
}

// SendFunc 零复制路径：业务直接写进 gate 的出站缓冲。
func (c *Conn[S]) SendFunc(n int, fill func(b []byte) (int, error)) error {
	core := c.core.Load()
	if core == nil {
		return ErrConnClosed
	}
	return core.out.sendFunc(n, fill)
}

// Writable 报告出站积压是否低于高水位（软背压信号）。
func (c *Conn[S]) Writable() bool {
	core := c.core.Load()
	if core == nil {
		return false
	}
	return core.out.isWritable()
}

// Close 关闭连接；reason 原样传给 OnClose（nil 表示正常关闭）。幂等。
func (c *Conn[S]) Close(reason error) {
	if core := c.core.Load(); core != nil {
		core.requestClose(reason)
	}
}

// SetCipher 设置逐连接加密：入站对下一个被解析的帧生效，出站经队列 barrier
// 生效。nil 表示关闭加密。
//
// **只能在事件循环线程上调用**——回调（OnOpen / OnMessage / OnClose）与 Post
// 的函数体。这比 State 的约束窄一格：AsyncDo 的函数体虽然属于业务状态意义上
// 的串行域，却跑在另一个 goroutine 上，而 cipher 是 loop 私有状态。
// 在 AsyncDo 里需要它时用 Post 排回去。
func (c *Conn[S]) SetCipher(ci Cipher) {
	if core := c.core.Load(); core != nil {
		core.setCipher(ci)
	}
}

// Post 把 f 排进该连接的串行域执行。已关闭 ⇒ 不执行、不报错。
func (c *Conn[S]) Post(f func(*Conn[S])) {
	if core := c.core.Load(); core != nil {
		core.post(func() { f(c) })
	}
}

// AsyncDo 在别的 goroutine 上跑 f，f 与该连接的其余回调不重叠（期间投递暂停），
// 因此在 f 里访问 State 是安全的。同一连接同时只允许一个在途任务。
//
// **只能在事件循环线程上调用**（回调与 Post 的函数体），不能在另一个 AsyncDo
// 的函数体里调——它会碰读闸与 poller 注册这些 loop 私有状态。
//
// 连接要关时（回调返回 error 或 panic）已注册的 f **不会启动**。
func (c *Conn[S]) AsyncDo(f func()) error {
	core := c.core.Load()
	if core == nil {
		return ErrConnClosed
	}
	return core.asyncDo(f)
}

// Pause 低级原语：只暂停入站，不提供独占保证。
// 返回的 resume 幂等、可从任意 goroutine 调用。
//
// **只能在事件循环线程上调用**（回调与 Post 的函数体）：它会改读闸深度、
// LRU 归属与 poller 注册，这些是 loop 私有状态。理由同 SetCipher。
func (c *Conn[S]) Pause() (resume func()) {
	core := c.core.Load()
	if core == nil {
		return func() {}
	}
	return core.pause()
}
