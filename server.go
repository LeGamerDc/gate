//go:build darwin || linux

package gate

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

func splitHostPort(addr string) (string, string, error) { return net.SplitHostPort(addr) }

func joinHostPort(host string, port uint16) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// Options 是 Listen/Run 的全部配置。按关注点分组，分组本身就是文档。
type Options[S any] struct {
	Addr      string // 必填，如 ":8081" / "127.0.0.1:0"（0 = 随机端口，用 Server.Addr() 取回）
	Loops     int    // 0 = NumCPU()，夹在 [1, NumCPU()]
	Handler   Handler[S]
	Outbound  Outbound
	Limits    Limits
	Socket    Socket
	WebSocket *WebSocketOptions // nil ⇒ 裸 TCP
	Proxy     ProxyMode         // PROXY protocol，默认 Off
	Log       Logger            // 未设时不输出（库不该往别人的 stdout 里写）
}

// Server 是运行中的网关实例。
type Server[S any] struct {
	opts  Options[S] // Listen 深拷贝冻结
	log   Logger
	loops []*loop
	lns   []int // listener fds
	addr  netip.AddrPort

	conns       atomic.Int64 // MaxConns 先原子占位再 accept
	handshaking atomic.Int64 // MaxHandshaking 同理

	frameMu  sync.Mutex
	frameEnc *zstd.Encoder // NewFrame 专用（冷路径，锁保护）

	closed  atomic.Bool
	done    chan struct{}
	wg      sync.WaitGroup
	waitErr error
}

// hsContext 是握手期（Accepted/Proxy/Handshaking）的临时状态，
// 进入 Open（bindConn）后释放（W9 的 server 侧对应物）。
type hsContext[S any] struct {
	srv    *Server[S]
	remote netip.AddrPort
	id     uint64
}

// Listen 启动 server，返回时监听套接字已就绪（测试可以直接连）。
func Listen[S any](opts Options[S]) (*Server[S], error) {
	if opts.Addr == "" {
		return nil, errors.New("gate: Options.Addr required")
	}
	if opts.Handler == nil {
		return nil, errors.New("gate: Options.Handler required")
	}
	// 深拷贝冻结：返回后调用方再改原结构不影响已启动的 server。
	if opts.Outbound.Dict != nil {
		opts.Outbound.Dict = append([]byte(nil), opts.Outbound.Dict...)
	}
	if opts.WebSocket != nil {
		ws := *opts.WebSocket
		opts.WebSocket = &ws
	}
	nloops := opts.Loops
	if nloops <= 0 || nloops > runtime.NumCPU() {
		nloops = runtime.NumCPU()
	}
	if nloops < 1 {
		nloops = 1
	}

	s := &Server[S]{opts: opts, done: make(chan struct{})}
	s.log = opts.Log
	if s.log == nil {
		s.log = nopLogger{}
	}

	cfg := normalizeLoopConfig(opts.Outbound, opts.Limits, opts.WebSocket != nil)
	encOpts := encoderOptions{level: opts.Outbound.CompressLevel, dict: opts.Outbound.Dict}

	// listener：Linux 每 loop 一个（SO_REUSEPORT）；macOS 单 listener 在 loop 0。
	// ":0" 陷阱（04）：不能让每个 listener 各自 bind 0——第一个取回真实端口，
	// 其余绑同一个端口。
	nListeners := 1
	if platformReusePort {
		nListeners = nloops
	}
	bindAddr := opts.Addr
	for i := range nListeners {
		fd, bound, err := listenTCP(bindAddr, platformReusePort && nloops > 1)
		if err != nil {
			s.closeListeners()
			return nil, err
		}
		s.lns = append(s.lns, fd)
		if i == 0 {
			s.addr = bound
			if bound.Port() != 0 && addrHasZeroPort(opts.Addr) {
				bindAddr = joinBound(opts.Addr, bound.Port())
			}
		}
	}

	for i := range nloops {
		p, err := newPoller()
		if err != nil {
			s.closeListeners()
			for _, l := range s.loops {
				_ = l.p.close()
			}
			return nil, err
		}
		l := newLoop(p, sysIO{}, monotonicNow(), cfg, encOpts)
		l.loopID = i
		l.onConnClosed = func(c *connCore) {
			s.conns.Add(-1)
			if c.hsCtx != nil { // 死在握手期：Open 前的计数要退回
				s.handshaking.Add(-1)
				l.stats.connsHandshaking.Add(-1)
			}
		}
		l.onAccept = func(lfd int) { s.accept(l, lfd) }
		l.onHsRead = func(c *connCore) { s.hsRead(l, c) }
		if fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0); err == nil {
			l.reserveFd = fd // EMFILE 的预留 fd（R11）
		}
		s.loops = append(s.loops, l)
	}
	// 注册 listener 并启动。
	for i, lfd := range s.lns {
		l := s.loops[0]
		if platformReusePort {
			l = s.loops[i]
		}
		l.listener = lfd
		if err := l.p.add(lfd, makeToken(tokListener, 0, uint32(lfd)), interestRead); err != nil {
			s.closeListeners()
			for _, l := range s.loops {
				_ = l.p.close()
			}
			return nil, err
		}
	}
	for _, l := range s.loops {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			l.run()
		}()
	}
	return s, nil
}

// Run = Listen + Wait，ctx 取消即优雅关闭。给 main() 用。
func Run[S any](ctx context.Context, opts Options[S]) error {
	s, err := Listen(opts)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(sctx)
	}()
	return s.Wait()
}

// Addr 返回实际监听地址（随机端口场景取回真实端口）。
func (s *Server[S]) Addr() netip.AddrPort { return s.addr }

// Wait 阻塞到 Shutdown 完成。
func (s *Server[S]) Wait() error {
	<-s.done
	return s.waitErr
}

// Stats 返回全 server 汇总（各 loop 计数器求和）。
func (s *Server[S]) Stats() Stats {
	var out Stats
	for _, l := range s.loops {
		addStats(&out, l.stats.snapshot())
	}
	return out
}

// LoopStats 逐 loop 返回，用来观察倾斜。
func (s *Server[S]) LoopStats() []Stats {
	out := make([]Stats, len(s.loops))
	for i, l := range s.loops {
		out[i] = l.stats.snapshot()
	}
	return out
}

// NewFrame 预编码一帧（帧头 + 按 server 的 Outbound 配置压缩），
// 供 SendFrame 广播。它是 Server 的方法：包级函数拿不到压缩配置。
func (s *Server[S]) NewFrame(payload []byte, opts ...FrameOption) (*Frame, error) {
	s.frameMu.Lock()
	defer s.frameMu.Unlock()
	if s.frameEnc == nil && s.opts.Outbound.CompressThreshold > 0 {
		eopts := []zstd.EOption{
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstdLevel(s.opts.Outbound.CompressLevel)),
		}
		if len(s.opts.Outbound.Dict) > 0 {
			eopts = append(eopts, zstd.WithEncoderDict(s.opts.Outbound.Dict))
		}
		enc, err := zstd.NewWriter(nil, eopts...)
		if err != nil {
			return nil, err
		}
		s.frameEnc = enc
	}
	return newFrame(payload, s.opts.Outbound, s.frameEnc, opts...)
}

// Shutdown 优雅关闭：停止 accept → 每条连接尽力 flush 后 OnClose(ErrServerClosed)
// → 全部关闭后返回。ctx 到期则放弃剩余 flush 直接关闭。幂等。
func (s *Server[S]) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		<-s.done
		return s.waitErr
	}
	// 1. 停止 accept：listener 只在此刻统一关闭（运行期关闭会让内核丢掉
	// 已排队未 accept 的连接——所以运行期绝不动它）。
	for _, l := range s.loops {
		l.box.push(func() {
			if l.listener >= 0 {
				_ = l.p.del(l.listener)
				_ = unixClose(l.listener)
				l.listener = -1
			}
			for i := range l.slots {
				if c := l.slots[i].c; c != nil {
					l.closeLocal(c, ErrServerClosed)
				}
			}
		})
		l.wk.maybeNotify()
	}
	// 2. 等排空或 ctx 到期。
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for s.conns.Load() > 0 {
		select {
		case <-ctx.Done():
			// 放弃剩余 flush：强制拆除。
			for _, l := range s.loops {
				l.box.push(func() {
					for i := range l.slots {
						if c := l.slots[i].c; c != nil {
							l.enterDraining(c)
							l.detach(c)
						}
					}
				})
				l.wk.maybeNotify()
			}
			s.waitFor(func() bool { return s.conns.Load() == 0 })
			goto stop
		case <-tick.C:
		}
	}
stop:
	for _, l := range s.loops {
		l.stop.Store(true)
		_ = l.p.notify()
	}
	s.wg.Wait()
	close(s.done)
	return s.waitErr
}

func (s *Server[S]) waitFor(cond func() bool) {
	deadline := time.Now().Add(time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

func (s *Server[S]) closeListeners() {
	for _, fd := range s.lns {
		_ = unix.Close(fd)
	}
}

// ─── accept ───

const acceptBudget = 64 // 每轮迭代的 accept 上限：连接风暴不饿死已有连接

func (s *Server[S]) accept(l *loop, lfd int) {
	for range acceptBudget {
		nfd, sa, err := sysAccept(lfd)
		if err != nil {
			switch {
			case errors.Is(err, unix.EAGAIN):
				return
			case errors.Is(err, unix.ECONNABORTED), errors.Is(err, unix.EINTR):
				continue
			case errors.Is(err, unix.EMFILE), errors.Is(err, unix.ENFILE),
				errors.Is(err, unix.ENOBUFS), errors.Is(err, unix.ENOMEM):
				s.fdExhausted(l, lfd, err)
				return
			default:
				s.log.Warn("gate: accept", zap.Error(err))
				return
			}
		}
		if s.closed.Load() {
			_ = unix.Close(nfd)
			continue
		}
		// MaxConns 先原子占位再放行：check-then-add 在多 loop 下不是原子的。
		// 满额时 accept 之后立刻 close（明确拒绝）——只跳过的话连接留在
		// backlog 里，水平触发每轮都报 listener 可读，空转。
		if mc := s.opts.Limits.MaxConns; mc > 0 && s.conns.Add(1) > int64(mc) {
			s.conns.Add(-1)
			_ = unix.Close(nfd)
			l.stats.connsRejected.Add(1)
			continue
		} else if mc <= 0 {
			s.conns.Add(1)
		}
		remote := sockaddrToAddrPort(sa)
		applySocketOpts(nfd, s.opts.Socket, keepAliveSecs(s.opts.Limits.KeepAlive))
		l.stats.connsAccepted.Add(1)

		if platformReusePort || len(s.loops) == 1 {
			s.registerConn(l, nfd, remote)
		} else {
			// macOS：单 acceptor，投递给连接数最少的 loop。
			target := s.leastLoadedLoop()
			target.box.push(func() { s.registerConn(target, nfd, remote) })
			target.wk.maybeNotify()
		}
	}
	// 没 accept 完不要紧：水平触发下一轮还会再报。
}

func keepAliveSecs(d time.Duration) int {
	switch {
	case d == 0:
		return 60 // 默认 60s
	case d < 0:
		return 0 // DisableKeepAlive
	default:
		return max(1, int(d/time.Second))
	}
}

func (s *Server[S]) leastLoadedLoop() *loop {
	best := s.loops[0]
	for _, l := range s.loops[1:] {
		if l.stats.connsOpen.Load() < best.stats.connsOpen.Load() {
			best = l
		}
	}
	return best
}

// fdExhausted 是 EMFILE/ENFILE/ENOBUFS/ENOMEM 的统一路径（R11/R-D6）：
// 用预留 fd 腾位置 → accept → 立刻关闭（明确拒绝）→ 摘掉读兴趣指数退避。
func (s *Server[S]) fdExhausted(l *loop, lfd int, cause error) {
	s.log.Warn("gate: fd exhausted, backing off", zap.Error(cause))
	if l.reserveFd >= 0 {
		_ = unix.Close(l.reserveFd)
		l.reserveFd = -1
		if nfd, _, err := sysAccept(lfd); err == nil {
			_ = unix.Close(nfd)
			l.stats.connsRejected.Add(1)
		}
		if fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0); err == nil {
			l.reserveFd = fd
		}
	}
	_ = l.p.mod(lfd, makeToken(tokListener, 0, uint32(lfd)), 0)
	if l.backoffCur == 0 {
		l.backoffCur = int64(10 * time.Millisecond)
	} else {
		l.backoffCur = min(l.backoffCur*2, int64(time.Second))
	}
	l.backoffUntil = l.now() + l.backoffCur
}

// registerConn 在目标 loop 上落地一条新连接（loop 线程）。
func (s *Server[S]) registerConn(l *loop, nfd int, remote netip.AddrPort) {
	c, err := l.attach(nfd, coreCallbacks{})
	if err != nil {
		_ = unix.Close(nfd)
		s.conns.Add(-1)
		s.log.Warn("gate: register conn", zap.Error(err))
		return
	}
	c.hsCtx = &hsContext[S]{srv: s, remote: remote, id: l.nextConnID()}

	needProxy := s.opts.Proxy != ProxyOff
	needWS := s.opts.WebSocket != nil
	if needProxy || needWS {
		if mh := s.maxHandshaking(); mh > 0 && s.handshaking.Add(1) > int64(mh) {
			s.handshaking.Add(-1)
			l.stats.connsRejected.Add(1)
			l.detach(c) // 未 Open 过：不会调 OnClose
			return
		} else if mh <= 0 {
			s.handshaking.Add(1)
		}
		l.stats.connsHandshaking.Add(1)
		if needProxy {
			c.state = stateProxy
		} else {
			c.state = stateHandshaking
		}
		c.out.setFlushable(false)
		l.hsLRU.pushBack(&c.tnode, l.now())
		return
	}
	s.finishOpen(l, c, nil)
}

func (s *Server[S]) maxHandshaking() int {
	if s.opts.Limits.MaxHandshaking > 0 {
		return s.opts.Limits.MaxHandshaking
	}
	if s.opts.Limits.MaxConns > 0 {
		return max(1, s.opts.Limits.MaxConns/16)
	}
	return 0
}

// finishOpen 绑定泛型壳并进入 Open（hs 非 nil = WebSocket 升级信息）。
func (s *Server[S]) finishOpen(l *loop, c *connCore, hs *Handshake) {
	ctx := c.hsCtx.(*hsContext[S])
	c.hsCtx = nil // 握手态释放（W9）
	if c.state == stateProxy || c.state == stateHandshaking {
		s.handshaking.Add(-1)
		l.stats.connsHandshaking.Add(-1)
	}
	bindConn(s.opts.Handler, c, ctx.id, ctx.remote, hs)
	l.openConn(c)
}

// hsRead 处理 Proxy / Handshaking 状态的读事件。
func (s *Server[S]) hsRead(l *loop, c *connCore) {
	switch c.state {
	case stateProxy:
		s.proxyRead(l, c)
	case stateHandshaking:
		ctx := c.hsCtx.(*hsContext[S])
		l.wsHsReadable(c, s.opts.WebSocket, func(hs *Handshake) {
			// wsHsReadable 之后紧跟 openConn：在回调里完成绑定。
			s.bindBeforeOpen(l, c, ctx, hs)
		})
	}
}

// bindBeforeOpen 在 101 已入队、openConn 之前绑定壳。
func (s *Server[S]) bindBeforeOpen(l *loop, c *connCore, ctx *hsContext[S], hs *Handshake) {
	c.hsCtx = nil
	s.handshaking.Add(-1)
	l.stats.connsHandshaking.Add(-1)
	bindConn(s.opts.Handler, c, ctx.id, ctx.remote, hs)
}

// proxyRead 解析 PROXY 头（v1/v2），成功后按配置转 Handshaking 或直接 Open。
func (s *Server[S]) proxyRead(l *loop, c *connCore) {
	for range readBudgetCalls {
		n, err := l.io.read(c.fd, l.rbuf)
		if !l.readOK(c, n, err) {
			return
		}
		if c.in.carry == nil {
			c.in.carry = append(poolGet(n)[:0], l.rbuf[:n]...)
		} else {
			nb := append(poolGet(len(c.in.carry) + n)[:0], c.in.carry...)
			nb = append(nb, l.rbuf[:n]...)
			poolPut(c.in.carry)
			c.in.carry = nb
		}
		src, consumed, perr := parseProxy(c.in.carry, s.opts.Proxy == ProxyRequired)
		if perr != nil {
			l.closeLocal(c, wrapProtocol(perr)) // 头非法：不发任何响应（06 迁移表）
			return
		}
		if consumed == 0 {
			if len(c.in.carry) > proxyV1MaxLen+proxyV2MinLen+216 {
				l.closeLocal(c, wrapProtocol(errProxyMalformed))
				return
			}
			continue // 等更多数据
		}
		ctx := c.hsCtx.(*hsContext[S])
		if consumed > 0 {
			if src.IsValid() {
				ctx.remote = src // Remote() 从此是真实来源
			}
			rest := c.in.carry[consumed:]
			if len(rest) > 0 {
				nb := append(poolGet(len(rest))[:0], rest...)
				poolPut(c.in.carry)
				c.in.carry = nb
			} else {
				poolPut(c.in.carry)
				c.in.carry = nil
			}
		}
		// consumed == -1：Optional 且不是 PROXY 头，carry 原样进入下一阶段。
		if s.opts.WebSocket != nil {
			c.state = stateHandshaking
			if c.in.carry != nil {
				// 头后紧跟的字节可能已含完整升级请求：立即尝试。
				s.hsRead(l, c)
			}
			return
		}
		// 裸 TCP：直接 Open，剩余字节（PROXY 头后紧跟第一个 gate 帧）进 carry 队列。
		s.finishOpen(l, c, nil)
		if c.state == stateOpen && c.in.carry != nil {
			l.queueCarry(c)
		}
		return
	}
	l.truncated = true
}

func addrHasZeroPort(addr string) bool {
	_, port, err := splitHostPort(addr)
	return err == nil && port == "0"
}

func joinBound(addr string, port uint16) string {
	host, _, err := splitHostPort(addr)
	if err != nil {
		return addr
	}
	return joinHostPort(host, port)
}
