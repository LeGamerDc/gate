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

	conns       atomic.Int64 // MaxConns 的准入占位；拆除（fd 归还）时释放，Shutdown 等的是它
	handshaking atomic.Int64 // MaxHandshaking 同理

	budget *serverBudget // MaxOutboundBytes：各 loop 按租约分配（近似口径）

	frameMu  sync.Mutex
	frameEnc *zstd.Encoder // NewFrame 专用（冷路径，锁保护）

	closed atomic.Bool
	done   chan struct{}
	wg     sync.WaitGroup
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
	// 全局出站预算：零值取默认 1GB，Unlimited 关闭（保护性选项零值=默认）。
	maxOut := opts.Limits.MaxOutboundBytes
	switch {
	case maxOut == 0:
		maxOut = 1 << 30
	case maxOut < 0:
		maxOut = 0 // Unlimited
	}
	s.budget = newServerBudget(maxOut)

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
		l.env.quota = newQuotaLease(s.budget)
		l.onConnClosed = func(c *connCore) {
			s.conns.Add(-1)
			s.releaseHandshake(l, c) // 死在握手期：退回握手计数（幂等）
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

// Wait 阻塞到 server 完全收尾：不只是 socket 拆除（那是 Shutdown 的返回点），
// 还包括给带在途 AsyncDo 的连接补上的最后一次 OnClose。业务任务永不返回时
// 它也不会返回——那需要业务自己的 context 收敛（01「ctx 约束的是什么」）。
//
// 目前恒返回 nil：事件循环没有能让整个 server 失败的错误路径。返回值留在
// 签名里是给将来那种错误用的，别把它当成「没出问题」的判据。
func (s *Server[S]) Wait() error {
	<-s.done
	return nil
}

// Stats 返回全 server 汇总（各 loop 计数器求和）。
func (s *Server[S]) Stats() Stats {
	var out Stats
	for _, l := range s.loops {
		addStats(&out, l.stats.snapshot())
	}
	out.PoolMiss = poolMisses.Load() // 分级池是全局的，不按 loop 求和
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
//
// 返回 ctx.Err() 表示**优雅的那一半没做完**：剩余 flush 被放弃、连接是被强拆的。
// socket 无论如何都已经拆干净（或已尽最大努力，见日志）。重复调用等到收尾后返回
// nil——它没有参与那次关闭，报不出它的结果。
func (s *Server[S]) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		<-s.done
		return nil
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
	// 2. 等每条连接的 socket 拆除完成。等的是 conns 而不是 live：
	// 01 明写「在途的 AsyncDo 不会被等待——这些连接的 socket 会被正常拆除，
	// 但它们的 OnClose 要等各自的 f 结束后才触发；进程先退出的话就不触发了」。
	// 等 OnClose 会让一个不返回的业务任务把 Shutdown 拖到 ctx 到期。
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	forced := false
drain:
	for s.conns.Load() > 0 {
		select {
		case <-ctx.Done():
			if !forced {
				forced = true
				// 放弃剩余 flush：强制拆除。仍然经收件箱回 loop 线程执行
				// （R2：所有 syscall 在 loop 上）。
				for _, l := range s.loops {
					l.box.push(func() {
						for i := range l.slots {
							if c := l.slots[i].c; c != nil {
								l.closeLocal(c, ErrServerClosed)
								l.detach(c)
							}
						}
					})
					l.wk.maybeNotify()
				}
			}
			// 强拆之后仍留一小段时间让 loop 真正执行它；到点就停——
			// 业务回调可能永不返回，那时谁也救不了（01「ctx 约束的是什么」）。
			if !s.waitFor(func() bool { return s.conns.Load() == 0 }) {
				s.log.Warn("gate: shutdown deadline exceeded, forcing loop stop",
					zap.Int64("conns_left", s.conns.Load()))
				break drain
			}
		case <-tick.C:
		}
	}
	// 3. socket 已经全部拆除。给 loop 置停止位后**立即返回**——它们可能还
	// 要多活一会儿，去给带在途 AsyncDo 的连接补最后一次 OnClose（01：
	// Shutdown 不等待这些任务）。真正的收尾由 Wait 观察。
	for _, l := range s.loops {
		l.stop.Store(true)
		_ = l.p.notify()
	}
	go func() {
		s.wg.Wait()
		close(s.done)
	}()
	if forced {
		return ctx.Err() // 强拆过：告诉调用方优雅的那一半没做完
	}
	return nil
}

// waitFor 等条件成立，最多 forceGrace；返回条件是否真的成立
// （旧实现不报告结果，超时后照样停 loop，残余 fd 无人回收）。
const forceGrace = 500 * time.Millisecond

func (s *Server[S]) waitFor(cond func() bool) bool {
	deadline := time.Now().Add(forceGrace)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func (s *Server[S]) closeListeners() {
	for _, fd := range s.lns {
		_ = unix.Close(fd)
	}
}

// ─── accept ───

// reserveSlot 在一个上限下原子占位；limit <= 0 表示不限。占不到返回 false，
// 且计数已经回滚。
//
// 先加再比、超了再退，而不是 check-then-add：后者在多 loop 并发 accept 下
// 根本不是原子的，两条连接能同时看到「还差一个」。
//
// 之所以是一个函数而不是在两个调用点各写一遍：那两处曾经写成
// `if limit > 0 && n.Add(1) > limit { n.Add(-1); ... } else if limit <= 0 { n.Add(1) }`
// ——正确，但正确性完全押在短路求值上，而「不限」分支的那次 Add 藏在 else if 里。
// 这种形状读起来像 bug，改起来会变成 bug。
func reserveSlot(n *atomic.Int64, limit int) bool {
	if n.Add(1) > int64(limit) && limit > 0 {
		n.Add(-1)
		return false
	}
	return true
}

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
		// MaxConns 先原子占位再放行。满额时 accept 之后立刻 close（明确拒绝）
		// ——只跳过的话连接留在 backlog 里，水平触发每轮都报 listener 可读，空转。
		if !reserveSlot(&s.conns, s.opts.Limits.MaxConns) {
			_ = unix.Close(nfd)
			l.stats.connsRejected.Add(1)
			continue
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
	// macOS 的跨 loop 投递让 accept 与注册之间隔了一次调度：Shutdown 可能
	// 恰好在这中间跑完「关闭现有 slots」的快照。不重查的话这条连接会活过
	// 整轮 Shutdown，而 conns 已经占位，无 deadline 的 Shutdown 会永久等待。
	if s.closed.Load() {
		_ = unix.Close(nfd)
		s.conns.Add(-1)
		return
	}
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
		// 计数与「已计数」标志一起置，两个释放点（进 Open / 拆除）都只认
		// 这个标志——拒绝路径先减一次、拆除时再减一次会把计数减成负数，
		// 之后所有慢握手连接都能绕过 MaxHandshaking。
		if !reserveSlot(&s.handshaking, s.maxHandshaking()) {
			l.stats.connsRejected.Add(1)
			l.detach(c) // 未 Open 过：不会调 OnClose
			return
		}
		c.hsCounted = true
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

// releaseHandshake 归还握手期计数，幂等：进入 Open 与拆除都会调用它，
// 而一条连接可能两条路都走过（先 Open 再关闭）。
func (s *Server[S]) releaseHandshake(l *loop, c *connCore) {
	if !c.hsCounted {
		return
	}
	c.hsCounted = false
	s.handshaking.Add(-1)
	l.stats.connsHandshaking.Add(-1)
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
	s.releaseHandshake(l, c)
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
	s.releaseHandshake(l, c)
	bindConn(s.opts.Handler, c, ctx.id, ctx.remote, hs)
}

// proxyRead 解析 PROXY 头（v1/v2），成功后按配置转 Handshaking 或直接 Open。
func (s *Server[S]) proxyRead(l *loop, c *connCore) {
	for range readBudgetCalls {
		n, err := l.io.read(c.fd, l.rbuf)
		if !l.readOK(c, n, err) {
			return
		}
		c.in.appendCarry(l.rbuf[:n]) // 上界是 proxyMaxHeaderLen，见下面的边界检查
		src, consumed, perr := parseProxy(c.in.carry, s.opts.Proxy == ProxyRequired)
		if perr != nil {
			l.closeLocal(c, wrapProtocol(perr)) // 头非法：不发任何响应（06 迁移表）
			return
		}
		if consumed == 0 {
			// v2 的长度字段是 16 位，TLV 区合法地可以很长；这里给一个宽松
			// 上界即可（真正的定界由 v1 的 \r\n 与 v2 的长度字段负责）。
			if len(c.in.carry) > proxyMaxHeaderLen {
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
