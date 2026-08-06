package gate

import (
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"time"
)

// 公平性预算（06）：一轮迭代里每一样能无限增长的东西都要有预算。
// 有任何一项被截断时，本轮 wait 的 timeout 取 0，下一轮立刻继续。
const (
	rbufSize        = 64 << 10
	readBudgetCalls = 4
	readBudgetBytes = 256 << 10
	deliverBudget   = 1024
	inboxBudget     = 4096
	maxPendingPosts = 1024
	maxLoopEvents   = 256
)

// loopConfig 的时间项以纳秒表示；<=0 表示禁用（linger 例外：0=立即，<0=无限）。
type loopConfig struct {
	idle       int64
	handshake  int64
	maxPause   int64
	stall      int64
	linger     int64
	maxPending int

	out outConfig
}

func normalizeLoopConfig(ob Outbound, li Limits, ws bool) loopConfig {
	cfg := loopConfig{
		idle:      int64(li.Idle),
		handshake: int64(li.HandshakeTimeout),
		maxPause:  int64(li.MaxPause),
		stall:     int64(ob.StallTimeout),
		linger:    int64(ob.CloseLinger),
		out:       normalizeOutConfig(ob, li, ws),
	}
	if li.HandshakeTimeout == 0 {
		cfg.handshake = int64(10 * time.Second)
	}
	if li.MaxPause == 0 {
		cfg.maxPause = int64(60 * time.Second)
	}
	switch {
	case ob.StallTimeout == 0:
		cfg.stall = int64(30 * time.Second)
	case ob.StallTimeout < 0:
		cfg.stall = 0 // Unlimited：禁用
	}
	// CloseLinger：零值 → 默认 1s（保护性选项零值=默认）；Unlimited → 立即关闭
	// （linger 的「保护」是给排空封顶，关掉上限即不等待）。01 的表述有歧义
	// （零值列写 1s、说明列写「0 表示立即」），按零值=默认解释，待回改文档。
	switch {
	case ob.CloseLinger == 0:
		cfg.linger = int64(time.Second)
	case ob.CloseLinger < 0:
		cfg.linger = 0
	}
	if li.Idle < 0 {
		cfg.idle = 0
	}
	cfg.maxPending = li.MaxPending
	if cfg.maxPending <= 0 {
		cfg.maxPending = cfg.out.maxMessage + rbufSize
	}
	return cfg
}

type slot struct {
	c   *connCore
	gen uint32
}

// loop = 一个 goroutine = 一个 poller = 一份内存池 = 一个 zstd 编码器。
// 连接 accept 时归属，直到关闭不换（R1）。所有 syscall 在 loop 线程上（R2）。
type loop struct {
	p   poller
	io  connIO
	now func() int64 // 可编程时钟（07 L0）：五条 LRU、backoff、wait timeout 全以它为准
	env *loopEnv
	cfg loopConfig

	rbuf   []byte
	events []event

	slots []slot
	free  []uint32

	dirty        dirtyList
	dirtyScratch []*outbound
	box          inbox
	wk           waker
	boxBacklog   *inboxNode

	idleLRU, hsLRU, pauseLRU, stallLRU, lingerLRU lruList

	carryQ    []*connCore
	truncated bool // 本轮有预算截断 ⇒ 下轮 timeout 0

	backoffUntil int64 // EMFILE 退避（accept 层使用）

	// onAccept 处理 listener 事件（server 装配）；onHsRead 处理
	// Proxy/Handshaking 的读事件。测试 harness 不经过它们。
	onAccept func(lfd int)
	onHsRead func(c *connCore)

	// server 装配注入的运行时（测试下为零值/nil）。
	loopID       int
	idSeq        uint64 // Conn.ID = loopID<<48 | idSeq++（永不复用，R6）
	stats        *loopStats
	onConnClosed func(*connCore) // detach 时回调（server 的全局计数）
	listener     int             // 本 loop 持有的 listener fd；-1 = 无
	reserveFd    int             // EMFILE 预留 fd（R11）；-1 = 未启用
	backoffCur   int64           // EMFILE 指数退避当前值（纳秒）
	stop         atomic.Bool     // Shutdown 置位

	nconns int
}

// run 驱动循环直到 stop 置位。退出前关闭 poller。
func (l *loop) run() {
	for !l.stop.Load() {
		l.step()
	}
	if l.reserveFd >= 0 {
		_ = unixClose(l.reserveFd)
	}
	_ = l.p.close()
}

func (l *loop) nextConnID() uint64 {
	l.idSeq++
	return uint64(l.loopID)<<48 | l.idSeq
}

func newLoop(p poller, io connIO, now func() int64, cfg loopConfig, enc encoderOptions) *loop {
	l := &loop{
		p:         p,
		io:        io,
		now:       now,
		env:       newLoopEnv(enc),
		cfg:       cfg,
		rbuf:      make([]byte, rbufSize),
		events:    make([]event, maxLoopEvents),
		listener:  -1,
		reserveFd: -1,
	}
	l.stats = l.env.stats
	l.wk.p = p
	l.idleLRU.init()
	l.hsLRU.init()
	l.pauseLRU.init()
	l.stallLRU.init()
	l.lingerLRU.init()
	return l
}

// dirtyPoster 实现：跨 goroutine 的 Send 只入队与标脏（R3）。
func (l *loop) postDirty(o *outbound) { l.dirty.push(o) }
func (l *loop) maybeNotify()          { l.wk.maybeNotify() }

func monotonicNow() func() int64 {
	base := time.Now()
	return func() int64 { return int64(time.Since(base)) }
}

// ─── 连接注册与打开 ───

func (l *loop) allocSlot(c *connCore) (idx uint32, gen uint32) {
	if n := len(l.free); n > 0 {
		idx = l.free[n-1]
		l.free = l.free[:n-1]
	} else {
		l.slots = append(l.slots, slot{})
		idx = uint32(len(l.slots) - 1)
	}
	s := &l.slots[idx]
	s.c = c
	return idx, s.gen
}

// attach 把一个已 accept 的 fd 挂进本 loop（Accepted 状态，已注册读兴趣）。
// 只在 loop 线程调用。
func (l *loop) attach(fd int, cb coreCallbacks) (*connCore, error) {
	c := &connCore{fd: fd, loop: l, cb: cb, state: stateAccepted}
	c.cdc = serverCodec(l.cfg.out.maxMessage)
	c.out = newOutbound(l.cfg.out, l.io, fd, l.env, l)
	c.out.owner = c
	// 写/编码失败：仲裁已在 failLoop 里完成，这里只推进状态机（幂等）。
	c.out.fatal = func(err error) { l.enterDraining(c) }
	c.out.onPanic = func(err error) { c.requestClose(err) }
	c.out.progress = func() {
		if c.snode.inList() {
			l.stallLRU.touch(&c.snode, l.now())
		}
	}
	c.tnode.c, c.snode.c, c.lnode.c = c, c, c
	c.gateSink = func(msg []byte, _ bool) error {
		l.stats.messagesIn.Add(1)
		cbErr := c.cb.onMessage(msg)
		// 回调返回点：无论成功还是 error，已注册的 AsyncDo 都要启动
		// （panic 出口由 connReadable / processCarry 的屏障兜住）。
		l.launchAsync(c)
		if cbErr != nil {
			return handlerErr{cbErr}
		}
		return nil
	}

	idx, gen := l.allocSlot(c)
	c.slotIdx, c.gen = idx, gen
	if err := l.p.add(fd, makeToken(tokConn, gen, idx), interestRead); err != nil {
		s := &l.slots[idx]
		s.c = nil
		l.free = append(l.free, idx)
		return nil, err
	}
	c.curInterest = interestRead
	l.nconns++
	return c, nil
}

// openConn 完成 * → Open 的迁移：唯一触发业务回调的状态。
// onOpen 返回 error ⇒ 拒绝连接，立即关闭且不配对 onClose。
func (l *loop) openConn(c *connCore) {
	l.hsLRU.remove(&c.tnode)
	c.state = stateOpen
	c.out.setFlushable(true)

	var openErr error
	func() {
		// AsyncDo 的启动点在 defer 里：OnOpen 返回 error 或 panic 时，
		// 已注册的任务同样必须启动，否则它会永久悬空。
		defer func() {
			if r := recover(); r != nil {
				openErr = fmt.Errorf("%w: OnOpen: %v", ErrHandlerPanic, r)
			}
			l.launchAsync(c)
		}()
		if c.cb.onOpen != nil {
			openErr = c.cb.onOpen()
		}
	}()
	if openErr != nil {
		l.closeLocal(c, openErr) // opened == false ⇒ 不会调 onClose
		return
	}
	c.opened = true
	l.stats.connsOpen.Add(1)
	// OnOpen 里可能 Pause / AsyncDo 过：那时 tnode 已经在 pause 链上，
	// 再挂一次 idle 链会让同一个侵入式节点同时属于两条链。
	if c.pauseDepth == 0 {
		l.idleLRU.pushBack(&c.tnode, l.now())
	}
}

// ─── 一轮迭代 ───

func (l *loop) step() {
	timeout := l.nextTimeout()
	n, err := l.p.wait(l.events, timeout)
	if err != nil {
		return
	}
	l.truncated = false

	// 阶段 0：上一轮被预算截断的收件箱余项。
	l.processInbox()

	// 阶段 1：分发事件。
	for i := range n {
		e := &l.events[i]
		switch e.tok.kind() {
		case tokNotify:
			l.wk.disarm() // 先清 armed 再 drain（R7）
			l.processInbox()
		case tokListener:
			if l.onAccept != nil {
				l.onAccept(int(e.tok.slot()))
			}
		case tokConn:
			if int(e.tok.slot()) >= len(l.slots) {
				continue
			}
			s := &l.slots[e.tok.slot()]
			if s.c == nil || s.gen != e.tok.gen() {
				continue // 陈旧事件（R5）
			}
			c := s.c
			if e.write || (e.hup && c.state == stateDraining) {
				l.connWritable(c) // 先写后读（R14）：先还欠下的字节
			}
			if e.read && s.c == c && c.state < stateDraining {
				l.connReadable(c)
			}
		}
	}

	// 阶段 1.5：处理排队的 carry（resume / 投递预算截断留下的完整帧）。
	if len(l.carryQ) > 0 {
		q := l.carryQ
		l.carryQ = l.carryQ[:0]
		for _, c := range q {
			c.carryQueued = false
			l.processCarry(c)
		}
	}

	// 阶段 2：flush 脏连接（幂等，阶段 1 已 flush 过的直接返回）。
	l.dirtyScratch = l.dirtyScratch[:0]
	for o := l.dirty.drain(); o != nil; {
		next := o.dirtyNext
		o.dirtyNext = nil
		l.dirtyScratch = append(l.dirtyScratch, o)
		o = next
	}
	for _, o := range l.dirtyScratch {
		c := o.owner
		st := o.flush(true, false)
		l.applyWriteStatus(c, st)
	}

	// 阶段 2.5：EMFILE 退避到期，恢复 listener 的读兴趣（R11）。
	if l.backoffUntil != 0 && l.now() >= l.backoffUntil {
		l.backoffUntil = 0
		if l.listener >= 0 {
			_ = l.p.mod(l.listener, makeToken(tokListener, 0, uint32(l.listener)), interestRead)
		}
	}

	// 阶段 3：超时扫描（expire 自带「到点才做」判断）。
	now := l.now()
	l.idleLRU.expire(l.cfg.idle, now, func(c *connCore) { l.closeLocal(c, ErrIdleTimeout) })
	l.hsLRU.expire(l.cfg.handshake, now, func(c *connCore) { l.closeLocal(c, ErrHandshakeTimeout) })
	l.pauseLRU.expire(l.cfg.maxPause, now, func(c *connCore) { l.closeLocal(c, ErrPauseTimeout) })
	l.stallLRU.expire(l.cfg.stall, now, func(c *connCore) { l.closeLocal(c, ErrBackpressure) })
	l.lingerLRU.expire(l.cfg.linger, now, func(c *connCore) { l.detach(c) })
}

func (l *loop) processInbox() {
	node := l.boxBacklog
	l.boxBacklog = nil
	if node == nil {
		node = l.box.drain()
	}
	budget := inboxBudget
	for node != nil {
		if budget == 0 {
			l.boxBacklog = node // 留到下一轮（timeout 0）
			l.truncated = true
			return
		}
		fn := node.fn
		node = node.next
		budget--
		fn()
	}
}

func (l *loop) nextTimeout() time.Duration {
	if l.truncated || l.boxBacklog != nil || len(l.carryQ) > 0 {
		return 0
	}
	var dl int64
	minDl := func(d int64) {
		if d > 0 && (dl == 0 || d < dl) {
			dl = d
		}
	}
	minDl(l.idleLRU.deadline(l.cfg.idle))
	minDl(l.hsLRU.deadline(l.cfg.handshake))
	minDl(l.pauseLRU.deadline(l.cfg.maxPause))
	minDl(l.stallLRU.deadline(l.cfg.stall))
	minDl(l.lingerLRU.deadline(l.cfg.linger))
	minDl(l.backoffUntil)
	if dl == 0 {
		return -1 // 无截止时刻：无限等待
	}
	return time.Duration(max(0, dl-l.now()))
}

// ─── 写侧 ───

func (l *loop) connWritable(c *connCore) {
	if c.state >= stateDetached {
		return
	}
	st, err := c.out.write()
	if err != nil {
		c.out.failLoop(err) // fatal → closeLocal（进入 Draining）
		st = writeFailed
	}
	l.applyWriteStatus(c, st)
}

func (l *loop) applyWriteStatus(c *connCore, st writeStatus) {
	switch st {
	case writeIdle:
		l.stallLRU.remove(&c.snode)
		if c.state == stateDraining {
			l.detach(c) // 排空 → Detached
			return
		}
		l.syncInterest(c)
	case writeBlocked:
		if !c.snode.inList() {
			l.stallLRU.pushBack(&c.snode, l.now()) // lastProgress 起点 = 进入阻塞时刻
		}
		l.syncInterest(c)
	case writeFailed:
		// fatal 已走 closeLocal → Draining（丢弃队列）。若失败发生在 Draining
		// 本身（linger 期间续写失败），closing 已置位、closeLocal 不再迁移，
		// 这里补 detach。
		if c.state == stateDraining {
			l.detach(c)
		}
	}
}

func (l *loop) modInterest(c *connCore, want interest) {
	if want == c.curInterest || c.state >= stateDetached {
		return
	}
	c.curInterest = want
	_ = l.p.mod(c.fd, makeToken(tokConn, c.gen, c.slotIdx), want)
}

// syncInterest 按状态推导应有兴趣并按需 mod（写兴趣增删是边沿动作，R9）。
func (l *loop) syncInterest(c *connCore) {
	l.modInterest(c, c.currentInterest())
}

// ─── 读侧 ───

// connReadable 处理一次读事件。恢复屏障在批边界：OnMessage / Cipher.Open 的
// panic 只关这一条连接（01「panic 屏障放在批边界」）。
func (l *loop) connReadable(c *connCore) {
	defer func() {
		if r := recover(); r != nil {
			l.closeLocal(c, fmt.Errorf("%w: %v", ErrHandlerPanic, r))
		}
		l.launchAsync(c) // panic 出口：已注册的 AsyncDo 仍要启动
	}()
	if c.state != stateOpen {
		if l.onHsRead != nil && (c.state == stateProxy || c.state == stateHandshaking) {
			l.onHsRead(c)
		}
		return
	}

	// in-loop 快路径：批内的回包不需要唤醒，批收尾必然 flush（O14 用 defer 兜住）。
	c.out.beginInLoop()
	defer l.flushBatchEnd(c)

	if c.ws != nil {
		l.wsReadable(c) // WS：原始字节先过帧层，再进同一份 gate 帧循环（W1）
		return
	}

	calls, bytes := 0, 0
	for calls < readBudgetCalls && bytes < readBudgetBytes {
		if c.state != stateOpen || c.pauseDepth > 0 || c.closing.Load() {
			return
		}
		// 大帧直读：目标缓冲一次申请到位，后续 read 直接填剩余区（无重复拷贝）。
		if c.in.body != nil {
			n, err := l.io.read(c.fd, c.in.body[c.in.got:])
			if !l.readOK(c, n, err) {
				return
			}
			calls++
			bytes += n
			l.stats.bytesIn.Add(uint64(n))
			c.in.got += n
			if c.in.got == len(c.in.body) {
				body := c.in.body
				c.in.body, c.in.got = nil, 0
				l.deliverFrameBuf(c, body)
			}
			continue
		}
		// 小帧快路径：残片拼上后在 rbuf 上原地解析。
		carryLen := len(c.in.carry)
		n, err := l.io.read(c.fd, l.rbuf[carryLen:])
		if !l.readOK(c, n, err) {
			return
		}
		calls++
		bytes += n
		l.stats.bytesIn.Add(uint64(n))
		if carryLen > 0 {
			copy(l.rbuf, c.in.carry)
			poolPut(c.in.carry)
			c.in.carry = nil
		}
		if !l.parseAndDeliver(c, l.rbuf[:carryLen+n]) {
			return
		}
	}
	l.truncated = true // 读预算耗尽：主动让出，水平触发下一轮还会报
}

// readOK 统一处理 read 的返回：EAGAIN 停止本轮、EOF 关（ErrPeerClosed）、
// 其余错误关（ErrPeerReset）。返回 false 表示不应继续读。
func (l *loop) readOK(c *connCore, n int, err error) bool {
	if err != nil {
		if !errors.Is(err, syscall.EAGAIN) {
			l.closeLocal(c, fmt.Errorf("%w: %w", ErrPeerReset, err))
		}
		return false
	}
	if n == 0 {
		l.closeLocal(c, ErrPeerClosed)
		return false
	}
	return true
}

// parseAndDeliver 在 data 上解析出一条条完整帧并投递。
// 返回 false 表示本次读事件到此为止（关闭 / 暂停 / 大帧接管 / 预算截断）。
func (l *loop) parseAndDeliver(c *connCore, data []byte) bool {
	delivered := 0
	defer func() {
		if delivered > 0 && c.state == stateOpen && c.pauseDepth == 0 {
			l.idleLRU.touch(&c.tnode, l.now()) // 活跃判据：交付了完整消息
		}
	}()
	for len(data) > 0 {
		if c.state != stateOpen || c.closing.Load() {
			return false // 已判死：同一批剩下的不再投递
		}
		if c.pauseDepth > 0 {
			// Pause 落在一批数据中间：已读出未投递的字节转 carry，MaxPending 封顶。
			l.stashCarry(c, data, true)
			return false
		}
		if delivered >= deliverBudget {
			l.stashCarry(c, data, false)
			l.queueCarry(c)
			l.truncated = true
			return false
		}
		f, need, ok, err := c.cdc.parse(data)
		if err != nil {
			l.closeLocal(c, wrapProtocol(err))
			return false
		}
		if !ok {
			if need > len(l.rbuf) {
				// 大帧：帧头校验已通过（谎报长度换不来分配），一次性申请整帧。
				c.in.body = poolGet(need)
				copy(c.in.body, data)
				c.in.got = len(data)
				return true
			}
			l.stashCarry(c, data, false) // 下一轮就够了；carry ≤ rbuf 容量
			return true
		}
		l.deliverFrame(c, f)
		delivered++
		data = data[need:] // ok 时 need 即本帧消费的字节数
	}
	return true
}

// handlerErr 区分「业务 OnMessage 返回的 error」与「协议违规」：
// 前者原样作为关闭原因，后者包进 ErrProtocol。
type handlerErr struct{ error }

func (l *loop) deliverFrame(c *connCore, f frame) {
	err := c.cdc.deliver(f, c.ciph, nil, c.gateSink)
	if err != nil {
		var he handlerErr
		if errors.As(err, &he) {
			l.closeLocal(c, he.error) // 返回 error 之前 Send 的仍会被送出（Draining）
		} else {
			l.closeLocal(c, wrapProtocol(err))
		}
	}
}

// deliverFrameBuf 交付一条大帧（body = 完整帧含头，来自分级池）。
// 语义：大帧在「收齐交付」时才算被解析——SetCipher 在积累期间生效时按新配置校验。
func (l *loop) deliverFrameBuf(c *connCore, body []byte) {
	defer poolPut(body)
	f, _, ok, err := c.cdc.parse(body)
	if err != nil || !ok {
		if err == nil {
			err = errors.New("gate: truncated frame buffer")
		}
		l.closeLocal(c, wrapProtocol(err))
		return
	}
	l.deliverFrame(c, f)
	if c.state == stateOpen && c.pauseDepth == 0 {
		l.idleLRU.touch(&c.tnode, l.now())
	}
}

func (l *loop) stashCarry(c *connCore, data []byte, pauseCheck bool) {
	if len(data) > 0 {
		buf := poolGet(len(data))
		copy(buf, data)
		c.in.carry = buf
	}
	if pauseCheck && len(data)+c.in.got > l.cfg.maxPending {
		l.closeLocal(c, ErrPendingOverflow)
	}
}

func (l *loop) queueCarry(c *connCore) {
	if c.carryQueued {
		return
	}
	c.carryQueued = true
	l.carryQ = append(l.carryQ, c)
}

// processCarry 在 resume / 预算截断后继续投递 carry 里的完整帧。
func (l *loop) processCarry(c *connCore) {
	if c.state != stateOpen || c.pauseDepth > 0 || c.in.carry == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			l.closeLocal(c, fmt.Errorf("%w: %v", ErrHandlerPanic, r))
		}
		l.launchAsync(c) // panic 出口：已注册的 AsyncDo 仍要启动
	}()
	c.out.beginInLoop()
	defer l.flushBatchEnd(c)
	carryLen := len(c.in.carry)
	copy(l.rbuf, c.in.carry)
	poolPut(c.in.carry)
	c.in.carry = nil
	l.parseAndDeliver(c, l.rbuf[:carryLen])
}

// ─── 批边界 ───

// flushBatchEnd 在处理完一个连接的入站批之后调用：清 inLoop 并 flush（O14）。
func (l *loop) flushBatchEnd(c *connCore) {
	st := c.out.flush(false, true)
	l.applyWriteStatus(c, st)
}

// ─── 关闭序列 ───

// enterDraining：任何关闭原因的第一站（closing 已置位、reason 已记录）。
// 停止入站投递、停止接受新消息入队、尝试写一次、挂 linger 链。
// 之后只在可写事件或 CloseLinger 到期时再动，绝不逐轮重试（R15）。
func (l *loop) enterDraining(c *connCore) {
	if c.state >= stateDraining {
		return
	}
	// 到这里 closing 一定已由 beginClose 置位（两个关闭入口都先仲裁）。
	// 极少数直达路径（Shutdown 的 ctx 强拆）没走仲裁，这里补一次。
	if !c.closing.Load() {
		c.beginClose(ErrServerClosed)
	}
	prev := c.state
	reason := c.closeReason()
	// 帧流已有洞（写失败/编码失败）⇒ 不发 close 帧、不再尝试任何写，
	// 直接拆除（05「本地主动关闭」的最后一行）。
	if c.out.wireBroken {
		c.state = stateDraining
		l.idleLRU.remove(&c.tnode)
		l.hsLRU.remove(&c.tnode)
		l.detach(c)
		return
	}
	if c.cb.onDrain != nil && prev == stateOpen {
		// closing 已置位：close 帧走 force 路径追加，FIFO 保证它在最后（W13）。
		c.cb.onDrain(reason)
	}
	c.state = stateDraining
	l.idleLRU.remove(&c.tnode)
	l.hsLRU.remove(&c.tnode)
	// pause 子状态保留（MaxPause 兜底 resume 永不来的场景）。
	l.lingerLRU.pushBack(&c.lnode, l.now())

	st := c.out.flush(false, false)
	if st == writeFailed {
		l.detach(c)
		return
	}
	if st == writeIdle || l.cfg.linger == 0 {
		l.detach(c) // 已排空，或 CloseLinger=0 表示立即关闭
		return
	}
	l.modInterest(c, interestWrite) // Draining：只有写（06 状态表）
}

// detach 执行 Draining → Detached 的拆除序列（顺序是规格，06）：
// fd 与 loop 资源立刻回收，不等任何业务代码；OnClose 视串行域是否空闲。
func (l *loop) detach(c *connCore) {
	if c.state >= stateDetached {
		return
	}
	c.state = stateDetached
	c.closing.Store(true) // 超时/写失败直达 detach 的路径也要挡住并发 Send

	// 2. 壳的 core 指针置 nil：公共方法从此返回 ErrConnClosed / no-op。
	if c.cb.detach != nil {
		c.cb.detach()
	}
	// 3. poller.del(fd) → close(fd)。
	_ = l.p.del(c.fd)
	_ = l.io.close(c.fd)
	// 4. 丢弃 stage1/stage2，归还池内存，退还预算。
	c.out.discard()
	c.in.release()
	if c.ws != nil {
		c.ws.release()
	}
	// 5. 从五条 LRU 摘除；槽位置 nil；gen++。
	l.idleLRU.remove(&c.tnode)
	l.hsLRU.remove(&c.tnode)
	l.pauseLRU.remove(&c.tnode)
	l.stallLRU.remove(&c.snode)
	l.lingerLRU.remove(&c.lnode)
	s := &l.slots[c.slotIdx]
	s.c = nil
	s.gen = (s.gen + 1) & genMask
	l.free = append(l.free, c.slotIdx)
	l.nconns--
	// 统计与 server 级计数。
	l.stats.countClose(c.closeReason())
	if c.opened {
		l.stats.connsOpen.Add(-1)
	}
	c.exitPaused(l) // 读闸的链表归属与计数在这里一次结清（幂等）
	if l.onConnClosed != nil {
		l.onConnClosed(c)
	}
	// 6. 串行域未空闲（在途 AsyncDo）⇒ 等 resume 控制项（直携 core 引用，
	// 不走 token——gen 已在上面递增）。
	if c.asyncBusy {
		c.pendingClose = true
		return
	}
	l.finishClose(c)
}

// finishClose：Detached → Closed。onClose 恰好一次，只与成功的 onOpen 配对。
func (l *loop) finishClose(c *connCore) {
	if c.state == stateClosed {
		return
	}
	c.state = stateClosed
	if !c.opened || c.cb.onClose == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			_ = r // OnClose panic：屏障接住；连接已经关了，无处可关
		}
	}()
	c.cb.onClose(c.closeReason())
}
