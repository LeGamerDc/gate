package gate

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// connState 是主状态（06「状态机」）。Open 是唯一触发业务回调的状态。
type connState uint8

const (
	stateAccepted connState = iota
	stateProxy
	stateHandshaking
	stateOpen
	stateDraining // 尽力排空出站，最长 CloseLinger
	stateDetached // fd 已关、loop 资源已归还；OnClose 尚未调用
	stateClosed   // OnClose 已调用，只剩壳
)

// coreCallbacks 是泛型壳（Conn[S] / Handler[S]）与非泛型核之间的桥。
// 闭包由装配层绑定，捕获壳与 handler；测试直接给普通函数。
type coreCallbacks struct {
	onOpen    func() error       // 进入 Open。返回 error ⇒ 拒绝连接，不配对 onClose
	onMessage func([]byte) error // msg 仅本次调用有效
	onClose   func(error)        // 恰好一次，且只与成功的 onOpen 配对
	detach    func()             // 壳的 core 指针置 nil（Draining→Detached 第 2 步）
	onDrain   func(error)        // 进入 Draining 时的钩子（M5：WS close 帧入队）；可为 nil
}

// inboundState 是入站增量状态机的每连接部分（06「入站的精确模型」）。
type inboundState struct {
	carry []byte // 分级池；不足一帧的残片，上界 = rbuf 容量
	body  []byte // 大帧目标缓冲（分级池，含帧头），非 nil 即处于大帧直读中
	got   int    // body 已填充字节数
}

func (in *inboundState) release() {
	if in.carry != nil {
		poolPut(in.carry)
		in.carry = nil
	}
	if in.body != nil {
		poolPut(in.body)
		in.body = nil
	}
	in.got = 0
}

// connCore 是连接的「核」：全部可变状态与 loop 资源。壳（泛型 Conn[S]）持
// atomic.Pointer 指向它，进入 Detached 时置 nil——泄漏的壳只钉住自己和 State。
//
// 除注明外，所有字段只在所属 loop 线程上访问。
type connCore struct {
	fd      int
	state   connState
	opened  bool // onOpen 成功返回过 ⇒ 关闭时要配对 onClose
	slotIdx uint32
	gen     uint32

	loop *loop
	out  *outbound
	cdc  codec  // 入站解析策略；requireEncrypt 随 cipher 即时切换
	ciph Cipher // 入站解密（串行域内读写；出站走队列 barrier）
	in   inboundState
	ws   *wsState // 非 nil ⇒ WebSocket 传输：读事件先过 WS 帧层

	// hsCtx 是握手期（Accepted/Proxy/Handshaking）的装配层临时状态，
	// 进入 Open 时清空；非 nil 也意味着「还没成为业务可见的连接」。
	hsCtx       any
	hsCounted   bool   // 已计入 MaxHandshaking（释放点认这个标志，不认状态）
	onFinalized func() // Closed 之后的最后一步（server 的 live 计数）

	// 读闸 / 异步（串行域）
	pauseDepth   int
	pendingPosts []func() // 暂停期间排队的 Post 闭包
	asyncBusy    bool
	asyncFn      func() // AsyncDo 暂存：当次回调返回之后才启动 goroutine
	asyncResume  func() // asyncFn 对应的 resume
	pendingClose bool   // Detached 但串行域未空闲，等 resume 后 onClose

	delivered    int      // 本轮迭代已投递的消息数（公平性预算）
	deliverTick  uint64   // delivered 对应的迭代序号
	curInterest  interest // 当前 poller 兴趣（边沿动作：只在变化时 mod）
	carryQueued  bool     // 已在 loop 的 carryQ 里
	pausedActive bool     // 读闸的链表归属与计数是否已生效（让 exitPaused 幂等）

	// 热路径闭包预建（attach/enableWS 时装配）：每帧/每段新建闭包会把
	// 「入站零堆分配」打破成每消息一次分配。
	gateSink func(msg []byte, owned bool) error
	wsEmitFn func(seg []byte) error
	wsCtrlFn func(op byte, payload []byte) error

	// closing 是 out.mu 下置位的关闭标志的无锁副本（loop 线程快查用）；
	// 真正的仲裁在 beginClose 里，与 reason 同一临界区。
	closing   atomic.Bool
	mu        sync.Mutex // 只保护 reason / reasonSet
	reason    error
	reasonSet bool

	cb coreCallbacks

	tnode lruNode // 状态链节点：idle / handshake / pause（按状态互斥）
	snode lruNode // stall 链节点（WriteBlocked 期间）
	lnode lruNode // linger 链节点（Draining 期间；暂停中进入 Draining 时与 tnode 并存）
}

func (c *connCore) closeReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// beginClose 是关闭的**唯一仲裁点**，也是 Close 的线性化点（06 线性化表）：
// 在 outbound 的临界区内一次性完成「置 closing + 记录 reason」。
//
// 两条硬保证都由这个临界区兑现：
//   - 「Close 返回之后 Send 一定报错」（01 D15）——Send 检查的就是 o.closing，
//     它在 Close 返回前就已经置位，而不是等 loop 处理完控制项。
//   - 「第一个 reason 生效」——CAS 与 reason 写入分离时，抢到 CAS 的那个
//     可能被抢占，让 CAS 失败的那个先写进 reason，胜负颠倒。
//
// 返回 true 表示本次调用是第一个关闭者，由它负责推进 Draining。
func (c *connCore) beginClose(reason error) bool {
	o := c.out
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return false
	}
	o.closing = true
	c.mu.Lock()
	c.reason = reason
	c.reasonSet = true // nil 也是合法 reason：记下「已定」，后来者不得替换
	c.mu.Unlock()
	o.mu.Unlock()
	c.closing.Store(true) // 供 loop 线程无锁快查
	return true
}

// requestClose 是任意 goroutine 可调的关闭入口（公开 Close 的落点）。
// 幂等；返回时 Send 已经一律 ErrConnClosed，进入 Draining 的实际动作
// 经收件箱回到 loop 线程。
func (c *connCore) requestClose(reason error) {
	if !c.beginClose(reason) {
		return
	}
	l := c.loop
	l.box.push(func() { l.enterDraining(c) })
	l.wk.maybeNotify()
}

// closeLocal 是 loop 线程上的关闭入口（协议错误、超时、写失败……）。
func (l *loop) closeLocal(c *connCore, reason error) {
	if !c.beginClose(reason) {
		return // 已有关闭者，且它的 reason 已经定了
	}
	l.enterDraining(c)
}

// setCipher 在串行域内调用：立刻换入站 cipher（下一个被解析的帧生效），
// 出站经队列 barrier 生效（06「Cipher epoch」）。
func (c *connCore) setCipher(ci Cipher) {
	if c.state != stateOpen {
		return
	}
	c.ciph = ci
	c.cdc.requireEncrypt = ci != nil
	c.out.setCipher(ci)
}

// pause 递增读闸深度（串行域内）。返回幂等的 resume。
// 非 Open 状态返回 no-op resume（合法性表脚注 ³）。
func (c *connCore) pause() (resume func()) {
	if c.state != stateOpen {
		return func() {}
	}
	l := c.loop
	c.pauseDepth++
	if c.pauseDepth == 1 {
		c.enterPaused(l)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.box.push(func() { l.resumeOnLoop(c) }) // 控制项直携 core 引用，不走 token
			l.wk.maybeNotify()
		})
	}
}

// resumeOnLoop 在 loop 线程上执行 resume 控制项（R16）。
func (l *loop) resumeOnLoop(c *connCore) {
	if c.pauseDepth > 0 {
		c.pauseDepth--
	}
	if c.pauseDepth > 0 {
		return // 还有别的 Pause 压着
	}
	c.exitPaused(l) // 幂等：detach 可能已经结过账

	if c.state >= stateDetached {
		// 拆除之后才到达的 resume（典型是在途 AsyncDo 结束）：
		// 资源都已归还，它唯一还要做的事就是解锁 pendingClose。
		if c.pendingClose && !c.asyncBusy {
			c.pendingClose = false
			l.finishClose(c)
		}
		return
	}

	// 串行域按序恢复：放出暂停期间排队的 Post 闭包（仅 Open；Draining 起丢弃）。
	// 逐个弹出而不是 range 一份快照——闭包里可以再次 Pause / AsyncDo，
	// 那一刻串行域重新被占，剩下的必须留到下一次 resume，否则它们会与
	// 新的异步任务并发跑，直接违反串行域。
	for c.state == stateOpen && c.pauseDepth == 0 && len(c.pendingPosts) > 0 {
		fn := c.pendingPosts[0]
		c.pendingPosts = c.pendingPosts[1:]
		l.runSafe(c, fn)
	}

	if c.pendingClose {
		c.pendingClose = false
		l.finishClose(c)
		return
	}
	// 上面的闭包可能重新 Pause（或关掉连接）：只有仍然 Open 且读闸已开
	// 才恢复读兴趣，否则会把一条本该暂停的连接放回 idle 链。
	if c.state != stateOpen || c.pauseDepth > 0 {
		return
	}
	l.modInterest(c, c.currentInterest()|interestRead)
	l.idleLRU.pushBack(&c.tnode, l.now())
	l.queueCarry(c) // carry 里可能有暂停时留下的完整帧
}

// enterPaused / exitPaused 把「读闸的链表归属 + 计数」收敛成一对，
// 保证 tnode 在 idle 与 pause 两条链之间**互斥**（同一个侵入式节点被挂进
// 两条链会直接覆盖指针，把链表结构毁掉）。
// startDeliverBudget 在一轮迭代里第一次触碰这条连接时重置投递计数。
// 按「轮」而不是按「读事件」计量：WS 下一个读事件会被切成很多段 emit，
// 而 carry 续投又是同一轮里的另一个阶段——按事件重置等于没有预算。
func (c *connCore) startDeliverBudget(l *loop) {
	if c.deliverTick != l.tick {
		c.deliverTick = l.tick
		c.delivered = 0
	}
}

func (c *connCore) enterPaused(l *loop) {
	l.modInterest(c, c.currentInterest()&^interestRead)
	l.idleLRU.remove(&c.tnode)
	l.pauseLRU.pushBack(&c.tnode, l.now())
	c.pausedActive = true
	l.stats.connsPaused.Add(1)
}

// exitPaused 幂等：detach 会先结清读闸的账，而 resume 控制项可能在那之后
// 才到达（它正是解锁 pendingClose 的那一步），两边都调这个函数。
func (c *connCore) exitPaused(l *loop) {
	if !c.pausedActive {
		return
	}
	c.pausedActive = false
	l.pauseLRU.remove(&c.tnode)
	l.stats.connsPaused.Add(-1)
}

// post 把闭包排进该连接的串行域（任意 goroutine 可调）。
// 已关闭 ⇒ 不执行、不报错；暂停中 ⇒ 排队，resume 后按序执行。
func (c *connCore) post(fn func()) {
	l := c.loop
	l.box.push(func() {
		// 控制项直携 core 引用；执行前查主状态（R5：gen 到 Detached 才递增，
		// 只查 gen 会留下「已承诺关闭但闭包仍执行」的窗口）。
		if c.state != stateOpen || c.closing.Load() {
			return
		}
		if c.pauseDepth > 0 {
			if len(c.pendingPosts) >= maxPendingPosts {
				l.closeLocal(c, ErrPendingOverflow)
				return
			}
			c.pendingPosts = append(c.pendingPosts, fn)
			return
		}
		l.runSafe(c, fn)
	})
	l.wk.maybeNotify()
}

// asyncDo 在串行域内调用：pause + 暂存任务，goroutine 在当次回调返回之后
// 才启动（由 loop 的投递循环在回调返回点触发，见 deliverCallback）。
func (c *connCore) asyncDo(fn func()) error {
	if c.state != stateOpen || c.closing.Load() {
		return ErrConnClosed
	}
	if c.asyncBusy {
		return ErrAsyncBusy
	}
	c.asyncBusy = true
	resume := c.pause()
	c.asyncFn = fn
	c.asyncResume = resume
	return nil
}

// launchAsync 在回调返回点由 loop 调用：真正起 goroutine。
//
// **必须在回调的所有出口都调用**，包括回调返回 error 和 panic 的出口：
// 任务已经注册（asyncBusy = true、读闸已降），goroutine 却没起来的话，
// 没有任何东西会送出 resume——连接进 Detached 后挂在 pendingClose 上，
// OnClose 永远不触发，core 与 State 永久留在 loop 的收件箱里。
func (l *loop) launchAsync(c *connCore) {
	fn, resume := c.asyncFn, c.asyncResume
	if fn == nil {
		return // 幂等：多个出口都调，只有第一个真正启动
	}
	c.asyncFn, c.asyncResume = nil, nil
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.requestClose(fmt.Errorf("%w: AsyncDo: %v", ErrHandlerPanic, r))
			}
			// 清 asyncBusy 必须回 loop（字段属于串行域），且先于 resume 的
			// depth-- 处理 pendingClose 的解锁。
			l.box.push(func() { c.asyncBusy = false })
			resume()
			l.wk.maybeNotify()
		}()
		fn()
	}()
}

// runSafe 在恢复屏障内执行串行域闭包；panic ⇒ 关连接（ErrHandlerPanic）。
// AsyncDo 的 goroutine 在**当次回调返回之后**启动——放在 defer 里，
// 保证 panic 出口也会启动（否则任务会永久悬空，见 launchAsync 的注释）。
func (l *loop) runSafe(c *connCore, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			l.closeLocal(c, fmt.Errorf("%w: %v", ErrHandlerPanic, r))
		}
		l.launchAsync(c)
	}()
	fn()
}

// currentInterest 按状态与子状态推导应有的兴趣集合。
func (c *connCore) currentInterest() interest {
	var want interest
	switch c.state {
	case stateProxy, stateHandshaking, stateOpen:
		if c.pauseDepth == 0 {
			want |= interestRead
		}
	case stateDraining:
		// 只有写（06 状态表）
	default:
		return 0
	}
	if c.out != nil && c.out.stage2Bytes > 0 {
		want |= interestWrite
	}
	return want
}
