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

	// 读闸 / 异步（串行域）
	pauseDepth   int
	pendingPosts []func() // 暂停期间排队的 Post 闭包
	asyncBusy    bool
	asyncFn      func() // AsyncDo 暂存：当次回调返回之后才启动 goroutine
	asyncResume  func() // asyncFn 对应的 resume
	pendingClose bool   // Detached 但串行域未空闲，等 resume 后 onClose

	curInterest interest // 当前 poller 兴趣（边沿动作：只在变化时 mod）
	carryQueued bool     // 已在 loop 的 carryQ 里

	// closing 可从任意 goroutine 置位（Close 幂等，第一个 reason 生效）。
	closing atomic.Bool
	mu      sync.Mutex // 只保护 reason
	reason  error

	cb coreCallbacks

	tnode lruNode // 状态链节点：idle / handshake / pause（按状态互斥）
	snode lruNode // stall 链节点（WriteBlocked 期间）
	lnode lruNode // linger 链节点（Draining 期间；暂停中进入 Draining 时与 tnode 并存）
}

func (c *connCore) setReason(err error) {
	c.mu.Lock()
	if c.reason == nil {
		c.reason = err
	}
	c.mu.Unlock()
}

func (c *connCore) closeReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// requestClose 是任意 goroutine 可调的关闭入口（公开 Close 的落点）。
// 幂等；进入 Draining 的实际动作经收件箱回到 loop 线程。
func (c *connCore) requestClose(reason error) {
	if !c.closing.CompareAndSwap(false, true) {
		return
	}
	c.setReason(reason)
	l := c.loop
	l.box.push(func() { l.enterDraining(c) })
	l.wk.maybeNotify()
}

// closeLocal 是 loop 线程上的关闭入口（协议错误、超时、写失败……）。
func (l *loop) closeLocal(c *connCore, reason error) {
	if !c.closing.CompareAndSwap(false, true) {
		c.setReason(reason) // 已在关闭中：只尝试补记原因（第一个生效）
		return
	}
	c.setReason(reason)
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
		l.modInterest(c, c.currentInterest()&^interestRead)
		l.idleLRU.remove(&c.tnode)
		l.pauseLRU.pushBack(&c.tnode, l.now())
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
	if c.pauseDepth == 0 {
		return // Closed 后的 resume：no-op
	}
	c.pauseDepth--
	if c.pauseDepth > 0 {
		return
	}
	l.pauseLRU.remove(&c.tnode)

	// 串行域按序恢复：先放出暂停期间排队的 Post 闭包（仅 Open；Draining 起丢弃）。
	posts := c.pendingPosts
	c.pendingPosts = nil
	if c.state == stateOpen {
		for _, fn := range posts {
			l.runSafe(c, fn)
			if c.state != stateOpen {
				break
			}
		}
	}

	if c.pendingClose {
		c.pendingClose = false
		l.finishClose(c)
		return
	}
	if c.state != stateOpen {
		return // Draining：读闸不再恢复，出站继续排空
	}
	l.modInterest(c, c.currentInterest()|interestRead)
	l.idleLRU.pushBack(&c.tnode, l.now())
	l.queueCarry(c) // carry 里可能有暂停时留下的完整帧
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
func (l *loop) launchAsync(c *connCore) {
	fn, resume := c.asyncFn, c.asyncResume
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
func (l *loop) runSafe(c *connCore, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			l.closeLocal(c, fmt.Errorf("%w: %v", ErrHandlerPanic, r))
		}
	}()
	fn()
	if c.asyncFn != nil {
		l.launchAsync(c) // AsyncDo 的 goroutine 在当次回调返回之后启动
	}
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
