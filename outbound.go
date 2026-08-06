package gate

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

// iovMax 是一次 writev 的 iovec 上限（Linux / macOS 都是 1024）。
const iovMax = 1024

// wireSlack 覆盖一条消息可能分摊到的帧级头部：compound 外层帧头（≤4 字节）
// 不属于任何一条子消息，WS 帧头（≤10 字节）不属于任何一条 gate 帧。
// 少了它，「reservedWire ≥ 真实占用」会以每帧几个字节的差距失守（06 预算模型）。
const (
	wireSlackTCP = 4
	wireSlackWS  = 14
)

type itemKind uint8

const (
	itemMessage itemKind = iota
	itemFrame
	itemCipherBarrier
	// itemRaw：已是最终线路字节的原始段（WS 的 101 响应、HTTP 拒绝、控制帧），
	// 绕过 gate 编码与 WS 封帧，但仍走同一条 FIFO——close 帧因此绝不可能
	// 插进半个数据帧中间（W13 的结构性保证）。
	itemRaw
)

// stage1Item：待编码队列的 tagged union（03「两级队列」）。
// SetCipher 必须在队列里留下可见的边界，所以 stage 1 不是单一的消息数组。
type stage1Item struct {
	kind itemKind
	// itemMessage
	data                    []byte // 分级池借出（Send 复制 / SendFunc 直填）
	maskPermit, maskAlready byte
	// itemFrame
	frame *Frame
	// itemCipherBarrier
	cipher Cipher
}

// dirtyPoster 是 outbound 对 loop 的最小依赖：把连接挂上 dirty 链并按需唤醒。
// postDirty 在持有 outbound 锁的临界区内被调用——dirty 节点的发布与 armed 置位
// 必须在同一临界区（O14）；maybeNotify 留在锁外。
type dirtyPoster interface {
	postDirty(o *outbound)
	maybeNotify()
}

type outConfig struct {
	maxBuffer         int // -1 = 无限
	highWater         int // <=0 = 不设软上限
	maxCluster        int // <=0 = 不合包；已夹进协议上限
	compressThreshold int
	maxMessage        int // Send 准入上限（线路口径 frameSize(len+Overhead)）
	wireSlack         int
	wsWrap            func(total int, dst []byte) int // 编码期 WS 封帧钩子；nil = 裸 TCP
}

// normalizeOutConfig 把 Outbound/Limits 的零值语义翻译成内部取值：
// 保护性选项零值给默认，Unlimited 才是关闭（01「配置」）。
func normalizeOutConfig(ob Outbound, li Limits, ws bool) outConfig {
	cfg := outConfig{
		maxBuffer:         ob.MaxBuffer,
		highWater:         ob.HighWater,
		maxCluster:        min(ob.MaxCluster, maxMessageSize),
		compressThreshold: ob.CompressThreshold,
		maxMessage:        li.MaxMessage,
		wireSlack:         wireSlackTCP,
	}
	if ws {
		cfg.wireSlack = wireSlackWS
		cfg.wsWrap = wsBinaryWrap // 编码期封帧（O12/W7）
	}
	switch {
	case cfg.maxBuffer == 0:
		cfg.maxBuffer = 1 << 20
	case cfg.maxBuffer < 0:
		cfg.maxBuffer = -1
	}
	if cfg.highWater <= 0 && cfg.maxBuffer > 0 {
		cfg.highWater = cfg.maxBuffer / 4
	}
	if cfg.maxMessage <= 0 || cfg.maxMessage > maxMessageSize {
		cfg.maxMessage = maxMessageSize
	}
	return cfg
}

// writeStatus 是一次 write() 的结果，宿主据此维护写兴趣与状态机。
type writeStatus uint8

const (
	writeIdle    writeStatus = iota // stage 2 已空，可摘掉写兴趣
	writeBlocked                    // 内核缓冲满（EAGAIN 或短写），保持/注册写兴趣
	writeFailed                     // 真错误，outbound 已自关，宿主负责关连接
)

// outbound 是单连接的发送侧：stage 1（待编码）+ stage 2（待发送链）+ 一处记账。
//
// 并发模型：mu 保护 stage 1、预算与标志位，任意 goroutine 可入队；
// stage 2 与编码状态只在连接所属的事件循环线程上触碰，无锁（O4）。
type outbound struct {
	mu           sync.Mutex
	q1           []stage1Item
	q1spare      []stage1Item
	tailCipher   Cipher // 队尾生效的 cipher（最后一个 barrier 之后）：charge 与 SendFrame 冲突检查用
	reservedWire int    // 保守上界的在途线路字节（O5：一处记账）
	closing      bool
	flushable    bool // 握手完成前 false：不 arm（O11）
	armed        bool // dirty 节点在链中
	inLoop       bool // loop 正在处理本连接的入站批

	overHigh bool // 已计入 ConnsOverHighWater（水位穿越是边沿事件）

	// dirtyNext 是 loop dirty 链的侵入式节点（mpsc.go）。armed 保证单一成员
	// 资格，因此它只会被「当前持有成员资格的那次 push」触碰。
	dirtyNext *outbound

	// ── 以下只在 loop 线程访问 ──
	head, tail  *chunk
	stage2Bytes int
	encCipher   Cipher // 编码流推进到的 cipher epoch
	// wireBroken：写失败或编码失败之后置位。帧流已经有洞，此后一个字节
	// 都不该再写出去——连 WS close 帧也不行（05：写失败不发 close 帧）。
	wireBroken bool

	cfg      outConfig
	io       connIO
	fd       int
	env      *loopEnv
	loop     dirtyPoster
	owner    *connCore   // 所属连接（loop 的 dirty 处理与写状态回填用）；测试可为 nil
	fatal    func(error) // 编码/写失败：outbound 已自关，宿主关连接（O8）。loop 线程。
	onPanic  func(error) // SendFunc 的 fill panic：按 OnMessage panic 处理（宿主关连接）。任意 goroutine。
	progress func()      // 每次写出 ≥1 字节时回调（stall LRU 的 lastProgress）。loop 线程。
}

func newOutbound(cfg outConfig, io connIO, fd int, env *loopEnv, loop dirtyPoster) *outbound {
	return &outbound{cfg: cfg, io: io, fd: fd, env: env, loop: loop, flushable: true}
}

func cipherOverhead(ci Cipher) int {
	if ci == nil {
		return 0
	}
	return ci.Overhead()
}

// charge 是入队时扣的保守上界（06 预算模型）。
func (o *outbound) charge(payloadLen int, ci Cipher) int {
	return frameSize(payloadLen+cipherOverhead(ci)) + o.cfg.wireSlack
}

// admitLocked 是入队前的两道准入：每连接的 MaxBuffer 与全局的
// MaxOutboundBytes（loop 配额租约）。通过时已完成扣减，调用方负责入队。
// 必须持有 o.mu。
func (o *outbound) admitLocked(charge int) bool {
	if o.cfg.maxBuffer >= 0 && o.reservedWire+charge > o.cfg.maxBuffer {
		return false
	}
	if !o.env.quota.acquire(charge) {
		return false // 全局预算耗尽：同样是准入拒绝，帧流没有洞
	}
	o.reservedWire += charge
	o.env.stats.outboundQueued.Add(int64(charge))
	o.syncWaterLocked()
	return true
}

// syncWaterLocked 维护 ConnsOverHighWater：水位穿越是边沿事件，
// 逐连接采样在 10 万连接下太贵。必须持有 o.mu。
func (o *outbound) syncWaterLocked() {
	over := o.cfg.highWater > 0 && o.reservedWire > o.cfg.highWater
	if over == o.overHigh {
		return
	}
	o.overHigh = over
	if over {
		o.env.stats.connsOverHighWater.Add(1)
	} else {
		o.env.stats.connsOverHighWater.Add(-1)
	}
}

// refund 退还 n 字节（编码退还、写出退还、abort、discard 都走这里），
// 保证 reservedWire 与配额租约永远同步。必须持有 o.mu。
func (o *outbound) refundLocked(n int) {
	if n == 0 {
		return
	}
	o.reservedWire -= n
	o.env.stats.outboundQueued.Add(int64(-n))
	o.env.quota.release(n)
	o.syncWaterLocked()
}

// enqueueLocked 完成入队与 arm，返回是否需要锁外唤醒。
// 调用方必须已持有 o.mu 并通过 admitLocked（charge 已扣）。
func (o *outbound) enqueueLocked(it stage1Item) (wake bool) {
	o.q1 = append(o.q1, it)
	if !o.armed && !o.inLoop && o.flushable {
		o.armed = true
		o.loop.postDirty(o) // 与 armed 同临界区（O14）
		return true
	}
	return false
}

// send 复制 b 入队。permit 记允许 gate 做哪些加工：Send 是 z|c|e，SendAlone 是 e
// （只关压缩与合包，不关加密——没有任何接口能绕过连接的 Cipher）。
func (o *outbound) send(b []byte, permit byte) error {
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return ErrConnClosed
	}
	if frameSize(len(b)+cipherOverhead(o.tailCipher)) > o.cfg.maxMessage {
		o.mu.Unlock()
		return ErrMessageTooLarge
	}
	if !o.admitLocked(o.charge(len(b), o.tailCipher)) {
		o.mu.Unlock()
		o.env.stats.sendQueueFull.Add(1)
		return ErrSendQueueFull // 消息从未入队，帧流没有洞（O1）
	}
	// 复制保持在锁内：挪到锁外会打开「检查与入队之间被 Close/SetCipher 改写前提」
	// 的窗口；memcpy 的量已被 maxMessage 封顶，且分级池取放无锁。
	data := poolGet(len(b))
	copy(data, b)
	wake := o.enqueueLocked(stage1Item{kind: itemMessage, data: data, maskPermit: permit})
	o.mu.Unlock()
	o.env.stats.messagesOut.Add(1)
	o.env.stats.bytesOutRaw.Add(uint64(len(b)))
	if wake {
		o.loop.maybeNotify()
	}
	return nil
}

// sendFrame 入队一个预编码帧。失败路径上绝不 retain（O9）：
// 全部检查通过之后才构造 item。
func (o *outbound) sendFrame(f *Frame) error {
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return ErrConnClosed
	}
	if o.tailCipher != nil {
		o.mu.Unlock()
		return ErrCipherConflict // 显式失败，不静默降级成明文
	}
	// 广播的 backpressure 关心「这条连接要传多少字节」：按该帧的线路长度计入。
	if !o.admitLocked(len(f.wire) + o.cfg.wireSlack) {
		o.mu.Unlock()
		o.env.stats.sendQueueFull.Add(1)
		return ErrSendQueueFull
	}
	o.env.stats.messagesOut.Add(1)
	o.env.stats.bytesOutRaw.Add(uint64(len(f.wire)))
	wake := o.enqueueLocked(stage1Item{kind: itemFrame, frame: f})
	o.mu.Unlock()
	if wake {
		o.loop.maybeNotify()
	}
	return nil
}

// sendFunc 是零复制路径的三阶段协议（06「SendFunc 的两阶段协议」）。
func (o *outbound) sendFunc(n int, fill func([]byte) (int, error)) error {
	if n < 0 {
		return ErrInvalidLength
	}
	// 1.【reserve】扣预算、取池内存——不是线性化点，消息还没入队。
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return ErrConnClosed
	}
	if frameSize(n+cipherOverhead(o.tailCipher)) > o.cfg.maxMessage {
		o.mu.Unlock()
		return ErrMessageTooLarge
	}
	reserved := o.charge(n, o.tailCipher)
	if !o.admitLocked(reserved) {
		o.mu.Unlock()
		o.env.stats.sendQueueFull.Add(1)
		return ErrSendQueueFull
	}
	o.mu.Unlock()
	buf := poolGet(n)

	// 2.【fill】锁外跑业务代码：持锁会把 Send/Close 挡在锁上，重入 Send 就是死锁。
	k, err := safeFill(fill, buf)

	// 3.【commit】重新加锁，此刻才线性化。
	if err != nil || k < 0 || k > n {
		o.mu.Lock()
		o.refundLocked(reserved)
		o.mu.Unlock()
		poolPut(buf)
		switch {
		case err == nil:
			return ErrInvalidLength // 业务 bug：k 越界
		case errors.Is(err, ErrHandlerPanic):
			// fill panic：缓冲照常回收之后，按 OnMessage panic 处理（关连接）。
			if o.onPanic != nil {
				o.onPanic(err)
			}
			return err
		default:
			return err // fill 的 error 原样返回，消息未入队
		}
	}
	o.mu.Lock()
	if o.closing {
		o.refundLocked(reserved)
		o.mu.Unlock()
		poolPut(buf)
		return ErrConnClosed // Close 不等待 fill；fill 期间关闭 ⇒ 不入队
	}
	// 按实际长度重算。tailCipher 可能已被队列里的 barrier 换掉——commit 在
	// barrier 之后线性化，编码用的就是新 cipher，所以长度上限要按**新** epoch
	// 重查：否则 SendFunc 返回 nil，却在编码期因新 Overhead 越限而关掉整条连接。
	if frameSize(k+cipherOverhead(o.tailCipher)) > o.cfg.maxMessage {
		o.refundLocked(reserved)
		o.mu.Unlock()
		poolPut(buf)
		return ErrMessageTooLarge
	}
	// MaxBuffer 也要按新 charge 重查：旧 cipher 下贴着上限的预留，换成
	// Overhead 更大的 cipher 之后就越限了。
	charge := o.charge(k, o.tailCipher)
	if o.cfg.maxBuffer >= 0 && o.reservedWire-reserved+charge > o.cfg.maxBuffer {
		o.refundLocked(reserved)
		o.mu.Unlock()
		poolPut(buf)
		o.env.stats.sendQueueFull.Add(1)
		return ErrSendQueueFull
	}
	if delta := charge - reserved; delta > 0 {
		o.reservedWire += delta
		o.env.stats.outboundQueued.Add(int64(delta))
		o.env.quota.borrow(delta) // 已过准入的消息不再拒绝，但账要对称
		o.syncWaterLocked()
	} else {
		o.refundLocked(-delta)
	}
	wake := o.enqueueLocked(stage1Item{kind: itemMessage, data: buf[:k], maskPermit: flagZ | flagC | flagE})
	o.mu.Unlock()
	o.env.stats.messagesOut.Add(1)
	o.env.stats.bytesOutRaw.Add(uint64(k))
	if wake {
		o.loop.maybeNotify()
	}
	return nil
}

// safeFill 在恢复屏障内调用业务的 fill。
func safeFill(fill func([]byte) (int, error), buf []byte) (k int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: SendFunc fill: %v", ErrHandlerPanic, r)
		}
	}()
	return fill(buf)
}

// sendRaw 入队一段已是最终线路字节的原始数据（loop 线程或握手路径调用；
// 复制入池）。charge 精确等于长度，无 slack——它不参与任何封装。
//
// force = true 用于**关闭已仲裁之后**追加的收尾帧（WS close 帧）：此刻
// closing 已置位，业务数据再也进不来，所以强行追加的这一段结构上必然是
// FIFO 的最后一段——close 帧绝不可能插进半个数据帧中间，也绝不可能被
// 后来的数据帧越过（W13）。它同时豁免 MaxBuffer 准入：一个 ≤127 字节的
// 收尾帧不值得为它放弃可解释的关闭码。
func (o *outbound) sendRaw(b []byte, force bool) error {
	o.mu.Lock()
	if o.closing && !force {
		o.mu.Unlock()
		return ErrConnClosed
	}
	if force {
		// 收尾帧豁免准入，但账要平：borrow 是不可拒绝的扣减，
		// 与后续的 release 严格对称（acquire 的返回值被忽略时，
		// release 会凭空造出额度）。
		o.reservedWire += len(b)
		o.env.stats.outboundQueued.Add(int64(len(b)))
		o.env.quota.borrow(len(b))
	} else if !o.admitLocked(len(b)) {
		o.mu.Unlock()
		o.env.stats.sendQueueFull.Add(1)
		return ErrSendQueueFull // 控制帧也吃准入：否则 ping 洪水可无限堆 pong
	}
	data := poolGet(len(b))
	copy(data, b)
	wake := o.enqueueLocked(stage1Item{kind: itemRaw, data: data})
	o.mu.Unlock()
	if wake {
		o.loop.maybeNotify()
	}
	return nil
}

// setCipher 往队尾追加 cipher barrier（03「SetCipher：队列里的 barrier」）。
// 入站方向的即时切换由 session 层完成；这里只管出站 epoch。
func (o *outbound) setCipher(ci Cipher) {
	o.mu.Lock()
	if o.closing {
		o.mu.Unlock()
		return
	}
	o.tailCipher = ci
	wake := o.enqueueLocked(stage1Item{kind: itemCipherBarrier, cipher: ci})
	o.mu.Unlock()
	if wake {
		o.loop.maybeNotify()
	}
}

// isWritable 实现 Conn.Writable()：积压未越过高水位且未关闭。
func (o *outbound) isWritable() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.closing && (o.cfg.highWater <= 0 || o.reservedWire <= o.cfg.highWater)
}

// closeSend 置 closing：后续 Send 一律 ErrConnClosed，已入队消息保留待尽力排空。
// 生产路径的关闭一律走 connCore.beginClose（它才是线性化点）；这里保留给
// 没有 core 的单元测试直接使用。
func (o *outbound) closeSend() {
	o.mu.Lock()
	o.closing = true
	o.mu.Unlock()
}

func (o *outbound) setFlushable(v bool) {
	o.mu.Lock()
	o.flushable = v
	o.mu.Unlock()
}

// ─── 以下方法只能在连接所属的事件循环线程上调用 ───

// beginInLoop 在 loop 开始处理本连接的入站批之前调用：期间的 Send 不需要唤醒
// （批收尾必然 flush）。调用方必须保证批边界处执行 flush(false, true)——
// 任何置 inLoop 的路径都要用 defer 兜住（O14 第二条）。
func (o *outbound) beginInLoop() {
	o.mu.Lock()
	o.inLoop = true
	o.mu.Unlock()
}

// take 取走 stage 1。clearArmed 在消费 dirty 节点时置 true（节点已出链），
// clearInLoop 在入站批收尾时置 true——都与取队列同临界区（03 in-loop 论证）。
func (o *outbound) take(clearArmed, clearInLoop bool) []stage1Item {
	o.mu.Lock()
	if clearArmed {
		o.armed = false
	}
	if clearInLoop {
		o.inLoop = false
	}
	items := o.q1
	o.q1 = o.q1spare[:0]
	o.q1spare = nil
	o.mu.Unlock()
	return items
}

// recycleQ1 归还 take 拿走的切片（清引用防滞留）。
func (o *outbound) recycleQ1(items []stage1Item) {
	if items == nil {
		return
	}
	clear(items)
	o.mu.Lock()
	if o.q1spare == nil {
		o.q1spare = items[:0]
	}
	o.mu.Unlock()
}

// flush：编码 stage 1 → 追加 stage 2 → 写。幂等，队列空就只推进写（O10）。
func (o *outbound) flush(clearArmed, clearInLoop bool) writeStatus {
	items := o.take(clearArmed, clearInLoop)
	if len(items) > 0 {
		if err := o.encodeItems(items); err != nil {
			o.failLoop(err)
			return writeFailed
		}
		o.recycleQ1(items)
	}
	st, err := o.write()
	if err != nil {
		o.failLoop(err)
		return writeFailed
	}
	return st
}

// encodeItems 按 03 的分组算法把一批 stage1Item 编码成 chunk 追加到 stage 2，
// 成功时退还「保守上界 − 实际线路字节」的差额。
//
// panic 屏障放在这个批边界（01「panic 屏障放在批边界」）：Cipher 的 Seal 在
// 这里被逐帧调用，屏障一批一次而不是一帧一次。任何失败（含 panic）都在返回前
// 释放已生成的 seg 与未消费的 items——之后由调用方走 failLoop。
// encCtx 是一批编码的段落状态。显式结构体而不是闭包——闭包环境每批一次
// 堆分配，会把「出站 flush 零堆分配」打破。
type encCtx struct {
	o                *outbound
	outHead, outTail *chunk // 本批最终追加到 stage 2 的链
	outBytes         int
	segHead, segTail *chunk // 当前 gate 帧段落
	segBytes         int
}

func (e *encCtx) appendOut(head, tail *chunk, n int) {
	if head == nil {
		return
	}
	if e.outTail == nil {
		e.outHead, e.outTail = head, tail
	} else {
		e.outTail.next = head
		e.outTail = tail
	}
	e.outBytes += n
}

// closeSeg 结束当前 gate 段落：WS 模式下前插一个封帧头 chunk（O12/W7）。
func (e *encCtx) closeSeg() {
	if e.segHead == nil {
		return
	}
	if e.o.cfg.wsWrap != nil {
		h := e.o.env.slab.get()
		h.kind = chunkInline
		h.n = int32(e.o.cfg.wsWrap(e.segBytes, h.hdr[:]))
		h.next = e.segHead
		e.segHead = h
		e.segBytes += int(h.n)
	}
	e.appendOut(e.segHead, e.segTail, e.segBytes)
	e.segHead, e.segTail, e.segBytes = nil, nil, 0
}

func (e *encCtx) appendChunk(c *chunk) {
	if e.segTail == nil {
		e.segHead, e.segTail = c, c
	} else {
		e.segTail.next = c
		e.segTail = c
	}
	e.segBytes += int(c.n)
}

func (e *encCtx) releaseAll() {
	e.closeSeg()
	for c := e.outHead; c != nil; {
		next := c.next
		c.release()
		e.o.env.slab.put(c)
		c = next
	}
	e.outHead, e.outTail, e.outBytes = nil, nil, 0
}

func clusterableItem(it *stage1Item) bool {
	return it.kind == itemMessage && it.maskPermit&flagC != 0 && it.maskAlready == 0
}

func (o *outbound) encodeItems(items []stage1Item) (err error) {
	// out*：本批最终追加到 stage 2 的链；seg*：当前 gate 帧段落。
	// 原始段（itemRaw）会结束当前段落——WS 封帧只包 gate 帧，101 响应与
	// WS 控制帧原样出线，但仍在同一条 FIFO 上（W13）。
	//
	// panic 屏障放在这个批边界（01「panic 屏障放在批边界」）：Cipher 的 Seal
	// 在这里被逐帧调用，屏障一批一次而不是一帧一次。任何失败（含 panic）都在
	// 返回前释放已生成的链与未消费的 items——之后由调用方走 failLoop。
	e := encCtx{o: o}
	charged := 0

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: cipher/encode: %v", ErrHandlerPanic, r)
		}
		if err != nil {
			e.releaseAll()
			o.releaseItems(items)
		}
	}()

	i := 0
	for i < len(items) {
		it := items[i]
		switch it.kind {
		case itemCipherBarrier:
			// barrier：切换编码 epoch，并天然结束了任何 compound 分组（O13）。
			o.encCipher = it.cipher
			i++

		case itemRaw:
			charged += len(it.data)
			e.closeSeg()
			c := o.env.slab.get()
			c.kind, c.buf, c.n = chunkPooled, it.data, int32(len(it.data))
			e.appendOut(c, c, int(c.n))
			items[i].data = nil
			i++

		case itemFrame:
			charged += len(it.frame.wire) + o.cfg.wireSlack
			o.env.stats.framesOut.Add(1)
			c := o.env.slab.get()
			c.kind, c.buf, c.n, c.frame = chunkFrame, it.frame.wire, int32(len(it.frame.wire)), it.frame
			e.appendChunk(c)
			items[i].frame = nil
			i++

		case itemMessage:
			if o.cfg.maxCluster > 0 && clusterableItem(&it) {
				// 分组上限再夹掉当前 cipher 的 Overhead：构造时夹不掉它——
				// cipher 是运行时换的，而 body+Overhead 不能越协议上限。
				limit := min(o.cfg.maxCluster, maxMessageSize-cipherOverhead(o.encCipher))
				j, size := i, 0
				for j < len(items) && clusterableItem(&items[j]) {
					fs := frameSize(len(items[j].data))
					if j > i && size+fs > limit {
						break
					}
					size += fs
					j++
				}
				if j-i >= 2 {
					if err := o.encodeCompound(items[i:j], size, &charged, &e); err != nil {
						return err
					}
					i = j
					continue
				}
			}
			if err := o.encodeSingle(it, &charged, &e); err != nil {
				return err
			}
			items[i].data = nil // 所有权已移交 chunk（或已在替换时归还池）
			i++
		}
	}

	e.closeSeg()
	if e.outHead != nil {
		if o.tail == nil {
			o.head, o.tail = e.outHead, e.outTail
		} else {
			o.tail.next = e.outHead
			o.tail = e.outTail
		}
		o.stage2Bytes += e.outBytes
	}

	// 编码退还（第一段）：此刻 WS 封帧已完成，线路字节精确可知（06 预算模型）。
	if refund := charged - e.outBytes; refund != 0 {
		o.mu.Lock()
		o.refundLocked(refund)
		o.mu.Unlock()
	}
	return nil
}

func (o *outbound) encodeSingle(it stage1Item, charged *int, e *encCtx) error {
	*charged += o.charge(len(it.data), o.encCipher)

	var ci Cipher
	if it.maskPermit&flagE != 0 {
		ci = o.encCipher
	}
	tryCompress := it.maskPermit&flagZ != 0 && it.maskAlready == 0 &&
		o.cfg.compressThreshold > 0 && len(it.data) > o.cfg.compressThreshold
	bf, err := buildFrameAAD(it.data, it.maskAlready, o.encoderIf(tryCompress), tryCompress, ci, maxMessageSize, &o.env.aadHdr)
	if err != nil {
		return err
	}
	if bf.bodyPooled {
		poolPut(it.data) // 原文已被压缩/AEAD 输出替换
	}
	o.pushFrameChunks(&bf, e)
	return nil
}

func (o *outbound) encodeCompound(group []stage1Item, size int, charged *int, e *encCtx) error {
	for gi := range group {
		*charged += o.charge(len(group[gi].data), o.encCipher)
	}
	body := poolGet(size)[:0]
	for gi := range group {
		body = appendSubFrame(body, group[gi].data)
		poolPut(group[gi].data)
		group[gi].data = nil
	}
	// 02：compound 按整批字节数与阈值比较（>=）。
	tryCompress := o.cfg.compressThreshold > 0 && size >= o.cfg.compressThreshold
	bf, err := buildFrameAAD(body, flagC, o.encoderIf(tryCompress), tryCompress, o.encCipher, maxMessageSize, &o.env.aadHdr)
	if err != nil {
		poolPut(body)
		return err
	}
	if bf.bodyPooled {
		poolPut(body)
	}
	o.pushFrameChunks(&bf, e)
	return nil
}

func (o *outbound) encoderIf(need bool) *zstd.Encoder {
	if !need {
		return nil
	}
	return o.env.encoder()
}

// pushFrameChunks 把 builtFrame 落成 inline(帧头) + pooled(body) 两个 chunk。
// 帧头内联在 chunk 节点里——它是持久线路字节，必须活到该 chunk 完全写出（O12）。
func (o *outbound) pushFrameChunks(bf *builtFrame, e *encCtx) {
	o.env.stats.framesOut.Add(1)
	h := o.env.slab.get()
	h.kind = chunkInline
	copy(h.hdr[:], bf.hdr[:bf.hdrLen])
	h.n = int32(bf.hdrLen)
	e.appendChunk(h)

	if len(bf.body) > 0 {
		p := o.env.slab.get()
		p.kind, p.buf, p.n = chunkPooled, bf.body, int32(len(bf.body))
		e.appendChunk(p)
	} else {
		// 空消息：没有字节就不占 chunk，但池借的零长缓冲要还。
		poolPut(bf.body)
	}
}

// 单连接一次事件的写预算（06「公平性预算」）：超出后保持写兴趣让给别的连接，
// 水平触发下一轮还会报可写。
const (
	writeBudgetBytes = 1 << 20
	writeBudgetCalls = 8
)

// write 按 06 的写循环执行：凑 iovec → writev → 推进 off → 退还预算。
// 返回 writeFailed 时错误一并返回，由调用方走 failLoop。
func (o *outbound) write() (writeStatus, error) {
	written, calls := 0, 0
	for o.head != nil {
		if written >= writeBudgetBytes || calls >= writeBudgetCalls {
			return writeBlocked, nil // 预算耗尽：保持写兴趣，下一轮继续
		}
		vec := o.env.iov[:0]
		want := 0
		for c := o.head; c != nil && len(vec) < iovMax; c = c.next {
			b := c.bytes()[c.off:]
			vec = append(vec, b)
			want += len(b)
		}
		n, err := o.io.writev(o.fd, vec)
		calls++
		for i := range vec {
			vec[i] = nil // 不让 iovec 暂存钉住已写完的缓冲
		}
		if n > 0 {
			written += n
			o.env.stats.bytesOut.Add(uint64(n))
			o.advance(n)
			o.mu.Lock()
			o.refundLocked(n) // 写出退还（第二段）
			o.mu.Unlock()
			if o.progress != nil {
				o.progress() // lastProgress：stall LRU 移尾
			}
		}
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				o.env.stats.writeEAGAIN.Add(1)
				return writeBlocked, nil
			}
			return writeFailed, err
		}
		if n < want {
			o.env.stats.writeEAGAIN.Add(1)
			return writeBlocked, nil // 内核缓冲满：注册写兴趣，等可写事件
		}
	}
	return writeIdle, nil
}

// advance 按已写字节数推进链头，写满的 chunk 出链并归还内存（03「写与部分写」：
// 归还时机是该 chunk 完全写出之后，不是 writev 返回之后）。
func (o *outbound) advance(n int) {
	o.stage2Bytes -= n
	for n > 0 {
		c := o.head
		rem := int(c.n - c.off)
		if n < rem {
			c.off += int32(n)
			return
		}
		n -= rem
		o.head = c.next
		c.release()
		o.env.slab.put(c)
	}
	if o.head == nil {
		o.tail = nil
	}
}

// discard 关闭并丢弃两级队列、清账（06 拆除序列第 4 步；写失败路径的前半）。
// 之后的 Send 一律 ErrConnClosed。只能在 loop 线程调用（要触碰 stage 2）。
func (o *outbound) discard() {
	o.mu.Lock()
	o.closing = true
	items := o.q1
	o.q1, o.q1spare = nil, nil
	o.refundLocked(o.reservedWire) // 丢弃队列：账面与配额一起清零
	o.mu.Unlock()

	o.releaseItems(items)
	for c := o.head; c != nil; {
		next := c.next
		c.release()
		o.env.slab.put(c)
		c = next
	}
	o.head, o.tail, o.stage2Bytes = nil, nil, 0
}

// failLoop 是写/编码失败路径（O8）：**先**把 outbound 置成关闭并丢弃队列，
// **再**通知宿主关连接。并发的 Send 从这一刻起一律 ErrConnClosed，
// 不可能往一条已经有洞的帧流里再写。
//
// 顺序上的一个要点：关闭仲裁（记录 reason）必须发生在 discard 之前——
// discard 直接把 closing 置为 true，之后再仲裁就抢不到「第一个关闭者」，
// OnClose 会拿到一个 nil reason。
func (o *outbound) failLoop(err error) {
	o.wireBroken = true
	if o.owner != nil {
		o.owner.beginClose(err) // 仲裁：置 closing + 记 reason（已有关闭者则保持其 reason）
	}
	o.discard()
	if o.fatal != nil {
		o.fatal(err)
	}
}

// releaseItems 归还未编码 item 持有的池内存 / Frame 引用。已消费的条目
// data/frame 均为 nil，天然跳过。
func (o *outbound) releaseItems(items []stage1Item) {
	for i := range items {
		if items[i].kind == itemMessage && items[i].data != nil {
			poolPut(items[i].data)
		}
		items[i] = stage1Item{}
	}
}
