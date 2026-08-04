package gate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"slices"
	"sync"

	"github.com/panjf2000/gnet/v2"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/klauspost/compress/zstd"
)

var (
	DefaultSenderBuilder = NewSenderBuilder(&SenderConfig{
		CompressThreshold: 1024,            // 1KB
		MaxBufferSize:     2 * 1024 * 1024, // 2MB
		MaxClusterSize:    32 * 1024,       // 32KB
	})

	enc *zstd.Encoder
)

// separateChunk 是 pushSeparate 单次 writev 携带的最大消息条数。
const separateChunk = 32

// maxHeaderLen 是 header 长度字段（12+16 位）能表达的长度上限（不含）。
//
// 一旦有人把 >= maxHeaderLen 的长度交给 encodeHeader，高位就会溢进首字节的
// m/z/c/e 标记位，长度和标记被同时改错，而且收发两端都察觉不到。协议上限
// maxMessageSize 落在这个范围之内，所以 sender 只要守住"任何写上线路的
// payload <= maxMessageSize"，就同时堵住了这个溢出。
const maxHeaderLen = 1 << 28

// 编译期断言：maxMessageSize 必须严格小于 header 能表达的上限。
// 一旦有人调大 maxMessageSize 越过这条线，下面这个负常量转 uint 会直接编译失败。
const _ = uint(maxHeaderLen - 1 - maxMessageSize)

// frameOverhead 返回 size 字节 payload 需要的 header 字节数，必须与 encodeHeader 一致。
func frameOverhead(size int) int {
	if size < moreHeaderSize {
		return 2
	}
	return 4
}

// frameSize 返回一条消息占用的完整字节数：header + payload。
//
// 所有的队列计量、预算扣减、分组累加都必须用它而不是 len(data)。一条空消息的
// len 是 0，但它在线路上仍然要占 2 字节 header，在内存里仍然要占一个 sendMsg：
// 按 len 计量意味着空消息不消耗任何配额，Send([]byte{}) 循环可以在 MaxBufferSize
// 已设的情况下把队列涨到 OOM（queued 永远是 0，ErrSendQueueFull 永远不触发），
// 而 cluster 也会把任意多条空消息收进同一个 compound——每条都在 body 里贡献
// 2 字节子 header，最终帧可以远远超过 MaxClusterSize。
func frameSize(size int) int {
	return size + frameOverhead(size)
}

// sendMsg 的字段顺序不是随手排的：切片放在最前面，三个单字节标记挤在尾部的
// padding 里。反过来（两个 byte 在前）会因为切片要 8 字节对齐而在中间垫 6 字节，
// 结构体从 32 涨到 40。队列是一整段 []sendMsg，每条消息多 8 字节意味着同样一次
// flush 要多碰 25% 的 cache line。
type sendMsg struct {
	data                    []byte
	maskPermit, maskAlready byte
	owned                   bool
}

// compoundable 报告这条消息能否被放进 compound 帧。
//
// 除了调用方允许合包之外还要求它不带任何 already 标记：compound 的子帧按协议
// 约定不得携带 z/c/e（见 header.go），一条"已经压缩好"的消息一旦进了 compound，
// 它的 z 标记就再也没有地方表达。
func (m sendMsg) compoundable() bool {
	return m.maskPermit&maskC != 0 && m.maskAlready == 0
}

type SenderConfig struct {
	// CompressThreshold 触发压缩的字节阈值，<=0 表示从不压缩。
	//
	// 独立帧：payload 超过该值才压缩；
	// 合并帧：整个 batch 的字节数达到该值才压缩。
	//
	// 它只控制压缩，不影响合并——两者由各自的开关独立决定。
	CompressThreshold int
	// MaxBufferSize 单连接允许积压的最大字节数，<=0 表示不限制。
	//
	// 它同时约束两处：sender 内部尚未 flush 的待发送队列（超出时 Send 返回
	// ErrSendQueueFull），以及底层连接的出站缓冲（超出时直接关闭连接）。
	// 后者不再是丢消息：从一条有状态的帧流中间抽掉几条消息，只会让对端的状态机
	// 静默错乱，而业务层什么都察觉不到。
	//
	// 两处的计量口径都是"线路字节"，含每条消息 2~4 字节的 header：按 payload
	// 长度记账时空消息不消耗任何配额，队列可以被 Send([]byte{}) 撑到 OOM。
	MaxBufferSize int
	// MaxClusterSize 单个合并帧的最大字节数，<=0 表示不合并。
	//
	// >0 即开启合并，与 CompressThreshold 无关：大量小消息正是"该合包但不值得
	// 压缩"的典型场景，合包本身能省下系统调用和小 TCP 段。
	//
	// 计量的是 compound body，含每条子消息的子 header。大于协议上限
	// （maxMessageSize，32MB）的取值会被夹到上限：再大的帧对端一定拒收。
	MaxClusterSize int
	// CompressLevel 压缩等级，零值（CompressBetter）保持 gate 的历史行为。
	// 它同时决定 zstd encoder 池的常驻内存，量级差 16 倍，见 CompressLevel。
	CompressLevel CompressLevel
}

type senderBuilder struct {
	c *SenderConfig
	// enc 按等级共享，构造 builder 时解析一次，不在热路径上查表。
	enc *zstd.Encoder
}

func (s *senderBuilder) Build(conn *Conn) SenderI {
	sd := &sender{
		c:          s.c,
		conn:       conn,
		enc:        s.enc,
		maxCluster: clampClusterSize(s.c.MaxClusterSize),
	}
	// 方法值只取一次。s.callback 每求值一次就要在堆上造一个闭包（接收者要装进
	// 闭包的上下文字），而它此前写在 Wake 的实参位置上——也就是每一次 arm 分配
	// 一次。实测这是 Send 热路径上仅剩的两次分配之一。同 Conn.deliverFn 的处理。
	sd.wakeCb = sd.callback
	// 自己登记排空钩子。业务可能用 wrapper 把这个 sender 包起来，那样 Conn 就再也
	// 认不出内层是个内置 sender 了，见 Conn.flushHook。
	conn.flushHook = sd
	return sd
}

// clampClusterSize 把 MaxClusterSize 夹到协议能表达的范围内。
//
// compound body（= Σ(子 header + payload)）就是最终写进外层 header 长度字段的
// 那个数。把 MaxClusterSize 配到 32MB 附近时，两条 16MB 的消息加上子 header 正好
// 是 32MB+8：帧一发出去，对端 codec 立刻按 ErrMaxMessageSize 断链。构造时夹一次
// 比在热路径上反复判断便宜，也让"compound body 不会超限"成为一个结构性事实。
func clampClusterSize(n int) int {
	return min(n, maxMessageSize)
}

func NewSenderBuilder(c *SenderConfig) SenderBuilder {
	return &senderBuilder{c: c, enc: encoderFor(c.CompressLevel.zstd())}
}

type sender struct {
	c    *SenderConfig
	conn *Conn
	// maxCluster 是 MaxClusterSize 的生效值：<=0 表示不合并，>0 时已经被夹进
	// 协议上限之内（见 clampClusterSize）。
	maxCluster int
	// wakeCb 是 callback 的常驻方法值，构造时求一次，见 senderBuilder.Build。
	wakeCb gnet.AsyncCallback
	// enc 是这份配置对应的 zstd 编码器，按等级在进程内共享（见 encoderFor）。
	enc *zstd.Encoder

	mu sync.Mutex
	// queue 是待 flush 的消息，nil 表示当前没有借出池化队列。
	queue *[]sendMsg
	// queued 是 queue 里的消息占用的线路字节数（payload + header），用于给待发送
	// 队列封顶。计量单位必须含 header，否则空消息完全不消耗配额（见 frameSize）。
	queued int
	// wakeGen 给每一次 arm 编号，见下面的「唤醒不变式」。
	//
	// 它排在两个 bool 前面是为了布局：uint64 要 8 字节对齐，夹在 bool 中间会
	// 前后各垫一段 padding，结构体从 80 涨到 88。
	wakeGen uint64
	// triggered 表示已经 arm 了一次唤醒、且它的 callback 还没跑到 take()。
	triggered bool
	closed    bool
}

/*
唤醒不变式（send / callback / take / flush 四者共同维护）

	triggered  == true 表示"已经 arm 了一次唤醒，它的 callback 还没跑到 take()"。
	wakeGen    给每一次 arm 编号，用来分辨"我 arm 的那一次"和"别人后来 arm 的那一次"。

规则：

 1. arm 只发生在 send() 里，且只在 triggered == false 时发生：
    triggered = true; wakeGen++。因此同一时刻至多有一次 arm 生效。

 2. disarm 只有两条路径：
    a) take()（由 Wake 的 callback 或 flush 调用）无条件复位 triggered——
    callback 已经跑到了，这一代的使命结束；
    b) send() 里 Wake 返回错误时的回滚，且必须带上自己的 wakeGen（见 disarm）。
    2b 不带 generation 就是个 bug：Wake 出错之前 callback 可能已经跑完、
    并且已经有更新的一代 arm 上了，无条件复位会把那一代的 triggered 抹掉，
    于是同一时刻出现多次唤醒在路上。

 3. gnet 的 poller.Trigger 是"先把 task 入队，再写 eventfd 唤醒 poller"
    （v2.9.7 Poller.Trigger），返回错误只说明唤醒信号可能没送达，不说明
    callback 不会跑。所以 2b 只保证"不会有 arm 泄漏导致 sender 永久熄火"，
    不保证 callback 不会来。多来一次 callback 是安全的：take() 会发现队列
    是空的然后直接返回。

 4. 反过来也不能假设 callback 一定会来。Wake 失败且队列非空时，这批消息已经
    没有任何人负责 flush，此时直接关连接：后续 Send 返回 ErrConnClosed，
    业务层至少能观测到，而不是静默滞留到天荒地老。

 5. flush() 在 WebSocket 握手完成时从事件循环同步调用 callback(nil, nil)。
    它走的是 2a，与 Wake 送来的 callback 完全同构，因此不会破坏 generation 状态。

 6. 连接还停在 stateHandshaking 时不 arm：这一代唤醒醒来也只会在 take() 里
    原路返回（连接还不能写），白跑一次 OnTraffic 和一次 poller task。而 take()
    是无条件复位 triggered 的，所以握手期间"每条 Send 都重新 arm 一次"——
    业务在 OnOpen/Build 阶段发 N 条消息就是 N 次空唤醒。

    不 arm 而不丢消息，靠的是下面这个交接：writable() 的读取和入队在同一个
    s.mu 临界区内，而 markOpen() 严格早于 notifyReady() 里的 flush()。
    按 s.mu 的获取顺序分两种情况：

      - send 先于 flush 的 take：消息已经在队列里，take 一定看得见；此时
        markOpen 已经发生，take 的 writable() 为真，于是被排空。
      - take 先于 send：那 markOpen 也已经发生（它在 flush 之前），所以 send
        在临界区里读到的 writable() 为真，走正常的 arm 路径。

    两种情况都有人负责，不存在"既没 arm 也没人 flush"的窗口。
    连接已经关闭（isClosed）时仍然 arm：那一代 callback 负责把队列排空归还。
*/

// callback 是 gnet Wake 的回调，跑在连接所属的事件循环 goroutine 上。
func (s *sender) callback(_ gnet.Conn, e error) (_ error) {
	q, flush := s.take(e)
	if q == nil {
		return
	}
	defer func() {
		releaseSendMsgs(*q)
		putQ(q)
	}()
	if !flush {
		log.Warnf("[gate] %s: dropping %d queued message(s), connection is gone: %v",
			s.conn.Remote(), len(*q), e)
		return
	}
	s.pushTcp(*q)
	return
}

// take 复位 triggered（唤醒不变式 2a）并取走待发送队列。
//
// 返回 q == nil 表示无事可做；flush == false 表示队列必须被排空归还（连接已经
// 没了），但不能再往 socket 上写。
func (s *sender) take(e error) (q *[]sendMsg, flush bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.triggered = false
	if s.closed || s.queue == nil || len(*s.queue) == 0 {
		return nil, false
	}
	// 连接还停在握手阶段（WebSocket）：消息留在队列里等下一次 Wake，
	// 而不是拿去撞 writeOutbound 的 writable 闸口白白丢掉。triggered 已经复位，
	// 所以后续任何一次 send 都能重新把它们捞出来。
	if !s.conn.writable() && !s.conn.isClosed() {
		return nil, false
	}
	q, s.queue, s.queued = s.queue, nil, 0
	return q, e == nil && !s.conn.isClosed()
}

// disarm 回滚一次失败的 arm（唤醒不变式 2b），返回仍然滞留在队列里、且已经没有
// 任何唤醒负责它们的消息条数；0 表示这次失败不影响任何东西。
func (s *sender) disarm(gen uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wakeGen != gen || !s.triggered {
		// callback 已经跑过（take 复位了 triggered），或者更新的一代已经接管。
		// 无论哪种，队列都有人负责，这次失败不该动任何状态。
		return 0
	}
	s.triggered = false
	if s.closed || s.queue == nil {
		return 0
	}
	return len(*s.queue)
}

func (s *sender) pushTcp(buf []sendMsg) {
	config := s.c
	if s.maxCluster <= 0 {
		// 不开启合并
		s.pushSeparate(buf)
		return
	}
	// 开启合并
	var (
		i, j, l = 0, 0, len(buf)
	)
	for i < l {
		if config.MaxBufferSize > 0 && s.conn.outboundBuffered() >= config.MaxBufferSize {
			s.closeBlocked(l - i)
			return
		}
		// push* 返回 false 表示它已经因为超限或缓冲打满关掉了连接。必须立刻停手：
		// 连接的关闭在 gnet 里是异步的，continue 下去只会把后面的消息继续写进
		// 一个已经判死的连接，并让"缓冲打满"的日志刷屏。
		if !buf[i].compoundable() {
			for j = i + 1; j < l && !buf[j].compoundable(); j++ {
			}
			if !s.pushSeparate(buf[i:j]) {
				return
			}
		} else {
			// 合并永远做，压缩只在阈值开启且这一组真的够大时才做。
			var size int
			if j, size = s.cluster(buf, i); j-i == 1 {
				if !s.pushSeparate(buf[i:j]) {
					return
				}
			} else if !s.pushCompound(buf[i:j], size, config.CompressThreshold > 0 && size >= config.CompressThreshold) {
				return
			}
		}
		i = j
	}
}

// cluster 从 buf[i] 起向后收集可合并的消息，返回结束下标 j（不含）以及
// buf[i:j] 编码成 compound body 之后的真实字节数。
//
// size 是 Σ frameSize(len(data))，也就是子 header 也算进去的那个数——它既是
// 和 MaxClusterSize / CompressThreshold 比较的口径，也是最终写进外层 header
// 长度字段的值，两者必须是同一个数。只按 payload 长度累加会漏掉每条 2~4 字节的
// 子 header：极端情况下（大量空消息）body 全是 header，size 却恒为 0，于是一组
// 可以无限长下去。
//
// size 只统计真正进组的消息。此前累加写在 break 判断之前，循环退出时 size 里
// 混进了那条被排除在外的消息：2×100 字节后面跟一条 5000 字节的不可合并消息，
// 一个实际只有 200 字节的组会拿着 5200 去和 CompressThreshold 比较，于是白白
// 压了一遍。
func (s *sender) cluster(buf []sendMsg, i int) (j, size int) {
	size = frameSize(len(buf[i].data))
	for j = i + 1; j < len(buf); j++ {
		if !buf[j].compoundable() {
			break
		}
		next := size + frameSize(len(buf[j].data))
		if next > s.maxCluster {
			break
		}
		size = next
	}
	return
}

// pushCompound 把 buf 合成一个 compound 帧写出去。
//
// 返回 false 表示这一组没有发出去、连接已经被关掉，调用方必须停止后续投递。
// body 必须是 Σ frameSize(len(data))，也就是 cluster 返回的那个 size：它既是
// compound body 的真实字节数，也是最终写进外层 header 长度字段的值。调用方已经
// 在分组时算过一遍，这里直接接过来而不是再遍历一次——64 条消息的组每次 flush
// 都要白走一趟 O(N)。
func (s *sender) pushCompound(buf []sendMsg, body int, compress bool) bool {
	// cluster + clampClusterSize 已经把 body 夹在 maxMessageSize 以内，这里是
	// 兜底：万一还有别的路径构造出超限的一组，拆成独立帧照样能发出去，而不是把
	// 一个对端必然拒收的帧丢上线路。先判后建，超限时连那块 body 缓冲都不用分配。
	if body > maxMessageSize {
		log.Warnf("[gate] %s: compound body %d exceeds the %d-byte protocol limit, falling back to separate frames",
			s.conn.Remote(), body, maxMessageSize)
		return s.pushSeparate(buf)
	}

	var (
		b      = getBuffer()
		header [4]byte
		flag   = maskC
		data   []byte
	)
	defer putBuffer(b)

	// 先占住 4 字节的外层 header 位置，压不压缩都能就地写头，省掉一次搬运。
	b.Grow(4 + body)
	b.Write(header[:4])
	for _, msg := range buf {
		n := encodeHeader(header[:], len(msg.data))
		b.Write(header[:n])
		b.Write(msg.data)
	}

	// 1. compress
	if compress {
		// pooled 必须在 EncodeAll 之前被 defer 捕获：不可压缩的输入会让 EncodeAll
		// 超出源长度并重新分配，而此前 defer 写在赋值之后、捕获的是返回值——
		// 原始的池化内存永远回不去，返回值的容量又多半不是 2 的幂，
		// mcache.Free 对它只是静默 no-op。
		pooled := mcache.Malloc(4, 4+body)
		defer mcache.Free(pooled)
		// 压完反而更大就丢掉压缩结果：既省掉对端一次无谓的解压，也保证一个刚好
		// 贴着上限的 body 不会因为 zstd 的膨胀而越过协议上限。
		if out := s.enc.EncodeAll(b.Bytes()[4:], pooled); len(out) < b.Len() {
			data = out
			flag |= maskZ
		}
	}
	if data == nil {
		data = b.Bytes()
	}
	// 2. encrypt（就地进行，不改变长度）
	if cipher := s.conn.getCipher(); cipher != nil {
		cipher.Encrypt(data[4:])
		flag |= maskE
	}
	// 3. write header
	size := len(data) - 4
	if size > maxMessageSize {
		// 走不到：body 已经过闸，压缩只会更小，加密不改长度。真到了这里说明上面
		// 的不变式破了，宁可关连接也不能写一个长度会溢出 header 的帧——那会把
		// 长度和 m/z/c/e 标记同时改错，收发两端都察觉不到。
		log.Errorf("[gate] %s: refusing to emit a %d-byte frame (limit %d), closing connection",
			s.conn.Remote(), size, maxMessageSize)
		s.conn.Close()
		return false
	}
	if size < moreHeaderSize {
		binary.BigEndian.PutUint16(data[2:4], uint16(size))
		data = data[2:]
	} else {
		binary.BigEndian.PutUint32(data[0:4], uint32(size))
		flag |= maskM
	}
	data[0] |= flag
	// 4. 预算：整组的真实字节数必须放得进剩余额度。只看"当前缓冲有没有超"是不够的，
	//    那样最后一组永远能把出站缓冲顶过 MaxBufferSize，而且因为后面没有别的组了，
	//    blocked 路径也永远不会触发。
	if s.c.MaxBufferSize > 0 && s.conn.outboundBuffered()+len(data) > s.c.MaxBufferSize {
		s.closeBlocked(len(buf))
		return false
	}
	// 5. send
	if err := s.conn.writeOutbound(data); err != nil {
		s.writeFailed(err)
		return false
	}
	return true
}

// pushSeparate 把 buf 里的消息逐条写成独立帧。
//
// 返回 false 表示这一批没有全部写出去、连接已经被关掉，调用方必须停止后续投递。
func (s *sender) pushSeparate(buf []sendMsg) bool {
	if len(buf) == 0 {
		return true
	}
	// vb/vc/headers 借自池而不是每次 flush 重新分配：此前每次 flush 固定两次
	// 分配、1920 字节（vb 一次，headers 因为要跟着 vb 进 writevOutbound 而逃逸
	// 一次）。它们的生命周期严格限制在这次调用内，而 pushSeparate 只在连接自己
	// 的事件循环上跑且中途不会让出，所以池里同时在外的对象数就等于事件循环数，
	// 不会随连接数增长——这一点比挂成 sender 的常驻字段重要：那样每条连接都要
	// 长期扛着 1.6KB，十万连接就是 160MB。
	sc := getSeparateScratch()
	defer putSeparateScratch(sc)

	var (
		rest = math.MaxInt
		sent int
	)
	for sub := range slices.Chunk(buf, separateChunk) {
		if s.c.MaxBufferSize > 0 {
			if rest = s.c.MaxBufferSize - s.conn.outboundBuffered(); rest <= 0 {
				s.closeBlocked(len(buf) - sent)
				return false
			}
		}
		vb, vc := sc.vb[:0], sc.vc[:0]
		blocked, oversized := false, false

		for i := range sub {
			var (
				msg  = sub[i]
				data = msg.data
				flag = msg.maskAlready
			)
			// 1. compress
			if msg.maskPermit&maskZ != 0 && s.c.CompressThreshold > 0 && len(data) > s.c.CompressThreshold {
				// 同 pushCompound：释放的必须是这块原始的池化内存，
				// 而不是 EncodeAll 可能重新分配出来的返回值。
				pooled := mcache.Malloc(0, len(data))
				vc = append(vc, pooled)
				// 压完反而更大就用原文：省掉对端一次无谓的解压，也保证一条刚好
				// 贴着上限的消息不会因为 zstd 的膨胀而越过协议上限。
				if out := s.enc.EncodeAll(data, pooled); len(out) < len(data) {
					data, flag = out, flag|maskZ
				}
			}
			// 2. encrypt（就地进行，不改变长度）
			if cipher := s.conn.getCipher(); cipher != nil && msg.maskPermit&maskE != 0 {
				cipher.Encrypt(data)
				flag |= maskE
			}
			// 3. 协议上限。send() 已经挡住了业务侧的超长消息，压缩也只会更小，
			//    所以这里只可能来自 gate 内部构造。真的越界了就不能写出去：对端
			//    codec 会立刻断链，而长度一旦到 1<<28 更会溢进 header 的标记位。
			if len(data) > maxMessageSize {
				log.Errorf("[gate] %s: refusing to emit a %d-byte frame (limit %d), closing connection",
					s.conn.Remote(), len(data), maxMessageSize)
				oversized = true
				break
			}
			// 4. 预算。必须判断这条消息"放不放得下"，而不是只判断额度有没有用完：
			//    后者会让每个 chunk 的最后一条消息合法地把出站缓冲顶过
			//    MaxBufferSize（1024 的额度、已用 900，一条 200 字节的消息照样
			//    整条写进去），而且因为它是最后一条，blocked 路径永远不会触发，
			//    连接也就不会被关掉——预算等于形同虚设。
			header := sc.headers[i*4 : i*4+4 : i*4+4]
			n := encodeHeader(header, len(data))
			if n+len(data) > rest {
				blocked = true
				break
			}
			header[0] |= flag
			vb = append(vb, header[:n], data)
			rest -= n + len(data)
			sent++
		}
		var werr error
		if len(vb) > 0 {
			werr = s.conn.writevOutbound(vb)
		}
		for _, c := range vc {
			mcache.Free(c)
		}
		// append 可能重新分配过底层数组，把增长后的那块留在池里。
		sc.vb, sc.vc = vb, vc
		switch {
		case werr != nil:
			// 归还压缩暂存区之后再处理，免得提前 return 漏掉 mcache.Free。
			s.writeFailed(werr)
			return false
		case oversized:
			s.conn.Close()
			return false
		case blocked:
			s.closeBlocked(len(buf) - sent)
			return false
		}
	}
	return true
}

// closeBlocked 在出站缓冲打满时关闭连接。
//
// 此前这里是 log.Warnf + 继续，也就是从一条有状态的帧流中间把消息丢掉：对端的
// 状态机会静默错乱，业务层永远收不到任何信号。关掉连接是更诚实的失败方式——
// 对端立刻知道会话没了，重连即可恢复。
func (s *sender) closeBlocked(unsent int) {
	log.Warnf("[gate] %s: outbound buffer exceeded %d bytes, closing connection (%d message(s) unsent)",
		s.conn.Remote(), s.c.MaxBufferSize, unsent)
	s.conn.Close()
}

func (s *sender) send(data []byte, maskP, maskA byte, static bool) error {
	if len(data) > maxMessageSize {
		return ErrMaxMessageSize
	}
	// 连接已经关闭时立刻失败：此前这里永远返回 nil，业务层拿不到任何断线信号，
	// 消息还会无限堆进一个再也不会被 flush 的队列。
	if s.conn.isClosed() {
		return ErrConnClosed
	}

	// 计量必须含 header：按 len(data) 记账时空消息不消耗任何配额，
	// Send([]byte{}) 循环可以在 MaxBufferSize 已设的情况下把队列涨到 OOM。
	cost := frameSize(len(data))
	// 这条消息本身就大过整个配额时立刻拒绝，不要先复制一份再扔掉。
	// 队列此刻是空是满都不影响结论，所以这个判断不需要锁。默认配置下
	// （MaxBufferSize 2MB）一条 32MB 的合法协议消息此前会先从 mcache 取 32MB、
	// 完整拷一遍、拿到锁发现永远放不下、再原样归还——纯粹烧内存带宽。
	if s.c.MaxBufferSize > 0 && cost > s.c.MaxBufferSize {
		// 再看一眼连接状态，保持和加锁路径一样的错误优先级：那条路径先判
		// s.closed 再判配额，所以一条"既超配额、连接又刚好关掉"的消息应该报
		// ErrConnClosed。业务通常按 ErrConnClosed 决定要不要重连。
		if s.conn.isClosed() {
			return ErrConnClosed
		}
		return ErrSendQueueFull
	}

	msg := sendMsg{
		maskPermit:  maskP,
		maskAlready: maskA,
		data:        data,
	}
	// 拷贝放在临界区之外：mcache.Malloc + copy 不该把其他 producer 挡在锁上。
	if !static && len(data) > 0 {
		msg.data = cloneSendData(data)
		msg.owned = true
	}

	s.mu.Lock()
	switch {
	case s.closed:
		s.mu.Unlock()
		releaseSendMsg(msg)
		return ErrConnClosed
	case s.c.MaxBufferSize > 0 && s.queued+cost > s.c.MaxBufferSize:
		// 出站缓冲的上限此前只在 flush 时才看，于是一条已经死掉、但还没被事件
		// 循环回收的连接可以把 queue 撑到任意大小。
		s.mu.Unlock()
		releaseSendMsg(msg)
		return ErrSendQueueFull
	}
	if s.queue == nil {
		s.queue = getQ()
	}
	*s.queue = append(*s.queue, msg)
	s.queued += cost
	// arm，唤醒不变式 1 和 6。
	var gen uint64
	wake := !s.triggered && (s.conn.writable() || s.conn.isClosed())
	if wake {
		s.triggered = true
		s.wakeGen++
		gen = s.wakeGen
	}
	s.mu.Unlock()

	if !wake {
		return nil
	}
	// Wake 是一次 poller trigger（外加 logErr 的格式化），必须在锁外做：
	// 放在临界区里会把所有 producer goroutine 串在同一次系统调用上。
	if err := s.conn.conn.Wake(s.wakeCb); err != nil {
		// gnet 的 Trigger 先入队 task 再写 eventfd，出错只代表唤醒信号可能没送达，
		// 不代表 callback 不会跑（唤醒不变式 3）。所以这里只回滚"本代"的 arm：
		// 无条件复位会抹掉一个更新的、正在生效的 arm。
		if stranded := s.disarm(gen); stranded > 0 {
			// 队列里还有消息，而已经没有任何唤醒负责它们了。与其让它们无声无息地
			// 烂在队列里（业务层永远收不到信号），不如关连接：后续 Send 会返回
			// ErrConnClosed，OnClose 也会把队列排空归还（唤醒不变式 4）。
			log.Errorf("[gate] %s: wake failed with %d message(s) queued and no wake left to flush them, closing connection: %v",
				s.conn.Remote(), stranded, err)
			s.conn.Close()
		} else {
			logErr(err)
		}
	}
	return nil
}

// Close 标记 sender 已关闭并排空待发送队列，把 owned 消息的池化内存还回去。
// 幂等，且可以安全地从事件循环里调用（gate 在 OnClose 中调用它）。
func (s *sender) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	q := s.queue
	s.queue, s.queued = nil, 0
	s.mu.Unlock()

	if q == nil {
		return
	}
	releaseSendMsgs(*q)
	putQ(q)
}

// flush 立即尝试把队列里的消息写出去，由 gate 在连接变为可写时调用。
//
// take 在连接还停在 stateHandshaking（WebSocket 握手中）时会把消息留在队列里
// 并复位 triggered，这样它们不会撞上 writeOutbound 的闸口被白白丢掉。但复位
// triggered 也意味着没有任何 Wake 在路上了：握手完成后如果业务不再调用 Send，
// 这批消息就会一直滞留。所以握手完成时必须主动踢一脚。
//
// 只在事件循环 goroutine 上调用（notifyReady 的调用点满足这一条），此时直接
// 走 callback 同步 flush 是安全的：它复位 triggered 的方式和 Wake 送来的
// callback 完全一样，因此不会打乱 wakeGen 的配对（唤醒不变式 5）。
func (s *sender) flush() {
	_ = s.callback(nil, nil)
}

func (s *sender) Send(data []byte) error {
	return s.send(data, maskZ|maskC|maskE, 0, false)
}

func (s *sender) SendNoEncrypt(data []byte) error {
	return s.send(data, 0, 0, false)
}

func (s *sender) SendStatic(data []byte, alreadyCompressed bool) error {
	if alreadyCompressed {
		return s.send(data, 0, maskZ, true)
	}
	return s.send(data, 0, 0, true)
}

// separateScratch 是 pushSeparate 一次调用期间需要的全部暂存空间。
//
// headers 是整个 chunk 共用的 header 暂存区：此前每条消息都在循环体里声明一个
// [4]byte 再切进 vb，于是每条消息都要单独逃逸一次。gnet.Writev 会改写传进去的
// bs[i] 切片头，所以每个元素都必须一直有效到 writev 返回——按 chunk 划分正好
// 满足这个要求。
type separateScratch struct {
	vb      [][]byte
	vc      [][]byte
	headers [separateChunk * 4]byte
}

// 池里存 *separateScratch：指针可以直接放进 interface 的数据字，
// 存值会在每次 Put 上装箱分配一次（staticcheck SA6002）。
var separateScratchPool = sync.Pool{New: func() interface{} {
	return &separateScratch{vb: make([][]byte, 0, 2*separateChunk)}
}}

func getSeparateScratch() *separateScratch {
	return separateScratchPool.Get().(*separateScratch)
}

func putSeparateScratch(sc *separateScratch) {
	// 必须清空再归还：vb 里装的是 mcache 借来的 payload，vc 里装的是压缩暂存区，
	// 两者在 pushSeparate 返回之后都会被归还并复用。把它们留在池对象里等于让一块
	// 已经还回去的内存被一个存活到下一次 flush 的对象继续引用着。
	//
	// 清到 cap 而不是 len：pushSeparate 按 chunk 循环，每个 chunk 都从 [:0] 重新
	// 填，归还时 sc.vb 的长度只是**最后一个** chunk 的。一次 33 条消息的 flush，
	// chunk1 写满 64 项、chunk2 只写 2 项，clear(sc.vb) 就只清掉 2 项，索引 2..63
	// 上还挂着 chunk1 那批已经还给 mcache 的 payload。
	clear(sc.vb[:cap(sc.vb)])
	clear(sc.vc[:cap(sc.vc)])
	sc.vb, sc.vc = sc.vb[:0], sc.vc[:0]
	separateScratchPool.Put(sc)
}

// queuePool 复用 sender buffer queue。
//
// 池里存的是 *[]sendMsg 而不是 []sendMsg：把切片直接塞进 interface{} 会在每次
// Put 上分配一次（staticcheck SA6002），而指针可以直接放进 interface 的数据字。
var queuePool = sync.Pool{New: func() interface{} {
	q := make([]sendMsg, 0, 8)
	return &q
}}

func getQ() *[]sendMsg {
	return queuePool.Get().(*[]sendMsg)
}

func putQ(q *[]sendMsg) {
	if q == nil || cap(*q) > 1024 {
		return
	}
	clear(*q)
	*q = (*q)[:0]
	queuePool.Put(q)
}

func cloneSendData(data []byte) []byte {
	cloned := mcache.Malloc(len(data))
	copy(cloned, data)
	return cloned
}

func releaseSendMsg(msg sendMsg) {
	if msg.owned {
		mcache.Free(msg.data)
	}
}

func releaseSendMsgs(q []sendMsg) {
	for _, msg := range q {
		releaseSendMsg(msg)
	}
}

// writeFailed 处理一次失败的出站写。调用方在它之后必须立刻停止投递。
//
// 写失败意味着这一帧（或它的一部分）没能上线路，帧流从此有个洞。继续把后面的
// 消息写进去，只会让对端在一个错位的流上继续解析——这正是 closeBlocked 上方
// 已经明确拒绝过的失败方式："从一条有状态的帧流中间抽掉几条，只会让对端的
// 状态机静默错乱，而业务层什么都察觉不到"。出站缓冲打满走的是关连接，一次真
// 正失败的写没有理由走另一套。
//
// 此前这里只是 writeErr 记一条日志然后接着写下一个 chunk：一次失败的 writev
// 之后，剩下的 chunk 会一条不落地继续灌进那个已经错位的流里。
//
// 连接已经关闭是一个正常竞态（业务可以从任意 goroutine 发送），不必再关一次，
// 也不值得按 error 级别刷屏。
func (s *sender) writeFailed(err error) {
	if errors.Is(err, ErrConnClosed) {
		return
	}
	log.Errorf("[gate] %s: outbound write failed, closing connection: %v", s.conn.Remote(), err)
	// 先把 sender 自己置成关闭，再去关连接。
	//
	// 只关连接是不够的：Conn.Close() 是异步的，它排一个 task 到事件循环，此刻
	// state 还是 open，而 writable() 刻意不看 closing（那是为了让"回一条拒绝消息
	// 再关连接"仍然能把消息刷出去）。与此同时 callback 一进来就在 take() 里复位了
	// triggered，所以并发的 Send 可以立刻重新入队并再排一次 Wake——那次 Wake 如果
	// 排在关闭 task 前面，就会往这条已经有洞的帧流里继续写。
	//
	// Close() 把 s.closed 置上之后，后续 send 直接返回 ErrConnClosed，take() 也
	// 一律返回 nil，这条流就真的停住了。它是幂等的，OnClose 之后还会再调一次。
	s.Close()
	s.conn.Close()
}

// byteBufferPool 复用 bytesBuffer
var byteBufferPool = sync.Pool{New: func() interface{} {
	return bytes.NewBuffer(make([]byte, 0, 2*1024))
}}

const maxReusableBufferCap = 256 * 1024

func getBuffer() *bytes.Buffer {
	b := byteBufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putBuffer(b *bytes.Buffer) {
	if b.Cap() > maxReusableBufferCap {
		return
	}
	b.Reset()
	byteBufferPool.Put(b)
}

// CompressLevel 选择出站压缩的 zstd 等级。
//
// 它同时是一个内存开关，而且量级很大：zstd 在第一次压缩时会按并发度造出对应
// 数量的 encoder，每个 encoder 自带一整套固定大小的哈希表。10 核机器实测
// （首次 EncodeAll 之后的堆增量 / 并发度）：
//
//	CompressFastest   约 0.27 MB/encoder
//	CompressBalanced  约 1.32 MB/encoder
//	CompressBetter    约 4.27 MB/encoder   <- 零值，gate 的历史默认
//
// 同一份"游戏协议 batch"样本上，Fastest 比 Better 快 24% 且压缩率一样
// （见 BenchmarkZstdLevel）。真实数据上 Better 通常确实更小，所以默认值保持不变，
// 但如果你的负载对 CPU 或常驻内存敏感，先试 Fastest。
type CompressLevel uint8

const (
	// CompressBetter 是零值，等价于 zstd.SpeedBetterCompression。
	CompressBetter CompressLevel = iota
	CompressFastest
	CompressBalanced
	CompressBest
)

func (l CompressLevel) zstd() zstd.EncoderLevel {
	switch l {
	case CompressFastest:
		return zstd.SpeedFastest
	case CompressBalanced:
		return zstd.SpeedDefault
	case CompressBest:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedBetterCompression
	}
}

var (
	encoderMu    sync.Mutex
	encoderCache = map[zstd.EncoderLevel]*zstd.Encoder{}
)

// encoderFor 返回某个等级共用的编码器。
//
// 按等级共享而不是每个 builder 一个：encoder 池是这个库里最大的一块常驻内存，
// 两个配置相同的 SenderBuilder 没有理由各自扛一份。zstd 自己是懒初始化的
// （第一次 EncodeAll 才真正造出 encoder），所以从不压缩的 gate 一分钱都不用付。
func encoderFor(level zstd.EncoderLevel) *zstd.Encoder {
	encoderMu.Lock()
	defer encoderMu.Unlock()
	if e, ok := encoderCache[level]; ok {
		return e
	}
	// EncodeAll 的实现是 `enc := <-e.encoders`，而那个 channel 的容量就等于
	// concurrency：写死成 1 会让所有 event loop 抢同一个 encoder，压缩直接退化
	// 成全局串行。10 核 M5 上并行压 8KB：concurrency=1 约 2650 ns/op，
	// concurrency=GOMAXPROCS 约 840 ns/op。
	//
	// 取 GOMAXPROCS 而不是 NumCPU：后者读的是宿主机的物理核数，完全无视 cgroup
	// 的 CPU quota。一个限额 2 核、跑在 64 核宿主上的容器，用 NumCPU 会在第一次
	// 压缩时造出 64 个 encoder——按上面的实测，Better 等级下就是约 275MB 常驻，
	// 而它永远也用不到 2 个以上。Go 1.25 的 GOMAXPROCS 本身已经是 cgroup 感知的，
	// 正好是"运行时真能并行跑多少个 P"这个我们要的数。
	e, _ := zstd.NewWriter(nil, // 只传合法选项，不会失败
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	encoderCache[level] = e
	return e
}

func init() {
	enc = encoderFor(CompressBetter.zstd())
}
