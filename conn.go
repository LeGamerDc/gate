package gate

import (
	"errors"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
)

const (
	maxMessageSize = 32 * 1024 * 1024
)

var errHandlerPanic = errors.New("gate: handler panicked")

// connState 是连接的生命周期状态。
//
// 引入它是为了堵住一类结构性缺陷：sender 持有 *Conn -> gnet.Conn -> fd，
// 而 gnet 关闭连接后会 close(fd)，该 fd 号可能立刻被新连接复用；gnet 的 Wake
// 对陈旧连接返回 nil error，于是发送回调会误以为一切正常，继续往一个已经属于
// 别人的 fd 上写数据。现在所有出站写都必须先过 writable()。
type connState uint32

const (
	// stateHandshaking 传输层尚未就绪（WebSocket 握手中）。此时不允许发送。
	stateHandshaking connState = iota
	// stateOpen 连接可读可写。
	stateOpen
	// stateClosed 连接已关闭，底层 fd 可能已被回收，任何写都必须被拒绝。
	stateClosed
)

// Handshake 保存 WebSocket 握手请求的信息，供业务层做鉴权或取真实来源 IP。
// TCP 传输下 Conn.Handshake() 返回 nil。
type Handshake struct {
	// URI 是握手请求行里的完整 request-URI（含 query）。
	URI string
	// Header 是握手请求的 HTTP 头。
	Header http.Header
}

type Conn struct {
	conn gnet.Conn

	remoteIp   net.IP
	remotePort int
	outbound   outboundWriter
	sender     SenderI
	handler    ConnHandler
	codec      codec

	// deliverFn 是 codec sink 的常驻实例。直接传 c.deliverOne 会在每条消息上
	// 生成一次方法值闭包，这里改成每连接一次。
	deliverFn func([]byte, bool) error

	// flushHook 是内置 sender 在构造时自己登记的排空钩子，连接变为可写时调用。
	//
	// 它不能靠对 c.sender 做类型断言来拿：业务完全可以（也应该可以）用一个
	// wrapper 包住内置 sender 做埋点、限流之类的事，
	//
	//	type metricsSender struct{ gate.SenderI }
	//
	// 而 flush() 是包私有方法——嵌入 SenderI 的 wrapper 永远不会满足那个接口，
	// 断言必然失败。于是 WebSocket 握手期间入队的消息就再没有人负责排空，
	// 如果业务此后不再 Send，它们会一直滞留到连接关闭。
	// 让内层 sender 自己登记，包了几层都不影响。
	flushHook interface{ flush() }

	// sinceTouch 是自上次发布活跃时间以来又交付了多少条消息，见 markActive。
	// 它只在连接所属的事件循环 goroutine 上读写（deliverOne 和两种传输的
	// onTraffic 收尾处），所以不需要任何同步。
	sinceTouch uint8

	// closing 表示已经有人调用过 Conn.Close，但 gnet 的 OnClose 还没跑到。
	//
	// gnet 的 Close 是异步的（往事件循环排一个 task），所以在 OnClose 真正把
	// state 落成 stateClosed 之前有一段窗口。入站投递必须在这段窗口里就停下来，
	// 不能等到 state 变化，见 stopped。
	//
	// 位置紧挨着 sinceTouch 不是随手排的：它落在 sinceTouch 之后、指针字段之前
	// 的那段对齐 padding 里，所以 Conn 的大小没有变。放到结构体末尾会让
	// Conn 从 184 涨到 192 字节。
	closing atomic.Bool

	cipher    atomic.Pointer[Cipher]
	handshake atomic.Pointer[Handshake]

	state      atomic.Uint32
	blocking   atomic.Int32
	lastActive atomic.Int64
}

func (c *Conn) init() {
	c.deliverFn = c.deliverOne
	c.touch()
}

func (c *Conn) touch() {
	c.lastActive.Store(time.Now().UnixNano())
}

// activityRefreshMessages 是两次发布活跃时间之间最多能隔多少条消息。
//
// 它把"时间戳可以有多旧"这件事变成一个结构性上界：陈旧度不会超过连续
// activityRefreshMessages 条消息的处理时长，与批次本身有多大无关。
const activityRefreshMessages = 64

// markActive 发布活跃时间，每 activityRefreshMessages 条消息一次。
//
// 活跃时间此前是在 deliverOne 里每条消息刷新一次的，语义没问题，代价却很高：
// time.Now() 同时要取墙上时钟和单调时钟，实测 31ns，而整条 TCP 入站路径处理
// 一条消息本来只要 31ns（parse 2.5ns + handler 1.7ns + 其余）——也就是说，
// 一多半的入站 CPU 花在了给一个秒级精度的空闲判定打时间戳上。摊到 64 条上
// 之后是约 0.5ns/条。
//
// 为什么必须在**进入 handler 之前**发布，而不是等整批处理完：OnTick 的空闲扫描
// 跑在 gnet 自己的 ticker goroutine 上，和事件循环是并发的。批次结束才发布意味着
// 整个批次期间 lastActive 都停在上一批的时刻，一条正在处理消息的连接会被判成空闲
// 关掉。
//
// 为什么还需要 64 这个上界、"每批一次"不够：一次读事件能带回的消息数只受
// 缓冲区大小约束（WebSocket 下 MaxWebSocketBufferedBytes 默认 32MB），所以
// 一批里大量各自很短的 handler 累加起来照样可以超过 IdleTimeout——不需要谁
// 真的阻塞。有了这个上界，误判要求"连续 64 条消息的处理时间超过 IdleTimeout"，
// 那已经不是空闲判定该管的问题了。
func (c *Conn) markActive() {
	if c.sinceTouch == 0 {
		c.touch()
	}
	if c.sinceTouch++; c.sinceTouch >= activityRefreshMessages {
		c.sinceTouch = 0
	}
}

// endEvent 结束本轮事件的活跃记账，由两种传输的 onTraffic 在收尾时各调用一次。
// 复位之后，下一轮事件的第一条消息一定会重新发布时间戳。
func (c *Conn) endEvent() {
	c.sinceTouch = 0
}

func (c *Conn) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, c.lastActive.Load()))
}

// idleSince 报告这条连接的最后活跃时刻是否早于 cutoff（UnixNano）。
// 空闲扫描用它而不是 idleFor：调用方把截止时刻算一次，每条连接就只剩
// 一次原子读加一次整数比较，不必逐条构造 time.Time。
func (c *Conn) idleSince(cutoff int64) bool {
	return c.lastActive.Load() <= cutoff
}

func (c *Conn) markOpen() {
	c.state.CompareAndSwap(uint32(stateHandshaking), uint32(stateOpen))
}

func (c *Conn) markClosed() {
	// 顺带把 closing 也置上，这样 stopped() 只需要读一个字段就能同时覆盖
	// "有人调用过 Close" 和 "OnClose 已经跑过" 两种情况。入站投递循环每条消息
	// 都要问一次，省下来的那次原子读是实打实的（那条路径每条消息总共才 8ns）。
	c.closing.Store(true)
	c.state.Store(uint32(stateClosed))
}

func (c *Conn) isClosed() bool {
	return connState(c.state.Load()) == stateClosed
}

// writable 报告现在是否允许向底层 socket 写。所有出站路径的唯一闸口。
//
// 刻意不看 closing：调用 Close 之后、gnet 的关闭 task 真正执行之前，fd 还是活的，
// 此时把已经入队的消息刷出去是正确的（业务常见的用法就是"发一条拒绝消息然后关
// 连接"）。closing 只用来停止**入站**投递，见 stopped。
func (c *Conn) writable() bool {
	return connState(c.state.Load()) == stateOpen
}

// stopped 报告这条连接是否已经不该再接收新的入站消息：业务调用过 Close，
// 或者 OnClose 已经跑完。
//
// 两个条件都要看，因为 gnet 的 Close 是异步的：调用 Conn.Close() 只是往事件
// 循环排了一个 task，state 要等 OnClose 才会变成 stateClosed。而入站投递的
// 循环就跑在同一个事件循环上——不看 closing 的话，handler 在第一条消息上判定
// "这条连接非法、关掉"之后，同一个读事件里剩下的消息照样会被投递进去。
//
// 这是有安全含义的：一个 TCP 读事件（或一个 WebSocket 二进制消息）里可以塞进
// 多条 gate 消息，把 [伪造的登录包, 真正想执行的命令] 拼在一起发出来，命令就会
// 在一条已经被判死的连接上执行。
// 一次原子读就够：markClosed 会同时置上 closing，所以这一个字段已经覆盖了
// "调用过 Close" 和 "OnClose 已经跑过" 两种情况。
func (c *Conn) stopped() bool {
	return c.closing.Load()
}

// onTraffic 消费当前连接缓冲区里所有已完整到达的消息。
//
// 这里整段 Peek 一次、最后 Discard 一次，而不是每条消息一次 Peek 加一个 Discard 闭包。
// 原因是 gnet 的 Peek 在数据跨越 ring buffer 边界时会从 bsPool 取一块临时缓冲挂到
// c.cache 上，而 c.cache 只在 Discard 时归还，且会被后续 Peek 直接覆盖——原先解析
// 一条消息最多要调三次 Peek，等于每条消息白白丢掉两块池化缓冲，外加每条消息两个
// 闭包分配（clean 闭包 + 包裹 defer 的匿名函数）。
//
// 稳态下（对端发的都是完整帧）inboundBuffer 是空的，Peek 直接返回事件循环读缓冲的
// 切片，完全不碰池。只有上一次事件留下了半个帧时才会走到 bsPool 路径；此时如果这次
// 依然凑不齐一个完整帧，consumed 为 0、不调 Discard，那块临时缓冲就不会归还池里
// （由 GC 回收）。这是 gnet Peek/Discard 接口形状带来的固有取舍，但比原先"每条消息
// 都丢两块"要好得多。
// inboundBufferSlack 是入站缓冲在"一条最大消息"之外还允许富余多少。
//
// 取值和 WebSocket 侧的默认值一致（MaxWebSocketBufferedBytes 默认是
// MaxMessageSize + 64KB），让两种传输在这件事上给出同一个数量级的保证。
const inboundBufferSlack = 64 * 1024

func (c *Conn) onTraffic() gnet.Action {
	// 阻塞期间给入站缓冲封顶。
	//
	// AsyncDo 挂起时 gate 一个字节都不消费，而 gnet 会继续把新到的数据追加进
	// 连接的弹性入站缓冲。此前这里没有任何上限：一个在慢速 AsyncDo 期间持续灌
	// 数据的客户端，可以把单条连接的内存一路撑下去，而默认不开 IdleTimeout，
	// 连接也不会被回收——一个可远程触发的内存 DoS。
	//
	// 只在阻塞时检查。没阻塞的时候 gate 每轮事件都会把所有完整帧消费掉，缓冲区
	// 里剩下的只可能是半个帧，那本来就被 MaxMessageSize 封住了；把检查放到每轮
	// 事件上只会误杀"一次读事件恰好带回很多数据"的正常连接。
	if c.isBlocking() {
		if limit := c.codec.maxMessage + inboundBufferSlack; c.conn.InboundBuffered() > limit {
			log.Warnf("[gate] %s: inbound buffer exceeded %d bytes while blocked, closing connection",
				c.Remote(), limit)
			return gnet.Close
		}
	}

	buf, err := c.conn.Peek(-1)
	if err != nil || len(buf) == 0 {
		return gnet.None
	}

	var (
		consumed int
		action   = gnet.None
	)
	for consumed < len(buf) {
		if c.isBlocking() {
			break
		}
		// handler 在上一条消息里关掉了连接：这一批剩下的消息不能再投递。见 stopped。
		if c.stopped() {
			break
		}
		f, n, ok, perr := c.codec.parse(buf[consumed:])
		if perr != nil {
			log.Warnf("[gate] %s: inbound protocol error: %v", c.Remote(), perr)
			action = gnet.Close
			break
		}
		if !ok {
			// 数据不足以构成完整帧，剩余部分留在 gnet 缓冲区里等下一次事件。
			break
		}
		// Cipher 必须每帧重新取，不能在循环外取一次快照：握手协商类的 handler
		// 会在处理第一帧时调用 UpdateCipher，而同一个 TCP 读批次里往往紧跟着
		// 用新密钥加密的第二帧。取快照会让它用旧密钥解密（或直接 ErrCipherRequired）。
		if derr := c.codec.deliver(f, c.getCipher(), nil, c.deliverFn); derr != nil {
			if !errors.Is(derr, errHandlerPanic) {
				log.Warnf("[gate] %s: inbound decode error: %v", c.Remote(), derr)
			}
			action = gnet.Close
			break
		}
		consumed += n
	}

	if consumed > 0 {
		if _, e := c.conn.Discard(consumed); e != nil {
			logErr(e)
		}
	}
	// 直接调用而不是 defer：这里只有一个出口，而 defer 在只带一条消息的批次上
	// 就是纯开销（实测约 2ns，占这种批次总成本的 6%）。deliverOne 自己兜住了
	// handler 的 panic，走不到"必须靠 defer 才能收尾"的路径；万一真有别的东西
	// panic，下一轮事件的第一条消息会重新发布活跃时间，什么都不会丢。
	c.endEvent()
	return action
}

// deliverOne 把一条业务消息交给 handler，并隔离 handler 的 panic。
//
// gnet 核心没有任何 recover，在此之前一条能让业务解码 panic 的畸形消息会打死
// 整个进程；现在爆炸半径收敛到单条连接。
func (c *Conn) deliverOne(msg []byte, _ bool) (err error) {
	// 活跃标记打在这里，而不是打在 onTraffic 入口。
	//
	// 两个原因：gnet 的 Wake 会先跑一遍 OnTraffic 再执行发送回调，所以在入口
	// 标记会让"服务端定期下推、客户端一言不发"的连接永远不被判定为空闲；
	// 另外只有真正交付了一条完整消息才算活跃，顺带让只发半个帧吊着连接的
	// slowloris 也会被 IdleTimeout 收走。两种传输共用这一个交付点，语义一致。
	//
	// markActive 只在本轮事件的第一条消息上真的取一次时钟，后续消息就是一次
	// 非原子的 bool 读（见 Conn.delivered）。
	c.markActive()

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("[gate] %s: handler panic: %v\n%s", c.Remote(), r, debug.Stack())
			err = errHandlerPanic
		}
	}()
	c.handler.Handle(msg)
	return nil
}

func (c *Conn) isBlocking() bool {
	return c.blocking.Load() > 0
}

// AsyncDo 阻塞 connection 继续处理消息，直到最后一个挂起的 f 完成。
// 对于一些有限制串行的消息有用。阻塞期间数据仍保留在 gnet 的连接缓冲区中，不会丢失；
// 支持重入，只有最外层未完成的 AsyncDo 结束后才会通过 Wake 继续消费这些消息。
//
// 注意：AsyncDo 只应在 ConnHandler.Handle 内部调用。从其他 goroutine 调用时
// "单连接内串行"的保证不成立，因为事件循环可能已经越过了 isBlocking 检查。
func (c *Conn) AsyncDo(f func()) {
	if c.isClosed() {
		return
	}
	c.blocking.Add(1)
	go func() {
		// defer 是 LIFO：先跑 recover 兜住 f 的 panic，再跑唤醒，
		// 保证挂起计数一定归零，连接不会被永久卡在 blocking 状态。
		defer func() {
			if c.blocking.Add(-1) == 0 && !c.isClosed() {
				c.wake()
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("[gate] %s: AsyncDo panic: %v\n%s", c.Remote(), r, debug.Stack())
			}
		}()
		f()
	}()
}

// Send 复制 data 后入队，允许 gate 自动压缩、合包和加密。
// 连接已关闭时返回 ErrConnClosed。
func (c *Conn) Send(data []byte) error {
	return c.sender.Send(data)
}

// SendNoEncrypt 发送明文单帧消息，并在入队时复制 data。
func (c *Conn) SendNoEncrypt(data []byte) error {
	return c.sender.SendNoEncrypt(data)
}

// SendStatic 发送调用方持有的只读数据，gate 既不复制也不改写它：
// 不压缩、不合包、不加密。
//
// data 必须永久保持不可变。gate 没有 flush 完成回调，调用方无从得知何时可以
// 安全地复用这块内存，因此只有生命周期长于连接的数据（例如预编码的固定回包、
// 广播帧）才适合走这个接口。alreadyCompressed=true 时只挂压缩标记。
func (c *Conn) SendStatic(data []byte, alreadyCompressed bool) error {
	return c.sender.SendStatic(data, alreadyCompressed)
}

// UpdateCipher 替换后续收发使用的 Cipher，可以从任意 goroutine 调用。
//
// 之前这里是一次无同步的裸赋值，而读取发生在事件循环线程（解密/加密路径）；
// 最典型的用法恰好是"异步握手完成后换密钥"，也就是从 AsyncDo 的 goroutine 里调，
// 那就是一个确定的 data race。
func (c *Conn) UpdateCipher(cipher Cipher) {
	if cipher == nil {
		c.cipher.Store(nil)
		return
	}
	c.cipher.Store(&cipher)
}

func (c *Conn) getCipher() Cipher {
	if p := c.cipher.Load(); p != nil {
		return *p
	}
	return nil
}

// Handshake 返回 WebSocket 握手信息；TCP 传输或握手尚未完成时返回 nil。
func (c *Conn) Handshake() *Handshake {
	return c.handshake.Load()
}

func (c *Conn) setHandshake(h *Handshake) {
	c.handshake.Store(h)
}

func (c *Conn) RemoteIp() string {
	return c.remoteIp.String()
}

func (c *Conn) RemotePort() int {
	return c.remotePort
}

func (c *Conn) Remote() string {
	return c.remoteIp.String() + ":" + strconv.Itoa(c.remotePort)
}

// wake 唤醒事件循环继续消费该连接的入站数据。
// c.conn 为 nil 的情况只出现在 OnOpen 失败的清理路径和单元测试里。
// wake 唤醒事件循环继续消费该连接的入站数据。
//
// 连接已经关闭时直接返回，不去碰底层 fd，理由见 Close。
func (c *Conn) wake() {
	if c.conn == nil || c.isClosed() {
		return
	}
	logErr(c.conn.Wake(nil))
}

// Close 关闭连接。可以重复调用。
//
// 已经关掉之后就不再往下走。这一层是给业务持有陈旧 *Conn 这种用法兜底的：
// 连接关掉之后业务手里往往还留着引用（清理逻辑、重连判断），再调一次 Close
// 不该变成一次跨线程的 poller task。
//
// 需要说清楚的是，这**不是**在修一个 gnet 的缺陷：当前锁定的 gnet v2.9.7 在
// Unix 上会先检查旧对象自己的 opened 标记（eventloop_unix.go 的 close/wake 两处），
// Windows 的连接表干脆以 *conn 为 key，所以陈旧对象本来就碰不到被复用的 fd。
// 这里只是把判断提前到 gate 自己这一侧，不依赖对方的实现细节。
//
// 它也不消除全部竞态：从别的 goroutine 调 Close 时，仍可能在 OnClose 把状态落成
// closed 之前挤进来——那种情况下走的是正常关闭路径，本来就是对的。
func (c *Conn) Close() {
	if c.conn == nil {
		c.markClosed()
		return
	}
	if c.isClosed() {
		return
	}
	// 先就地记下"要关了"，再排 gnet 的异步关闭 task：入站投递循环靠这个标记
	// 立刻停手，不必等 OnClose，见 stopped。
	c.closing.Store(true)
	logErr(c.conn.Close())
}

func logErr(err error) {
	if err != nil {
		log.Errorf("[gate] %v", err)
	}
}
