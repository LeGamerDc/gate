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

func (c *Conn) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, c.lastActive.Load()))
}

func (c *Conn) markOpen() {
	c.state.CompareAndSwap(uint32(stateHandshaking), uint32(stateOpen))
}

func (c *Conn) markClosed() {
	c.state.Store(uint32(stateClosed))
}

func (c *Conn) isClosed() bool {
	return connState(c.state.Load()) == stateClosed
}

// writable 报告现在是否允许向底层 socket 写。所有出站路径的唯一闸口。
func (c *Conn) writable() bool {
	return connState(c.state.Load()) == stateOpen
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
func (c *Conn) onTraffic() gnet.Action {
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
	return action
}

// deliverOne 把一条业务消息交给 handler，并隔离 handler 的 panic。
//
// gnet 核心没有任何 recover，在此之前一条能让业务解码 panic 的畸形消息会打死
// 整个进程；现在爆炸半径收敛到单条连接。
func (c *Conn) deliverOne(msg []byte, _ bool) (err error) {
	// 活跃时间在这里刷新，而不是在 onTraffic 入口。
	//
	// 两个原因：gnet 的 Wake 会先跑一遍 OnTraffic 再执行发送回调，所以在入口
	// touch 会让"服务端定期下推、客户端一言不发"的连接永远不被判定为空闲；
	// 另外只在真正交付了一条完整消息时才算活跃，顺带让只发半个帧吊着连接的
	// slowloris 也会被 IdleTimeout 收走。两种传输共用这一个交付点，语义一致。
	c.touch()

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
func (c *Conn) wake() {
	if c.conn == nil {
		return
	}
	logErr(c.conn.Wake(nil))
}

func (c *Conn) Close() {
	if c.conn == nil {
		c.markClosed()
		return
	}
	logErr(c.conn.Close())
}

func logErr(err error) {
	if err != nil {
		log.Errorf("[gate] %v", err)
	}
}
