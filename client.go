package gate

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	ErrClientClosed = errors.New("client closed")
	// ErrHandlerPanic ClientHandler 的回调 panic 了。
	//
	// 业务代码的 panic 不允许打死整个进程（server 侧的每个回调边界都有
	// safeCall，client 必须给出同样的保证），所以 OnMessage panic 会被捕获并
	// 转成这个错误关闭当前 client，由 Close/Wait/OnClose 报告给业务层。
	ErrHandlerPanic = errors.New("gate: client handler panicked")
)

// ClientConfig controls how a Client connects and processes messages.
type ClientConfig struct {
	Addr    string
	Handler ClientHandler
	Cipher  Cipher
	// MaxMessageSize 单条消息的字节上限，<=0 时使用默认值 32MB。
	// 超过 32MB 会被 purge 拒绝：32MB 是协议上限，见下方 purge 的注释。
	MaxMessageSize int
	SendQueueSize  int // default: 1024
}

func (c *ClientConfig) purge() error {
	if c.Addr == "" {
		return errors.New("client addr must set")
	}
	if c.Handler == nil {
		return errors.New("client handler must set")
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = maxMessageSize
	}
	// 与 Config.purge 一致：超限直接报错而不是悄悄夹取。
	// 头部长度字段只有 12+16=28 位，长度一旦达到 1<<28，encodeHeader 写出的
	// 长度位就会撞进 m/z/c/e 标记位，长度和标记会同时被写坏，而且对端只会看到
	// 一个"合法但内容错误"的帧。让调用方在启动时就知道配错了，比在线上发出
	// 一批服务端必然拒收的帧要好。
	if c.MaxMessageSize > maxMessageSize {
		return fmt.Errorf("max message size %d exceeds protocol ceiling %d", c.MaxMessageSize, maxMessageSize)
	}
	if c.SendQueueSize <= 0 {
		c.SendQueueSize = 1024
	}
	return nil
}

// ClientHandler receives connection lifecycle callbacks and decoded messages.
//
// 回调的 panic 保护与 server 侧一致，但 OnConnect 是个例外：
//   - OnMessage / OnClose 跑在 gate 自己的 goroutine 上，panic 会被捕获并记录。
//     OnMessage panic 还会以 ErrHandlerPanic 关闭该 client。
//   - OnConnect 跑在 DialContext 调用方的 goroutine 上，panic 会原样抛回给调用方
//     （吞成 error 会和"拨号失败"混为一谈，也会掩盖业务 bug）。抛出之前 gate 会
//     把连接关掉、后台 loop 停掉，不留下半构造的 client；此时 client 从未对外
//     可见，OnClose 不会被调用。
type ClientHandler interface {
	OnConnect(*Client)
	OnMessage(*Client, []byte)
	OnClose(*Client, error)
}

const (
	// clientHeaderReserve 是 send 在 payload 前面预留出来的字节数，取 header 的
	// 最大长度（4）。这样 writeLoop 可以就地把 header 写进这块空位，而不必再
	// 分配一块新内存把 header 和 payload 拼起来。
	clientHeaderReserve = 4
	// clientReadBuffer 是 readLoop 的缓冲大小。
	//
	// codec.readFrame 会分别读 2 字节基础头、可能的 2 字节扩展头、以及 payload。
	// 直接对着裸 net.Conn 读时，这就是每帧 2~3 次 read 系统调用；小包场景下几乎
	// 全部时间都花在这上面。夹一层 bufio 之后一次 read 能覆盖多个帧。
	//
	// 取 4KB 而不是更大：客户端可能被拿去开几万条连接做压测，缓冲是按连接算的。
	clientReadBuffer = 4096
	// clientWriteBatch 是一次 writev 最多携带的帧数，与服务端的 separateChunk 同理。
	clientWriteBatch = 32
)

type outboundMsg struct {
	// buf 的前 clientHeaderReserve 字节是留给 header 的空位，payload 紧随其后。
	buf     []byte
	encrypt bool
}

func (m outboundMsg) payload() []byte { return m.buf[clientHeaderReserve:] }

// Client is a simple reference gate client built on top of net.Conn.
//
// Outbound messages are always sent as standalone frames: no compression and
// no compound packing. Inbound messages fully support decrypt/decompress/
// uncompound before being delivered to the handler.
type Client struct {
	conn net.Conn
	// br 是 conn 之上的读缓冲。readLoop 必须只经它读，不能再直接碰 conn：
	// 缓冲里可能已经攒着后续帧的字节。
	br             *bufio.Reader
	handler        ClientHandler
	maxMessageSize int
	codec          codec
	sendCh         chan outboundMsg
	closed         chan struct{}
	done           chan struct{}
	dec            *zstd.Decoder

	// cipherMu 既保护 cipher 字段本身，也串行化对它的 Encrypt/Decrypt 调用，
	// 详见 encrypt/decrypt 上方的注释。
	cipherMu sync.Mutex
	cipher   Cipher

	closeOnce sync.Once
	closeMu   sync.Mutex
	closeErr  error
	wg        sync.WaitGroup
}

// DialContext creates a client connection and starts background read/write
// loops.
func DialContext(ctx context.Context, cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	if err := cfg.purge(); err != nil {
		return nil, err
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	client, err := newClientWithConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func newClientWithConn(conn net.Conn, cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("client config must set")
	}

	// newClientWithConn 也被 websocket_client.go 直接调用，不保证 cfg 一定过了
	// purge()，所以这里兜一次底：maxMessage 为 0 会让 codec 拒绝所有帧，
	// 也会让解压上限退化成默认的 64GB；超过协议上限则会让 encodeHeader 把长度位
	// 写进标记位。真正的报错发生在 purge，这里只是最后一道夹取。
	maxMsg := cfg.MaxMessageSize
	if maxMsg <= 0 || maxMsg > maxMessageSize {
		maxMsg = maxMessageSize
	}

	dec, err := newZstdDecoder(maxMsg)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn:           conn,
		br:             bufio.NewReaderSize(conn, clientReadBuffer),
		handler:        cfg.Handler,
		maxMessageSize: maxMsg,
		codec:          clientCodec(maxMsg, maxMsg),
		sendCh:         make(chan outboundMsg, cfg.SendQueueSize),
		closed:         make(chan struct{}),
		done:           make(chan struct{}),
		dec:            dec,
		cipher:         cfg.Cipher,
	}

	// 起 loop 的顺序被两个方向的约束夹住，只有 write→OnConnect→read 同时满足：
	//   - writeLoop 必须在 OnConnect 之前起来。从 OnConnect 里发登录/握手包是这个
	//     回调最主要的用法，而 Send 在 sendCh 满时会阻塞；没人排空队列的话，
	//     SendQueueSize 条消息之后 DialContext 就永久挂死了。
	//   - readLoop 必须在 OnConnect 之后再起。readLoop 一旦启动就可能立刻投递消息，
	//     而业务层还没拿到 *Client、也没做好初始化。
	c.wg.Add(1)
	go c.writeLoop()

	c.callOnConnect()

	c.wg.Add(1)
	go c.readLoop()
	go c.waitLoop()

	return c, nil
}

// callOnConnect 在调用方 goroutine 上执行 OnConnect。
//
// OnConnect 里 panic 视为调用方自己的 bug，panic 原样向上抛（见 ClientHandler
// 的文档）。但 gate 不能因此留下一个半构造的 client：此刻 writeLoop 已经在跑、
// 连接已经建立，而 *Client 还没有返回给任何人，谁也没法再关掉它。所以在把 panic
// 放走之前先关连接、等 writeLoop 退出、释放解码器。
func (c *Client) callOnConnect() {
	done := false
	defer func() {
		if done {
			return
		}
		c.closeWithError(ErrHandlerPanic)
		c.wg.Wait()
		c.dec.Close()
		// panic 的调用栈由 runtime 打印，这里只补一句"client 已经被拆掉了"，
		// 以防调用方在更外层 recover 掉之后完全看不到这件事。
		log.Errorf("[gate] panic in ClientHandler.OnConnect: client aborted and connection closed")
	}()
	c.handler.OnConnect(c)
	done = true
}

func (c *Client) waitLoop() {
	c.wg.Wait()
	c.dec.Close()
	// OnClose 是业务代码，panic 不能连累 close(c.done)——否则所有等在 Wait()
	// 上的 goroutine 会一起挂死。
	safeCall("ClientHandler.OnClose", func() { c.handler.OnClose(c, c.err()) })
	close(c.done)
}

func (c *Client) readLoop() {
	defer c.wg.Done()

	// header 暂存区在整个读循环里复用：交给 readFrame 的话它会被传进 io.Reader
	// 接口调用而逃逸，等于每帧多一次 4 字节的堆分配。readLoop 是单 goroutine 的，
	// 这块暂存区不会被别人看到。
	var header [4]byte
	for {
		f, err := c.codec.readFrameInto(c.br, &header)
		if err != nil {
			if c.isClosed() {
				return
			}
			c.closeWithError(err)
			return
		}
		if err = c.deliver(f); err != nil {
			c.closeWithError(err)
			return
		}
	}
}

// deliver 把一个已收齐的帧解码成若干条业务消息交给 handler。
//
// 所有权规则：只有在 msg 借用了别人的底层数组时才拷贝。
//   - owned == true：codec 本次解码新分配的独占内存（解压输出），直接交出去。
//   - !f.c：整帧就是一条消息，payload 来自 codec.readFrameInto，每帧新分配，
//     sink 只会被调用一次，之后 readLoop 不再引用它，同样是独占内存。
//   - 其余情况（compound 的子消息）：msg 是父 payload 的一段切片，必须拷贝。
//
// 最后这条为什么还留着：codec.parse 现在用三索引切片把子消息的 cap 封在自己的
// payload 末尾，所以 handler 已经不可能通过 append 越界读写兄弟消息了。但
// ClientHandler.OnMessage 没有"不得留存"的约定，而留住一条 20 字节的子消息就会
// 把整个 compound 父 buffer（可能是几十 KB）一起钉在堆上。一条 compound 里有 N
// 条子消息就是 N 次分配，这是明知的热路径成本，换的是"每条消息都能被独立留存"。
func (c *Client) deliver(f frame) error {
	// 解密在这里做完，codec 就不必再持有 Cipher。
	//
	// 此前每帧都要先 inboundCipher() 取一次 cipherMu 拿到包装器，加密帧随后在
	// serialCipher.Decrypt 里再取一次；明文帧则白取一次锁。compound 的子帧按协议
	// 不得携带 e 标记（subCodec 的 allow 是 0），所以整条入站路径上加密只可能
	// 出现在这一个位置，提到这里来做不会漏掉任何东西。
	if f.e {
		if !c.decrypt(f.payload) {
			// 保留原来的语义：收到加密帧但没有配置 Cipher 是协议错误。
			return ErrCipherRequired
		}
		f.e = false
	}

	standalone := !f.c
	return c.codec.deliver(f, nil, c.dec, func(msg []byte, owned bool) error {
		if !owned && !standalone {
			msg = append([]byte(nil), msg...)
		}
		// OnMessage 是业务代码，跑在 readLoop 的 goroutine 上：不兜住 panic 的话，
		// 一条能让业务解码 panic 的报文就能打死整个进程。返回错误会让 readLoop
		// 以 ErrHandlerPanic 关闭这个 client，业务层从 OnClose/Wait 能看到原因。
		if !safeCall("ClientHandler.OnMessage", func() { c.handler.OnMessage(c, msg) }) {
			return ErrHandlerPanic
		}
		return nil
	})
}

// writeLoop 把出站消息编码后写进连接。
//
// 每次醒来都会把此刻已经排在 sendCh 里的消息一并取走，攒成一次 writev
// （TCP 下 net.Buffers.WriteTo 走 writev，其余传输退化成逐条 Write，与原先一致）。
// 这里只 drain "已经在队列里"的消息，不会为了凑批而等待，所以不引入任何额外延迟。
//
// 此前是一条消息一次 Write：一个高频发小包的客户端几乎所有时间都耗在系统调用上，
// 而服务端一侧早就在用同样的批量思路（见 sender.pushSeparate）。
func (c *Client) writeLoop() {
	defer c.wg.Done()
	// 退出前把队列里剩下的消息丢掉。
	//
	// Send 会为每条消息分配并复制一份完整 payload，而 writeLoop 一旦退出就再也
	// 没有人排空 sendCh。业务只要还留着这个已关闭的 Client（很常见：拿它读
	// Wait() 的错误、做重连决策），整个队列的 payload 就一直可达——一个在拥塞
	// 时被关掉的 client 可以就这样钉住上千条消息。
	defer c.drainSendQueue()

	frames := make([][]byte, 0, clientWriteBatch)
	for {
		select {
		case <-c.closed:
			return
		case msg := <-c.sendCh:
			if c.isClosed() {
				return
			}
			var err error
			if frames, err = c.collectFrames(msg, frames[:0]); err != nil {
				c.closeWithError(err)
				return
			}

			// Buffers.WriteTo 会就地消费传进去的切片（推进头部、把发完的元素置 nil），
			// 所以给它一个副本头，下一轮仍从 frames[:0] 重新填。
			batch := net.Buffers(frames)
			if _, err := batch.WriteTo(c.conn); err != nil {
				if c.isClosed() {
					return
				}
				c.closeWithError(err)
				return
			}
		}
	}
}

// collectFrames 编码 first，再把此刻已经排在 sendCh 里的消息一并编码进 dst，
// 最多 clientWriteBatch 条。
//
// 关键是那个 default 分支：只取"已经到了"的消息，绝不等待。攒批因此完全由实际
// 到达速率决定，不会给任何一条消息引入额外延迟——队列里只有一条时就发一条。
func (c *Client) collectFrames(first outboundMsg, dst [][]byte) ([][]byte, error) {
	frame, err := c.encodeOutbound(first)
	if err != nil {
		return dst, err
	}
	dst = append(dst, frame)

	for len(dst) < clientWriteBatch {
		// 每轮先看一眼是否已经关闭，再决定要不要继续攒。
		//
		// 关闭后队列里的消息本来就允许被丢弃，但继续 drain 意味着继续调用业务的
		// Cipher.Encrypt：一个昂贵的 Cipher 会把 Close 拖长最多 31 次加密的时间，
		// 而一个会阻塞的 Cipher 干脆能让 Wait() 永远不返回。
		//
		// 这个检查不能写成 select 的一个 case：closed 和 sendCh 同时就绪时 Go 会
		// 在两者之间随机挑，停不干净。isClosed 是一次带 default 的非阻塞收，
		// 约 2ns，而且是确定的。
		if c.isClosed() {
			return dst, nil
		}
		select {
		case msg := <-c.sendCh:
			if frame, err = c.encodeOutbound(msg); err != nil {
				return dst, err
			}
			dst = append(dst, frame)
		default:
			return dst, nil
		}
	}
	return dst, nil
}

// drainSendQueue 丢弃尚未发出的消息，释放它们持有的缓冲。
//
// 关闭之后队列里的消息本来就允许被丢弃（和服务端 sender.Close 的语义一致），
// 这里只是把它们的内存也一并放掉，而不是留在 channel 里等业务扔掉整个 Client。
func (c *Client) drainSendQueue() {
	for {
		select {
		case <-c.sendCh:
		default:
			return
		}
	}
}

// Send sends exactly one gate frame. The client never compresses or compounds
// outbound messages.
func (c *Client) Send(data []byte) error {
	return c.send(data, true)
}

// SendNoEncrypt sends exactly one plaintext gate frame.
func (c *Client) SendNoEncrypt(data []byte) error {
	return c.send(data, false)
}

func (c *Client) send(data []byte, encrypt bool) error {
	if len(data) > c.maxMessageSize {
		return ErrMaxMessageSize
	}
	if c.isClosed() {
		return ErrClientClosed
	}

	// 一次分配就把整帧凑齐：前 clientHeaderReserve 字节留给 header，payload 紧随
	// 其后。此前这里 append 拷一份，writeLoop 的 encodeOutbound 再 make 一块新的
	// 把 header 和 payload 拼进去——一条消息两次分配、两次拷贝。
	buf := make([]byte, clientHeaderReserve+len(data))
	copy(buf[clientHeaderReserve:], data)

	select {
	case <-c.closed:
		return ErrClientClosed
	case c.sendCh <- outboundMsg{buf: buf, encrypt: encrypt}:
		// closed 和"队列还有空位"同时就绪时，Go 会在两个 case 之间随机挑，
		// 所以这条消息完全可能是在 writeLoop 已经排空并退出**之后**才塞进去的。
		// 那样它既不会被发出去，也再没有人排空——一直钉在 channel 里，
		// 跟着这个已关闭的 Client 一起被业务持有（默认队列 1024 条）。
		//
		// 这里自己收一次尾：报出真实结果，并把内存放掉。
		if c.isClosed() {
			c.drainSendQueue()
			return ErrClientClosed
		}
		return nil
	}
}

// UpdateCipher swaps the cipher used by subsequent inbound/outbound encrypted
// frames.
func (c *Client) UpdateCipher(cipher Cipher) {
	c.cipherMu.Lock()
	c.cipher = cipher
	c.cipherMu.Unlock()
}

func (c *Client) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the connection and stops background loops. It returns the error
// that terminated the connection, if any: Close 可能是在 readLoop 已经因为协议
// 错误关掉连接之后才被调用的，此时吞掉那个错误会让业务层完全看不到失败原因。
func (c *Client) Close() error {
	c.closeWithError(nil)
	return c.err()
}

// Wait blocks until the client has fully stopped.
func (c *Client) Wait() error {
	<-c.done
	return c.err()
}

// encodeOutbound 就地把 header 写进 send 预留的空位里，返回完整的一帧。
// 返回的切片是 msg.buf 的一段，不做任何新的分配。
func (c *Client) encodeOutbound(msg outboundMsg) ([]byte, error) {
	payload := msg.payload()
	if len(payload) > c.maxMessageSize {
		return nil, ErrMaxMessageSize
	}

	flag := byte(0)
	if msg.encrypt && c.encrypt(payload) {
		flag |= maskE
	}

	// header 贴着 payload 向前生长：短消息占 2 字节，长消息占 4 字节，
	// 所以起点是 clientHeaderReserve - n。
	n := frameOverhead(len(payload))
	off := clientHeaderReserve - n
	encodeHeader(msg.buf[off:], len(payload))
	msg.buf[off] |= flag
	return msg.buf[off:], nil
}

// encrypt/decrypt 是 client 侧唯一的 Cipher 调用入口，两个方向都在 cipherMu
// 下串行执行。两者都返回"当时是否真的配置了 Cipher"。
//
// 为什么需要串行：readLoop 解密入站帧、writeLoop 加密出站帧，是两个不同的
// goroutine。server 侧所有 Cipher 调用都被 gnet 事件循环串行化，因此 Cipher
// 接口从来只要求"单线程可用"；client 不该凭空要求实现方给出更强的保证——流式
// 密码、带 nonce 计数器、或复用一块 scratch buffer 的实现，在并发调用下会静默
// 地把两个方向的数据都搞坏。
//
// 为什么不用"在文档里加一条并发要求"来解决：那等于让每个已经能在 server 上
// 正常工作的 Cipher 实现，在被 client 复用时变成一个不会报错的数据损坏 bug。
//
// 代价：出站路径的加解锁次数和原来完全一样（原先 getCipher 也要取一次
// cipherMu，只是取到接口值就放开了），这里只是把临界区扩到调用结束，所以并没有
// 给每条消息新增一把锁。只有收发真正同时发生时才会有一次极短的争用，临界区就是
// 一条消息的加解密。附带修掉了另一个窗口：UpdateCipher 不再可能插在"取到旧
// cipher"和"用它加密"之间生效。
func (c *Client) encrypt(data []byte) bool {
	c.cipherMu.Lock()
	defer c.cipherMu.Unlock()
	if c.cipher == nil {
		return false
	}
	c.cipher.Encrypt(data)
	return true
}

func (c *Client) decrypt(data []byte) bool {
	c.cipherMu.Lock()
	defer c.cipherMu.Unlock()
	if c.cipher == nil {
		return false
	}
	c.cipher.Decrypt(data)
	return true
}

func (c *Client) closeWithError(err error) {
	c.closeOnce.Do(func() {
		c.setErr(err)
		close(c.closed)
		if closeErr := c.conn.Close(); err == nil && closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			c.setErr(closeErr)
		}
	})
}

func (c *Client) setErr(err error) {
	c.closeMu.Lock()
	c.closeErr = err
	c.closeMu.Unlock()
}

func (c *Client) err() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closeErr
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
