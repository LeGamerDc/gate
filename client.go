package gate

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type outboundMsg struct {
	data    []byte
	encrypt bool
}

// Client is a simple reference gate client built on top of net.Conn.
//
// Outbound messages are always sent as standalone frames: no compression and
// no compound packing. Inbound messages fully support decrypt/decompress/
// uncompound before being delivered to the handler.
type Client struct {
	conn           net.Conn
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

	for {
		f, err := c.codec.readFrame(c.conn)
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
//   - !f.c：整帧就是一条消息，payload 来自 codec.readFrame，每帧新分配，
//     sink 只会被调用一次，之后 readLoop 不再引用它，同样是独占内存。
//   - 其余情况（compound 的子消息）：msg 是父 payload 的一段切片，父 buffer 还要
//     继续切出后面的子消息，handler 若留存就会读到别的消息，必须拷贝。
//
// 拿不准就拷贝——多一次分配远比 aliasing bug 便宜。
func (c *Client) deliver(f frame) error {
	standalone := !f.c
	return c.codec.deliver(f, c.inboundCipher(), c.dec, func(msg []byte, owned bool) error {
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

func (c *Client) writeLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closed:
			return
		case msg := <-c.sendCh:
			if c.isClosed() {
				return
			}
			frame, err := c.encodeOutbound(msg)
			if err != nil {
				c.closeWithError(err)
				return
			}
			if err := writeAll(c.conn, frame); err != nil {
				if c.isClosed() {
					return
				}
				c.closeWithError(err)
				return
			}
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

	msg := outboundMsg{
		data:    append([]byte(nil), data...),
		encrypt: encrypt,
	}

	select {
	case <-c.closed:
		return ErrClientClosed
	case c.sendCh <- msg:
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

func (c *Client) encodeOutbound(msg outboundMsg) ([]byte, error) {
	if len(msg.data) > c.maxMessageSize {
		return nil, ErrMaxMessageSize
	}

	flag := byte(0)
	if msg.encrypt && c.encrypt(msg.data) {
		flag |= maskE
	}

	var header [4]byte
	n := encodeHeader(header[:], len(msg.data))
	header[0] |= flag

	frame := make([]byte, n+len(msg.data))
	copy(frame, header[:n])
	copy(frame[n:], msg.data)
	return frame, nil
}

// encrypt/decrypt 是 client 侧唯一的 Cipher 调用入口，两个方向都在 cipherMu
// 下串行执行。
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

func (c *Client) decrypt(data []byte) {
	c.cipherMu.Lock()
	defer c.cipherMu.Unlock()
	if c.cipher != nil {
		c.cipher.Decrypt(data)
	}
}

// inboundCipher 返回交给 codec 的 Cipher。没配置 Cipher 时必须返回 nil 接口值，
// 否则 codec 无法判断"收到加密帧但没有 cipher"（ErrCipherRequired）。
func (c *Client) inboundCipher() Cipher {
	c.cipherMu.Lock()
	defer c.cipherMu.Unlock()
	if c.cipher == nil {
		return nil
	}
	return serialCipher{c: c}
}

// serialCipher 把 client 的 Cipher 以"每次调用都持锁"的形式暴露给 codec：
// codec 只认识 Cipher 接口，串行化必须发生在接口调用的内部。
type serialCipher struct{ c *Client }

func (s serialCipher) Encrypt(data []byte) { s.c.encrypt(data) }
func (s serialCipher) Decrypt(data []byte) { s.c.decrypt(data) }

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

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
