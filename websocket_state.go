package gate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/panjf2000/gnet/v2"
)

// maxCapturedHandshakeHeaders 是握手期间最多记录多少条 HTTP 头。
//
// maxHandshakeBytes 已经限制了握手的原始字节总量，这里再挡一次"海量极短头"
// 的情况：16KB 的请求可以塞下上千条 "a:\r\n"，每条都会在 http.Header 里生成
// 一个 map 项加一个切片。
//
// 超过上限时直接拒绝握手，而不是静默截断。HTTP 头没有"重要的排在前面"这种
// 保证，顺序完全由客户端和中间代理决定：静默丢弃意味着攻击者只要在前面塞满
// 64 条无关头，OnWebSocketUpgrade 就再也看不到 Authorization / Cookie /
// X-Forwarded-For，把"头不存在"当成匿名放行的鉴权逻辑会被直接绕过。
// 宁可失败也不要让鉴权基于一份被悄悄截断的请求头做判断。
const maxCapturedHandshakeHeaders = 64

// wsIdleReleasePasses 是把超大缓冲区真正交还给 GC 之前，需要连续观察到的
// 空闲事件次数。见 releaseIdleBuffers。
const wsIdleReleasePasses = 16

// wsIdleReleaseAfter 是另一条独立的收缩条件：距上次交付消息超过这么久，
// 下一次事件到来时立刻收缩，不必再等够 wsIdleReleasePasses 次。
const wsIdleReleaseAfter = 30 * time.Second

// wsEndOfHeaders 是 HTTP 请求头的结束标记，用于精确切出一个握手请求。
var wsEndOfHeaders = []byte("\r\n\r\n")

// errWebSocketReadyPanic 业务的 OnReady 回调 panic 了，连接必须关闭。
var errWebSocketReadyPanic = errors.New("gate: websocket OnReady panicked")

type wsConnState struct {
	conn *Conn

	upgrader ws.Upgrader
	upgraded bool

	maxHandshakeBytes int
	maxBufferedBytes  int
	onUpgrade         func(*Handshake) error

	// 握手期间累积的请求信息。Upgrade 在数据不足时会整体重放，
	// 所以每次尝试之前都要清空，见 resetHandshakeCapture。
	hsURI      string
	hsHeader   http.Header
	hsCount    int
	hsOverflow bool

	buf         bytes.Buffer
	gateBuf     bytes.Buffer
	fragmentBuf wsFragmentBuf

	// peakBuffered 是本轮事件里缓冲占用的峰值，idlePasses 是连续多少轮事件
	// 没有真正用到大缓冲。两者共同决定何时收缩，见 releaseIdleBuffers。
	peakBuffered int
	idlePasses   int
}

type wsReadWrite struct {
	io.Reader
	io.Writer
}

type wsFragmentBuf struct {
	active     bool
	payloadLen int
	payload    bytes.Buffer
}

var errWebSocketClosed = errors.New("websocket connection closed")

var (
	errWebSocketHandshakeTooLarge = errors.New("websocket handshake exceeds buffered limit")
	errWebSocketBufferLimit       = errors.New("websocket buffered data exceeds limit")
	errWebSocketUpgradeRejected   = errors.New("websocket upgrade rejected")
)

func newWSConnState(conn *Conn, path string, maxHandshakeBytes, maxBufferedBytes int, onUpgrade func(*Handshake) error) *wsConnState {
	w := &wsConnState{
		conn:              conn,
		maxHandshakeBytes: maxHandshakeBytes,
		maxBufferedBytes:  maxBufferedBytes,
		onUpgrade:         onUpgrade,
	}
	w.upgrader = ws.Upgrader{
		OnRequest: func(uri []byte) error {
			w.hsURI = string(uri)
			if matchesWebSocketPath(w.hsURI, path) {
				return nil
			}
			return ws.RejectConnectionError(
				ws.RejectionStatus(http.StatusNotFound),
				ws.RejectionReason("unexpected websocket path"),
			)
		},
		// Host 走的是独立回调，不会出现在 OnHeader 里。
		OnHost: func(host []byte) error {
			return w.captureHandshakeHeader("Host", host)
		},
		OnHeader: func(key, value []byte) error {
			return w.captureHandshakeHeader(string(key), value)
		},
		// Sec-WebSocket-Protocol 也不会进 OnHeader，它走子协议协商回调。
		// 返回 ("", true) 表示"不协商任何子协议"，与不设该回调时的行为完全
		// 一致（响应里不会出现 Sec-WebSocket-Protocol），只是顺手把客户端
		// 提供的原始值记进 Handshake——浏览器常用它携带鉴权 token。
		ProtocolCustom: func(v []byte) (string, bool) {
			// 这个回调没有 error 返回值，超限只能记下标记，
			// 由 OnBeforeUpgrade 统一拒绝。
			_ = w.captureHandshakeHeader("Sec-WebSocket-Protocol", v)
			return "", true
		},
		// OnBeforeUpgrade 是 gobwas 在写出 101 之前的最后一个钩子，也是唯一
		// 一个"所有头都已解析完"的时机。
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			return nil, w.finishHandshake()
		},
	}
	return w
}

// captureHandshakeHeader 记录一条握手请求头。key 会被规范化成 http.Header 的
// 标准写法，因此业务层可以直接用 Header.Get("Authorization") 取值。
func (w *wsConnState) captureHandshakeHeader(key string, value []byte) error {
	if w.hsCount >= maxCapturedHandshakeHeaders {
		w.hsOverflow = true
		return ws.RejectConnectionError(
			ws.RejectionStatus(http.StatusRequestHeaderFieldsTooLarge),
			ws.RejectionReason("too many request headers"),
		)
	}
	if w.hsHeader == nil {
		w.hsHeader = make(http.Header, 8)
	}
	w.hsCount++
	w.hsHeader.Add(key, string(value))
	return nil
}

func (w *wsConnState) resetHandshakeCapture() {
	w.hsURI = ""
	w.hsHeader = nil
	w.hsCount = 0
	w.hsOverflow = false
}

// finishHandshake 组装 Handshake，交给业务钩子做最后一次判定，并在通过后把它
// 挂到 Conn 上——这样 OnReady 和 Handle 都能读到握手信息（鉴权结果、
// X-Forwarded-For 里的真实来源 IP 等）。
func (w *wsConnState) finishHandshake() error {
	// 头被截断过就不能再让业务基于它做鉴权判断——见 maxCapturedHandshakeHeaders。
	// ProtocolCustom 那条路径没有 error 返回值，只能在这里统一拦下。
	if w.hsOverflow {
		return ws.RejectConnectionError(
			ws.RejectionStatus(http.StatusRequestHeaderFieldsTooLarge),
			ws.RejectionReason("too many request headers"),
		)
	}

	header := w.hsHeader
	if header == nil {
		header = http.Header{}
	}
	hs := &Handshake{URI: w.hsURI, Header: header}

	if w.onUpgrade != nil {
		var err error
		// 钩子是业务代码，跑在事件循环 goroutine 上，panic 必须就地兜住。
		if !safeCall("Config.OnWebSocketUpgrade", func() { err = w.onUpgrade(hs) }) {
			return ws.RejectConnectionError(
				ws.RejectionStatus(http.StatusInternalServerError),
				ws.RejectionReason("websocket upgrade hook failed"),
			)
		}
		if err != nil {
			return w.rejection(err)
		}
	}

	w.conn.setHandshake(hs)
	return nil
}

// rejection 把业务钩子返回的错误翻译成一个 HTTP 拒绝响应。
//
// 钩子可以直接返回 ws.RejectConnectionError(...) 自选状态码；否则一律按 401
// 处理，并且只回一句固定文案——错误详情写进服务端日志，不下发给对端。
func (w *wsConnState) rejection(err error) error {
	var rejected *ws.ConnectionRejectedError
	if errors.As(err, &rejected) {
		return err
	}
	log.Warnf("[gate] %s: websocket upgrade rejected: %v", w.conn.Remote(), err)
	return ws.RejectConnectionError(
		ws.RejectionStatus(http.StatusUnauthorized),
		ws.RejectionReason(errWebSocketUpgradeRejected.Error()),
	)
}

func (w *wsConnState) onTraffic() gnet.Action {
	// 活跃时间由 Conn.deliverOne 在真正交付一条完整消息时刷新，两种传输共用
	// 同一个交付点。这里不能刷新：gnet 的 Wake 会先跑一遍 OnTraffic 再执行
	// 发送回调，在入口 touch 会让"服务端定期下推、客户端一言不发"的连接
	// 永远不被判定为空闲。
	defer w.releaseIdleBuffers()

	if err := w.readBufferBytes(); err != nil {
		logErr(err)
		return gnet.Close
	}

	ok, err := w.upgrade()
	if err != nil {
		logErr(err)
		return gnet.Close
	}
	if !ok {
		return gnet.None
	}

	if err := w.decodeMessages(); err != nil {
		if errors.Is(err, errWebSocketClosed) {
			return gnet.Close
		}
		w.logInbound(err)
		return gnet.Close
	}
	if w.conn.isBlocking() {
		return gnet.None
	}
	if err := w.drainGateMessages(); err != nil {
		w.logInbound(err)
		return gnet.Close
	}
	return gnet.None
}

// logInbound 记录一个会导致连接关闭的入站错误。handler 的 panic 已经在
// Conn.deliverOne 里打过完整堆栈，这里不再重复。
func (w *wsConnState) logInbound(err error) {
	if errors.Is(err, errHandlerPanic) {
		return
	}
	log.Warnf("[gate] %s: websocket inbound error: %v", w.conn.Remote(), err)
}

func (w *wsConnState) readBufferBytes() error {
	size := w.conn.conn.InboundBuffered()
	if size == 0 {
		return nil
	}
	if err := w.ensureBufferedLimit(size); err != nil {
		return err
	}
	buf, err := w.conn.conn.Next(size)
	if err != nil {
		return err
	}
	_, _ = w.buf.Write(buf)
	return nil
}

func (w *wsConnState) upgrade() (bool, error) {
	if w.upgraded {
		return true, nil
	}

	// 自己定位请求边界，只把恰好一个完整的 HTTP 请求喂给 Upgrade。
	//
	// 原先是把整个缓冲区交给 Upgrade，再用 oldLen-tmpReader.Len() 反推消耗了
	// 多少字节。但 Upgrade 内部套了一层池化 bufio.Reader，它会预读到自己的
	// 缓冲里，于是这个差值通常大于握手本身的长度——紧跟在握手后面的数据帧
	// 字节会被一并 Next 掉。RFC 要求客户端等到 101 之后才发帧，所以合规实现
	// 碰不到；但流水线发送的客户端会静默丢数据。
	//
	// 顺带的好处：数据不足时根本不用调 Upgrade，省掉一次整体重放。
	data := w.buf.Bytes()
	idx := bytes.Index(data, wsEndOfHeaders)
	if idx < 0 {
		// 请求还没收全。缓冲区上限由 ensureBufferedLimit 把关。
		return false, nil
	}
	end := idx + len(wsEndOfHeaders)

	w.resetHandshakeCapture()
	_, err := w.upgrader.Upgrade(wsReadWrite{
		Reader: bytes.NewReader(data[:end]),
		Writer: w.conn.conn,
	})
	// 无论成功失败都只消费这一个请求，后面的字节留给数据帧解析。
	w.buf.Next(end)
	if err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			// 请求以 \r\n\r\n 结尾却仍然不完整，说明它本身是畸形的。
			return false, fmt.Errorf("malformed websocket handshake: %w", err)
		}
		return false, err
	}

	w.upgraded = true
	// Handshake 已经转交给 Conn，这里只放掉自己这份引用。
	w.hsHeader = nil

	// 101 响应此刻已经写进 socket，连接从这一刻起才允许发送业务数据。
	// 顺序很关键：先 markOpen 打开出站闸口，再 notifyReady 通知业务，
	// 最后才轮到 decodeMessages 投递入站数据。
	w.conn.markOpen()
	if !w.conn.notifyReady() {
		// OnReady panic 说明业务的连接级初始化没走完，不能继续投递入站数据。
		return false, errWebSocketReadyPanic
	}
	return true, nil
}

func (w *wsConnState) decodeMessages() error {
	for {
		frame, ok, err := w.nextFrame()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := w.handleFrame(frame); err != nil {
			return err
		}
		w.buf.Next(frame.size)
	}
}

type wsFrame struct {
	header  ws.Header
	payload []byte
	size    int
}

func (w *wsConnState) nextFrame() (wsFrame, bool, error) {
	data := w.buf.Bytes()
	if len(data) < ws.MinHeaderSize {
		return wsFrame{}, false, nil
	}

	r := bytes.NewReader(data)
	oldLen := r.Len()
	header, err := ws.ReadHeader(r)
	if err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			return wsFrame{}, false, nil
		}
		return wsFrame{}, false, err
	}

	headerLen := oldLen - r.Len()
	if header.Length > maxMessageSize {
		return wsFrame{}, false, ErrMaxMessageSize
	}
	total := headerLen + int(header.Length)
	if len(data) < total {
		return wsFrame{}, false, nil
	}
	if !header.Masked {
		return wsFrame{}, false, fmt.Errorf("unmasked websocket client frame")
	}
	if header.Rsv != 0 || header.OpCode.IsReserved() {
		return wsFrame{}, false, fmt.Errorf("unsupported websocket frame header")
	}
	if header.OpCode.IsControl() && !header.Fin {
		return wsFrame{}, false, fmt.Errorf("fragmented websocket control frame")
	}

	payload := data[headerLen:total]
	if header.Masked {
		ws.Cipher(payload, header.Mask, 0)
		header.Masked = false
	}
	return wsFrame{
		header:  header,
		payload: payload,
		size:    total,
	}, true, nil
}

func (w *wsConnState) handleFrame(frame wsFrame) error {
	if frame.header.OpCode.IsControl() {
		return w.handleControlFrame(frame)
	}

	switch frame.header.OpCode {
	case ws.OpBinary:
		return w.handleBinaryFrame(frame)
	case ws.OpContinuation:
		return w.handleContinuationFrame(frame)
	default:
		return fmt.Errorf("unsupported websocket opcode %v", frame.header.OpCode)
	}
}

func (w *wsConnState) handleControlFrame(frame wsFrame) error {
	if err := wsutil.HandleClientControlMessage(w.conn.conn, wsutil.Message{
		OpCode:  frame.header.OpCode,
		Payload: frame.payload,
	}); err != nil {
		return err
	}
	if frame.header.OpCode == ws.OpClose {
		return errWebSocketClosed
	}
	return nil
}

func (w *wsConnState) handleBinaryFrame(frame wsFrame) error {
	if w.fragmentBuf.active {
		return fmt.Errorf("unexpected websocket binary frame while fragmented message is active")
	}
	if frame.header.Fin {
		return w.handleGatePayload(frame.payload)
	}
	if err := w.ensureBufferedLimit(len(frame.payload)); err != nil {
		return err
	}
	return w.fragmentBuf.start(frame.payload)
}

func (w *wsConnState) handleContinuationFrame(frame wsFrame) error {
	if !w.fragmentBuf.active {
		return fmt.Errorf("unexpected websocket continuation frame")
	}
	if err := w.ensureBufferedLimit(len(frame.payload)); err != nil {
		return err
	}
	if err := w.fragmentBuf.append(frame.payload); err != nil {
		return err
	}
	if !frame.header.Fin {
		return nil
	}
	if err := w.handleGatePayload(w.fragmentBuf.payload.Bytes()); err != nil {
		return err
	}
	w.fragmentBuf.reset()
	return nil
}

func (w *wsConnState) handleGatePayload(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.shouldBufferGatePayload() {
		return w.appendGatePayload(payload)
	}
	return w.deliverGatePayload(payload)
}

func (w *wsConnState) shouldBufferGatePayload() bool {
	return w.gateBuf.Len() != 0 || w.conn.isBlocking() || w.conn.handler == nil
}

// deliverGatePayload 从一段已经解出 WebSocket 外壳的字节里，尽可能多地取出
// 完整的 gate 帧交给业务，剩下的半截留到 gateBuf 里等后续数据。
//
// 解析走 Conn 的 codec：服务端策略只允许对端加密，带 z/c 标记的上行帧会在
// parse 阶段就被 ErrFlagNotAllowed 拒掉。之前这里用的是一个只认 e 标记、
// 把 z/c 默默忽略掉的私有解析器，客户端只要置一位 flag 就能把压缩后的原始
// 字节或未展开的 compound 直接塞给业务层。
func (w *wsConnState) deliverGatePayload(data []byte) error {
	c := w.conn.codec
	for !w.conn.isBlocking() && len(data) > 0 {
		f, n, ok, err := c.parse(data)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		// decoder 恒为 nil：服务端策略本就不接受压缩上行。
		if err = c.deliver(f, w.conn.getCipher(), nil, w.conn.deliverFn); err != nil {
			return err
		}
		data = data[n:]
	}
	if len(data) == 0 {
		return nil
	}
	return w.appendGatePayload(data)
}

func (b *wsFragmentBuf) start(payload []byte) error {
	if len(payload) > maxMessageSize {
		return ErrMaxMessageSize
	}
	b.active = true
	b.payloadLen = len(payload)
	b.payload.Reset()
	_, _ = b.payload.Write(payload)
	return nil
}

func (b *wsFragmentBuf) append(payload []byte) error {
	if !b.active {
		return fmt.Errorf("websocket fragment buffer is not active")
	}
	if len(payload) > maxMessageSize-b.payloadLen {
		return ErrMaxMessageSize
	}
	b.payloadLen += len(payload)
	_, _ = b.payload.Write(payload)
	return nil
}

func (b *wsFragmentBuf) reset() {
	b.active = false
	b.payloadLen = 0
	b.payload.Reset()
}

// drainGateMessages 消费 AsyncDo 挂起期间攒下来的 gate 帧。
func (w *wsConnState) drainGateMessages() error {
	c := w.conn.codec
	for !w.conn.isBlocking() {
		f, n, ok, err := c.parse(w.gateBuf.Bytes())
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		// 无论 deliver 结果如何都要推进读游标，否则一条出错的帧会被反复重放。
		err = func() error {
			defer w.gateBuf.Next(n)
			return c.deliver(f, w.conn.getCipher(), nil, w.conn.deliverFn)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *wsConnState) appendGatePayload(payload []byte) error {
	if err := w.ensureBufferedLimit(len(payload)); err != nil {
		return err
	}
	_, _ = w.gateBuf.Write(payload)
	return nil
}

func (w *wsConnState) bufferedBytes() int {
	return w.buf.Len() + w.gateBuf.Len() + w.fragmentBuf.payloadLen
}

func (w *wsConnState) ensureBufferedLimit(extra int) error {
	used := w.bufferedBytes() + extra
	if used > w.peakBuffered {
		w.peakBuffered = used
	}

	limit := w.maxBufferedBytes
	if !w.upgraded {
		limit = w.maxHandshakeBytes
	}
	if limit <= 0 || used <= limit {
		return nil
	}
	if !w.upgraded {
		return errWebSocketHandshakeTooLarge
	}
	return errWebSocketBufferLimit
}

// releaseIdleBuffers 在每个 traffic 事件末尾把缓冲区归位，并在连接安静下来
// 之后把超大的那块内存整个还给 GC。
//
// 之前这里没有"安静下来"这一步：只要缓冲区当前为空且 cap 超过
// maxReusableBufferCap 就直接 *buf = bytes.Buffer{}。而"当前为空"恰恰是每条
// 消息处理完之后的常态，于是一个稳定收发大消息的连接会在每条消息上把几百 KB
// 的缓冲区丢掉再从零扩容一遍。现在只有连续 wsIdleReleasePasses 个事件都没有
// 真正用到大缓冲时才收缩：内存上限没变（真正空闲的连接一样会被回收），但热
// 路径上的反复重建没了。
// releaseIdleBuffers 决定何时把超大缓冲交还给 GC。
//
// 权衡：立刻收缩会让"持续搬运大块数据"的连接每条消息都重新扩容一遍；
// 只按连续空闲次数收缩，又会让"突发型"连接（长间隔 + 每次少量事件）迟迟凑不满
// 次数，把一块大缓冲一直挂到断开。所以两个条件取或：连续空闲够多次，或者距上
// 一次活跃已经过了足够久（复用 Conn 已有的 lastActive，不额外引入时间源）。
//
// 残留：完全不再产生任何 traffic 事件的连接不会走到这里，那块缓冲会留到连接
// 关闭为止。它被 MaxWebSocketBufferedBytes 封顶，并且由 TCPKeepAlive
// （默认 60s）和 IdleTimeout 负责回收连接本身。
func (w *wsConnState) releaseIdleBuffers() {
	// 这一轮真的搬过大块数据，或者还有数据没处理完，都不算空闲。
	if w.peakBuffered > maxReusableBufferCap || w.bufferedBytes() != 0 {
		w.idlePasses = 0
	} else if w.idlePasses < wsIdleReleasePasses {
		w.idlePasses++
	}
	w.peakBuffered = 0

	drop := w.idlePasses >= wsIdleReleasePasses ||
		(w.bufferedBytes() == 0 && w.conn.idleFor(time.Now()) >= wsIdleReleaseAfter)
	releaseIdleBuffer(&w.buf, drop)
	releaseIdleBuffer(&w.gateBuf, drop)
	if !w.fragmentBuf.active {
		releaseIdleBuffer(&w.fragmentBuf.payload, drop)
	}
}

func releaseIdleBuffer(buf *bytes.Buffer, drop bool) {
	if buf.Len() != 0 {
		return
	}
	if drop && buf.Cap() > maxReusableBufferCap {
		*buf = bytes.Buffer{}
		return
	}
	buf.Reset()
}

func matchesWebSocketPath(uri, path string) bool {
	if uri == path {
		return true
	}
	return len(uri) > len(path) && uri[:len(path)] == path && uri[len(path)] == '?'
}
