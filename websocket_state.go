package gate

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/panjf2000/gnet/v2"
)

type wsConnState struct {
	conn *Conn

	upgrader ws.Upgrader
	upgraded bool

	maxHandshakeBytes int
	maxBufferedBytes  int

	buf         bytes.Buffer
	gateBuf     bytes.Buffer
	fragmentBuf wsFragmentBuf
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
)

func newWSConnState(conn *Conn, path string, maxHandshakeBytes, maxBufferedBytes int) *wsConnState {
	return &wsConnState{
		conn:              conn,
		maxHandshakeBytes: maxHandshakeBytes,
		maxBufferedBytes:  maxBufferedBytes,
		upgrader: ws.Upgrader{
			OnRequest: func(uri []byte) error {
				if matchesWebSocketPath(string(uri), path) {
					return nil
				}
				return ws.RejectConnectionError(
					ws.RejectionStatus(404),
					ws.RejectionReason("unexpected websocket path"),
				)
			},
		},
	}
}

func (w *wsConnState) onTraffic() gnet.Action {
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
		logErr(err)
		return gnet.Close
	}
	if w.conn.isBlocking() {
		return gnet.None
	}
	if err := w.drainGateMessages(); err != nil {
		logErr(err)
		return gnet.Close
	}
	return gnet.None
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

	tmpReader := bytes.NewReader(w.buf.Bytes())
	oldLen := tmpReader.Len()
	_, err := w.upgrader.Upgrade(wsReadWrite{Reader: tmpReader, Writer: w.conn.conn})
	skipN := oldLen - tmpReader.Len()
	if err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		if skipN > 0 {
			w.buf.Next(skipN)
		}
		return false, err
	}

	if skipN > 0 {
		w.buf.Next(skipN)
	}
	w.upgraded = true
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

func (w *wsConnState) deliverGatePayload(data []byte) error {
	for !w.conn.isBlocking() && len(data) > 0 {
		msg, n, ok, err := consumeServerMessage(data, w.conn.cipher)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		w.conn.handler.Handle(msg)
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

func (w *wsConnState) drainGateMessages() error {
	for !w.conn.isBlocking() {
		msg, n, ok, err := consumeServerMessage(w.gateBuf.Bytes(), w.conn.cipher)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		func() {
			defer w.gateBuf.Next(n)
			w.conn.handler.Handle(msg)
		}()
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
	limit := w.maxBufferedBytes
	if !w.upgraded {
		limit = w.maxHandshakeBytes
	}
	if limit <= 0 || w.bufferedBytes()+extra <= limit {
		return nil
	}
	if !w.upgraded {
		return errWebSocketHandshakeTooLarge
	}
	return errWebSocketBufferLimit
}

func (w *wsConnState) releaseIdleBuffers() {
	releaseIdleBuffer(&w.buf)
	releaseIdleBuffer(&w.gateBuf)
	if !w.fragmentBuf.active {
		releaseIdleBuffer(&w.fragmentBuf.payload)
	}
}

func releaseIdleBuffer(buf *bytes.Buffer) {
	if buf.Len() != 0 {
		return
	}
	if buf.Cap() > maxReusableBufferCap {
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
