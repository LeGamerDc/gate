package gate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestWebSocketServerAcceptsMultipleGateFramesInSingleBinaryMessage(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					_ = conn.SendNoEncrypt(msg)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()

	frame1 := buildFrame([]byte("first"), 0)
	frame2 := buildFrame([]byte("second"), 0)
	if err := wsutil.WriteClientBinary(conn, append(frame1, frame2...)); err != nil {
		t.Fatal(err)
	}

	got := waitWebSocketMessages(t, conn, nil, 2)
	if !bytes.Equal(got[0], []byte("first")) {
		t.Fatalf("unexpected first payload: %q", got[0])
	}
	if !bytes.Equal(got[1], []byte("second")) {
		t.Fatalf("unexpected second payload: %q", got[1])
	}
}

func TestWebSocketServerAcceptsFragmentedBinaryMessage(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					_ = conn.SendNoEncrypt(msg)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()

	frame := buildFrame([]byte("fragmented"), 0)
	first := ws.MaskFrame(ws.NewFrame(ws.OpBinary, false, frame[:2]))
	second := ws.MaskFrame(ws.NewFrame(ws.OpContinuation, true, frame[2:]))
	if err := ws.WriteFrame(conn, first); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteFrame(conn, second); err != nil {
		t.Fatal(err)
	}

	got := waitWebSocketMessages(t, conn, nil, 1)
	if !bytes.Equal(got[0], []byte("fragmented")) {
		t.Fatalf("unexpected fragmented payload: %q", got[0])
	}
}

func TestWebSocketServerPreservesRemainderWhenAsyncDoBlocksSingleBinaryMessage(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					if bytes.Equal(msg, []byte("first")) {
						conn.AsyncDo(func() {
							time.Sleep(100 * time.Millisecond)
							_ = conn.SendNoEncrypt(msg)
						})
						return
					}
					_ = conn.SendNoEncrypt(msg)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()

	frame1 := buildFrame([]byte("first"), 0)
	frame2 := buildFrame([]byte("second"), 0)
	if err := wsutil.WriteClientBinary(conn, append(frame1, frame2...)); err != nil {
		t.Fatal(err)
	}

	got := waitWebSocketMessages(t, conn, nil, 2)
	if !bytes.Equal(got[0], []byte("first")) {
		t.Fatalf("unexpected first payload: %q", got[0])
	}
	if !bytes.Equal(got[1], []byte("second")) {
		t.Fatalf("unexpected second payload: %q", got[1])
	}
}

func TestWebSocketServerAcceptsGateFrameSplitAcrossBinaryMessages(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					_ = conn.SendNoEncrypt(msg)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()

	frame := buildFrame([]byte("split-binary-messages"), 0)
	cut := len(frame) / 2
	if err := wsutil.WriteClientBinary(conn, frame[:cut]); err != nil {
		t.Fatal(err)
	}
	if err := wsutil.WriteClientBinary(conn, frame[cut:]); err != nil {
		t.Fatal(err)
	}

	got := waitWebSocketMessages(t, conn, nil, 1)
	if !bytes.Equal(got[0], []byte("split-binary-messages")) {
		t.Fatalf("unexpected split payload: %q", got[0])
	}
}

func TestWebSocketServerRejectsUnexpectedPath(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
		}},
	})

	conn, _, _, err := ws.Dial(context.Background(), "ws://"+srv.Addr()+"/bad")
	if err == nil {
		conn.Close()
		t.Fatal("expected websocket handshake to fail on unexpected path")
	}
}

func TestWebSocketServerClosesOversizedHandshakeBuffer(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		Transport:                  ServerTransportWebSocket,
		WebSocketPath:              "/ws",
		MaxWebSocketHandshakeBytes: 64,
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
		}},
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := "GET /ws HTTP/1.1\r\nHost: localhost\r\nCookie: " + strings.Repeat("a", 128) + "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected oversized handshake connection to close")
	}
}

func TestReleaseIdleBuffersDropsOversizedCapacityOnceIdle(t *testing.T) {
	state := newOversizedState()

	for i := 0; i < wsIdleReleasePasses; i++ {
		state.releaseIdleBuffers()
	}

	if state.buf.Cap() > maxReusableBufferCap {
		t.Fatalf("buf retained oversized capacity: %d", state.buf.Cap())
	}
	if state.gateBuf.Cap() > maxReusableBufferCap {
		t.Fatalf("gateBuf retained oversized capacity: %d", state.gateBuf.Cap())
	}
	if state.fragmentBuf.payload.Cap() > maxReusableBufferCap {
		t.Fatalf("fragment payload retained oversized capacity: %d", state.fragmentBuf.payload.Cap())
	}
}

// 反向用例：旧实现每个 traffic 事件都会把超大缓冲整块丢掉，一个稳定收发大
// 消息的连接因此在每条消息上都要从零重新扩容。
func TestReleaseIdleBuffersKeepsCapacityWhileBusy(t *testing.T) {
	state := newOversizedState()

	// 单次空闲不足以触发收缩。
	state.releaseIdleBuffers()
	if state.buf.Cap() <= maxReusableBufferCap {
		t.Fatal("buf was dropped after a single idle pass")
	}

	// 每一轮都真的用到了大缓冲：无论跑多少轮都不应该收缩。
	for i := 0; i < 4*wsIdleReleasePasses; i++ {
		state.peakBuffered = maxReusableBufferCap + 1
		state.releaseIdleBuffers()
	}
	if state.buf.Cap() <= maxReusableBufferCap {
		t.Fatal("buf was dropped while the connection kept using it")
	}
	if state.gateBuf.Cap() <= maxReusableBufferCap {
		t.Fatal("gateBuf was dropped while the connection kept using it")
	}
}

func newOversizedState() *wsConnState {
	// releaseIdleBuffers 会通过 conn 读取活跃时间来决定是否提前收缩，
	// 所以这里必须挂一个真实的 Conn，而不是零值 wsConnState。
	conn := &Conn{codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()
	state := &wsConnState{conn: conn}
	fillBuffer(&state.buf, maxReusableBufferCap+1024)
	fillBuffer(&state.gateBuf, maxReusableBufferCap+1024)
	fillBuffer(&state.fragmentBuf.payload, maxReusableBufferCap+1024)
	state.fragmentBuf.active = false

	state.buf.Reset()
	state.gateBuf.Reset()
	state.fragmentBuf.payload.Reset()
	return state
}

func TestWebSocketHandleFrameRejectsBufferedOverflowOnFragmentCopy(t *testing.T) {
	state := &wsConnState{
		conn:             &Conn{},
		upgraded:         true,
		maxBufferedBytes: 64,
	}
	fillBuffer(&state.buf, 40)
	fillBuffer(&state.gateBuf, 10)

	err := state.handleFrame(wsFrame{
		header:  ws.Header{OpCode: ws.OpBinary, Fin: false},
		payload: bytes.Repeat([]byte("x"), 20),
	})
	if !errors.Is(err, errWebSocketBufferLimit) {
		t.Fatalf("expected websocket buffer limit error, got %v", err)
	}
	if state.fragmentBuf.active {
		t.Fatal("fragment buffer should stay inactive when frame is rejected")
	}
}

func TestWebSocketHandleFrameRejectsBufferedOverflowOnGateCopy(t *testing.T) {
	state := &wsConnState{
		conn:             &Conn{},
		upgraded:         true,
		maxBufferedBytes: 96,
	}
	fillBuffer(&state.buf, 50)
	fillBuffer(&state.gateBuf, 30)

	err := state.handleFrame(wsFrame{
		header:  ws.Header{OpCode: ws.OpBinary, Fin: true},
		payload: bytes.Repeat([]byte("y"), 20),
	})
	if !errors.Is(err, errWebSocketBufferLimit) {
		t.Fatalf("expected websocket buffer limit error, got %v", err)
	}
	if state.gateBuf.Len() != 30 {
		t.Fatalf("gate buffer mutated after rejected frame: %d", state.gateBuf.Len())
	}
}

// wsOutbound 自己编码帧头以便和 payload 一次写出，这里校验它和 gobwas 生成的
// 头逐字节一致。
func TestEncodeWSBinaryHeaderMatchesGobwas(t *testing.T) {
	for _, size := range []int{0, 1, 125, 126, 127, 65535, 65536, 1 << 20} {
		payload := make([]byte, size)
		want := ws.MustCompileFrame(ws.NewBinaryFrame(payload))

		var hdr [wsMaxServerHeaderSize]byte
		n := encodeWSBinaryHeader(hdr[:], size)
		if !bytes.Equal(hdr[:n], want[:len(want)-size]) {
			t.Fatalf("size %d: header % x, want % x", size, hdr[:n], want[:len(want)-size])
		}
	}
}

// wsOutbound 必须把帧头和 payload 合成一次写：write 是 2 个 iovec，
// writev 是 1+n 个，都不再额外拷贝 payload。
func TestWSOutboundWritesHeaderAndPayloadInOneCall(t *testing.T) {
	rec := &recordingWSWriter{}
	out := &wsOutbound{conn: rec}

	if err := out.write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if rec.writes != 0 {
		t.Fatalf("write used %d plain Write call(s)", rec.writes)
	}
	if rec.writevs != 1 {
		t.Fatalf("write used %d Writev call(s), want 1", rec.writevs)
	}
	if got := rec.lastIOV; got != 2 {
		t.Fatalf("write produced %d iovecs, want 2", got)
	}
	if !bytes.Equal(rec.buf.Bytes(), ws.MustCompileFrame(ws.NewBinaryFrame([]byte("hello")))) {
		t.Fatalf("unexpected framing: % x", rec.buf.Bytes())
	}

	rec.reset()
	if err := out.writev([][]byte{[]byte("ab"), []byte("cd"), []byte("ef")}); err != nil {
		t.Fatal(err)
	}
	if rec.writevs != 1 {
		t.Fatalf("writev used %d Writev call(s), want 1", rec.writevs)
	}
	if got := rec.lastIOV; got != 4 {
		t.Fatalf("writev produced %d iovecs, want 4 (header + 3 payload slices)", got)
	}
	if !bytes.Equal(rec.buf.Bytes(), ws.MustCompileFrame(ws.NewBinaryFrame([]byte("abcdef")))) {
		t.Fatalf("unexpected framing: % x", rec.buf.Bytes())
	}
}

type recordingWSWriter struct {
	buf     bytes.Buffer
	writes  int
	writevs int
	lastIOV int
}

func (r *recordingWSWriter) Write(p []byte) (int, error) {
	r.writes++
	return r.buf.Write(p)
}

func (r *recordingWSWriter) Writev(bs [][]byte) (int, error) {
	r.writevs++
	r.lastIOV = len(bs)
	n := 0
	for _, b := range bs {
		m, err := r.buf.Write(b)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (r *recordingWSWriter) reset() {
	r.buf.Reset()
	r.writes, r.writevs, r.lastIOV = 0, 0, 0
}

func startTestWebSocketServer(t *testing.T, cfg *Config) *Server {
	t.Helper()

	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.LoopCount == 0 {
		cfg.LoopCount = 1
	}
	if cfg.SB == nil {
		cfg.SB = DefaultSenderBuilder
	}
	if cfg.Logger == nil {
		cfg.Logger = noopTestLogger{}
	}
	cfg.Transport = ServerTransportWebSocket
	if cfg.WebSocketPath == "" {
		cfg.WebSocketPath = "/"
	}

	srv, err := StartServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			t.Errorf("stop websocket server: %v", err)
		}
		if err := srv.Wait(); err != nil {
			t.Errorf("wait websocket server: %v", err)
		}
	})

	return srv
}

func dialTestWebSocketConn(t *testing.T, addr, path string) net.Conn {
	t.Helper()

	conn, br, _, err := ws.Dial(context.Background(), "ws://"+addr+path)
	if err != nil {
		t.Fatal(err)
	}
	if br != nil && br.Buffered() > 0 {
		return &bufferedConn{
			Conn: conn,
			r:    io.MultiReader(br, conn),
		}
	}
	return conn
}

func waitWebSocketMessages(t *testing.T, conn net.Conn, cipher Cipher, want int) [][]byte {
	t.Helper()

	msgs := make([][]byte, 0, want)
	for len(msgs) < want {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		payload, err := wsutil.ReadServerBinary(conn)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeWebSocketGatePayload(payload, cipher)
		if err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, decoded...)
	}
	return msgs[:want]
}

// decodeWebSocketGatePayload 用 client 侧的 codec 策略解开一条 WS 二进制消息里
// 携带的全部 gate 帧（服务端下行可能压缩、合包、加密）。
func decodeWebSocketGatePayload(data []byte, cipher Cipher) ([][]byte, error) {
	c := clientCodec(maxMessageSize, maxMessageSize)
	dec, err := newZstdDecoder(maxMessageSize)
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	var msgs [][]byte
	for len(data) > 0 {
		f, n, ok, err := c.parse(data)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		if err = c.deliver(f, cipher, dec, func(msg []byte, _ bool) error {
			msgs = append(msgs, append([]byte(nil), msg...))
			return nil
		}); err != nil {
			return nil, err
		}
		data = data[n:]
	}
	return msgs, nil
}

type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func fillBuffer(buf *bytes.Buffer, n int) {
	buf.Grow(n)
	_, _ = buf.Write(bytes.Repeat([]byte("x"), n))
}
