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
	"github.com/klauspost/compress/zstd"
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

func TestReleaseIdleBuffersDropsOversizedCapacity(t *testing.T) {
	state := &wsConnState{}
	fillBuffer(&state.buf, maxReusableBufferCap+1024)
	fillBuffer(&state.gateBuf, maxReusableBufferCap+1024)
	fillBuffer(&state.fragmentBuf.payload, maxReusableBufferCap+1024)
	state.fragmentBuf.active = false

	state.buf.Reset()
	state.gateBuf.Reset()
	state.fragmentBuf.payload.Reset()
	state.releaseIdleBuffers()

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

func decodeWebSocketGatePayload(data []byte, cipher Cipher) ([][]byte, error) {
	var msgs [][]byte
	for len(data) > 0 {
		frame, n, err := consumeFrame(data, maxMessageSize)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeGateFrame(frame, cipher)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, decoded...)
		data = data[n:]
	}
	return msgs, nil
}

func decodeGateFrame(frame inboundFrame, cipher Cipher) ([][]byte, error) {
	data := append([]byte(nil), frame.payload...)
	if frame.e {
		if cipher == nil {
			return nil, errors.New("cipher required")
		}
		cipher.Decrypt(data)
	}
	if frame.z {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		defer dec.Close()
		data, err = dec.DecodeAll(data, nil)
		if err != nil {
			return nil, err
		}
	}
	if !frame.c {
		return [][]byte{data}, nil
	}

	var msgs [][]byte
	for len(data) > 0 {
		sub, n, err := consumeFrame(data, maxMessageSize)
		if err != nil {
			return nil, err
		}
		subMsgs, err := decodeGateFrame(sub, cipher)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, subMsgs...)
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
