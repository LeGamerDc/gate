package gate

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

func TestE2EPlainRoundTrip(t *testing.T) {
	srv := startTestServer(t, &Config{
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

	client, handler := startTestClient(t, srv.Addr(), nil)
	msg := []byte("hello gate")
	if err := client.SendNoEncrypt(msg); err != nil {
		t.Fatal(err)
	}

	got := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got, msg) {
		t.Fatalf("unexpected round-trip payload: %q", got)
	}
}

func TestE2EEncryptedCompressedRoundTrip(t *testing.T) {
	cipher := xorCipher{key: 0x33}
	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			conn.UpdateCipher(cipher)
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					_ = conn.Send(msg)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 1,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	client, handler := startTestClient(t, srv.Addr(), cipher)
	msg := bytes.Repeat([]byte("x"), 8*1024)
	if err := client.Send(msg); err != nil {
		t.Fatal(err)
	}

	got := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got, msg) {
		t.Fatal("unexpected encrypted+compressed round-trip payload")
	}
}

func TestE2ESendCanUseBorrowedRawWithoutManualCopy(t *testing.T) {
	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					want := append([]byte(nil), raw...)
					_ = conn.SendNoEncrypt(raw)
					if len(raw) > 0 {
						raw[0] ^= 0xff
					}
					_ = conn.SendNoEncrypt(want)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	client, handler := startTestClient(t, srv.Addr(), nil)
	msg := []byte("borrowed buffer")
	if err := client.SendNoEncrypt(msg); err != nil {
		t.Fatal(err)
	}

	got1 := waitMessage(t, handler.msgCh)
	got2 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got1, msg) {
		t.Fatalf("unexpected first copied payload: %q", got1)
	}
	if !bytes.Equal(got2, msg) {
		t.Fatalf("unexpected second copied payload: %q", got2)
	}
}

func TestE2ESendEncryptedLeavesCallerBufferUntouched(t *testing.T) {
	cipher := xorCipher{key: 0x2a}
	payload := []byte("shared caller payload")
	want := append([]byte(nil), payload...)

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			conn.UpdateCipher(cipher)
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					_ = conn.Send(payload)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	client, handler := startTestClient(t, srv.Addr(), cipher)
	if err := client.SendNoEncrypt([]byte("trigger")); err != nil {
		t.Fatal(err)
	}

	got := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected encrypted payload: %q", got)
	}
	if !bytes.Equal(payload, want) {
		t.Fatal("Send mutated caller-owned payload")
	}
}

func TestE2ESendSharedSkipsEncryptAndLeavesSharedBufferUntouched(t *testing.T) {
	cipher := xorCipher{key: 0x44}
	shared := []byte("shared payload")
	want := append([]byte(nil), shared...)

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			conn.UpdateCipher(cipher)
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					_ = conn.SendStatic(shared, false)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 1,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    32 * 1024,
		}),
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write(buildFrame([]byte("trigger"), 0)); err != nil {
		t.Fatal(err)
	}

	frame := readTestFrame(t, conn)
	if frame.e {
		t.Fatal("shared frame should not be encrypted")
	}
	if frame.z {
		t.Fatal("shared frame should not be compressed")
	}
	if frame.c {
		t.Fatal("shared frame should not be compound")
	}
	if !bytes.Equal(frame.payload, want) {
		t.Fatal("unexpected shared frame payload")
	}
	if !bytes.Equal(shared, want) {
		t.Fatal("shared payload was mutated")
	}
}

func TestE2ESendSharedCompressedSetsFlagWithoutMutatingPayload(t *testing.T) {
	cipher := xorCipher{key: 0x55}
	plain := bytes.Repeat([]byte("z"), 4096)
	shared := enc.EncodeAll(plain, nil)
	want := append([]byte(nil), shared...)

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			conn.UpdateCipher(cipher)
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					_ = conn.SendStatic(shared, true)
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 1,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    32 * 1024,
		}),
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write(buildFrame([]byte("trigger"), 0)); err != nil {
		t.Fatal(err)
	}

	frame := readTestFrame(t, conn)
	if frame.e {
		t.Fatal("shared compressed frame should not be encrypted")
	}
	if !frame.z {
		t.Fatal("shared compressed frame should keep compressed flag")
	}
	if frame.c {
		t.Fatal("shared compressed frame should not be compound")
	}
	if !bytes.Equal(frame.payload, want) {
		t.Fatal("unexpected shared compressed payload")
	}
	if !bytes.Equal(shared, want) {
		t.Fatal("shared compressed payload was mutated")
	}
}

func TestE2EAsyncDoPreservesOrder(t *testing.T) {
	srv := startTestServer(t, &Config{
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

	client, handler := startTestClient(t, srv.Addr(), nil)
	if err := client.SendNoEncrypt([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := client.SendNoEncrypt([]byte("second")); err != nil {
		t.Fatal(err)
	}

	got1 := waitMessage(t, handler.msgCh)
	got2 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got1, []byte("first")) {
		t.Fatalf("unexpected first message: %q", got1)
	}
	if !bytes.Equal(got2, []byte("second")) {
		t.Fatalf("unexpected second message: %q", got2)
	}
}

func TestE2EAsyncDoReentrantPreservesOrder(t *testing.T) {
	fastReady := make(chan struct{})
	slowReady := make(chan struct{})

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					msg := append([]byte(nil), raw...)
					if bytes.Equal(msg, []byte("first")) {
						conn.AsyncDo(func() {
							<-fastReady
							_ = conn.SendNoEncrypt([]byte("fast"))
						})
						conn.AsyncDo(func() {
							<-slowReady
							_ = conn.SendNoEncrypt([]byte("slow"))
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

	client, handler := startTestClient(t, srv.Addr(), nil)
	if err := client.SendNoEncrypt([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := client.SendNoEncrypt([]byte("second")); err != nil {
		t.Fatal(err)
	}

	close(fastReady)
	got1 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got1, []byte("fast")) {
		t.Fatalf("unexpected first reentrant AsyncDo message: %q", got1)
	}

	select {
	case msg := <-handler.msgCh:
		t.Fatalf("unexpected message before slow AsyncDo completed: %q", msg)
	case <-time.After(100 * time.Millisecond):
	}

	close(slowReady)
	got2 := waitMessage(t, handler.msgCh)
	got3 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got2, []byte("slow")) {
		t.Fatalf("unexpected second reentrant AsyncDo message: %q", got2)
	}
	if !bytes.Equal(got3, []byte("second")) {
		t.Fatalf("unexpected third reentrant AsyncDo message: %q", got3)
	}
}

type testHandlerBuilder struct {
	build func(*Conn) ConnHandler
}

func (b testHandlerBuilder) Build(conn *Conn) ConnHandler {
	return b.build(conn)
}

type testConnHandler struct {
	conn   *Conn
	handle func(*Conn, []byte)
	close  func()
}

func (h *testConnHandler) Handle(raw []byte) {
	h.handle(h.conn, raw)
}

func (h *testConnHandler) Close() {
	if h.close != nil {
		h.close()
	}
}

func startTestServer(t *testing.T, cfg *Config) *Server {
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

	srv, err := StartServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			t.Errorf("stop server: %v", err)
		}
		if err := srv.Wait(); err != nil {
			t.Errorf("wait server: %v", err)
		}
	})

	return srv
}

func startTestClient(t *testing.T, addr string, cipher Cipher) (*Client, *testClientHandler) {
	t.Helper()

	handler := &testClientHandler{
		msgCh:     make(chan []byte, 16),
		closeCh:   make(chan error, 1),
		connectCh: make(chan struct{}, 1),
	}
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    addr,
		Handler: handler,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = client.Close()
		if err := client.Wait(); err != nil {
			t.Errorf("wait client: %v", err)
		}
	})

	return client, handler
}

type noopTestLogger struct{}

func (noopTestLogger) Debugf(string, ...interface{}) {}
func (noopTestLogger) Infof(string, ...interface{})  {}
func (noopTestLogger) Warnf(string, ...interface{})  {}
func (noopTestLogger) Errorf(string, ...interface{}) {}
func (noopTestLogger) Fatalf(string, ...interface{}) {}
