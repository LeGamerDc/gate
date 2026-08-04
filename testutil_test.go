package gate

import (
	"context"
	"testing"
	"time"
)

// 共享测试 harness。
//
// 这些 helper 此前散在 e2e_test.go / gate_audit_test.go / websocket_server_test.go /
// client_audit_test.go 四个文件里，同一件事（起一个测试 server、连一个 client）
// 有三套写法。集中到一处之后，新增用例不必再挑一套抄。

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
