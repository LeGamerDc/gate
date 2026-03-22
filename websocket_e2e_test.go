package gate

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestWebSocketClientE2EPlainRoundTrip(t *testing.T) {
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

	client, handler := startTestWebSocketClient(t, "ws://"+srv.Addr()+"/ws", nil)
	msg := []byte("hello websocket gate")
	if err := client.SendNoEncrypt(msg); err != nil {
		t.Fatal(err)
	}

	got := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got, msg) {
		t.Fatalf("unexpected websocket round-trip payload: %q", got)
	}
}

func TestWebSocketClientE2EEncryptedCompressedRoundTrip(t *testing.T) {
	cipher := xorCipher{key: 0x33}
	srv := startTestWebSocketServer(t, &Config{
		Transport:     ServerTransportWebSocket,
		WebSocketPath: "/ws",
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

	client, handler := startTestWebSocketClient(t, "ws://"+srv.Addr()+"/ws", cipher)
	msg := bytes.Repeat([]byte("x"), 8*1024)
	if err := client.Send(msg); err != nil {
		t.Fatal(err)
	}

	got := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got, msg) {
		t.Fatal("unexpected encrypted+compressed websocket round-trip payload")
	}
}

func TestWebSocketClientE2EAsyncDoPreservesOrder(t *testing.T) {
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

	client, handler := startTestWebSocketClient(t, "ws://"+srv.Addr()+"/ws", nil)
	if err := client.SendNoEncrypt([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := client.SendNoEncrypt([]byte("second")); err != nil {
		t.Fatal(err)
	}

	got1 := waitMessage(t, handler.msgCh)
	got2 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got1, []byte("first")) {
		t.Fatalf("unexpected first websocket message: %q", got1)
	}
	if !bytes.Equal(got2, []byte("second")) {
		t.Fatalf("unexpected second websocket message: %q", got2)
	}
}

func startTestWebSocketClient(t *testing.T, url string, cipher Cipher) (*Client, *testClientHandler) {
	t.Helper()

	handler := &testClientHandler{
		msgCh:     make(chan []byte, 16),
		closeCh:   make(chan error, 1),
		connectCh: make(chan struct{}, 1),
	}
	client, err := DialWebSocketContext(context.Background(), &WebSocketClientConfig{
		URL:     url,
		Handler: handler,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = client.Close()
		if err := client.Wait(); err != nil {
			t.Errorf("wait websocket client: %v", err)
		}
	})

	return client, handler
}
