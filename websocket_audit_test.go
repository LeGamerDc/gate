package gate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// 服务端策略只允许对端加密：带 z / c 标记的上行帧必须被拒绝并关闭连接。
//
// 旧的 WS 解析器直接无视这两个标记，于是客户端只要置一位 flag，就能让业务层
// 拿到一段未解压的原始字节，或者一个没有被展开的 compound blob。
func TestWebSocketServerRejectsCompressedUplinkFrame(t *testing.T) {
	assertUplinkFlagRejected(t, maskZ)
}

func TestWebSocketServerRejectsCompoundUplinkFrame(t *testing.T) {
	assertUplinkFlagRejected(t, maskC)
}

func assertUplinkFlagRejected(t *testing.T, flag byte) {
	t.Helper()

	delivered := make(chan []byte, 1)
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(_ *Conn, raw []byte) {
				delivered <- append([]byte(nil), raw...)
			}}
		}},
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()

	// payload 本身完全合法，只有 header 上的 flag 是对端不该设置的。
	if err := wsutil.WriteClientBinary(conn, buildFrame([]byte("payload"), flag)); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed after a disallowed uplink flag")
	}

	select {
	case msg := <-delivered:
		t.Fatalf("frame with disallowed flag reached the handler: %q", msg)
	default:
	}
}

func TestWebSocketUpgradeHookSeesRequestURIAndHeaders(t *testing.T) {
	type capture struct {
		uri   string
		auth  string
		xff   string
		proto string
		host  string
	}
	captured := make(chan capture, 1)

	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		OnWebSocketUpgrade: func(h *Handshake) error {
			captured <- capture{
				uri:   h.URI,
				auth:  h.Header.Get("Authorization"),
				xff:   h.Header.Get("X-Forwarded-For"),
				proto: h.Header.Get("Sec-WebSocket-Protocol"),
				host:  h.Header.Get("Host"),
			}
			return nil
		},
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
		}},
	})

	conn := dialTestWebSocketConnWithHeader(t, srv.Addr(), "/ws?token=abc", http.Header{
		"Authorization":          []string{"Bearer secret"},
		"X-Forwarded-For":        []string{"203.0.113.7"},
		"Sec-WebSocket-Protocol": []string{"gate.v1"},
	})
	defer conn.Close()

	var got capture
	select {
	case got = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnWebSocketUpgrade")
	}

	if got.uri != "/ws?token=abc" {
		t.Fatalf("handshake URI = %q", got.uri)
	}
	if got.auth != "Bearer secret" {
		t.Fatalf("Authorization = %q", got.auth)
	}
	// 负载均衡后面取真实来源 IP 的唯一途径。
	if got.xff != "203.0.113.7" {
		t.Fatalf("X-Forwarded-For = %q", got.xff)
	}
	// gobwas 不会把 Sec-WebSocket-Protocol 交给 OnHeader，它走子协议协商回调。
	if got.proto != "gate.v1" {
		t.Fatalf("Sec-WebSocket-Protocol = %q", got.proto)
	}
	if got.host != srv.Addr() {
		t.Fatalf("Host = %q, want %q", got.host, srv.Addr())
	}
}

func TestWebSocketUpgradeHookRejectsHandshake(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			// 普通错误统一按 401 处理，错误详情只进服务端日志。
			name: "plain error",
			err:  errors.New("bad token"),
			want: http.StatusUnauthorized,
		},
		{
			// 钩子也可以自己挑状态码。
			name: "explicit rejection",
			err: ws.RejectConnectionError(
				ws.RejectionStatus(http.StatusForbidden),
				ws.RejectionReason("banned"),
			),
			want: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			built := make(chan struct{}, 1)
			srv := startTestWebSocketServer(t, &Config{
				WebSocketPath:      "/ws",
				OnWebSocketUpgrade: func(*Handshake) error { return tt.err },
				CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
					built <- struct{}{}
					return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
				}},
			})

			conn, _, _, err := ws.Dial(context.Background(), "ws://"+srv.Addr()+"/ws")
			if err == nil {
				conn.Close()
				t.Fatal("expected the upgrade hook to reject the handshake")
			}
			var status ws.StatusError
			if !errors.As(err, &status) {
				t.Fatalf("unexpected handshake error %v (%T)", err, err)
			}
			if int(status) != tt.want {
				t.Fatalf("handshake rejected with %d, want %d", int(status), tt.want)
			}
		})
	}
}

// Handshake 必须在 OnReady 和 Handle 里都能读到：前者用于按鉴权结果下发首包，
// 后者用于每条消息的上下文（真实来源 IP、会话标识等）。
func TestWebSocketHandshakeVisibleToHandler(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &readyEchoHandler{
				conn: conn,
				onReady: func(c *Conn) {
					_ = c.SendNoEncrypt([]byte(handshakeField(c, func(h *Handshake) string { return h.URI })))
				},
				onHandle: func(c *Conn, _ []byte) {
					_ = c.SendNoEncrypt([]byte(handshakeField(c, func(h *Handshake) string {
						return h.Header.Get("Authorization")
					})))
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			CompressThreshold: 0,
			MaxBufferSize:     2 * 1024 * 1024,
			MaxClusterSize:    0,
		}),
	})

	conn := dialTestWebSocketConnWithHeader(t, srv.Addr(), "/ws?room=42", http.Header{
		"Authorization": []string{"Bearer secret"},
	})
	defer conn.Close()

	got := waitWebSocketMessages(t, conn, nil, 1)
	if string(got[0]) != "/ws?room=42" {
		t.Fatalf("OnReady saw handshake URI %q", got[0])
	}

	if err := wsutil.WriteClientBinary(conn, buildFrame([]byte("ping"), 0)); err != nil {
		t.Fatal(err)
	}
	got = waitWebSocketMessages(t, conn, nil, 1)
	if string(got[0]) != "Bearer secret" {
		t.Fatalf("Handle saw Authorization %q", got[0])
	}
}

// 这是"握手完成才算就绪"的回归测试。
//
// 之前 handler 在 OnOpen 里就被构造出来了，那时 WS 握手还没写完：任何在构造
// 阶段发出的数据都会变成一个插进 HTTP 响应中间的 WS 数据帧，直接把连接搞坏。
// 现在只有 OnReady 之后才允许发送，并且 OnReady 保证在 101 已经写出之后触发。
func TestWebSocketOnReadyFiresAfterHandshakeCompletes(t *testing.T) {
	type snapshot struct {
		handshake bool
		writable  bool
	}
	var (
		atBuild = make(chan snapshot, 1)
		atReady = make(chan snapshot, 1)
	)

	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			atBuild <- snapshot{handshake: conn.Handshake() != nil, writable: conn.writable()}
			return &readyEchoHandler{
				conn: conn,
				onReady: func(c *Conn) {
					atReady <- snapshot{handshake: c.Handshake() != nil, writable: c.writable()}
					_ = c.SendNoEncrypt([]byte("welcome"))
				},
				onHandle: func(c *Conn, raw []byte) {
					_ = c.SendNoEncrypt(append([]byte(nil), raw...))
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

	build := waitSnapshot(t, atBuild, "handler build")
	if build.handshake || build.writable {
		t.Fatalf("handler was built on an already-open connection: %+v", build)
	}
	ready := waitSnapshot(t, atReady, "OnReady")
	if !ready.handshake || !ready.writable {
		t.Fatalf("OnReady fired before the handshake completed: %+v", ready)
	}

	// 首包必须是一个完整、可解析的 WS 二进制帧——它没有被塞进握手响应里。
	got := waitWebSocketMessages(t, conn, nil, 1)
	if !bytes.Equal(got[0], []byte("welcome")) {
		t.Fatalf("unexpected OnReady payload: %q", got[0])
	}

	// 连接在首包之后依然正常。
	if err := wsutil.WriteClientBinary(conn, buildFrame([]byte("ping"), 0)); err != nil {
		t.Fatal(err)
	}
	got = waitWebSocketMessages(t, conn, nil, 1)
	if !bytes.Equal(got[0], []byte("ping")) {
		t.Fatalf("unexpected echo payload: %q", got[0])
	}
}

func waitSnapshot[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

func handshakeField(c *Conn, get func(*Handshake) string) string {
	hs := c.Handshake()
	if hs == nil {
		return "<nil handshake>"
	}
	return get(hs)
}

type readyEchoHandler struct {
	conn     *Conn
	onReady  func(*Conn)
	onHandle func(*Conn, []byte)
}

func (h *readyEchoHandler) Handle(raw []byte) { h.onHandle(h.conn, raw) }

func (h *readyEchoHandler) Close() {}

func (h *readyEchoHandler) OnReady() { h.onReady(h.conn) }

var _ ReadyHandler = (*readyEchoHandler)(nil)

func dialTestWebSocketConnWithHeader(t *testing.T, addr, path string, header http.Header) net.Conn {
	t.Helper()

	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(header)}
	conn, br, _, err := dialer.Dial(context.Background(), "ws://"+addr+path)
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
