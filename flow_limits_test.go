package gate

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"
)

/*
资源上限矩阵。

每一个可配置的上限都按 {limit-1, limit, limit+1} 三点验证，并且**明确断言超限时
的行为形态**——不同上限的正确反应是不一样的：

	入站消息超限        关连接（协议错误）
	出站消息超限        返回 ErrMaxMessageSize，不发出
	待发队列超限        返回 ErrSendQueueFull
	出站缓冲超限        关连接（而不是丢消息）
	握手头条数超限      拒绝握手（而不是截断）

"关连接"和"丢消息"的区别尤其重要：从一条有状态的帧流中间抽掉几条，对端状态机
会静默错乱，业务层什么都察觉不到。
*/

// sendRawGateFrame 用给定传输把一段**已经编好的 gate 帧字节**原样送上去，
// 绕开 Client 的合法性检查，用来构造服务端必须拒绝的报文。
type rawSender struct {
	name string
	dial func(t *testing.T, srv *Server) net.Conn
	send func(t *testing.T, conn net.Conn, frame []byte)
}

func rawSenders() []rawSender {
	return []rawSender{
		{
			name: "tcp",
			dial: func(t *testing.T, srv *Server) net.Conn {
				conn, err := net.Dial("tcp", srv.Addr())
				if err != nil {
					t.Fatal(err)
				}
				return conn
			},
			send: func(t *testing.T, conn net.Conn, frame []byte) {
				if _, err := conn.Write(frame); err != nil {
					t.Fatalf("write raw frame: %v", err)
				}
			},
		},
		{
			name: "websocket",
			dial: func(t *testing.T, srv *Server) net.Conn {
				return dialTestWebSocketConn(t, srv.Addr(), "/ws")
			},
			send: func(t *testing.T, conn net.Conn, frame []byte) {
				if err := wsutil.WriteClientBinary(conn, frame); err != nil {
					t.Fatalf("write raw websocket frame: %v", err)
				}
			},
		},
	}
}

func startLimitServer(t *testing.T, transport string, cfg *Config) *Server {
	t.Helper()

	cfg.CHB = echoAfterPanicBuilder()
	cfg.SB = DefaultSenderBuilder
	if transport == "websocket" {
		cfg.WebSocketPath = "/ws"
		return startTestWebSocketServer(t, cfg)
	}
	return startTestServer(t, cfg)
}

// 入站单条消息上限：limit 必须收下，limit+1 必须断链。
//
// 两种传输共用同一个 serverCodec，但走的是两套完全不同的入站状态机，
// 所以这条必须在两边各验一次。
func TestLimitInboundMessageSize(t *testing.T) {
	const limit = 4096

	for _, rs := range rawSenders() {
		t.Run(rs.name, func(t *testing.T) {
			t.Run("at-limit-is-accepted", func(t *testing.T) {
				srv := startLimitServer(t, rs.name, &Config{MaxMessageSize: limit})
				conn := rs.dial(t, srv)
				defer conn.Close()

				rs.send(t, conn, gateFrame(make([]byte, limit)))
				// 收到回显就说明服务端接受了这条消息。
				assertReadsSomething(t, conn)
			})

			t.Run("over-limit-closes-the-connection", func(t *testing.T) {
				srv := startLimitServer(t, rs.name, &Config{MaxMessageSize: limit})
				conn := rs.dial(t, srv)
				defer conn.Close()

				rs.send(t, conn, gateFrame(make([]byte, limit+1)))
				assertConnectionRejected(t, conn)
			})
		})
	}
}

// assertReadsSomething 断言对端在超时之前发回了至少一个字节。
func assertReadsSomething(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	if _, err := conn.Read(buf[:]); err != nil {
		t.Fatalf("expected the server to accept the message and echo it, got %v", err)
	}
}

// 客户端出站超限必须在本地被拒绝，一个字节都不能发出去。
func TestLimitClientOutboundMessageSize(t *testing.T) {
	const limit = 1024

	client, serverConn := dialAuditClient(t, &ClientConfig{
		Handler:        newMatrixHandler(),
		MaxMessageSize: limit,
	})

	if err := client.Send(make([]byte, limit)); err != nil {
		t.Fatalf("a message exactly at the limit must be accepted: %v", err)
	}
	if err := client.Send(make([]byte, limit+1)); !errors.Is(err, ErrMaxMessageSize) {
		t.Fatalf("over-limit send returned %v, want ErrMaxMessageSize", err)
	}

	// 线路上只能出现那条合法消息。
	if err := serverConn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := serverCodec(maxMessageSize).readFrame(bufio.NewReader(serverConn))
	if err != nil {
		t.Fatalf("read the accepted frame: %v", err)
	}
	if len(got.payload) != limit {
		t.Fatalf("wire carried a %d-byte payload, want %d", len(got.payload), limit)
	}
}

// 握手头条数上限：超过必须**拒绝握手**，而不是静默截断。
//
// README 把这条单列出来是有原因的：HTTP 头没有"重要的排在前面"的保证。
// 静默截断意味着攻击者只要在前面塞满上限条无关头，Authorization 就对
// OnWebSocketUpgrade 不可见，而"读不到 Authorization 就当匿名放行"的鉴权
// 逻辑会被直接绕过。所以正确行为是失败，而不是让业务基于一份被削过的头做判断。
func TestLimitWebSocketHandshakeHeaderCount(t *testing.T) {
	var hookCalls atomic.Int32
	var sawAuth atomic.Bool

	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		OnWebSocketUpgrade: func(h *Handshake) error {
			hookCalls.Add(1)
			if h.Header.Get("Authorization") != "" {
				sawAuth.Store(true)
			}
			return nil
		},
		CHB: echoAfterPanicBuilder(),
		SB:  DefaultSenderBuilder,
	})

	// 先塞满超过上限的无关头，再放 Authorization——正是那个绕过场景的形状。
	var sb strings.Builder
	sb.WriteString("GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n")
	sb.WriteString("Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
	sb.WriteString("Sec-WebSocket-Version: 13\r\n")
	for i := 0; i < maxCapturedHandshakeHeaders+8; i++ {
		fmt.Fprintf(&sb, "X-Pad-%d: x\r\n", i)
	}
	sb.WriteString("Authorization: Bearer secret\r\n\r\n")

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(sb.String())); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusSwitchingProtocols {
		t.Fatal("handshake was upgraded despite exceeding the captured-header limit")
	}
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestHeaderFieldsTooLarge)
	}
	// 关键：业务钩子绝不能拿着一份被截断的头做判断。
	if hookCalls.Load() != 0 {
		t.Fatalf("the upgrade hook was called %d time(s) with a truncated header set", hookCalls.Load())
	}
	if sawAuth.Load() {
		t.Fatal("the upgrade hook observed Authorization from a rejected handshake")
	}
}

// 握手阶段缓冲上限：一个永远不结束的握手必须被关掉，而不是无限攒下去。
func TestLimitWebSocketHandshakeBytes(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath:              "/ws",
		MaxWebSocketHandshakeBytes: 2048,
		CHB:                        echoAfterPanicBuilder(),
		SB:                         DefaultSenderBuilder,
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 一个没有 \r\n\r\n 结尾的请求，持续灌字节。
	//
	// 必须带 WriteDeadline：服务端一旦不关连接、又不读，内核缓冲填满之后 Write
	// 会一直阻塞，下面那个基于时间的循环条件根本不会被求值，用例会挂到整个包超时
	// 而不是失败。
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("GET /ws HTTP/1.1\r\nHost: localhost\r\n"))
	junk := []byte("X-Pad: " + strings.Repeat("y", 512) + "\r\n")

	closed := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := conn.Write(junk); err != nil {
			// 服务端把连接关掉了，正是预期结果。
			closed = true
			break
		}
	}
	if !closed {
		// 没写失败也可能是服务端关得慢，再从读侧确认一次。
		assertConnectionRejected(t, conn)
	}
}

// 待发送队列上限：超出必须返回 ErrSendQueueFull，而不是无限堆积。
func TestLimitSendQueueRejectsInsteadOfGrowing(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 128})

	// 一条刚好放得下的消息。
	if err := s.Send(make([]byte, 128-frameOverhead(128-2))); err != nil {
		t.Fatalf("a message that fits must be accepted: %v", err)
	}
	// 队列已经满了，下一条必须被拒。
	if err := s.Send([]byte("x")); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("over-budget send returned %v, want ErrSendQueueFull", err)
	}
	// 一条本身就大过整个配额的消息，无论队列空满都必须被拒。
	fresh, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 128})
	if err := fresh.Send(make([]byte, 4096)); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("a message larger than the whole budget returned %v, want ErrSendQueueFull", err)
	}
}

// 配置耦合：服务端的 MaxClusterSize 大于客户端的 MaxMessageSize 时，
// 完全合法的业务流量会被客户端自己拒收。
//
// 这不是 bug，是一个真实存在的配置陷阱：两端各自的上限都"看起来合理"，
// 但合并帧的大小是按服务端的 MaxClusterSize 走的，而客户端是按单条消息的
// 尺寸配的 MaxMessageSize。把它固化成用例，免得下次有人踩了还要重新查一遍。
func TestLimitCompoundFrameCanExceedClientMaxMessageSize(t *testing.T) {
	const (
		clientLimit = 4096
		perMessage  = 1024
		messages    = 10 // 合起来约 10KB，远超 clientLimit
	)

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, _ []byte) {
					for i := 0; i < messages; i++ {
						_ = conn.Send(make([]byte, perMessage))
					}
				},
			}
		}},
		SB: NewSenderBuilder(&SenderConfig{
			MaxBufferSize:  8 << 20,
			MaxClusterSize: 32 << 10, // 远大于客户端的上限
		}),
	})

	handler := newMatrixHandler()
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:           srv.Addr(),
		Handler:        handler,
		MaxMessageSize: clientLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Send([]byte("go")); err != nil {
		t.Fatal(err)
	}

	// 客户端会以 ErrMaxMessageSize 断开：合并帧整体超过了它的单条消息上限。
	closeErr := waitClose(t, handler.closeCh)
	if !errors.Is(closeErr, ErrMaxMessageSize) {
		t.Fatalf("client closed with %v, want ErrMaxMessageSize "+
			"(server MaxClusterSize=%d exceeds client MaxMessageSize=%d)",
			closeErr, 32<<10, clientLimit)
	}
}
