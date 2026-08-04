package gate

import (
	"bytes"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

/*
完整生命周期与时序。

README 和源码注释对**顺序**做了几条很强的承诺，但此前没有任何测试断言顺序，
只断言了结果：

  - 服务端回调顺序：SenderBuilder.Build → ConnHandlerBuilder.Build → OnReady
    → Handle... → ConnHandler.Close → SenderI.Close
  - WebSocket 下 markOpen 严格早于 notifyReady，notifyReady 又严格早于
    decodeMessages（websocket_state.go 的注释写着"顺序很关键"）
  - Build 阶段入队的消息必须排在 OnReady 阶段入队的消息前面
  - IdleTimeout 的判据是"最后一次交付完整消息"，所以服务端单向下推不算活跃，
    而只发半个帧吊着的 slowloris 会被回收

顺序错了不会让任何"能不能收到消息"的断言失败——只会在生产上表现成偶发的
连接作废或空闲连接堆积。所以这些必须单独测。
*/

// eventLog 记录带顺序的生命周期事件。
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// waitFor 等到日志里出现 want 事件，或超时。
func (l *eventLog) waitFor(t *testing.T, want string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range l.snapshot() {
			if e == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for event %q; got %v", want, l.snapshot())
}

// recordingSenderBuilder 包住内置 sender，只为记录 Build / Close 的时机。
//
// 用嵌入的方式包装是业务的正常用法，也顺带守住一件事：内置 sender 必须自己
// 登记 flushHook，否则被包一层之后握手期入队的消息就再没人排空（见 Conn.flushHook）。
type recordingSenderBuilder struct {
	log   *eventLog
	inner SenderBuilder
}

func (b *recordingSenderBuilder) Build(conn *Conn) SenderI {
	b.log.add("sender.Build")
	return &recordingSender{SenderI: b.inner.Build(conn), log: b.log}
}

type recordingSender struct {
	SenderI
	log *eventLog
}

func (s *recordingSender) Close() {
	s.log.add("sender.Close")
	s.SenderI.Close()
}

// lifecycleHandler 记录 handler 侧的全部回调。
type lifecycleHandler struct {
	conn *Conn
	log  *eventLog
}

func (h *lifecycleHandler) Handle(raw []byte) {
	h.log.add("handle:" + string(raw))
	_ = h.conn.SendNoEncrypt(raw)
}

func (h *lifecycleHandler) Close()   { h.log.add("handler.Close") }
func (h *lifecycleHandler) OnReady() { h.log.add("onReady") }

var _ ReadyHandler = (*lifecycleHandler)(nil)

func lifecycleConfig(log *eventLog) *Config {
	return &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			log.add("handler.Build")
			return &lifecycleHandler{conn: conn, log: log}
		}},
		SB: &recordingSenderBuilder{log: log, inner: DefaultSenderBuilder},
	}
}

// requireOrder 断言 want 里的事件按给定顺序出现在 got 中（允许中间夹杂别的事件）。
func requireOrder(t *testing.T, got []string, want ...string) {
	t.Helper()

	i := 0
	for _, e := range got {
		if i < len(want) && e == want[i] {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("events did not occur in the expected order\n want (in order): %v\n got:             %v",
			want, got)
	}
}

func TestLifecycleCallbackOrderTCP(t *testing.T) {
	log := &eventLog{}
	srv := startTestServer(t, lifecycleConfig(log))

	client, handler := startMatrixTCPClient(t, srv.Addr(), nil)
	if err := client.Send([]byte("m1")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, handler.msgCh)

	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	log.waitFor(t, "sender.Close")

	requireOrder(t, log.snapshot(),
		"sender.Build", "handler.Build", "onReady", "handle:m1", "handler.Close", "sender.Close")
}

func TestLifecycleCallbackOrderWebSocket(t *testing.T) {
	log := &eventLog{}
	cfg := lifecycleConfig(log)
	cfg.WebSocketPath = "/ws"
	srv := startTestWebSocketServer(t, cfg)

	client, handler := startMatrixWSClient(t, "ws://"+srv.Addr()+"/ws", nil)
	if err := client.Send([]byte("m1")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, handler.msgCh)

	if err := client.Close(); err != nil {
		t.Fatalf("close websocket client: %v", err)
	}
	log.waitFor(t, "sender.Close")

	requireOrder(t, log.snapshot(),
		"sender.Build", "handler.Build", "onReady", "handle:m1", "handler.Close", "sender.Close")
}

// Build 阶段发的消息必须排在 OnReady 阶段发的消息前面。
//
// WebSocket 下 Build 发生在握手完成之前，那时连接还不可写；sender 会把消息留在
// 队列里且不 arm 唤醒，靠 notifyReady 里的 flush 排空。顺序错了或者根本没排空，
// 这条用例都会失败。TCP 走的是另一条路径（OnOpen 时就已经可写），同样要保证顺序。
func TestLifecycleBuildStageMessagesPrecedeOnReadyMessages(t *testing.T) {
	for _, tc := range transportCases() {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.start(t, &Config{
				CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
					// Build 阶段就发：WebSocket 下此刻握手响应还没写完。
					if err := conn.SendNoEncrypt([]byte("from-build")); err != nil {
						t.Errorf("send during Build: %v", err)
					}
					return &orderedReadyHandler{conn: conn}
				}},
				SB: DefaultSenderBuilder,
			})

			_, handler := tc.dial(t, srv, nil)
			got := collectMessages(t, handler.msgCh, 2)
			if string(got[0]) != "from-build" || string(got[1]) != "from-ready" {
				t.Fatalf("expected [from-build from-ready], got [%s %s]", got[0], got[1])
			}
		})
	}
}

type orderedReadyHandler struct{ conn *Conn }

func (h *orderedReadyHandler) Handle([]byte) {}
func (h *orderedReadyHandler) Close()        {}
func (h *orderedReadyHandler) OnReady() {
	_ = h.conn.SendNoEncrypt([]byte("from-ready"))
}

// WebSocket 握手响应必须完整写出，中间不能被 WS 数据帧字节插进来。
//
// 失败模式是那条已经写进源码注释的：一个 WS 数据帧插进还没写完的 HTTP 握手
// 响应里，连接直接作废。这里用裸 socket 看字节。
//
// 局限说清楚：它只检查第一次 Read 读到的是一个以 101 开头、且头部完整结束的
// 响应，并不解析后面的数据帧、也不断言帧的条数——"握手之后一条业务帧都没发"
// 同样能通过。业务帧本身的顺序由
// TestLifecycleBuildStageMessagesPrecedeOnReadyMessages 负责。
func TestLifecycleHandshakeResponsePrecedesDataFrames(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			_ = conn.SendNoEncrypt([]byte("early"))
			return &orderedReadyHandler{conn: conn}
		}},
		SB: DefaultSenderBuilder,
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n" +
		"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	got := buf[:n]

	if !bytes.HasPrefix(got, []byte("HTTP/1.1 101")) {
		t.Fatalf("expected a 101 handshake response first, got %q", firstLine(got))
	}
	// 响应头必须完整结束之后才允许出现数据帧字节。
	if idx := bytes.Index(got, []byte("\r\n\r\n")); idx < 0 {
		t.Fatalf("handshake response was truncated by frame bytes: %q", got)
	}
}

func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\r'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

// 客户端 Close 之后，服务端必须走完整条拆解链路。
// 反复 Close 必须幂等，不 panic 也不重复触发回调。
func TestLifecycleCloseIsIdempotent(t *testing.T) {
	log := &eventLog{}
	srv := startTestServer(t, lifecycleConfig(log))

	client, handler := startMatrixTCPClient(t, srv.Addr(), nil)
	if err := client.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, handler.msgCh)

	for i := 0; i < 3; i++ {
		if err := client.Close(); err != nil {
			t.Fatalf("close #%d: %v", i, err)
		}
	}
	log.waitFor(t, "sender.Close")

	// 服务端的拆解回调各自只能发生一次。
	events := log.snapshot()
	if n := countEvent(events, "handler.Close"); n != 1 {
		t.Fatalf("handler.Close fired %d times, want 1 (%v)", n, events)
	}
	if n := countEvent(events, "sender.Close"); n != 1 {
		t.Fatalf("sender.Close fired %d times, want 1 (%v)", n, events)
	}
}

func countEvent(events []string, want string) int {
	n := 0
	for _, e := range events {
		if e == want {
			n++
		}
	}
	return n
}

// IdleTimeout 的判据是"最后一次交付完整消息"，所以只发了半个帧就吊着的连接
// 必须被回收——这正是 slowloris 的形状。
func TestLifecycleIdleTimeoutReapsHalfFrameSlowloris(t *testing.T) {
	srv := startTestServer(t, &Config{
		IdleTimeout: 2 * time.Second,
		CHB:         echoAfterPanicBuilder(),
		SB:          DefaultSenderBuilder,
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 半个 header：永远凑不成一个完整帧，所以永远不会有消息被交付。
	if _, err := conn.Write([]byte{0x00}); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	if _, err := conn.Read(buf[:]); err == nil {
		t.Fatal("expected the idle sweep to close the connection")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("a half-frame slowloris survived IdleTimeout")
	}
}

// 服务端单向下推不能让连接显得活跃：IdleTimeout 判的是"收到了完整消息"，
// 不是"这条连接上有过流量"。
//
// 这条承诺很容易在重构中被破坏——只要有人把 markActive 挪到 onTraffic 入口，
// 或者在发送路径上顺手 touch 一下，一条纯下推的连接就再也不会被回收了。
func TestLifecycleServerPushDoesNotKeepConnectionAlive(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	srv := startTestServer(t, &Config{
		IdleTimeout: 2 * time.Second,
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			// 持续下推，直到连接被回收。
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if err := conn.SendNoEncrypt([]byte("push")); err != nil {
							return // 连接已被回收，正是预期结果
						}
					}
				}
			}()
			return &orderedReadyHandler{conn: conn}
		}},
		SB: DefaultSenderBuilder,
	})

	handler := newMatrixHandler()
	client, err := DialContext(t.Context(), &ClientConfig{
		Addr:    srv.Addr(),
		Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = client.Wait()
	}()

	// 先确认下推确实在发生（否则这条用例可能因为"根本没推"而假通过）。
	waitMessage(t, handler.msgCh)

	// 客户端一言不发，连接必须仍然被空闲扫描回收。
	select {
	case <-clientDone(client):
	case <-time.After(15 * time.Second):
		t.Fatal("a connection that only receives server pushes was never reaped by IdleTimeout")
	}
}

// clientDone 在 client 完全停下来时关闭返回的 channel。
func clientDone(c *Client) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = c.Wait()
		close(done)
	}()
	return done
}

// 握手信息必须在 OnReady 和 Handle 里都能读到，且 TCP 下恒为 nil。
func TestLifecycleHandshakeVisibilityPerTransport(t *testing.T) {
	t.Run("tcp-has-no-handshake", func(t *testing.T) {
		seen := make(chan *Handshake, 1)
		srv := startTestServer(t, &Config{
			CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
				return &testConnHandler{
					conn: conn,
					handle: func(conn *Conn, _ []byte) {
						seen <- conn.Handshake()
						_ = conn.SendNoEncrypt([]byte("ok"))
					},
				}
			}},
			SB: DefaultSenderBuilder,
		})
		client, handler := startMatrixTCPClient(t, srv.Addr(), nil)
		if err := client.Send([]byte("hi")); err != nil {
			t.Fatal(err)
		}
		waitMessage(t, handler.msgCh)
		if hs := <-seen; hs != nil {
			t.Fatalf("TCP transport must report a nil handshake, got %+v", hs)
		}
	})

	t.Run("websocket-exposes-uri-and-headers", func(t *testing.T) {
		seen := make(chan *Handshake, 1)
		srv := startTestWebSocketServer(t, &Config{
			WebSocketPath: "/ws",
			CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
				return &testConnHandler{
					conn: conn,
					handle: func(conn *Conn, _ []byte) {
						seen <- conn.Handshake()
						_ = conn.SendNoEncrypt([]byte("ok"))
					},
				}
			}},
			SB: DefaultSenderBuilder,
		})
		client, handler := startMatrixWSClient(t, "ws://"+srv.Addr()+"/ws?token=abc", nil)
		if err := client.Send([]byte("hi")); err != nil {
			t.Fatal(err)
		}
		waitMessage(t, handler.msgCh)

		hs := <-seen
		if hs == nil {
			t.Fatal("websocket transport must expose the handshake")
		}
		if !strings.HasPrefix(hs.URI, "/ws") {
			t.Fatalf("handshake URI = %q, want it to start with /ws", hs.URI)
		}
		if hs.Header.Get("Host") == "" {
			t.Fatalf("handshake headers are missing Host: %v", hs.Header)
		}
	})
}
