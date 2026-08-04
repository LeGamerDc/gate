package gate

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

/*
panic 隔离边界矩阵。

gnet 核心一个 recover 都没有，任何从事件循环逃逸的 panic 都会终止整个进程。
README 因此承诺"在每一个回调业务代码的边界都加了 recover barrier"，并列出了
具体的边界清单。但此前只有其中 5 个有测试：

	已有   Handle / SenderI.Close / OnReady(TCP) / AsyncDo / ClientHandler.OnMessage
	缺失   ConnHandlerBuilder.Build / SenderBuilder.Build / ConnHandler.Close /
	       OnWebSocketUpgrade / ClientHandler.OnClose / OnReady(WebSocket)

这些用例的共同断言有两条：**进程活着**（测试跑完本身就是证明），以及
**爆炸半径只有这一条连接**（后续连接照常工作）。
*/

// panicOnceBuilder 只让第一条连接的 Build panic，之后恢复正常。
//
// 这样同一个 server 上可以先观测"出事的连接被干净地关掉"，再观测
// "下一条连接完全不受影响"——也就是 README 说的爆炸半径收敛。
type panicOnceBuilder struct {
	calls atomic.Int32
	inner ConnHandlerBuilder
}

func (b *panicOnceBuilder) Build(conn *Conn) ConnHandler {
	if b.calls.Add(1) == 1 {
		panic("ConnHandlerBuilder.Build exploded")
	}
	return b.inner.Build(conn)
}

type panicOnceSenderBuilder struct {
	calls atomic.Int32
	inner SenderBuilder
}

func (b *panicOnceSenderBuilder) Build(conn *Conn) SenderI {
	if b.calls.Add(1) == 1 {
		panic("SenderBuilder.Build exploded")
	}
	return b.inner.Build(conn)
}

// panicCloseHandler 在 ConnHandler.Close 里 panic。
type panicCloseHandler struct{ conn *Conn }

func (h *panicCloseHandler) Handle(raw []byte) { _ = h.conn.Send(raw) }
func (h *panicCloseHandler) Close()            { panic("ConnHandler.Close exploded") }

// echoAfterPanicBuilder 造正常的 echo handler，供 panicOnce* 在第二条连接上使用。
func echoAfterPanicBuilder() ConnHandlerBuilder {
	return testHandlerBuilder{build: func(conn *Conn) ConnHandler {
		return &testConnHandler{
			conn:   conn,
			handle: func(conn *Conn, raw []byte) { _ = conn.SendNoEncrypt(raw) },
		}
	}}
}

// assertConnectionRejected 断言这条连接被服务端立刻关掉了。
//
// 判据是"读到 EOF/RST"，而不是"Send 报错"：Send 只是把消息放进本地队列，
// 服务端已经关掉的连接要等一个 RTT 才反映到客户端。
func assertConnectionRejected(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	switch _, err := conn.Read(buf[:]); {
	case err == nil:
		t.Fatal("expected the server to close the connection, but it sent data")
	case errors.Is(err, net.ErrClosed):
		t.Fatal("the client side socket was closed, not the server side")
	default:
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			t.Fatal("the server kept the connection open after the callback panicked")
		}
	}
}

func TestPanicInConnHandlerBuilderClosesOnlyThatConnection(t *testing.T) {
	builder := &panicOnceBuilder{inner: echoAfterPanicBuilder()}
	srv := startTestServer(t, &Config{CHB: builder, SB: DefaultSenderBuilder})

	// 第一条连接：Build panic，服务端必须把它关掉。
	victim, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()
	assertConnectionRejected(t, victim)

	// 第二条连接：完全不受影响。
	client, handler := startMatrixTCPClient(t, srv.Addr(), nil)
	msg := []byte("still alive")
	if err := client.Send(msg); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); string(got) != string(msg) {
		t.Fatalf("survivor connection round-trip: got %q, want %q", got, msg)
	}
}

func TestPanicInSenderBuilderClosesOnlyThatConnection(t *testing.T) {
	builder := &panicOnceSenderBuilder{inner: DefaultSenderBuilder}
	srv := startTestServer(t, &Config{CHB: echoAfterPanicBuilder(), SB: builder})

	victim, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()
	assertConnectionRejected(t, victim)

	client, handler := startMatrixTCPClient(t, srv.Addr(), nil)
	msg := []byte("sender builder recovered")
	if err := client.Send(msg); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); string(got) != string(msg) {
		t.Fatalf("survivor connection round-trip: got %q, want %q", got, msg)
	}
}

// ConnHandler.Close 的 panic 发生在 OnClose 里，也就是连接已经在拆解的路上。
// 它不能把事件循环带走，也不能妨碍同一个 server 继续服务。
func TestPanicInConnHandlerCloseDoesNotKillTheEventLoop(t *testing.T) {
	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &panicCloseHandler{conn: conn}
		}},
		SB: DefaultSenderBuilder,
	})

	// 先跑一次正常收发，确认连接确实建立起来了，再断开触发 OnClose。
	first, handler := startMatrixTCPClient(t, srv.Addr(), nil)
	if err := first.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, handler.msgCh)
	if err := first.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	// 事件循环必须还活着：换一条新连接照常收发。
	second, handler2 := startMatrixTCPClient(t, srv.Addr(), nil)
	msg := []byte("event loop survived")
	if err := second.Send(msg); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler2.msgCh); string(got) != string(msg) {
		t.Fatalf("round-trip after a panicking ConnHandler.Close: got %q", got)
	}
}

// OnWebSocketUpgrade 跑在事件循环上，它 panic 时必须被翻译成一次握手拒绝，
// 而不是把进程带走。
func TestPanicInWebSocketUpgradeHookRejectsHandshake(t *testing.T) {
	var calls atomic.Int32
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		OnWebSocketUpgrade: func(*Handshake) error {
			if calls.Add(1) == 1 {
				panic("OnWebSocketUpgrade exploded")
			}
			return nil
		},
		CHB: echoAfterPanicBuilder(),
		SB:  DefaultSenderBuilder,
	})

	// 第一次握手：钩子 panic，必须失败。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := DialWebSocketContext(ctx, &WebSocketClientConfig{
		URL:     "ws://" + srv.Addr() + "/ws",
		Handler: newMatrixHandler(),
	}); err == nil {
		t.Fatal("expected the handshake to be rejected when the upgrade hook panics")
	}

	// 第二次握手：正常。
	client, handler := startMatrixWSClient(t, "ws://"+srv.Addr()+"/ws", nil)
	msg := []byte("upgrade hook recovered")
	if err := client.Send(msg); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); string(got) != string(msg) {
		t.Fatalf("round-trip after a panicking upgrade hook: got %q", got)
	}
}

// WebSocket 下 OnReady 的 panic 此前只在 TCP 上测过。两种传输的 OnReady 由
// 完全不同的代码路径触发（ev.OnOpen vs wsConnState.upgrade），必须各测一次。
func TestPanicInWebSocketOnReadyClosesConnection(t *testing.T) {
	srv := startTestWebSocketServer(t, &Config{
		WebSocketPath: "/ws",
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &panicReadyHandler{}
		}},
		SB: DefaultSenderBuilder,
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/ws")
	defer conn.Close()
	assertConnectionRejected(t, conn)
}

// ClientHandler.OnClose 的 panic 是最凶险的一个：它跑在 waitLoop 上，
// 逃逸出去就会跳过 close(c.done)，让所有等在 Wait() 上的 goroutine 永久挂起。
func TestPanicInClientOnCloseStillReleasesWait(t *testing.T) {
	handler := &panicOnCloseHandler{}
	client, serverConn := dialAuditClient(t, &ClientConfig{Handler: handler})

	// 从服务端断开，触发 client 的关闭流程。
	_ = serverConn.Close()

	// 关键断言：Wait() 必须返回。waitClientStop 带 5s 超时，
	// 挂死会以失败而不是整个测试卡住的形式暴露出来。
	_ = waitClientStop(t, client)
	if !handler.closed.Load() {
		t.Fatal("OnClose was never called")
	}
}

// OnConnect 的 panic 是唯一一个**不该**被吞掉的：它跑在 DialContext 调用方的
// goroutine 上，吞成 error 会和"拨号失败"混为一谈，也会掩盖业务自己的 bug。
//
// 但 gate 不能因此留下一个半构造的 client：此刻 writeLoop 已经在跑、连接已经
// 建立，而 *Client 还没有返回给任何人，谁也没法再关掉它。所以放走 panic 之前
// 必须先把连接关掉、把后台 loop 停掉。而且 OnClose 不能被调用——业务从来没有
// 拿到过这个 client。
func TestPanicInClientOnConnectPropagatesAfterTeardown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	connCh := make(chan net.Conn, 1)
	go func() {
		if c, aerr := ln.Accept(); aerr == nil {
			connCh <- c
		}
	}()

	handler := &panicOnConnectHandler{}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("the OnConnect panic must propagate to the DialContext caller")
			}
		}()
		_, _ = DialContext(context.Background(), &ClientConfig{
			Addr:    ln.Addr().String(),
			Handler: handler,
		})
	}()

	// 连接必须已经被拆干净：服务端侧读到 EOF。
	select {
	case serverConn := <-connCh:
		defer serverConn.Close()
		if err := serverConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var buf [1]byte
		if _, err := serverConn.Read(buf[:]); err == nil {
			t.Fatal("the connection was left open after the OnConnect panic")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the client never connected")
	}

	if handler.closeCalls.Load() != 0 {
		t.Fatalf("OnClose was called %d time(s) for a client that was never handed to the caller",
			handler.closeCalls.Load())
	}
}

type panicOnConnectHandler struct{ closeCalls atomic.Int32 }

func (h *panicOnConnectHandler) OnConnect(*Client)         { panic("ClientHandler.OnConnect exploded") }
func (h *panicOnConnectHandler) OnMessage(*Client, []byte) {}
func (h *panicOnConnectHandler) OnClose(*Client, error)    { h.closeCalls.Add(1) }

type panicOnCloseHandler struct{ closed atomic.Bool }

func (h *panicOnCloseHandler) OnConnect(*Client)         {}
func (h *panicOnCloseHandler) OnMessage(*Client, []byte) {}
func (h *panicOnCloseHandler) OnClose(*Client, error) {
	h.closed.Store(true)
	panic("ClientHandler.OnClose exploded")
}
