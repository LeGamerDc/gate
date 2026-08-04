package gate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 这个文件是 client 侧协议解析的回归测试：每个用例都对应一个真实被观测到的缺陷，
// 而且都走完整的 Dial → readLoop → codec → handler 路径，而不是直接调 codec，
// 目的是证明 client.go 确实接上了统一的 codec，而不只是 codec 本身正确。

// dialAuditClient 起一个本地 listener，接受一条连接，返回 client 与 server 侧的 conn。
// cfg.Addr 由本函数填写，其余字段由调用方决定。
func dialAuditClient(t *testing.T, cfg *ClientConfig) (*Client, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	connCh := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			connCh <- conn
		}
	}()

	cfg.Addr = ln.Addr().String()
	client, err := DialContext(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = client.Wait()
	})

	select {
	case conn := <-connCh:
		t.Cleanup(func() { _ = conn.Close() })
		return client, conn
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the server side connection")
		return nil, nil
	}
}

func waitClose(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the connection to close")
		return nil
	}
}

// waitClientStop 带超时地等 client 停稳。直接调 Wait() 的话，回调 panic 导致
// close(done) 没执行时整个测试会永久挂住，看不出是哪里坏了。
func waitClientStop(t *testing.T, c *Client) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the client to stop")
		return nil
	}
}

func assertNoMessage(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("handler received %d bytes from a frame that should have been rejected", len(msg))
	default:
	}
}

// compound 的子消息按协议不得再带任何标记。旧 client 对子消息复用同一个递归解析器，
// 于是 c=1 的子消息会再次递归 —— 这既是嵌套爆栈的入口，也让 client 接受了
// server 侧根本不会产生的报文。
func TestClientAuditRejectsFlaggedSubFrame(t *testing.T) {
	for name, flag := range map[string]byte{"z": maskZ, "c": maskC, "e": maskE} {
		t.Run(name, func(t *testing.T) {
			handler := &testClientHandler{
				msgCh:   make(chan []byte, 4),
				closeCh: make(chan error, 1),
			}
			client, serverConn := dialAuditClient(t, &ClientConfig{
				Handler: handler,
				Cipher:  xorCipher{key: 0x11},
			})

			sub := buildFrame([]byte("sub"), flag)
			if _, err := serverConn.Write(buildFrame(sub, maskC)); err != nil {
				t.Fatal(err)
			}

			if err := waitClose(t, handler.closeCh); !errors.Is(err, ErrFlagNotAllowed) {
				t.Fatalf("close err = %v, want ErrFlagNotAllowed", err)
			}
			assertNoMessage(t, handler.msgCh)

			// Close 以前恒返回 nil，业务层拿不到任何失败原因。
			if err := client.Close(); !errors.Is(err, ErrFlagNotAllowed) {
				t.Fatalf("Close err = %v, want ErrFlagNotAllowed", err)
			}
		})
	}
}

// 深度嵌套的 compound：旧实现每层递归一次，1.6MB 的帧就能撑出约 64MB 栈，
// 逼近 32MB 帧上限时会直接 fatal error（recover 也接不住）。现在第一层就被拒。
func TestClientAuditRejectsDeeplyNestedCompound(t *testing.T) {
	handler := &testClientHandler{
		msgCh:   make(chan []byte, 4),
		closeCh: make(chan error, 1),
	}
	_, serverConn := dialAuditClient(t, &ClientConfig{Handler: handler})

	const depth = 2000
	var wire []byte
	for i := 0; i < depth; i++ {
		wire = buildFrame(wire, maskC)
	}
	t.Logf("nested compound: depth=%d wire=%d bytes", depth, len(wire))

	if _, err := serverConn.Write(wire); err != nil {
		t.Fatal(err)
	}

	if err := waitClose(t, handler.closeCh); !errors.Is(err, ErrFlagNotAllowed) {
		t.Fatalf("close err = %v, want ErrFlagNotAllowed", err)
	}
	assertNoMessage(t, handler.msgCh)
}

func TestClientAuditDeliversCompoundInOrder(t *testing.T) {
	handler := &testClientHandler{
		msgCh:   make(chan []byte, 8),
		closeCh: make(chan error, 1),
	}
	_, serverConn := dialAuditClient(t, &ClientConfig{Handler: handler})

	want := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		// 超过 4096，顺带覆盖子消息的 4 字节扩展头。
		bytes.Repeat([]byte("gamma"), 1200),
	}
	if _, err := serverConn.Write(buildFrame(buildCompoundPayload(want...), maskC)); err != nil {
		t.Fatal(err)
	}

	for i := range want {
		got := waitMessage(t, handler.msgCh)
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("message[%d] = %d bytes (%.16q), want %d bytes (%.16q)",
				i, len(got), got, len(want[i]), want[i])
		}
	}
	assertNoMessage(t, handler.msgCh)
}

// 旧 client 直接 dec.DecodeAll，没有任何输出上限：实测 8206 字节的帧解出 256MB。
func TestClientAuditRejectsDecompressionBomb(t *testing.T) {
	const (
		limit      = 1 << 20
		decoded    = 16 << 20
		bombSource = decoded
	)

	handler := &testClientHandler{
		msgCh:   make(chan []byte, 4),
		closeCh: make(chan error, 1),
	}
	_, serverConn := dialAuditClient(t, &ClientConfig{
		Handler:        handler,
		MaxMessageSize: limit,
	})

	bomb := enc.EncodeAll(make([]byte, bombSource), nil)
	t.Logf("wire frame = %d bytes, would decompress to %d bytes (limit %d)", len(bomb), decoded, limit)
	if len(bomb) > limit {
		t.Fatalf("bomb frame itself is %d bytes, over the %d limit; the test would not exercise decompression", len(bomb), limit)
	}

	if _, err := serverConn.Write(buildFrame(bomb, maskZ)); err != nil {
		t.Fatal(err)
	}

	err := waitClose(t, handler.closeCh)
	if err == nil {
		t.Fatal("expected the decompression bomb to be rejected")
	}
	t.Logf("rejected with: %v", err)
	assertNoMessage(t, handler.msgCh)
}

// readLoop 以前先于 OnConnect 启动，于是业务层可能在拿到 *Client 之前就收到消息。
func TestClientAuditOnConnectBeforeOnMessage(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	connCh := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		// 一握手就下发，尽最大可能和 OnConnect 抢跑。
		_, _ = conn.Write(buildFrame([]byte("early"), 0))
		connCh <- conn
	}()

	handler := &orderingClientHandler{
		connectDelay: 200 * time.Millisecond,
		msgCh:        make(chan []byte, 4),
	}
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    ln.Addr().String(),
		Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = client.Wait()
	}()

	serverConn := <-connCh
	defer serverConn.Close()

	waitMessage(t, handler.msgCh)

	events := handler.snapshot()
	if len(events) < 2 || events[0] != "connect" || events[1] != "message" {
		t.Fatalf("callback order = %v, want connect strictly before message", events)
	}
}

// 从 OnConnect 里发登录/握手包是这个回调最主要的用法。writeLoop 若晚于 OnConnect
// 启动，sendCh 一满就再也没人排空：SendQueueSize=1 时第二个 Send 直接把
// DialContext 永久挂死。这里用超时而不是裸等，回归时立刻失败而不是拖死整个测试集。
func TestClientAuditOnConnectCanSendWithTinyQueue(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	connCh := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			connCh <- conn
		}
	}()

	payloads := [][]byte{[]byte("login"), []byte("handshake"), []byte("subscribe"), []byte("ping")}
	handler := &connectSendHandler{
		payloads: payloads,
		sendErrs: make(chan error, len(payloads)),
	}

	type dialResult struct {
		client *Client
		err    error
	}
	resCh := make(chan dialResult, 1)
	go func() {
		client, dialErr := DialContext(context.Background(), &ClientConfig{
			Addr:    ln.Addr().String(),
			Handler: handler,
			// 队列只有 1，而 OnConnect 要发 4 条：writeLoop 必须已经在排空。
			SendQueueSize: 1,
		})
		resCh <- dialResult{client: client, err: dialErr}
	}()

	var res dialResult
	select {
	case res = <-resCh:
	case <-time.After(5 * time.Second):
		t.Fatal("DialContext never returned: nobody drained the sends issued from OnConnect")
	}
	if res.err != nil {
		t.Fatal(res.err)
	}
	client := res.client
	t.Cleanup(func() {
		_ = client.Close()
		_ = client.Wait()
	})

	close(handler.sendErrs)
	for sendErr := range handler.sendErrs {
		if sendErr != nil {
			t.Fatalf("Send from OnConnect failed: %v", sendErr)
		}
	}

	serverConn := <-connCh
	defer serverConn.Close()

	// 顺带确认这些包真的按序落到了线路上，而不只是"没有卡住"。
	for i, want := range payloads {
		f := readTestFrame(t, serverConn)
		if f.z || f.c || f.e {
			t.Fatalf("frame[%d] flags z=%v c=%v e=%v, want all clear", i, f.z, f.c, f.e)
		}
		if !bytes.Equal(f.payload, want) {
			t.Fatalf("frame[%d] = %q, want %q", i, f.payload, want)
		}
	}
}

type connectSendHandler struct {
	payloads [][]byte
	sendErrs chan error
}

func (h *connectSendHandler) OnConnect(c *Client) {
	for _, p := range h.payloads {
		h.sendErrs <- c.Send(p)
	}
}

func (h *connectSendHandler) OnMessage(*Client, []byte) {}
func (h *connectSendHandler) OnClose(*Client, error)    {}

// 业务回调的 panic 不允许打死进程：server 侧每个回调边界都有 safeCall，client
// 必须给出同样的保证。OnMessage 的 panic 额外要以 ErrHandlerPanic 关闭连接，
// 否则业务层只会看到一个莫名其妙停掉的 client。
func TestClientAuditRecoversHandlerPanic(t *testing.T) {
	t.Run("OnMessage", func(t *testing.T) {
		handler := &panicClientHandler{
			panicOnMessage: true,
			closeCh:        make(chan error, 1),
		}
		client, serverConn := dialAuditClient(t, &ClientConfig{Handler: handler})

		if _, err := serverConn.Write(buildFrame([]byte("boom"), 0)); err != nil {
			t.Fatal(err)
		}

		if err := waitClose(t, handler.closeCh); !errors.Is(err, ErrHandlerPanic) {
			t.Fatalf("close err = %v, want ErrHandlerPanic", err)
		}
		if err := waitClientStop(t, client); !errors.Is(err, ErrHandlerPanic) {
			t.Fatalf("Wait err = %v, want ErrHandlerPanic", err)
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		handler := &panicClientHandler{
			panicOnClose: true,
			closeCh:      make(chan error, 1),
		}
		client, _ := dialAuditClient(t, &ClientConfig{Handler: handler})

		_ = client.Close()
		waitClose(t, handler.closeCh)

		// OnClose 的 panic 若逃出 waitLoop，close(done) 永远不会执行，
		// 所有等在 Wait() 上的调用方一起挂死（前提是进程还活着）。
		if err := waitClientStop(t, client); err != nil {
			t.Fatalf("Wait err = %v, want nil", err)
		}
	})
}

type panicClientHandler struct {
	panicOnMessage bool
	panicOnClose   bool
	closeCh        chan error
	once           sync.Once
}

func (h *panicClientHandler) OnConnect(*Client) {}

func (h *panicClientHandler) OnMessage(*Client, []byte) {
	if h.panicOnMessage {
		panic("client handler exploded while decoding a message")
	}
}

func (h *panicClientHandler) OnClose(_ *Client, err error) {
	// 先上报再炸，否则测试无从判断 close 的原因。
	if h.closeCh != nil {
		h.once.Do(func() { h.closeCh <- err })
	}
	if h.panicOnClose {
		panic("client handler exploded while closing")
	}
}

// 头部长度字段只有 12+16=28 位，长度达到 1<<28 时会撞进 m/z/c/e 标记位，
// 长度和标记一起写坏。配置超过 32MB 必须在建连之前就被拒掉，而不是等到
// 线上发出一批服务端必然拒收的帧。
func TestClientAuditRejectsMaxMessageSizeAboveCeiling(t *testing.T) {
	t.Run("tcp", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		accepted := make(chan net.Conn, 1)
		go func() {
			conn, acceptErr := ln.Accept()
			if acceptErr == nil {
				accepted <- conn
			}
		}()

		client, err := DialContext(context.Background(), &ClientConfig{
			Addr:           ln.Addr().String(),
			Handler:        &testClientHandler{},
			MaxMessageSize: maxMessageSize + 1,
		})
		if err == nil {
			_ = client.Close()
			t.Fatalf("DialContext accepted MaxMessageSize %d, above the %d protocol ceiling",
				maxMessageSize+1, maxMessageSize)
		}
		t.Logf("rejected with: %v", err)

		select {
		case conn := <-accepted:
			_ = conn.Close()
			t.Fatal("the config was rejected only after dialing; purge must reject before touching the network")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("ceiling is inclusive", func(t *testing.T) {
		cfg := &ClientConfig{
			Addr:           "127.0.0.1:1",
			Handler:        &testClientHandler{},
			MaxMessageSize: maxMessageSize,
		}
		if err := cfg.purge(); err != nil {
			t.Fatalf("exactly the ceiling must be accepted, got %v", err)
		}
	})

	// WebSocketClientConfig 把校验完全委托给 ClientConfig.purge，两种传输不能
	// 出现两套上限。
	t.Run("websocket", func(t *testing.T) {
		cfg := &WebSocketClientConfig{
			URL:            "ws://127.0.0.1:1/",
			Handler:        &testClientHandler{},
			MaxMessageSize: maxMessageSize + 1,
		}
		if err := cfg.purge(); err == nil {
			t.Fatal("WebSocketClientConfig.purge must inherit the ClientConfig ceiling")
		}
	})
}

// Cipher 的实现只被要求"单线程可用"：server 侧所有调用都被 gnet 事件循环串行化。
// client 的 readLoop 解密入站帧、writeLoop 加密出站帧却是两个 goroutine，只要
// 不串行化，任何带状态的 Cipher（流式、nonce 计数器、复用 scratch buffer）都会
// 被并发调用并静默损坏数据。
//
// auditRaceCipher 用两种手段抓这件事：一个故意不加保护的计数器（-race 直接报），
// 外加一个重叠检测器（不开 -race 时也能报）。
func TestClientAuditSerialisesCipherAcrossLoops(t *testing.T) {
	const (
		rounds = 50
		key    = byte(0x5a)
		// 把每次加解密的窗口拉宽，未串行化时两个方向必然撞上。
		hold = 200 * time.Microsecond
	)

	cipher := &auditRaceCipher{key: key, hold: hold}
	handler := &testClientHandler{
		msgCh:   make(chan []byte, rounds),
		closeCh: make(chan error, 1),
	}
	client, serverConn := dialAuditClient(t, &ClientConfig{
		Handler: handler,
		Cipher:  cipher,
	})

	// 服务端必须用另一个实例：否则测试自己就在并发调用被测的那个 cipher。
	serverCipher := xorCipher{key: key}

	inbound := make(chan []byte, rounds)
	serverErr := make(chan error, 1)
	go func() {
		c := clientCodec(maxMessageSize, maxMessageSize)
		for i := 0; i < rounds; i++ {
			f, err := c.readFrame(serverConn)
			if err != nil {
				serverErr <- err
				return
			}
			if !f.e {
				serverErr <- fmt.Errorf("uplink frame %d is not encrypted", i)
				return
			}
			serverCipher.Decrypt(f.payload)
			inbound <- f.payload
		}
		serverErr <- nil
	}()
	go func() {
		for i := 0; i < rounds; i++ {
			payload := []byte(fmt.Sprintf("srv-%03d", i))
			serverCipher.Encrypt(payload)
			if _, err := serverConn.Write(buildFrame(payload, maskE)); err != nil {
				return
			}
		}
	}()

	// 一口气全塞进去：中途等回包会在两个 loop 之间建立 happens-before，
	// 反而把要抓的竞争掩盖掉。
	for i := 0; i < rounds; i++ {
		if err := client.Send([]byte(fmt.Sprintf("cli-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < rounds; i++ {
		want := fmt.Sprintf("srv-%03d", i)
		if got := waitMessage(t, handler.msgCh); string(got) != want {
			t.Fatalf("downlink[%d] = %q, want %q", i, got, want)
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rounds; i++ {
		want := fmt.Sprintf("cli-%03d", i)
		if got := <-inbound; string(got) != want {
			t.Fatalf("uplink[%d] = %q, want %q", i, got, want)
		}
	}

	if cipher.overlap.Load() {
		t.Fatal("Cipher was called concurrently by readLoop and writeLoop; client-side cipher use must be serialised")
	}

	_ = client.Close()
	if err := waitClientStop(t, client); err != nil {
		t.Fatalf("Wait err = %v, want nil", err)
	}
	// Wait() 之后两个 loop 都已退出，这时读 calls 才是安全的。
	t.Logf("cipher calls = %d", cipher.calls)
}

type auditRaceCipher struct {
	key byte
	// calls 故意不做任何保护：两个 loop 未串行化时 -race 会直接报出来，
	// 即使两次调用没有在物理时间上重叠也一样（happens-before 检测）。
	calls    int
	inFlight atomic.Int32
	overlap  atomic.Bool
	hold     time.Duration
}

func (c *auditRaceCipher) Encrypt(data []byte) { c.apply(data) }
func (c *auditRaceCipher) Decrypt(data []byte) { c.apply(data) }

func (c *auditRaceCipher) apply(data []byte) {
	if c.inFlight.Add(1) > 1 {
		c.overlap.Store(true)
	}
	c.calls++
	for i := range data {
		data[i] ^= c.key
	}
	time.Sleep(c.hold)
	c.inFlight.Add(-1)
}

type orderingClientHandler struct {
	mu           sync.Mutex
	events       []string
	connectDelay time.Duration
	msgCh        chan []byte
}

func (h *orderingClientHandler) OnConnect(*Client) {
	// 故意慢：readLoop 若已经在跑，这段时间足够 OnMessage 抢在前面。
	time.Sleep(h.connectDelay)
	h.record("connect")
}

func (h *orderingClientHandler) OnMessage(_ *Client, msg []byte) {
	h.record("message")
	if h.msgCh != nil {
		h.msgCh <- msg
	}
}

func (h *orderingClientHandler) OnClose(*Client, error) {
	h.record("close")
}

func (h *orderingClientHandler) record(event string) {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.mu.Unlock()
}

func (h *orderingClientHandler) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...)
}
