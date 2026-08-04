package gate

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dialRaw 打开一条裸 TCP 连接，用于构造协议层的非法输入。
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// expectServerClose 断言服务端在超时前主动断开了这条连接。
func expectServerClose(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var buf [64]byte
	for {
		n, err := c.Read(buf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("server kept the connection open; expected it to be closed")
			}
			return // RST 之类也算关闭
		}
		if n == 0 {
			return
		}
	}
}

func echoServer(t *testing.T, cfg *Config) *Server {
	t.Helper()
	if cfg.CHB == nil {
		cfg.CHB = testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(conn *Conn, raw []byte) {
				_ = conn.SendNoEncrypt(raw)
			}}
		}}
	}
	return startTestServer(t, cfg)
}

// 一条能让业务 panic 的消息此前会打死整个进程：gnet 核心没有任何 recover。
// 现在爆炸半径必须收敛到单条连接。
func TestHandlerPanicClosesOnlyThatConnection(t *testing.T) {
	var panicked atomic.Bool
	srv := echoServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(conn *Conn, raw []byte) {
				if string(raw) == "boom" {
					panicked.Store(true)
					panic("handler exploded")
				}
				_ = conn.SendNoEncrypt(raw)
			}}
		}},
	})

	victim := dialRaw(t, srv.Addr())
	if _, err := victim.Write(mkFrame([]byte("boom"), 0)); err != nil {
		t.Fatal(err)
	}
	expectServerClose(t, victim)

	if !panicked.Load() {
		t.Fatal("handler was never invoked")
	}

	// 进程和 server 都还活着，另一条连接必须照常工作。
	survivor, handler := startTestClient(t, srv.Addr(), nil)
	if err := survivor.SendNoEncrypt([]byte("still alive")); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); string(got) != "still alive" {
		t.Fatalf("survivor round-trip = %q", got)
	}
}

// 服务端此前静默忽略上行的 z/c 标记，把压缩字节或未展开的 compound 直接丢给业务。
func TestServerRejectsDisallowedUplinkFlags(t *testing.T) {
	for name, flag := range map[string]byte{"compressed": maskZ, "compound": maskC} {
		t.Run(name, func(t *testing.T) {
			var delivered atomic.Bool
			srv := echoServer(t, &Config{
				CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
					return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {
						delivered.Store(true)
					}}
				}},
			})

			c := dialRaw(t, srv.Addr())
			if _, err := c.Write(mkFrame([]byte("payload"), flag)); err != nil {
				t.Fatal(err)
			}
			expectServerClose(t, c)
			if delivered.Load() {
				t.Fatal("a frame with a disallowed flag reached the handler")
			}
		})
	}
}

// MaxMessageSize 此前是硬编码的 32MB，任何客户端都能让服务端为它缓冲 32MB。
func TestServerEnforcesConfiguredMaxMessageSize(t *testing.T) {
	srv := echoServer(t, &Config{MaxMessageSize: 1024})

	c := dialRaw(t, srv.Addr())
	// 只写头，不写 body：服务端必须在按长度分配之前就拒绝。
	oversize := mkFrame(make([]byte, 8000), 0)
	if _, err := c.Write(oversize[:4]); err != nil {
		t.Fatal(err)
	}
	expectServerClose(t, c)
}

func TestServerAcceptsMessageAtConfiguredLimit(t *testing.T) {
	const limit = 4096
	srv := echoServer(t, &Config{MaxMessageSize: limit})

	client, handler := startTestClient(t, srv.Addr(), nil)
	msg := make([]byte, limit)
	for i := range msg {
		msg[i] = byte(i)
	}
	if err := client.SendNoEncrypt(msg); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); len(got) != limit {
		t.Fatalf("round-trip len = %d, want %d", len(got), limit)
	}
}

func TestConfigRejectsMaxMessageSizeAboveProtocolCeiling(t *testing.T) {
	cfg := &Config{
		MaxMessageSize: maxMessageSize + 1,
		CHB:            testHandlerBuilder{build: func(*Conn) ConnHandler { return nil }},
		SB:             DefaultSenderBuilder,
		Logger:         noopTestLogger{},
	}
	if err := cfg.purge(); err == nil {
		t.Fatal("expected purge to reject a limit above the protocol ceiling")
	}
}

func TestServerEnforcesMaxConnections(t *testing.T) {
	srv := echoServer(t, &Config{MaxConnections: 2})

	// 前两条必须活着并能正常回环。
	for i := 0; i < 2; i++ {
		client, handler := startTestClient(t, srv.Addr(), nil)
		if err := client.SendNoEncrypt([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if got := waitMessage(t, handler.msgCh); string(got) != "ping" {
			t.Fatalf("conn %d round-trip = %q", i, got)
		}
	}

	// 第三条应当被立刻拒绝。
	expectServerClose(t, dialRaw(t, srv.Addr()))
}

// 没有 keep-alive 也没有 idle 超时时，被 NAT 静默丢弃的半开连接会永远占着资源。
func TestServerClosesIdleConnections(t *testing.T) {
	srv := echoServer(t, &Config{IdleTimeout: time.Second})

	c := dialRaw(t, srv.Addr())
	// 建立后什么都不发，等待空闲扫描把它收走。
	expectServerClose(t, c)
}

func TestServerKeepsActiveConnectionAlive(t *testing.T) {
	srv := echoServer(t, &Config{IdleTimeout: 2 * time.Second})

	client, handler := startTestClient(t, srv.Addr(), nil)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := client.SendNoEncrypt([]byte("keepalive")); err != nil {
			t.Fatalf("active connection was closed: %v", err)
		}
		if got := waitMessage(t, handler.msgCh); string(got) != "keepalive" {
			t.Fatalf("round-trip = %q", got)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// UpdateCipher 此前是无同步裸赋值，而读取发生在事件循环线程；
// 最典型的用法就是从 AsyncDo 的 goroutine 里换密钥。必须在 -race 下干净。
func TestUpdateCipherIsRaceFree(t *testing.T) {
	var c Conn
	var wg sync.WaitGroup
	wg.Add(3)

	for i := 0; i < 2; i++ {
		go func(k byte) {
			defer wg.Done()
			for j := 0; j < 5000; j++ {
				c.UpdateCipher(xorCipher{key: k})
			}
		}(byte(i + 1))
	}
	go func() {
		defer wg.Done()
		for j := 0; j < 5000; j++ {
			if ci := c.getCipher(); ci != nil {
				ci.Encrypt(nil)
			}
		}
	}()
	wg.Wait()

	c.UpdateCipher(nil)
	if c.getCipher() != nil {
		t.Fatal("UpdateCipher(nil) should clear the cipher")
	}
}

// writeOutbound 是所有出站数据的唯一闸口；连接关闭后它必须拒绝一切写，
// 否则 sender 会往一个已被 gnet 回收、可能已被新连接复用的 fd 上写数据。
func TestWriteOutboundIsFencedByConnState(t *testing.T) {
	var c Conn

	if c.writable() {
		t.Fatal("a fresh Conn must not be writable before markOpen")
	}
	if err := c.writeOutbound([]byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("write while handshaking = %v, want ErrConnClosed", err)
	}
	if err := c.writevOutbound([][]byte{[]byte("x")}); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("writev while handshaking = %v, want ErrConnClosed", err)
	}

	c.markOpen()
	if !c.writable() {
		t.Fatal("Conn must be writable after markOpen")
	}

	c.markClosed()
	if c.writable() || !c.isClosed() {
		t.Fatal("Conn must be closed after markClosed")
	}
	if err := c.writeOutbound([]byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("write after close = %v, want ErrConnClosed", err)
	}
	if err := c.writevOutbound([][]byte{[]byte("x")}); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("writev after close = %v, want ErrConnClosed", err)
	}
	if n := c.outboundBuffered(); n != 0 {
		t.Fatalf("outboundBuffered after close = %d, want 0", n)
	}

	// markClosed 之后不允许被 markOpen 重新打开。
	c.markOpen()
	if c.writable() {
		t.Fatal("markOpen must not resurrect a closed Conn")
	}
}

// AsyncDo 里的 panic 此前会让 blocking 计数归零逻辑与 recover 顺序纠缠；
// 无论 f 是否 panic，挂起计数都必须归零，否则连接永久停止读消息。
func TestAsyncDoPanicStillClearsBlocking(t *testing.T) {
	var c Conn
	c.markOpen()

	done := make(chan struct{})
	c.AsyncDo(func() {
		defer close(done)
		panic("async exploded")
	})
	<-done

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.isBlocking() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("blocking counter never returned to zero after an AsyncDo panic")
}

// 这是本次修复里最严重那个缺陷的回归测试。
//
// 原来的链路是 sender -> *Conn -> gnet.Conn -> fd。gnet 关闭连接后会 close(fd)，
// 而这个 fd 号可能立刻被新接受的连接复用；偏偏 gnet 的 Wake 对陈旧连接返回的是
// nil error，于是发送回调会认为一切正常，继续往一个已经属于别人的 socket 上写数据
// ——在连接频繁建立/断开的网关里就是跨连接串包。
//
// 触发条件非常日常：广播 goroutine 正在给某个连接发消息，而这个连接同时断开。
func TestSendRacingWithCloseNeverWritesToRecycledFd(t *testing.T) {
	var handled atomic.Int64
	srv := echoServer(t, &Config{
		LoopCount: 2,
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(conn *Conn, raw []byte) {
				handled.Add(1)
				_ = conn.SendNoEncrypt(raw)
			}}
		}},
	})

	const rounds = 40
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			raw, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
			if err != nil {
				return
			}
			// 抓住服务端的 *Conn，模拟"业务侧长期持有连接引用"。
			_, _ = raw.Write(mkFrame([]byte("hello"), 0))

			// 一边猛发，一边把连接从底下抽掉。
			var senders sync.WaitGroup
			stop := make(chan struct{})
			for s := 0; s < 4; s++ {
				senders.Add(1)
				go func() {
					defer senders.Done()
					for {
						select {
						case <-stop:
							return
						default:
							_, _ = raw.Write(mkFrame([]byte("spam"), 0))
						}
					}
				}()
			}
			time.Sleep(time.Duration(10+i%20) * time.Millisecond)
			_ = raw.Close()
			close(stop)
			senders.Wait()
		}()
	}
	wg.Wait()

	// server 必须还活着并能正常服务新连接。
	client, handler := startTestClient(t, srv.Addr(), nil)
	if err := client.SendNoEncrypt([]byte("after the storm")); err != nil {
		t.Fatal(err)
	}
	if got := waitMessage(t, handler.msgCh); string(got) != "after the storm" {
		t.Fatalf("server unusable after close storm: %q", got)
	}
	if handled.Load() == 0 {
		t.Fatal("no messages were handled; the test did not exercise anything")
	}
	t.Logf("handled %d messages across %d churning connections", handled.Load(), rounds)
}

// Cipher 必须每帧重新取。握手协商类的 handler 会在处理第一帧时 UpdateCipher，
// 而同一个 TCP 读批次里往往紧跟着用新密钥加密的第二帧——在循环外取一次快照
// 会让第二帧用旧密钥解密。
func TestCipherSwapAppliesToLaterFramesInSameBatch(t *testing.T) {
	const key = 0x5a
	got := make(chan string, 4)

	srv := echoServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(conn *Conn, raw []byte) {
				got <- string(raw)
				if string(raw) == "negotiate" {
					// 业务在处理第一帧时切换密钥，后续帧都用新密钥。
					conn.UpdateCipher(xorCipher{key: key})
				}
			}}
		}},
	})

	c := dialRaw(t, srv.Addr())

	// 两帧一次写出，确保它们落在同一个 gnet 读批次里。
	secret := []byte("after the swap")
	encrypted := append([]byte(nil), secret...)
	xorCipher{key: key}.Encrypt(encrypted)

	var wire []byte
	wire = append(wire, mkFrame([]byte("negotiate"), 0)...)
	wire = append(wire, mkFrame(encrypted, maskE)...)
	if _, err := c.Write(wire); err != nil {
		t.Fatal(err)
	}

	if first := waitString(t, got); first != "negotiate" {
		t.Fatalf("first frame = %q", first)
	}
	if second := waitString(t, got); second != string(secret) {
		t.Fatalf("second frame = %q, want %q (cipher snapshot went stale)", second, secret)
	}
}

// OnReady panic 必须关闭连接，而不是留着一个半初始化的 handler 继续收消息。
func TestOnReadyPanicClosesConnection(t *testing.T) {
	var handled atomic.Bool
	srv := echoServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &panicReadyHandler{conn: conn, handled: &handled}
		}},
	})

	c := dialRaw(t, srv.Addr())
	expectServerClose(t, c)
	if handled.Load() {
		t.Fatal("handler kept receiving messages after OnReady panicked")
	}
}

type panicReadyHandler struct {
	conn    *Conn
	handled *atomic.Bool
}

func (h *panicReadyHandler) Handle([]byte) { h.handled.Store(true) }
func (h *panicReadyHandler) Close()        {}
func (h *panicReadyHandler) OnReady()      { panic("OnReady exploded") }

// 自定义 SenderI 的 Close 也是业务代码，panic 不能逃到事件循环外面。
func TestSenderCloseParanoiaIsIsolated(t *testing.T) {
	srv := echoServer(t, &Config{
		SB: panicCloseSenderBuilder{},
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
		}},
	})

	c := dialRaw(t, srv.Addr())
	_ = c.Close()

	// 进程存活即通过：再开一条连接确认 server 还在工作。
	c2 := dialRaw(t, srv.Addr())
	if _, err := c2.Write(mkFrame([]byte("ping"), 0)); err != nil {
		t.Fatal(err)
	}
}

type panicCloseSenderBuilder struct{}

func (panicCloseSenderBuilder) Build(c *Conn) SenderI {
	return &panicCloseSender{SenderI: DefaultSenderBuilder.Build(c)}
}

type panicCloseSender struct{ SenderI }

func (p *panicCloseSender) Close() { panic("sender Close exploded") }

func TestConfigRejectsTooShortKeepAlive(t *testing.T) {
	base := func() *Config {
		return &Config{
			CHB:    testHandlerBuilder{build: func(*Conn) ConnHandler { return nil }},
			SB:     DefaultSenderBuilder,
			Logger: noopTestLogger{},
		}
	}

	c := base()
	c.TCPKeepAlive = time.Second // gnet 会把 TCP_KEEPINTVL 算成 0，静默失效
	if err := c.purge(); err == nil {
		t.Fatal("expected purge to reject a sub-5s keep-alive")
	}

	c = base()
	c.TCPKeepAlive = DisableKeepAlive
	if err := c.purge(); err != nil {
		t.Fatalf("DisableKeepAlive must be accepted: %v", err)
	}
	if got := c.keepAlive(); got != 0 {
		t.Fatalf("keepAlive() = %v, want 0", got)
	}

	c = base()
	if err := c.purge(); err != nil {
		t.Fatal(err)
	}
	if c.TCPKeepAlive != defaultTCPKeepAlive {
		t.Fatalf("default keep-alive = %v, want %v", c.TCPKeepAlive, defaultTCPKeepAlive)
	}
}

func waitString(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a message")
		return ""
	}
}

func TestAsyncDoIsNoOpOnClosedConn(t *testing.T) {
	var c Conn
	c.markClosed()

	ran := make(chan struct{})
	c.AsyncDo(func() { close(ran) })

	select {
	case <-ran:
		t.Fatal("AsyncDo ran work on a closed connection")
	case <-time.After(200 * time.Millisecond):
	}
	if c.isBlocking() {
		t.Fatal("closed connection must not be left in a blocking state")
	}
}
