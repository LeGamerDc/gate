//go:build darwin || linux

package gate

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 握手计数在拒绝路径上被减了两次（拒绝分支一次、拆除回调一次），
// 每拒绝一条净减 1 → 计数变负 → 之后所有慢握手连接绕过 MaxHandshaking。
func TestServer_HandshakeCounterNeverGoesNegative(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) {
		o.WebSocket = &WebSocketOptions{}
		o.Limits.MaxHandshaking = 2
		o.Loops = 1
	}, nil)

	// 开一批只连不发的连接（全部停在 Handshaking），超出上限的会被拒。
	var conns []net.Conn
	for range 12 {
		c, err := net.DialTimeout("tcp", env.addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	t.Cleanup(func() {
		for _, c := range conns {
			_ = c.Close()
		}
	})

	waitCond(t, func() bool { return env.srv.handshaking.Load() <= 2 })
	if got := env.srv.handshaking.Load(); got < 0 {
		t.Fatalf("MaxHandshaking 计数变成负数: %d（拒绝路径双重递减）", got)
	}
	if got := env.srv.Stats().ConnsHandshaking; got < 0 {
		t.Fatalf("ConnsHandshaking=%d", got)
	}
	// 全部断开后计数必须回到 0，而不是负数。
	for _, c := range conns {
		_ = c.Close()
	}
	waitCond(t, func() bool { return env.srv.handshaking.Load() == 0 })
	if got := env.srv.Stats().ConnsHandshaking; got != 0 {
		t.Fatalf("全部断开后 ConnsHandshaking=%d, want 0", got)
	}
}

// 01 明写：Shutdown **不等待**在途 AsyncDo——这些连接的 socket 会被正常
// 拆除（fd、缓冲、槽位都回收），但 OnClose 要等各自的 f 结束后才触发，
// 进程先退出就不触发了。等 OnClose 会让一个不返回的业务任务把 Shutdown
// 拖到 ctx 到期，那正是这条规格要避免的。
func TestServer_ShutdownDoesNotWaitForAsync(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var closed atomic.Int32
	var closeReason atomic.Value

	srv, err := Listen(Options[string]{
		Addr:  "127.0.0.1:0",
		Loops: 1,
		Handler: funcHandler[string]{
			msg: func(c *Conn[string], m []byte) error {
				return c.AsyncDo(func() {
					close(started)
					<-release
				})
			},
			clsd: func(c *Conn[string], reason error) {
				closeReason.Store(reason)
				closed.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cl := dialTCP(t, srv.Addr().String(), nil, nil)
	cl.sendMsgs([]byte("go"))
	<-started

	// 任务还卡着，但 socket 的拆除不依赖它：Shutdown 必须及时返回。
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown 被在途 AsyncDo 拖住了（01 明写不等待）")
	}
	if closed.Load() != 0 {
		t.Fatal("任务未结束，OnClose 不该已触发")
	}
	close(release)
	// 进程仍在，f 结束后 OnClose 才补上——「进程先退出就不触发」的另一半。
	waitCond(t, func() bool { return closed.Load() == 1 })
	if r, _ := closeReason.Load().(error); !errors.Is(r, ErrServerClosed) {
		t.Fatalf("reason=%v", r)
	}
}

// Shutdown 期间到达的新连接不得活过这一轮（macOS 的跨 loop 注册窗口）。
func TestServer_NoConnSurvivesShutdown(t *testing.T) {
	for range 20 {
		srv, err := Listen(Options[string]{
			Addr: "127.0.0.1:0", Loops: 2, Handler: funcHandler[string]{},
		})
		if err != nil {
			t.Fatal(err)
		}
		addr := srv.Addr().String()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 8 {
				c, err := net.DialTimeout("tcp", addr, time.Second)
				if err == nil {
					defer c.Close()
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := srv.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		wg.Wait()
		if got := srv.conns.Load(); got != 0 {
			t.Fatalf("Shutdown 之后仍有 %d 条连接的 socket 未拆除", got)
		}
	}
}

// ProxyOptional：首字节撞上 PROXY v2 签名的**合法 gate 帧**不得被误判。
// m=0 且长度在 [0x0D00,0x0DFF] 的帧首字节正好是 0x0D。
func TestServer_ProxyOptionalFallbackOnSignatureCollision(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) { o.Proxy = ProxyOptional }, nil)
	cl := dialTCP(t, env.addr, nil, nil)

	payload := mkPayload(0x0D00) // 帧头首字节 = 0x0D
	bf, err := buildFrame(append([]byte(nil), payload...), 0, nil, false, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	w := wire(&bf)
	if w[0] != proxyV2Sig[0] {
		t.Fatalf("构造失败：首字节 %#x", w[0])
	}
	if _, err := cl.conn.Write(w); err != nil {
		t.Fatal(err)
	}
	got := cl.readMsgs(1)
	if len(got[0]) != len("echo:")+len(payload) {
		t.Fatalf("首字节撞签名的合法帧被当成畸形 PROXY 头断开了")
	}
}

func TestServer_ProxyOptionalWithoutHeader(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) { o.Proxy = ProxyOptional }, nil)
	cl := dialTCP(t, env.addr, nil, nil)
	cl.sendMsgs([]byte("plain"))
	got := cl.readMsgs(1)
	if string(got[0]) != "echo:plain" {
		t.Fatalf("got %q", got)
	}
}

func TestServer_ProxyV2RealRemote(t *testing.T) {
	seen := make(chan netip.AddrPort, 1)
	env := startEcho(t, func(o *Options[string]) {
		o.Proxy = ProxyRequired
		h := o.Handler.(funcHandler[string])
		h.open = func(c *Conn[string]) (string, error) { seen <- c.Remote(); return "s", nil }
		o.Handler = h
	}, nil)

	hdr := append([]byte(nil), proxyV2Sig...)
	hdr = append(hdr, 0x21, 0x11, 0x00, 12) // v2|PROXY, AF_INET|STREAM, len=12
	hdr = append(hdr, 198, 51, 100, 23)     // src ip
	hdr = append(hdr, 10, 0, 0, 1)          // dst ip
	hdr = append(hdr, 0x30, 0x39)           // src port 12345
	hdr = append(hdr, 0x00, 0x50)           // dst port 80

	cl := dialTCP(t, env.addr, nil, nil)
	if _, err := cl.conn.Write(hdr); err != nil {
		t.Fatal(err)
	}
	cl.sendMsgs([]byte("v2"))
	cl.readMsgs(1)
	select {
	case r := <-seen:
		if r.String() != "198.51.100.23:12345" {
			t.Fatalf("Remote()=%s", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnOpen 未触发")
	}
}

func waitCond(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("waitCond 超时")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
