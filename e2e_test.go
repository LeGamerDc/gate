//go:build darwin || linux

package gate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ─── 真实 socket 的端到端矩阵（07 第 4 层的本机子集）───

// tcpClient 是测试客户端：生产 codec 的 client 策略 + 独立读循环。
type tcpClient struct {
	t    *testing.T
	conn net.Conn
	cdc  codec
	recv Cipher // 解下行
	send Cipher // 封上行
	buf  []byte
	msgs [][]byte
}

func dialTCP(t *testing.T, addr string, sendCi, recvCi Cipher) *tcpClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cdc := clientCodec(maxMessageSize, maxMessageSize)
	cdc.requireEncrypt = recvCi != nil
	return &tcpClient{t: t, conn: conn, cdc: cdc, recv: recvCi, send: sendCi}
}

func (c *tcpClient) sendMsgs(msgs ...[]byte) {
	c.t.Helper()
	var out []byte
	for _, m := range msgs {
		bf, err := buildFrame(append([]byte(nil), m...), 0, nil, false, c.send, maxMessageSize)
		if err != nil {
			c.t.Fatal(err)
		}
		out = append(out, wire(&bf)...)
	}
	if _, err := c.conn.Write(out); err != nil {
		c.t.Fatal(err)
	}
}

// readMsgs 读到凑齐 n 条消息为止。
func (c *tcpClient) readMsgs(n int) [][]byte {
	c.t.Helper()
	zdec := mustDecoder(c.t)
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	tmp := make([]byte, 64<<10)
	for len(c.msgs) < n {
		k, err := c.conn.Read(tmp)
		if err != nil {
			c.t.Fatalf("read: %v (已收 %d/%d)", err, len(c.msgs), n)
		}
		c.buf = append(c.buf, tmp[:k]...)
		for {
			f, used, ok, perr := c.cdc.parse(c.buf)
			if perr != nil {
				c.t.Fatalf("parse: %v", perr)
			}
			if !ok {
				break
			}
			if err := c.cdc.deliver(f, c.recv, zdec, func(msg []byte, _ bool) error {
				c.msgs = append(c.msgs, append([]byte(nil), msg...))
				return nil
			}); err != nil {
				c.t.Fatalf("deliver: %v", err)
			}
			c.buf = c.buf[used:]
		}
	}
	return c.msgs
}

type e2eEnv struct {
	srv  *Server[string]
	addr string
}

func startEcho(t *testing.T, mut func(*Options[string]), serverCipher func() Cipher) e2eEnv {
	t.Helper()
	opts := Options[string]{
		Addr:  "127.0.0.1:0",
		Loops: 2,
		Handler: funcHandler[string]{
			open: func(c *Conn[string]) (string, error) {
				if serverCipher != nil {
					c.SetCipher(serverCipher())
				}
				return "s", nil
			},
			msg: func(c *Conn[string], m []byte) error {
				return c.Send(append([]byte("echo:"), m...))
			},
		},
	}
	if mut != nil {
		mut(&opts)
	}
	srv, err := Listen(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return e2eEnv{srv: srv, addr: srv.Addr().String()}
}

func TestE2E_TCPEchoMatrix(t *testing.T) {
	type cipherPair struct {
		name   string
		server func() Cipher // 服务端 OnOpen 里 SetCipher
		up     func() Cipher // 客户端封上行
		down   func() Cipher // 客户端解下行
	}
	key := [16]byte{7, 7, 7}
	pairs := []cipherPair{
		{"无密", nil, func() Cipher { return nil }, func() Cipher { return nil }},
		{"Overhead0", func() Cipher { return &xorCipher{key: 0x3c} },
			func() Cipher { return &xorCipher{key: 0x3c} },
			func() Cipher { return &xorCipher{key: 0x3c} }},
		// pairGCM 的第二个实例就是对端：其 Seal 方向对上服务端 Open，反之亦然。
		{"AEAD16", func() Cipher { s, _ := pairGCM(key); return s },
			func() Cipher { _, r := pairGCM(key); return r },
			func() Cipher { _, r := pairGCM(key); return r }},
	}
	for _, pc := range pairs {
		for _, compress := range []int{0, 64} {
			for _, cluster := range []int{0, 256} {
				name := fmt.Sprintf("%s/z%d/c%d", pc.name, compress, cluster)
				t.Run(name, func(t *testing.T) {
					env := startEcho(t, func(o *Options[string]) {
						o.Outbound.CompressThreshold = compress
						o.Outbound.MaxCluster = cluster
					}, pc.server)
					cl := dialTCP(t, env.addr, pc.up(), pc.down())
					var want [][]byte
					for i := range 20 {
						m := bytes.Repeat([]byte{byte('a' + i%26)}, 5+i*17)
						want = append(want, append([]byte("echo:"), m...))
						cl.sendMsgs(m)
					}
					got := cl.readMsgs(len(want))
					for i := range want {
						if !bytes.Equal(got[i], want[i]) {
							t.Fatalf("%s 第 %d 条不一致", name, i)
						}
					}
				})
			}
		}
	}
}

// 第三方 WS 客户端互通（gorilla/websocket）。
func TestE2E_WSGorillaInterop(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) {
		o.WebSocket = &WebSocketOptions{Path: "/game"}
		o.Outbound.MaxCluster = 256
	}, nil)

	ws, _, err := websocket.DefaultDialer.Dial("ws://"+env.addr+"/game", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	// 一个 WS 二进制消息装两条 gate 帧。
	if err := ws.WriteMessage(websocket.BinaryMessage, gateWire(t, []byte("m1"), []byte("m2"))); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	zdec := mustDecoder(t)
	cdc := clientCodec(maxMessageSize, maxMessageSize)
	var buf []byte
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	for len(got) < 2 {
		mt, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if mt != websocket.BinaryMessage {
			t.Fatalf("mt=%d", mt)
		}
		buf = append(buf, data...)
		for {
			f, used, ok, perr := cdc.parse(buf)
			if perr != nil || (!ok && used == 0 && len(buf) > 0 && len(got) == 2) {
				t.Fatalf("parse: %v", perr)
			}
			if !ok {
				break
			}
			_ = cdc.deliver(f, nil, zdec, func(m []byte, _ bool) error {
				got = append(got, append([]byte(nil), m...))
				return nil
			})
			buf = buf[used:]
		}
	}
	if string(got[0]) != "echo:m1" || string(got[1]) != "echo:m2" {
		t.Fatalf("got %q", got)
	}

	// Shutdown → 对端应收到 1001 going away 的 close 帧。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = env.srv.Shutdown(ctx) }()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			var ce *websocket.CloseError
			if !errors.As(err, &ce) || ce.Code != websocket.CloseGoingAway {
				t.Fatalf("want 1001, got %v", err)
			}
			break
		}
	}
}

func TestE2E_WSUpgradeReject(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) {
		o.WebSocket = &WebSocketOptions{
			OnUpgrade: func(hs *Handshake) error {
				if hs.Header.Get("Authorization") == "" {
					return RejectUpgrade(401, "unauthorized")
				}
				return nil
			},
		}
	}, nil)
	_, resp, err := websocket.DefaultDialer.Dial("ws://"+env.addr+"/", nil)
	if err == nil {
		t.Fatal("无鉴权头应被拒")
	}
	if resp == nil || resp.StatusCode != 401 {
		t.Fatalf("resp=%v", resp)
	}
	// 带鉴权头则升级成功。
	hdr := map[string][]string{"Authorization": {"token"}}
	ws, _, err := websocket.DefaultDialer.Dial("ws://"+env.addr+"/", hdr)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()
}

func TestE2E_ShutdownNotifiesConns(t *testing.T) {
	var mu sync.Mutex
	var reasons []error
	env := startEcho(t, func(o *Options[string]) {
		h := o.Handler.(funcHandler[string])
		h.clsd = func(c *Conn[string], reason error) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		}
		o.Handler = h
	}, nil)

	cl := dialTCP(t, env.addr, nil, nil)
	cl.sendMsgs([]byte("hi"))
	cl.readMsgs(1) // 确保连接已 Open

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := env.srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || !errors.Is(reasons[0], ErrServerClosed) {
		t.Fatalf("reasons=%v", reasons)
	}
	if err := env.srv.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_MaxConnsRejects(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) {
		o.Limits.MaxConns = 2
	}, nil)
	c1 := dialTCP(t, env.addr, nil, nil)
	c2 := dialTCP(t, env.addr, nil, nil)
	c1.sendMsgs([]byte("a"))
	c2.sendMsgs([]byte("b"))
	c1.readMsgs(1)
	c2.readMsgs(1) // 前两条活着

	c3, err := net.DialTimeout("tcp", env.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	_ = c3.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := c3.Read(make([]byte, 1)); err == nil {
		t.Fatal("第 3 条连接应被 accept 后立即关闭（明确拒绝）")
	}
	if got := env.srv.Stats().ConnsRejected; got < 1 {
		t.Fatalf("ConnsRejected=%d", got)
	}
}

func TestE2E_ProxyV1RealRemote(t *testing.T) {
	seen := make(chan netip.AddrPort, 1)
	env := startEcho(t, func(o *Options[string]) {
		o.Proxy = ProxyRequired
		h := o.Handler.(funcHandler[string])
		h.open = func(c *Conn[string]) (string, error) {
			seen <- c.Remote()
			return "s", nil
		}
		o.Handler = h
	}, nil)

	cl := dialTCP(t, env.addr, nil, nil)
	if _, err := cl.conn.Write([]byte("PROXY TCP4 203.0.113.9 10.0.0.1 34567 80\r\n")); err != nil {
		t.Fatal(err)
	}
	cl.sendMsgs([]byte("after-proxy"))
	got := cl.readMsgs(1)
	if string(got[0]) != "echo:after-proxy" {
		t.Fatalf("got %q", got)
	}
	select {
	case r := <-seen:
		if r.String() != "203.0.113.9:34567" {
			t.Fatalf("Remote()=%s（应是 PROXY 头里的真实来源）", r)
		}
	case <-time.After(time.Second):
		t.Fatal("OnOpen 未触发")
	}

	// ProxyRequired 且没有头：连接被拒（对端读到 EOF）。
	bare := dialTCP(t, env.addr, nil, nil)
	bare.sendMsgs([]byte("no proxy header"))
	_ = bare.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := bare.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("缺 PROXY 头应被关闭")
	}
}

// 并发多客户端：每客户端严格 FIFO（07 第 5 层的实机版；-race 全程有效）。
func TestE2E_ConcurrentClientsFIFO(t *testing.T) {
	env := startEcho(t, func(o *Options[string]) {
		o.Outbound.MaxCluster = 512
		o.Outbound.CompressThreshold = 128
	}, nil)
	const clients, per = 8, 200
	var wg sync.WaitGroup
	for id := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := dialTCP(t, env.addr, nil, nil)
			for i := range per {
				cl.sendMsgs(fmt.Appendf(nil, "c%d-%d", id, i))
			}
			got := cl.readMsgs(per)
			for i := range per {
				want := fmt.Sprintf("echo:c%d-%d", id, i)
				if string(got[i]) != want {
					t.Errorf("client %d 第 %d 条: %q != %q", id, i, got[i], want)
					return
				}
			}
		}()
	}
	wg.Wait()
	st := env.srv.Stats()
	if st.MessagesIn != clients*per || st.MessagesOut != clients*per {
		t.Fatalf("stats: in=%d out=%d", st.MessagesIn, st.MessagesOut)
	}
}

func TestE2E_RunCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options[string]{
			Addr:    "127.0.0.1:0",
			Loops:   1,
			Handler: funcHandler[string]{},
		})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run 未随 ctx 取消退出")
	}
}
