package gate

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/*
并发与压力。

已有的并发用例都是"单点"的：sender 层的并发 Send、Conn 层的 UpdateCipher 竞态、
client 层的 cipher 串行化。缺的是把它们放在一起跑的场景——多连接同时收发、
发送与关闭真正并发、以及带在途流量关服。

这些用例在 -race 下的价值最高：单连接顺序收发的用例几乎不可能触发跨 goroutine
的交错，而 gate 的状态（连接状态机、sender 的唤醒代次、client 的 cipherMu）
恰恰全都是并发结构。
*/

// 多连接压力：每条连接发自己那一份带编号的消息，回来的必须是同一份、同一顺序。
//
// 用带编号的 payload 而不是计数：跨连接串包（把 A 的消息发给 B）在只数数量的
// 断言下完全看不出来。
func TestConcurrentMultiConnectionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}

	const (
		conns       = 24
		perConn     = 40
		payloadSize = 300
	)

	for _, tc := range transportCases() {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.start(t, &Config{
				MaxMessageSize: 1 << 20,
				CHB:            echoBuilder(nil),
				SB: NewSenderBuilder(&SenderConfig{
					CompressThreshold: 256,
					MaxBufferSize:     8 << 20,
					MaxClusterSize:    32 << 10,
				}),
			})

			var wg sync.WaitGroup
			errCh := make(chan error, conns)
			for c := 0; c < conns; c++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					client, handler := tc.dial(t, srv, nil)
					want := make([][]byte, perConn)
					for i := range want {
						want[i] = taggedPayload(id, i, payloadSize)
						if err := client.Send(want[i]); err != nil {
							errCh <- fmt.Errorf("conn %d send %d: %w", id, i, err)
							return
						}
					}

					for i := 0; i < perConn; i++ {
						select {
						case got := <-handler.msgCh:
							if !bytes.Equal(got, want[i]) {
								errCh <- fmt.Errorf("conn %d message %d: content mismatch (cross-connection delivery?)", id, i)
								return
							}
						case <-time.After(30 * time.Second):
							errCh <- fmt.Errorf("conn %d timed out after %d/%d messages", id, i, perConn)
							return
						}
					}
				}(c)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Error(err)
			}
		})
	}
}

// taggedPayload 造一段能自证身份的载荷：前缀标明它属于哪条连接的第几条消息。
func taggedPayload(conn, seq, size int) []byte {
	b := make([]byte, size)
	copy(b, fmt.Sprintf("c%04d-m%04d|", conn, seq))
	for i := len("c0000-m0000|"); i < size; i++ {
		b[i] = byte('a' + (conn+seq+i)%26)
	}
	return b
}

// 同一条连接上多 goroutine 并发调用 **Client.Send**：一条都不能丢，一条都不能重复。
//
// 说明打的是哪一侧：这里验证的是客户端的有界队列 + writeLoop 批量写在多 producer
// 下不丢不重，走的是真实 socket。服务端 sender 的唤醒不变式由
// TestConcurrentServerSideSendRacingWithClose 和 sender_test.go 里那组用例负责。
func TestConcurrentSendFromManyGoroutinesLosesNothing(t *testing.T) {
	const (
		senders   = 8
		perSender = 50
		total     = senders * perSender
	)

	var (
		mu   sync.Mutex
		seen = make(map[string]int, total)
		done = make(chan struct{})
		got  atomic.Int32
	)

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(_ *Conn, raw []byte) {
					mu.Lock()
					seen[string(raw)]++
					mu.Unlock()
					if got.Add(1) == total {
						close(done)
					}
				},
			}
		}},
		SB: DefaultSenderBuilder,
	})

	client, _ := startMatrixTCPClient(t, srv.Addr(), nil)

	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				if err := client.Send([]byte(fmt.Sprintf("s%02d-m%03d", id, i))); err != nil {
					t.Errorf("sender %d message %d: %v", id, i, err)
					return
				}
			}
		}(s)
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("only %d/%d messages arrived", got.Load(), total)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("received %d distinct messages, want %d", len(seen), total)
	}
	for msg, n := range seen {
		if n != 1 {
			t.Fatalf("message %q was delivered %d times", msg, n)
		}
	}
}

// 发送与关闭真正并发：不能 panic，不能死锁，也不能把数据写到一个已经回收的 fd 上。
//
// 判据刻意宽松——Send 返回 nil 不保证消息一定送达（连接可能在那之后立刻关掉）。
// 这条用例守的是"不崩、不卡、-race 干净"，那才是这个交错真正的风险。
func TestConcurrentSendRacingWithClose(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		func() {
			srv := startTestServer(t, &Config{
				CHB: echoAfterPanicBuilder(),
				SB:  DefaultSenderBuilder,
			})

			client := dialUnmanagedClient(t, srv.Addr())

			var wg sync.WaitGroup
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						// 关闭之后 Send 返回 ErrClientClosed 是正常结果，不是失败。
						_ = client.Send([]byte("racing"))
					}
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = client.Close()
			}()

			wg.Wait()
			// 只断言"停得下来"。关闭竞态下 readLoop 可能先于 Close 观测到 EOF 或
			// RST，那都是正常终止；真正的风险是挂死或 panic，waitClientStop 带超时。
			_ = waitClientStop(t, client)
		}()
	}
}

// dialUnmanagedClient 拨一个**不带"必须干净关闭"断言**的 client。
//
// startMatrixTCPClient 的 cleanup 会对 Wait() 的错误报 t.Errorf，那对绝大多数
// 用例是对的。但"故意把服务端关掉"和"故意和 Close 抢跑"这两类用例里，客户端
// 读到 EOF 恰恰是预期结果，用那个 cleanup 会把预期行为报成失败。
func dialUnmanagedClient(t *testing.T, addr string) *Client {
	t.Helper()

	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    addr,
		Handler: newMatrixHandler(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// 服务端侧：业务 goroutine 并发调用 Conn.Send，同时连接从底下被抽掉。
//
// 上面那条 TestConcurrentSendRacingWithClose 打的是 Client；这条打的是 server 的
// sender——两者是完全不同的代码：sender 有队列、唤醒代次（wakeGen）、以及
// writeOutbound 的 writable 闸口，而这些状态恰好都要在"业务从任意 goroutine 发送"
// 和"事件循环正在跑 OnClose"之间保持自洽。
//
// 断言刻意宽松：Send 返回 nil 不保证消息送达（连接可能下一刻就没了），关闭时
// sender 明确允许丢弃未发送的消息。这里守的是不 panic、不写已回收的 fd
// （靠 -race 和"服务端事后仍能正常服务"来体现）、以及 Send 永远返回而不是挂死。
func TestConcurrentServerSideSendRacingWithClose(t *testing.T) {
	conns := make(chan *Conn, 64)
	srv := startTestServer(t, &Config{
		LoopCount: 2,
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, _ []byte) {
					select {
					case conns <- conn:
					default:
					}
				},
			}
		}},
		SB: DefaultSenderBuilder,
	})

	const rounds = 24
	for i := 0; i < rounds; i++ {
		raw, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Write(gateFrame([]byte("hello"))); err != nil {
			t.Fatal(err)
		}

		var conn *Conn
		select {
		case conn = <-conns:
		case <-time.After(5 * time.Second):
			t.Fatal("handler never ran")
		}

		// 业务侧多 goroutine 猛发，同时把连接抽掉。
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						// ErrConnClosed / ErrSendQueueFull 都是正常结果。
						_ = conn.Send([]byte("racing with close"))
					}
				}
			}()
		}
		_ = raw.Close()
		time.Sleep(time.Duration(2+i%5) * time.Millisecond)
		close(stop)
		wg.Wait()
	}

	// 服务端必须还能正常服务新连接。
	client, _ := startMatrixTCPClient(t, srv.Addr(), nil)
	if err := client.Send([]byte("after the storm")); err != nil {
		t.Fatal(err)
	}
	select {
	case conn := <-conns:
		if conn == nil {
			t.Fatal("server stopped handling connections")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server is unusable after the close storm")
	}
}

// 带在途流量关服：Stop 必须干净返回，事件循环不能 panic。
func TestConcurrentServerStopWithInflightTraffic(t *testing.T) {
	srv := startTestServer(t, &Config{
		CHB: echoAfterPanicBuilder(),
		SB:  DefaultSenderBuilder,
	})

	// 这条用例会把服务端关掉，客户端读到 EOF 是预期结果，
	// 所以不能用那个断言"必须干净关闭"的 cleanup。
	client := dialUnmanagedClient(t, srv.Addr())

	// 先确认链路通了，再在持续发送的同时关服。
	if err := client.Send([]byte("warmup")); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = client.Send([]byte("inflight"))
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("stop server under load: %v", err)
	}
	close(stop)
	wg.Wait()

	if err := srv.Wait(); err != nil {
		t.Fatalf("server exited with %v", err)
	}
}

// 服务端在收发过程中换密钥：换钥之后的帧必须用新钥解，且不能有 data race。
//
// 这是 UpdateCipher 最典型的用法（异步握手完成后换钥），而它跨 goroutine：
// 调用方通常在 AsyncDo 的 goroutine 上，读取方在事件循环上。
func TestConcurrentCipherSwapDuringTraffic(t *testing.T) {
	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					_ = conn.SendNoEncrypt(raw)
				},
			}
		}},
		SB: DefaultSenderBuilder,
	})

	probeState := &cipherProbeState{}
	client, handler := startMatrixTCPClient(t, srv.Addr(), &concurrencyProbeCipher{st: probeState})

	// 关键是出站必须真的走 Send（会加密），下行也必须真的走解密路径，
	// 否则 UpdateCipher 和 encrypt/decrypt 之间根本没有并发访问，
	// 这条用例就退化成"调 UpdateCipher 不 panic"。
	//
	// 这里断言的是 cipherMu 的作用：Encrypt/Decrypt/UpdateCipher 三者串行，
	// 不会把一个正在被使用的 Cipher 换掉、也不会两个方向同时进同一个 Cipher。
	// 内容正确性不做断言——两端没有协商机制，换钥期间服务端用旧钥解新钥的帧
	// 是合法结果（服务端会因为解出垃圾而断链，那也在预期内）。
	// 换上去的也一直是 probe：换成别的实现的话，换钥之后的调用就不再被观测，
	// 这条用例会因为"没观测到重叠"而假通过。
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			client.UpdateCipher(&concurrencyProbeCipher{st: probeState})
		}
	}()

	var sent int
	for i := 0; i < 200; i++ {
		if err := client.Send([]byte("ciphertext")); err != nil {
			break
		}
		sent++
	}
	wg.Wait()
	_ = handler

	// Send 只是入队，真正的加密发生在 writeLoop 上。必须等它确实跑过，
	// 否则在机器负载高的时候断言会跑在 writeLoop 前面，calls 还是 0——
	// 这条用例最初就是这么 flaky 的。
	deadline := time.Now().Add(10 * time.Second)
	for probeState.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if probeState.overlaps.Load() != 0 {
		t.Fatalf("Cipher was entered concurrently %d time(s); cipherMu must serialise it",
			probeState.overlaps.Load())
	}
	if sent == 0 {
		t.Fatal("no message was sent; the test exercised nothing")
	}
	if probeState.calls.Load() == 0 {
		t.Fatal("the cipher was never invoked; the swap raced against nothing")
	}
}

// cipherProbeState 是所有 probe 实例共享的计数器：换钥会换掉实例，
// 计数必须活得比任何一个实例长。
type cipherProbeState struct {
	inside   atomic.Int32
	calls    atomic.Int64
	overlaps atomic.Int64
}

// concurrencyProbeCipher 检测自己是否被并发进入。
//
// Cipher 的契约只要求"单线程可用"（server 侧所有调用都被事件循环串行化），
// 所以 client 必须自己保证同一时刻只有一个 goroutine 在里面——一个带 nonce
// 计数器或复用 scratch buffer 的实现，在并发调用下会静默把两个方向的数据都搞坏。
type concurrencyProbeCipher struct{ st *cipherProbeState }

func (c *concurrencyProbeCipher) Encrypt(data []byte) { c.apply(data) }
func (c *concurrencyProbeCipher) Decrypt(data []byte) { c.apply(data) }

func (c *concurrencyProbeCipher) apply(data []byte) {
	if c.st.inside.Add(1) != 1 {
		c.st.overlaps.Add(1)
	}
	c.st.calls.Add(1)
	for i := range data {
		data[i] ^= 0x5a
	}
	c.st.inside.Add(-1)
}
