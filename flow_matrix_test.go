package gate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

/*
特性矩阵：同一份用例在两种传输上各跑一遍。

此前 e2e 覆盖的是"挑几个有代表性的组合各写一个用例"，于是两类问题没人守：

  - 尺寸边界。gate 的 header 在 4096 字节处从 2 字节切到 4 字节（moreHeaderSize），
    WebSocket 帧头在 126 和 65536 处切换长度字段宽度。三个切换点附近此前没有任何
    端到端用例。
  - 传输一致性。TCP 和 WebSocket 是两套完全独立的入站状态机（conn.onTraffic 与
    wsConnState.onTraffic），却承诺对业务层呈现相同语义。用同一份断言跑两遍是
    唯一能持续证明这件事的办法。
*/

// transportCase 把"起一个 server + 连一个 client"抽象成传输无关的两步，
// 让同一份用例可以在 TCP 和 WebSocket 上各跑一遍。
type transportCase struct {
	name string
	// start 起一个该传输的 server。cfg 里与传输相关的字段由 start 负责填。
	start func(t *testing.T, cfg *Config) *Server
	// dial 连上 srv，返回 client 和它的记录型 handler。
	dial func(t *testing.T, srv *Server, cipher Cipher) (*Client, *testClientHandler)
}

func transportCases() []transportCase {
	return []transportCase{
		{
			name:  "tcp",
			start: startTestServer,
			dial: func(t *testing.T, srv *Server, cipher Cipher) (*Client, *testClientHandler) {
				return startMatrixTCPClient(t, srv.Addr(), cipher)
			},
		},
		{
			name: "websocket",
			start: func(t *testing.T, cfg *Config) *Server {
				cfg.Transport = ServerTransportWebSocket
				cfg.WebSocketPath = "/ws"
				return startTestWebSocketServer(t, cfg)
			},
			dial: func(t *testing.T, srv *Server, cipher Cipher) (*Client, *testClientHandler) {
				return startMatrixWSClient(t, "ws://"+srv.Addr()+"/ws", cipher)
			},
		},
	}
}

// matrixQueueSize 要能装下一次批量用例的全部消息：OnMessage 会阻塞 readLoop，
// 缓冲太小的话用例本身的"先全发再全收"写法会自己把自己卡住。
const matrixQueueSize = 512

func newMatrixHandler() *testClientHandler {
	return &testClientHandler{
		msgCh:     make(chan []byte, matrixQueueSize),
		closeCh:   make(chan error, 1),
		connectCh: make(chan struct{}, 1),
	}
}

func startMatrixTCPClient(t *testing.T, addr string, cipher Cipher) (*Client, *testClientHandler) {
	t.Helper()

	handler := newMatrixHandler()
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    addr,
		Handler: handler,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		if err := client.Wait(); !isPeerShutdown(err) {
			t.Errorf("wait client: %v", err)
		}
	})
	return client, handler
}

// isPeerShutdown 报告这个终止原因是不是"对端把连接关了"。
//
// 用例的 cleanup 需要区分两件事：客户端因为**协议错误**收场（真问题），
// 和服务端主动关掉了连接（很多用例的预期结果，比如 handler 回一条拒绝消息就关连接、
// 或者故意把 server 停掉）。
//
// 服务端主动关闭在 WebSocket 上一律表现为 1006 abnormal closure——gate 在本地
// 主动关闭时不发 WS close 帧，直接关 TCP，所以对端看不到状态码。
func isPeerShutdown(err error) bool {
	switch {
	case err == nil,
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, syscall.ECONNRESET):
		return true
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway,
			websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived:
			return true
		}
	}
	return false
}

func startMatrixWSClient(t *testing.T, url string, cipher Cipher) (*Client, *testClientHandler) {
	t.Helper()

	handler := newMatrixHandler()
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
		if err := client.Wait(); !isPeerShutdown(err) {
			t.Errorf("wait websocket client: %v", err)
		}
	})
	return client, handler
}

// echoBuilder 造一个把收到的消息原样回发的 handler，并按需给连接装上 cipher。
//
// 回发走 conn.Send（而不是 SendNoEncrypt）：只有它才会让 sender 的压缩、合包、
// 加密三条路径真正参与进来，这正是矩阵要覆盖的东西。
func echoBuilder(cipher Cipher) ConnHandlerBuilder {
	return testHandlerBuilder{build: func(conn *Conn) ConnHandler {
		if cipher != nil {
			conn.UpdateCipher(cipher)
		}
		return &testConnHandler{
			conn: conn,
			handle: func(conn *Conn, raw []byte) {
				// raw 在 Handle 返回后不允许再持有，但 Send 会立即复制，
				// 所以可以直接传进去。
				_ = conn.Send(raw)
			},
		}
	}}
}

// collectMessages 按顺序取 n 条消息，超时即失败。
func collectMessages(t *testing.T, ch <-chan []byte, n int) [][]byte {
	t.Helper()

	got := make([][]byte, 0, n)
	deadline := time.After(15 * time.Second)
	for len(got) < n {
		select {
		case msg := <-ch:
			got = append(got, append([]byte(nil), msg...))
		case <-deadline:
			t.Fatalf("timeout after %d/%d message(s)", len(got), n)
		}
	}
	return got
}

// boundarySizes 覆盖两个协议各自的长度字段切换点，以及它们的两侧。
//
// 这里的数字是**业务消息长度**，而两个协议的切换点是按各自的载荷算的，
// 中间差着一个 gate header，所以不能直接把切换点当成业务长度用：
//
//	gate header : 业务长度 <4096 用 2 字节，>=4096 用 4 字节（moreHeaderSize）
//	              → 切换点就是业务长度 4095 / 4096
//	ws  header  : WS 载荷 <126 用 7 位，<=65535 用 7+16 位，更大用 7+64 位
//	              而 WS 载荷 = gate header + 业务长度：
//	              → 126 的切换点是业务长度 123/124（2 字节头）
//	              → 65536 的切换点是业务长度 65531/65532（4 字节头）
//
// 0 单独重要：一条空消息在线路上仍然占 2 字节 header，而按 len 记账的代码会
// 认为它不存在（见 sender.frameSize 的注释）。
var boundarySizes = []int{
	0, 1, 2,
	123, 124, 125, 126, // WS 7 位 ↔ 16 位（业务长度 +2）
	4095, 4096, 4097, // gate 2 字节头 ↔ 4 字节头
	65531, 65532, // WS 16 位 ↔ 64 位（业务长度 +4）
	65535, 65536,
	100_000,
}

func TestFlowMessageSizeBoundaries(t *testing.T) {
	for _, tc := range transportCases() {
		for _, enc := range []struct {
			name   string
			cipher Cipher
		}{
			{"plain", nil},
			{"encrypted", xorCipher{key: 0x5a}},
		} {
			t.Run(tc.name+"/"+enc.name, func(t *testing.T) {
				srv := tc.start(t, &Config{
					MaxMessageSize: 1 << 20,
					CHB:            echoBuilder(enc.cipher),
					// 这一组刻意**关掉压缩**：开着压缩的话线路上的字节数由 zstd
					// 决定，boundarySizes 里精心挑出来的那些长度根本落不到目标
					// 分支上——一个 65532 字节的可压缩载荷压完可能只剩几百字节，
					// WS 的 64 位长度分支一次都走不到。压缩与合包的组合由
					// TestFlowBatchedSendPreservesOrderAndContent 覆盖。
					SB: NewSenderBuilder(&SenderConfig{
						CompressThreshold: 0,
						MaxBufferSize:     8 * 1024 * 1024,
						MaxClusterSize:    32 * 1024,
					}),
				})
				client, handler := tc.dial(t, srv, enc.cipher)

				for _, size := range boundarySizes {
					t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
						msg := sizedPayload(size)
						if err := client.Send(msg); err != nil {
							t.Fatalf("send %d bytes: %v", size, err)
						}
						got := collectMessages(t, handler.msgCh, 1)[0]
						if !bytes.Equal(got, msg) {
							t.Fatalf("round-trip mismatch at %d bytes: got %d bytes", size, len(got))
						}
					})
				}
			})
		}
	}
}

// sizedPayload 造一段可压缩但不是全同字节的数据：全同字节会让 zstd 压到极致，
// 掩盖掉"压缩后反而更大"之类的分支。
func sizedPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i*7+i/13)%26)
	}
	return b
}

// senderFeatureCases 是 sender 的两个独立开关的四种组合。
//
// 它们确实是独立的：README 明确说关掉压缩不会连带关掉合并，
// 大量小消息正是"该合包但不值得压缩"的场景。
func senderFeatureCases() []struct {
	name string
	cfg  *SenderConfig
} {
	return []struct {
		name string
		cfg  *SenderConfig
	}{
		{"plain", &SenderConfig{MaxBufferSize: 8 << 20}},
		{"compress", &SenderConfig{CompressThreshold: 256, MaxBufferSize: 8 << 20}},
		{"compound", &SenderConfig{MaxBufferSize: 8 << 20, MaxClusterSize: 32 << 10}},
		{"compress+compound", &SenderConfig{CompressThreshold: 256, MaxBufferSize: 8 << 20, MaxClusterSize: 32 << 10}},
	}
}

// 批量回发：一条触发消息让 handler 在同一次 Handle 里连发 N 条。
//
// 这样 N 条消息一定在同一次 flush 里被 sender 看到，合包/压缩路径才会真的被走到；
// 靠客户端连发 N 条来碰运气的话，它们可能被拆进 N 次事件，一条也合不起来。
func TestFlowBatchedSendPreservesOrderAndContent(t *testing.T) {
	const batch = 33 // 跨过 separateChunk=32，强制走多个 chunk

	for _, tc := range transportCases() {
		for _, fc := range senderFeatureCases() {
			for _, enc := range []struct {
				name   string
				cipher Cipher
			}{
				{"plain", nil},
				{"encrypted", xorCipher{key: 0x2c}},
			} {
				t.Run(tc.name+"/"+fc.name+"/"+enc.name, func(t *testing.T) {
					want := make([][]byte, batch)
					for i := range want {
						// 混合尺寸：跨过 4096 的 header 切换点，也混进空消息。
						want[i] = sizedPayload((i * 271) % 5000)
					}

					srv := tc.start(t, &Config{
						MaxMessageSize: 1 << 20,
						CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
							if enc.cipher != nil {
								conn.UpdateCipher(enc.cipher)
							}
							return &testConnHandler{
								conn: conn,
								handle: func(conn *Conn, _ []byte) {
									for _, msg := range want {
										if err := conn.Send(msg); err != nil {
											t.Errorf("server send: %v", err)
											return
										}
									}
								},
							}
						}},
						SB: NewSenderBuilder(fc.cfg),
					})
					client, handler := tc.dial(t, srv, enc.cipher)

					if err := client.Send([]byte("go")); err != nil {
						t.Fatal(err)
					}

					got := collectMessages(t, handler.msgCh, batch)
					for i := range want {
						if !bytes.Equal(got[i], want[i]) {
							t.Fatalf("message %d mismatch: got %d bytes, want %d bytes",
								i, len(got[i]), len(want[i]))
						}
					}
				})
			}
		}
	}
}

// 上行方向：客户端连发 N 条，服务端必须按序、按内容收齐。
//
// 注意它证明的是**顺序与完整性**，不是"批量写生效了"——把 collectFrames 换回
// 逐条 Write，这条用例照样通过。批量本身是性能行为，由
// TestClientCollectFramesBatchesQueuedMessages 和 BenchmarkClientEncodeOutbound 覆盖。
// 这里真正守的是 client 的 net.Buffers 写法没有打乱帧边界或帧顺序。
func TestFlowClientBatchedUplinkPreservesOrder(t *testing.T) {
	const batch = 64

	for _, tc := range transportCases() {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan []byte, batch)
			srv := tc.start(t, &Config{
				MaxMessageSize: 1 << 20,
				CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
					return &testConnHandler{
						conn: conn,
						handle: func(_ *Conn, raw []byte) {
							got <- append([]byte(nil), raw...)
						},
					}
				}},
				SB: DefaultSenderBuilder,
			})
			client, _ := tc.dial(t, srv, nil)

			want := make([][]byte, batch)
			for i := range want {
				want[i] = sizedPayload((i * 137) % 9000)
				if err := client.Send(want[i]); err != nil {
					t.Fatalf("send %d: %v", i, err)
				}
			}

			for i := range want {
				select {
				case msg := <-got:
					if !bytes.Equal(msg, want[i]) {
						t.Fatalf("uplink message %d mismatch: got %d bytes, want %d bytes",
							i, len(msg), len(want[i]))
					}
				case <-time.After(15 * time.Second):
					t.Fatalf("timeout waiting for uplink message %d", i)
				}
			}
		})
	}
}
