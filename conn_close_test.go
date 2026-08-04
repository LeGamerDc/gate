package gate

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
)

/*
handler 调用 Conn.Close() 之后，同一批里剩下的消息不能再被投递。

这条不是洁癖。最典型的用法是"第一条消息鉴权失败 → 关连接"，而一个 TCP 读事件
（或一个 WebSocket 二进制消息）里完全可以塞进多条消息。如果关闭之后剩下的消息
照样进 Handle，那么攻击者只要把 [伪造的登录包, 真正想执行的命令] 拼在一个包里
发出去，命令就会在一条已经被判定为非法的连接上被执行。

gnet 的 Close 是异步的（排一个 task 到事件循环），所以"已经调用过 Close"这件事
必须由 gate 自己就地记下来，不能等 OnClose 把状态改成 closed 之后才生效。
*/

// twoMessagesInOneBatch 把两条 gate 帧拼成一段字节，让它们尽可能落在同一个读事件里。
func twoMessagesInOneBatch(first, second []byte) []byte {
	return append(gateFrame(first), gateFrame(second)...)
}

func TestConnCloseStopsDeliveringRestOfTCPBatch(t *testing.T) {
	delivered := make(chan string, 4)
	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{
				conn: conn,
				handle: func(conn *Conn, raw []byte) {
					delivered <- string(raw)
					// 第一条就判定这条连接不该继续服务。
					conn.Close()
				},
			}
		}},
		SB: DefaultSenderBuilder,
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write(twoMessagesInOneBatch([]byte("auth-fail"), []byte("exploit"))); err != nil {
		t.Fatal(err)
	}

	if got := <-delivered; got != "auth-fail" {
		t.Fatalf("first message = %q", got)
	}
	select {
	case got := <-delivered:
		t.Fatalf("message %q was delivered after the handler closed the connection", got)
	case <-time.After(500 * time.Millisecond):
	}
}

// WebSocket 侧同理：一个二进制消息里可以承载多条 gate 帧。
func TestConnCloseStopsDeliveringRestOfWebSocketMessage(t *testing.T) {
	state, g, delivered := newFakeWSConn(t, "/ws", 4096, 1<<20)
	state.conn.handler = &funcHandler{fn: func(b []byte) {
		*delivered = append(*delivered, append([]byte(nil), b...))
		state.conn.Close()
	}}

	g.in.Write(handshakeRequest("/ws"))
	if action := state.onTraffic(); action != gnet.None {
		t.Fatalf("handshake failed: %v", action)
	}

	g.in.Write(maskedWSFrame(t, ws.OpBinary,
		twoMessagesInOneBatch([]byte("auth-fail"), []byte("exploit"))))
	state.onTraffic()

	if len(*delivered) != 1 {
		t.Fatalf("got %d message(s), want 1; delivery must stop once the handler closes the connection: %q",
			len(*delivered), *delivered)
	}
	if !bytes.Equal((*delivered)[0], []byte("auth-fail")) {
		t.Fatalf("first message = %q", (*delivered)[0])
	}
}

// 一条已经关闭的连接上，业务留着的旧 *Conn 再调一次 Close 不能真的碰底层 fd。
//
// gnet 判断"这个连接是不是陈旧的"只看 fd 号还在不在当前事件循环的表里，不看
// 表里那个对象是不是同一条连接。fd 号在连接关闭之后会被操作系统立刻复用，
// 所以一次迟到的 Close 完全可能落到一条毫不相干的新连接上，把别人的会话关掉。
func TestConnCloseIsSuppressedAfterClose(t *testing.T) {
	g := &fakeGnetConn{}
	conn := &Conn{conn: g}
	conn.init()
	conn.markOpen()

	conn.Close()
	if g.closeCount() != 1 {
		t.Fatalf("first Close issued %d gnet close(s), want 1", g.closeCount())
	}

	// 连接完成关闭之后，业务手里的旧 *Conn 再关一次。
	conn.markClosed()
	conn.Close()
	conn.Close()
	if g.closeCount() != 1 {
		t.Fatalf("a stale *Conn issued %d gnet close(s); the fd may already belong to another connection",
			g.closeCount())
	}
}

// 同理，迟到的 wake 也不能碰一个已经回收的 fd。
func TestConnWakeIsSuppressedAfterClose(t *testing.T) {
	g := &fakeGnetConn{}
	conn := &Conn{conn: g}
	conn.init()
	conn.markClosed()

	conn.wake()
	if g.wakeCount() != 0 {
		t.Fatalf("a stale *Conn issued %d wake(s) on a possibly recycled fd", g.wakeCount())
	}
}

// OnOpen 阶段构造失败时，这个 *Conn 必须就地落成 closed。
//
// 那条路径返回 gnet.Close 之后 gnet 会走 OnClose，但此刻还没 SetContext，
// connOf() 取不到它，OnClose 直接返回——markClosed 不会被执行。而 Build 拿到过
// 这个 *Conn，业务完全可能在 panic 之前就把它存到了别处；一个永远停在 stateOpen
// 的陈旧 Conn，之后任何一次 Send/Close 都会去碰一个已经被回收的 fd。
func TestConnIsClosedWhenBuildFails(t *testing.T) {
	cases := []struct {
		name string
		cfg  func(captured chan *Conn) *Config
	}{
		{
			name: "handler builder panics",
			cfg: func(captured chan *Conn) *Config {
				return &Config{
					CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
						captured <- conn
						panic("build exploded")
					}},
					SB: DefaultSenderBuilder,
				}
			},
		},
		{
			name: "handler builder returns nil",
			cfg: func(captured chan *Conn) *Config {
				return &Config{
					CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
						captured <- conn
						return nil
					}},
					SB: DefaultSenderBuilder,
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captured := make(chan *Conn, 1)
			srv := startTestServer(t, tc.cfg(captured))

			conn, err := net.Dial("tcp", srv.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			var got *Conn
			select {
			case got = <-captured:
			case <-time.After(5 * time.Second):
				t.Fatal("builder was never called")
			}

			// captured 是在 Build **内部**发出来的，也就是在 panic / return nil
			// 之前——此刻 OnOpen 还没走到善后逻辑。所以这里要等一小会儿，
			// 而不是立刻断言（否则测的是调度顺序，不是被测行为）。
			deadline := time.Now().Add(5 * time.Second)
			for !got.isClosed() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !got.isClosed() {
				t.Fatal("a Conn whose build failed was left in a non-closed state")
			}
			if !got.stopped() {
				t.Fatal("a Conn whose build failed must not accept further delivery")
			}
			if err := got.Send([]byte("x")); err == nil {
				t.Fatal("Send on a failed-build Conn must fail")
			}
		})
	}
}

// "回一条拒绝消息，然后关连接"是最常见的业务模式之一，必须仍然能把那条消息送达。
//
// closing 只停入站投递，不进 writable()：调用 Close 之后、gnet 的关闭 task 真正
// 执行之前 fd 还是活的，已经入队的消息应当照常刷出去。把 closing 也算进出站闸口
// 的话，这条消息会被静默丢掉，而业务完全看不出来。
func TestConnCloseStillFlushesAlreadyQueuedMessage(t *testing.T) {
	for _, tc := range transportCases() {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.start(t, &Config{
				CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
					return &testConnHandler{
						conn: conn,
						handle: func(conn *Conn, _ []byte) {
							if err := conn.SendNoEncrypt([]byte("rejected")); err != nil {
								t.Errorf("send rejection: %v", err)
							}
							conn.Close()
						},
					}
				}},
				SB: DefaultSenderBuilder,
			})

			client, handler := tc.dial(t, srv, nil)
			if err := client.Send([]byte("please")); err != nil {
				t.Fatal(err)
			}
			if got := waitMessage(t, handler.msgCh); string(got) != "rejected" {
				t.Fatalf("got %q, want %q", got, "rejected")
			}
		})
	}
}
