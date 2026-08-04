package gate

import (
	"bytes"
	"net"
	"testing"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
)

// addrOnlyConn 是一个只回答 RemoteAddr 的 gnet.Conn，用来喂 remoteAddrOf
// 那些 gnet 真的会返回的退化取值。
type addrOnlyConn struct {
	gnet.Conn
	addr net.Addr
}

func (c *addrOnlyConn) RemoteAddr() net.Addr { return c.addr }

// unknownAddr 是一个既不是 TCPAddr 也不是 UDPAddr 的地址实现。
type unknownAddr struct{}

func (unknownAddr) Network() string { return "unknown" }
func (unknownAddr) String() string  { return "unknown" }

/*
畸形输入与协议违规。

判据统一是两条：**必须报错**（进而关连接），并且 **handler 一次都不能被调用**。
第二条尤其重要——一个"报了错但已经把半条消息交给业务"的实现，会让业务层看到
根本不该存在的状态。
*/

// wsFrameBytes 把一个 ws.Frame 编码成线路字节，mask 决定是否按客户端帧加掩码。
func wsFrameBytes(tb testing.TB, f ws.Frame, mask bool) []byte {
	tb.Helper()

	if mask {
		f = ws.MaskFrameInPlace(f)
	}
	b, err := ws.CompileFrame(f)
	if err != nil {
		tb.Fatalf("compile frame: %v", err)
	}
	return b
}

func newFrameWith(op ws.OpCode, fin bool, payload []byte) ws.Frame {
	return ws.NewFrame(op, fin, append([]byte(nil), payload...))
}

// 非法的 WebSocket 帧头必须在投递给业务之前就被拒掉。
func TestFailureWebSocketRejectsIllegalFrames(t *testing.T) {
	valid := gateFrame([]byte("payload"))

	cases := []struct {
		name  string
		build func(tb testing.TB) []byte
	}{
		{
			// RFC6455 §5.1：客户端发往服务端的帧必须带掩码。
			name: "unmasked client frame",
			build: func(tb testing.TB) []byte {
				return wsFrameBytes(tb, newFrameWith(ws.OpBinary, true, valid), false)
			},
		},
		{
			// 没有协商任何扩展，RSV 位必须为 0。
			name: "reserved bits set",
			build: func(tb testing.TB) []byte {
				f := newFrameWith(ws.OpBinary, true, valid)
				f.Header.Rsv = ws.Rsv(true, false, false)
				return wsFrameBytes(tb, f, true)
			},
		},
		{
			name: "reserved opcode",
			build: func(tb testing.TB) []byte {
				return wsFrameBytes(tb, newFrameWith(ws.OpCode(0x3), true, valid), true)
			},
		},
		{
			// gate 只承载二进制帧；文本帧不是合法的 gate 载体。
			name: "text frame",
			build: func(tb testing.TB) []byte {
				return wsFrameBytes(tb, newFrameWith(ws.OpText, true, []byte("hello")), true)
			},
		},
		{
			name: "continuation without a start frame",
			build: func(tb testing.TB) []byte {
				return wsFrameBytes(tb, newFrameWith(ws.OpContinuation, true, valid), true)
			},
		},
		{
			// 分片还没结束又来一个新的起始帧。
			name: "new binary frame while fragmented",
			build: func(tb testing.TB) []byte {
				var buf bytes.Buffer
				buf.Write(wsFrameBytes(tb, newFrameWith(ws.OpBinary, false, valid[:3]), true))
				buf.Write(wsFrameBytes(tb, newFrameWith(ws.OpBinary, true, valid), true))
				return buf.Bytes()
			},
		},
		{
			// RFC6455 §5.5：控制帧不得分片。
			name: "fragmented control frame",
			build: func(tb testing.TB) []byte {
				return wsFrameBytes(tb, newFrameWith(ws.OpPing, false, []byte("x")), true)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, _ := newControlState(t)
			var delivered int
			state.conn.handler = &funcHandler{fn: func([]byte) { delivered++ }}
			state.buf.Write(tc.build(t))

			if err := state.decodeMessages(); err == nil {
				t.Fatal("expected the frame to be rejected")
			}
			if delivered != 0 {
				t.Fatalf("handler was called %d time(s) for a rejected frame", delivered)
			}
		})
	}
}

// 握手路径必须精确匹配，不能被前缀伪造绕过。
//
// matchesWebSocketPath 允许的只有 path 本身和 path+"?query"；
// "/wsx" 这种共享前缀的路径必须被拒。
func TestFailureWebSocketPathMatching(t *testing.T) {
	cases := []struct {
		uri  string
		path string
		want bool
	}{
		{"/ws", "/ws", true},
		{"/ws?token=1", "/ws", true},
		{"/ws?", "/ws", true},
		{"/wsx", "/ws", false},
		{"/ws/sub", "/ws", false},
		{"/", "/ws", false},
		{"/w", "/ws", false},
		{"", "/ws", false},
		{"/", "/", true},
		{"/?a=b", "/", true},
	}

	for _, tc := range cases {
		if got := matchesWebSocketPath(tc.uri, tc.path); got != tc.want {
			t.Errorf("matchesWebSocketPath(%q, %q) = %t, want %t", tc.uri, tc.path, got, tc.want)
		}
	}
}

// sink 返回错误时必须立刻停止投递后续子消息。
//
// 反过来的实现（继续遍历、只记住最后一个错误）会让一条 compound 里
// handler panic 之后的兄弟消息继续被交付给一个已经出问题的 handler。
func TestFailureCompoundStopsDeliveringAfterSinkError(t *testing.T) {
	body := bytes.Join([][]byte{
		gateFrame([]byte("one")),
		gateFrame([]byte("two")),
		gateFrame([]byte("three")),
	}, nil)

	c := clientCodec(maxMessageSize, maxMessageSize)
	var seen [][]byte
	err := c.deliver(frame{payload: body, c: true}, nil, nil, func(msg []byte, _ bool) error {
		seen = append(seen, append([]byte(nil), msg...))
		if len(seen) == 2 {
			return errHandlerPanic
		}
		return nil
	})

	if err == nil {
		t.Fatal("expected the sink error to propagate")
	}
	if len(seen) != 2 {
		t.Fatalf("sink was called %d time(s), want 2 (delivery must stop at the failing message)", len(seen))
	}
}

// compound 子消息的 cap 必须封在自己的 payload 末尾。
//
// 少了这层三索引切片，sink 里对第一条子消息做一次 append 就能悄悄覆盖掉紧随
// 其后的兄弟消息，而且两边都察觉不到。
func TestFailureCompoundSubMessagesAreCapIsolated(t *testing.T) {
	body := bytes.Join([][]byte{
		gateFrame([]byte("aaa")),
		gateFrame([]byte("bbb")),
	}, nil)

	c := clientCodec(maxMessageSize, maxMessageSize)
	var msgs [][]byte
	if err := c.deliver(frame{payload: body, c: true}, nil, nil, func(msg []byte, _ bool) error {
		if len(msgs) == 0 {
			if cap(msg) != len(msg) {
				t.Fatalf("sub-message cap %d exceeds len %d; an append would reach into its sibling",
					cap(msg), len(msg))
			}
			// 真的 append 一次：有三索引切片保护的话这会分配一块新内存，
			// 兄弟消息不受影响。
			_ = append(msg, 'X')
		}
		msgs = append(msgs, append([]byte(nil), msg...))
		return nil
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	if len(msgs) != 2 {
		t.Fatalf("got %d sub-messages, want 2", len(msgs))
	}
	if string(msgs[1]) != "bbb" {
		t.Fatalf("sibling message was corrupted by an append into the first: %q", msgs[1])
	}
}

// remoteAddrOf 的兜底路径：gnet 在 getpeername 失败时会返回 nil。
// 裸类型断言会在事件循环 goroutine 里 panic，进而打死整个进程。
func TestFailureRemoteAddrHandlesMissingPeer(t *testing.T) {
	cases := []struct {
		name string
		conn *addrOnlyConn
	}{
		{"nil addr", &addrOnlyConn{}},
		{"unknown addr type", &addrOnlyConn{addr: unknownAddr{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip, port := remoteAddrOf(tc.conn)
			if ip != nil || port != 0 {
				t.Fatalf("remoteAddrOf = %v:%d, want nil:0", ip, port)
			}
			// Remote() 也必须能安全地格式化出来——它会被写进每一条错误日志。
			c := &Conn{remoteIp: ip, remotePort: port}
			if c.Remote() == "" {
				t.Fatal("Remote() returned an empty string")
			}
		})
	}
}
