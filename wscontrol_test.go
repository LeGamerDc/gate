package gate

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
)

// WebSocket 控制帧（ping / pong / close）此前是整个仓库覆盖率为 0 的一块，
// 而它是 RFC6455 的必须项：不回 pong 的服务端会被浏览器和大多数 LB 判死。
//
// 这里的用例直接驱动 wsConnState，而不是走真实 socket：控制帧的响应由
// wsutil 直接写在 gnet.Conn 上（见 handleControlFrame），用一个只实现 Write
// 的假连接才能逐字节检查它写出去的是什么。

// wsControlConn 是一个只实现 Write 的 gnet.Conn。
//
// handleControlFrame 把 w.conn.conn 当成 io.Writer 直接交给 wsutil，
// 其余方法一律不该被碰到——留给内嵌的 nil 接口，越界调用会直接 panic。
type wsControlConn struct {
	gnet.Conn

	mu  sync.Mutex
	out bytes.Buffer
}

func (c *wsControlConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.Write(p)
}

func (c *wsControlConn) wire() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.out.Bytes()...)
}

// newControlState 造一条已完成握手的 WebSocket 连接状态，出站落到 wsControlConn。
func newControlState(tb testing.TB) (*wsConnState, *wsControlConn) {
	tb.Helper()

	g := &wsControlConn{}
	conn := &Conn{conn: g, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()
	return &wsConnState{
		conn:             conn,
		upgraded:         true,
		maxBufferedBytes: 1 << 20,
	}, g
}

// readServerFrame 解出服务端写回的一个 WebSocket 帧。
func readServerFrame(tb testing.TB, wire []byte) ws.Frame {
	tb.Helper()

	f, err := ws.ReadFrame(bytes.NewReader(wire))
	if err != nil {
		tb.Fatalf("read server frame: %v", err)
	}
	return f
}

func TestWebSocketPingIsAnsweredWithUnmaskedPong(t *testing.T) {
	state, g := newControlState(t)

	payload := []byte("heartbeat")
	if err := state.handleFrame(wsFrame{
		header:  ws.Header{Fin: true, OpCode: ws.OpPing, Length: int64(len(payload))},
		payload: payload,
	}); err != nil {
		t.Fatalf("handle ping: %v", err)
	}

	f := readServerFrame(t, g.wire())
	if f.Header.OpCode != ws.OpPong {
		t.Fatalf("expected a pong, got opcode %v", f.Header.OpCode)
	}
	if !f.Header.Fin {
		t.Fatal("pong must be a final frame")
	}
	// 服务端发往客户端的帧一律不得带掩码（RFC6455 §5.1）。带掩码的话
	// 浏览器会以协议错误断链。
	if f.Header.Masked {
		t.Fatal("server frames must not be masked")
	}
	if !bytes.Equal(f.Payload, payload) {
		t.Fatalf("pong payload = %q, want %q", f.Payload, payload)
	}
}

func TestWebSocketEmptyPingIsAnsweredWithEmptyPong(t *testing.T) {
	state, g := newControlState(t)

	if err := state.handleFrame(wsFrame{
		header: ws.Header{Fin: true, OpCode: ws.OpPing},
	}); err != nil {
		t.Fatalf("handle empty ping: %v", err)
	}

	f := readServerFrame(t, g.wire())
	if f.Header.OpCode != ws.OpPong {
		t.Fatalf("expected a pong, got opcode %v", f.Header.OpCode)
	}
	if len(f.Payload) != 0 {
		t.Fatalf("expected an empty pong, got %d byte(s)", len(f.Payload))
	}
	if f.Header.Masked {
		t.Fatal("server frames must not be masked")
	}
}

// 客户端主动发来的 pong 是合法的单向心跳，按 RFC6455 §5.5.3 不需要回应，
// 更不能因此断链。
func TestWebSocketClientPongIsIgnored(t *testing.T) {
	state, g := newControlState(t)

	if err := state.handleFrame(wsFrame{
		header:  ws.Header{Fin: true, OpCode: ws.OpPong, Length: 2},
		payload: []byte("hi"),
	}); err != nil {
		t.Fatalf("handle pong: %v", err)
	}
	if got := g.wire(); len(got) != 0 {
		t.Fatalf("a client pong must not produce a response, got %d byte(s)", len(got))
	}
}

// 一次正常的关闭握手必须被识别成"正常关闭"，而不是入站错误。
//
// onTraffic 靠 errors.Is(err, errWebSocketClosed) 区分这两者：匹配上就静默关连接，
// 匹配不上会走 logInbound 打一条 warn。而 wsutil.ControlHandler.HandleClose 在
// 一切正常时也返回一个非 nil 的 ClosedError，所以 handleControlFrame 必须把它
// 翻译过来——否则每一次正常的客户端断开都会在服务端刷一条错误日志。
func TestWebSocketCloseFrameIsReportedAsNormalClosure(t *testing.T) {
	state, g := newControlState(t)

	body := ws.NewCloseFrameBody(ws.StatusNormalClosure, "bye")
	err := state.handleFrame(wsFrame{
		header:  ws.Header{Fin: true, OpCode: ws.OpClose, Length: int64(len(body))},
		payload: body,
	})
	if !errors.Is(err, errWebSocketClosed) {
		t.Fatalf("close frame must report errWebSocketClosed, got %v (%T)", err, err)
	}

	// 并且必须回一个 close 帧完成握手。
	f := readServerFrame(t, g.wire())
	if f.Header.OpCode != ws.OpClose {
		t.Fatalf("expected a close frame in response, got opcode %v", f.Header.OpCode)
	}
	if f.Header.Masked {
		t.Fatal("server frames must not be masked")
	}
}

// 不带 status code 的 close 帧同样是正常关闭（RFC6455 §7.1.5 视为 1005）。
func TestWebSocketEmptyCloseFrameIsReportedAsNormalClosure(t *testing.T) {
	state, _ := newControlState(t)

	err := state.handleFrame(wsFrame{
		header: ws.Header{Fin: true, OpCode: ws.OpClose},
	})
	if !errors.Is(err, errWebSocketClosed) {
		t.Fatalf("empty close frame must report errWebSocketClosed, got %v (%T)", err, err)
	}
}

// 非法的 close status code 是真正的协议错误，不能被当成正常关闭吞掉。
func TestWebSocketInvalidCloseCodeIsReportedAsError(t *testing.T) {
	state, _ := newControlState(t)

	// 1005 是"保留、不得出现在线路上"的状态码。
	body := ws.NewCloseFrameBody(ws.StatusNoStatusRcvd, "")
	err := state.handleFrame(wsFrame{
		header:  ws.Header{Fin: true, OpCode: ws.OpClose, Length: int64(len(body))},
		payload: body,
	})
	if err == nil {
		t.Fatal("a reserved close code must be reported as an error")
	}
	if errors.Is(err, errWebSocketClosed) {
		t.Fatalf("a protocol violation must not be reported as a normal closure: %v", err)
	}
}

// RFC6455 §5.5 规定控制帧的 payload 不得超过 125 字节，且不得分片。
// gate 挡住了"不得分片"，但没有挡长度。
//
// 后果不只是协议不合规：nextFrame 对控制帧用的是 maxMessageSize（32MB）这个上限，
// 于是一个声称自己有 32MB payload 的 ping 会让服务端一路把字节攒进 w.buf，
// 直到撞上 MaxWebSocketBufferedBytes 才失败。而按 RFC 判的话，读到帧头的第
// 二个字节就能拒掉。每条连接 32MB 的差距，对一个网关来说是实打实的内存放大。
func TestWebSocketOversizedControlFrameIsRejectedByHeader(t *testing.T) {
	for _, op := range []ws.OpCode{ws.OpPing, ws.OpPong, ws.OpClose} {
		t.Run(fmt.Sprintf("opcode-%#x", byte(op)), func(t *testing.T) {
			state, g := newControlState(t)
			// 只喂帧头和掩码，**不喂 payload**。
			//
			// 喂完整 126 字节的话，把长度检查错误地挪到"整帧到齐之后"仍然能通过，
			// 而那正是这条用例要挡住的写法——按 RFC 判的话，读到帧头第二个字节就
			// 该拒掉，根本不该等 payload。
			full := maskedWSFrame(t, op, make([]byte, 126))
			state.buf.Write(full[:4]) // 2 字节基础头 + 2 字节扩展长度
			state.buf.Write(full[4:8])

			err := state.decodeMessages()
			if err == nil {
				t.Fatalf("a %v frame with a 126-byte payload must be rejected", op)
			}
			// 而且必须在**解析帧头**时就拒掉，不能已经动手回了半个 pong。
			if got := g.wire(); len(got) != 0 {
				t.Fatalf("rejected control frame still produced %d byte(s) of output", len(got))
			}
		})
	}
}

// 125 字节是允许的上界，必须照常处理。
func TestWebSocketControlFrameAtMaxPayloadIsAccepted(t *testing.T) {
	state, g := newControlState(t)
	payload := bytes.Repeat([]byte("z"), 125)
	state.buf.Write(maskedWSFrame(t, ws.OpPing, payload))

	if err := state.decodeMessages(); err != nil {
		t.Fatalf("a 125-byte ping is legal, got %v", err)
	}
	f := readServerFrame(t, g.wire())
	if f.Header.OpCode != ws.OpPong || !bytes.Equal(f.Payload, payload) {
		t.Fatalf("expected a pong echoing 125 bytes, got %v/%d", f.Header.OpCode, len(f.Payload))
	}
}

// 控制帧夹在两条数据帧中间时，两条业务消息都必须照常投递，
// 控制帧不能吃掉或错位任何 gate 帧。
func TestWebSocketControlFrameBetweenDataFramesKeepsStreamIntact(t *testing.T) {
	state, g := newControlState(t)

	var got [][]byte
	state.conn.handler = &funcHandler{fn: func(b []byte) {
		got = append(got, append([]byte(nil), b...))
	}}

	first, second := []byte("alpha"), []byte("omega")
	state.buf.Write(maskedWSFrame(t, ws.OpBinary, gateFrame(first)))
	state.buf.Write(maskedWSFrame(t, ws.OpPing, []byte("mid")))
	state.buf.Write(maskedWSFrame(t, ws.OpBinary, gateFrame(second)))

	if err := state.decodeMessages(); err != nil {
		t.Fatalf("decode messages: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 delivered messages, got %d", len(got))
	}
	if !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) {
		t.Fatalf("delivered %q, want [%q %q]", got, first, second)
	}
	if state.buf.Len() != 0 {
		t.Fatalf("%d unconsumed byte(s) left in the read buffer", state.buf.Len())
	}

	f := readServerFrame(t, g.wire())
	if f.Header.OpCode != ws.OpPong || !bytes.Equal(f.Payload, []byte("mid")) {
		t.Fatalf("expected a pong carrying %q, got %v %q", "mid", f.Header.OpCode, f.Payload)
	}
}

// gateFrame 把一条业务消息编码成一个明文 gate 帧。
func gateFrame(payload []byte) []byte {
	var header [4]byte
	n := encodeHeader(header[:], len(payload))
	return append(append([]byte(nil), header[:n]...), payload...)
}

// maskedWSFrame 造一个带掩码的客户端 WebSocket 帧（服务端只接受带掩码的上行帧）。
//
// payload 必须先复制一份：ws.NewFrame 是按引用持有的，MaskFrameInPlace 会就地
// 异或它。不复制的话调用方手里那份 payload 会被悄悄改成掩码后的字节，
// 之后再拿它做断言就是在和一个错误的期望值比较。
func maskedWSFrame(tb testing.TB, op ws.OpCode, payload []byte) []byte {
	tb.Helper()

	f := ws.NewFrame(op, true, append([]byte(nil), payload...))
	f = ws.MaskFrameInPlace(f)
	var buf bytes.Buffer
	if err := ws.WriteFrame(&buf, f); err != nil {
		tb.Fatalf("write websocket frame: %v", err)
	}
	return buf.Bytes()
}
