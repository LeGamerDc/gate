package gate

import (
	"bytes"
	"testing"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
)

/*
用一个可控的假连接完整驱动 wsConnState.onTraffic。

真实 socket 上没法稳定构造"握手和数据帧落在同一个读事件里"这类交错——
TCP 什么时候切段不由测试决定。这个假连接把入站字节和读事件的边界都交给测试，
于是缓冲计费、握手边界、流水线这些只在特定切分下才暴露的问题变成可复现的。
*/

// wsFakeConn 实现 wsConnState 真正会用到的那几个 gnet.Conn 方法：
// InboundBuffered / Next 供入站读取，Write 供握手响应和控制帧写出。
// 其余方法留给内嵌的 nil 接口，越界调用会直接 panic。
type wsFakeConn struct {
	gnet.Conn

	in     bytes.Buffer
	out    bytes.Buffer
	closes int
}

func (c *wsFakeConn) InboundBuffered() int { return c.in.Len() }

func (c *wsFakeConn) Next(n int) ([]byte, error) {
	if n < 0 || n > c.in.Len() {
		n = c.in.Len()
	}
	return c.in.Next(n), nil
}

func (c *wsFakeConn) Write(p []byte) (int, error) { return c.out.Write(p) }

// Close 必须实现出来，哪怕只是记个数：不实现的话它会落到内嵌的 nil gnet.Conn 上
// 直接 panic，而 Conn.Close 常常是在 deliverOne 的 recover 范围内被调用的——
// 那样一个"关连接"的测试会因为 panic 被兜住而假通过。
func (c *wsFakeConn) Close() error {
	c.closes++
	return nil
}

// newFakeWSConn 造一条尚未握手的 WebSocket 连接状态。
func newFakeWSConn(tb testing.TB, path string, maxHandshake, maxBuffered int) (*wsConnState, *wsFakeConn, *[][]byte) {
	tb.Helper()

	g := &wsFakeConn{}
	conn := &Conn{conn: g, outbound: &wsOutbound{conn: g}, codec: serverCodec(maxMessageSize)}
	conn.init()

	delivered := new([][]byte)
	conn.handler = &funcHandler{fn: func(b []byte) {
		*delivered = append(*delivered, append([]byte(nil), b...))
	}}

	state := newWSConnState(conn, path, maxHandshake, maxBuffered, nil)
	return state, g, delivered
}

func handshakeRequest(path string) []byte {
	return []byte("GET " + path + " HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n" +
		"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")
}

// 流水线跟在握手后面的数据帧，不能被算进**握手**的预算里。
//
// MaxWebSocketHandshakeBytes 的语义是"握手请求本身最多多大"，它存在的理由是
// 挡住一个永远不发 \r\n\r\n 的连接把内存吃光。但此前 readBufferBytes 是拿
// 整个 InboundBuffered 去和这个上限比的，而握手和紧随其后的数据帧完全可能
// 落在同一个读事件里——于是一条合法的连接会因为"第一个业务包比较大"被判成
// 握手超限断开，报的还是一个完全对不上的原因。
//
// gate 是明确支持流水线的（见 upgrade() 里那段注释和
// TestWebSocketHandshakePipelinedFrameIsNotDiscarded），所以这两件事是冲突的。
func TestWebSocketPipelinedFrameDoesNotCountAgainstHandshakeBudget(t *testing.T) {
	req := handshakeRequest("/ws")
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	frame := maskedWSFrame(t, ws.OpBinary, gateFrame(payload))

	// 握手预算给得足够放下握手本身、但放不下"握手 + 数据帧"。
	maxHandshake := len(req) + 64

	state, g, delivered := newFakeWSConn(t, "/ws", maxHandshake, 1<<20)
	g.in.Write(req)
	g.in.Write(frame)

	if action := state.onTraffic(); action != gnet.None {
		t.Fatalf("connection was closed (action=%v); a pipelined data frame must not "+
			"count against the handshake budget", action)
	}
	if !state.upgraded {
		t.Fatal("handshake did not complete")
	}
	if len(*delivered) != 1 || !bytes.Equal((*delivered)[0], payload) {
		t.Fatalf("pipelined message was not delivered: got %d message(s)", len(*delivered))
	}
}

// 与上一条互补：真正超限的**握手请求本身**必须照常被拒。
//
// 攻击形态是一个永远不发 \r\n\r\n 的连接，靠不断灌字节把服务端内存吃掉。
// 修掉上一条不能把这条一起放过去。
func TestWebSocketOversizedHandshakeRequestIsStillRejected(t *testing.T) {
	state, g, _ := newFakeWSConn(t, "/ws", 512, 1<<20)

	// 一个没有结束标记的请求头，长度超过握手预算。
	g.in.Write([]byte("GET /ws HTTP/1.1\r\nHost: localhost\r\n"))
	g.in.Write(bytes.Repeat([]byte("X-Pad: yyyyyyyyyyyyyyyy\r\n"), 64))

	if action := state.onTraffic(); action != gnet.Close {
		t.Fatalf("an unterminated oversized handshake must close the connection, got %v", action)
	}
	if state.upgraded {
		t.Fatal("connection must not be upgraded")
	}
}

// 握手请求本身超限（有结束标记，但请求头总长超过预算）同样必须被拒。
func TestWebSocketTerminatedButOversizedHandshakeIsRejected(t *testing.T) {
	state, g, _ := newFakeWSConn(t, "/ws", 512, 1<<20)

	var req bytes.Buffer
	req.WriteString("GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	req.Write(bytes.Repeat([]byte("X-Pad: yyyyyyyyyyyyyyyy\r\n"), 32))
	req.WriteString("\r\n")

	g.in.Write(req.Bytes())

	if action := state.onTraffic(); action != gnet.Close {
		t.Fatalf("an oversized handshake request must close the connection, got %v", action)
	}
	if state.upgraded {
		t.Fatal("connection must not be upgraded")
	}
}

// 握手完成之后，缓冲上限切回 MaxWebSocketBufferedBytes。
func TestWebSocketBufferLimitAppliesAfterUpgrade(t *testing.T) {
	req := handshakeRequest("/ws")
	state, g, _ := newFakeWSConn(t, "/ws", len(req)+64, 4096)

	g.in.Write(req)
	if action := state.onTraffic(); action != gnet.None {
		t.Fatalf("handshake failed: %v", action)
	}

	// 一个超过 MaxWebSocketBufferedBytes 的分片消息必须被拒。
	g.in.Write(maskedWSFrame(t, ws.OpBinary, make([]byte, 8192)))
	if action := state.onTraffic(); action != gnet.Close {
		t.Fatalf("a frame beyond MaxWebSocketBufferedBytes must close the connection, got %v", action)
	}
}
