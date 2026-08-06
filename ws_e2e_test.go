package gate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ─── 服务端流解码（测试侧独立实现）───

type wsSrvFrame struct {
	op      byte
	payload []byte
}

func decodeServerWS(t *testing.T, b []byte) []wsSrvFrame {
	t.Helper()
	var out []wsSrvFrame
	for len(b) > 0 {
		if len(b) < 2 {
			t.Fatalf("残缺服务端帧头: %x", b)
		}
		if b[1]&wsMaskBit != 0 {
			t.Fatal("服务端帧不得带掩码")
		}
		op := b[0] & 0x0F
		n := int64(b[1] & 0x7F)
		off := 2
		switch n {
		case 126:
			n = int64(binary.BigEndian.Uint16(b[2:4]))
			off = 4
		case 127:
			n = int64(binary.BigEndian.Uint64(b[2:10]))
			off = 10
		}
		if int64(len(b)) < int64(off)+n {
			t.Fatalf("服务端帧不完整: 需要 %d 有 %d", int64(off)+n, len(b))
		}
		out = append(out, wsSrvFrame{op: op, payload: append([]byte(nil), b[off:int64(off)+n]...)})
		b = b[int64(off)+n:]
	}
	return out
}

// gateMsgsFromWS 把服务端二进制帧的 payload 串起来按 gate 帧解码。
func gateMsgsFromWS(t *testing.T, frames []wsSrvFrame) [][]byte {
	t.Helper()
	var stream []byte
	for _, f := range frames {
		if f.op == wsOpBinary || f.op == wsOpContinuation {
			stream = append(stream, f.payload...)
		}
	}
	if len(stream) == 0 {
		return nil
	}
	return decodeStream(t, stream, nil, maxMessageSize)
}

func wsHarnessOpt(c *loopConfig) {
	c.out.wsWrap = wsBinaryWrap
	c.out.wireSlack = wireSlackWS
}

func newWSHarness(t *testing.T, opts ...harnessOpt) *harness {
	h := newHarness(t, append([]harnessOpt{wsHarnessOpt}, opts...)...)
	enableWS(h.c)
	return h
}

// ─── 回显与批语义 ───

func TestWS_EchoBatchInOneFrame(t *testing.T) {
	h := newWSHarness(t)
	h.onMsg = func(msg []byte) error {
		h.msgs = append(h.msgs, append([]byte(nil), msg...))
		return h.c.out.send(append([]byte("re:"), msg...), flagZ|flagC|flagE)
	}
	gate := gateWire(t, []byte("a"), []byte("bb"))
	h.io.feed(wsClientFrame(true, wsOpBinary, gate, testMask))
	h.readable()

	if len(h.msgs) != 2 {
		t.Fatalf("msgs=%q", h.msgs)
	}
	frames := decodeServerWS(t, h.io.wrote.Bytes())
	if len(frames) != 1 || frames[0].op != wsOpBinary {
		t.Fatalf("一次编码的一批应装进一个 WS 二进制帧（W7），got %d 帧", len(frames))
	}
	got := gateMsgsFromWS(t, frames)
	if len(got) != 2 || string(got[0]) != "re:a" || string(got[1]) != "re:bb" {
		t.Fatalf("got %q", got)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 一条 gate 帧跨两个 WS 数据帧（分片）+ 跨读事件：残片机制自然接上（W2）。
func TestWS_GateFrameAcrossFragmentsAndReads(t *testing.T) {
	h := newWSHarness(t)
	gate := gateWire(t, mkPayload(200), []byte("tail"))
	var raw []byte
	raw = append(raw, wsClientFrame(false, wsOpBinary, gate[:63], testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpContinuation, gate[63:], testMask)...)
	// 任意位置切成两次读。
	h.io.feed(raw[:97], raw[97:])
	h.readable()
	if len(h.msgs) != 2 || !bytes.Equal(h.msgs[0], mkPayload(200)) || string(h.msgs[1]) != "tail" {
		t.Fatalf("msgs=%d", len(h.msgs))
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

func TestWS_PingGetsPong(t *testing.T) {
	h := newWSHarness(t)
	h.io.feed(wsClientFrame(true, wsOpPing, []byte("payload"), testMask))
	h.readable()
	frames := decodeServerWS(t, h.io.wrote.Bytes())
	if len(frames) != 1 || frames[0].op != wsOpPong || string(frames[0].payload) != "payload" {
		t.Fatalf("frames=%v", frames)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 大帧直读经 WS 层：>64KB 的 gate 帧流式去掩码后直接填 frameBuf。
func TestWS_LargeGateFrameStreaming(t *testing.T) {
	h := newWSHarness(t)
	big := mkPayload(100 << 10)
	gate := gateWire(t, big)
	raw := wsClientFrame(true, wsOpBinary, gate, testMask)
	// 分四次读。
	q := len(raw) / 4
	h.io.feed(raw[:q], raw[q:2*q], raw[2*q:3*q], raw[3*q:])
	for range 4 {
		h.readable()
	}
	if len(h.msgs) != 1 || !bytes.Equal(h.msgs[0], big) {
		t.Fatal("大帧未完整投递")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 暂停落在 WS 帧中间：剩余字节（已去掩码）转 gate carry，resume 后继续。
func TestWS_PauseMidFrame(t *testing.T) {
	var resume func()
	h := newWSHarness(t)
	h.onMsg = func(msg []byte) error {
		h.msgs = append(h.msgs, append([]byte(nil), msg...))
		if len(h.msgs) == 1 {
			resume = h.c.pause()
		}
		return nil
	}
	gate := gateWire(t, []byte("m1"), []byte("m2"), []byte("m3"))
	h.io.feed(wsClientFrame(true, wsOpBinary, gate, testMask))
	h.readable()
	if len(h.msgs) != 1 {
		t.Fatalf("暂停后仍在投递: %q", h.msgs)
	}
	resume()
	h.l.step()
	if len(h.msgs) != 3 {
		t.Fatalf("resume 后 carry 未续投: %q", h.msgs)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// ─── 关闭状态机（W10/W13）───

func TestWS_PeerCloseHandshake(t *testing.T) {
	h := newWSHarness(t)
	p := []byte{0x03, 0xE8} // 1000
	h.io.feed(wsClientFrame(true, wsOpClose, p, testMask))
	h.readable()
	h.l.step() // draining → detach
	if h.closed != 1 || !errors.Is(h.closeReason, ErrPeerClosed) {
		t.Fatalf("正常关闭必须翻译成 ErrPeerClosed（不是错误路径）: %v", h.closeReason)
	}
	frames := decodeServerWS(t, h.io.wrote.Bytes())
	if len(frames) != 1 || frames[0].op != wsOpClose ||
		binary.BigEndian.Uint16(frames[0].payload) != 1000 {
		t.Fatalf("应回 close 完成握手: %v", frames)
	}
	h.verifyConservation()
}

// CLOSE_RECEIVED 之后的数据帧不再投递（同一次读里 close 后面跟数据帧）。
func TestW13_DataAfterCloseNotDelivered(t *testing.T) {
	h := newWSHarness(t)
	var raw []byte
	raw = append(raw, wsClientFrame(true, wsOpBinary, gateWire(t, []byte("before")), testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpClose, []byte{0x03, 0xE8}, testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpBinary, gateWire(t, []byte("after")), testMask)...)
	h.io.feed(raw)
	h.readable()
	h.l.step()
	if len(h.msgs) != 1 || string(h.msgs[0]) != "before" {
		t.Fatalf("close 之后的数据帧被投递了: %q", h.msgs)
	}
	h.verifyConservation()
}

func TestW10_LocalCloseCodeMapping(t *testing.T) {
	cases := []struct {
		name   string
		reason error
		code   int // 0 = 不发 close 帧
	}{
		{"正常关闭", nil, 1000},
		{"业务自定义", errKicked, 1000},
		{"服务器下线", ErrServerClosed, 1001},
		{"空闲回收", ErrIdleTimeout, 1001},
		{"协议违规", wrapProtocol(errWSRsv), 1002},
		{"pending超限", ErrPendingOverflow, 1009},
		{"业务panic", fmt.Errorf("%w: x", ErrHandlerPanic), 1011},
		{"背压", ErrBackpressure, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newWSHarness(t)
			h.c.requestClose(tc.reason)
			h.l.step()
			frames := decodeServerWS(t, h.io.wrote.Bytes())
			if tc.code == 0 {
				if len(frames) != 0 {
					t.Fatalf("背压/写失败不该发 close 帧: %v", frames)
				}
				return
			}
			if len(frames) != 1 || frames[0].op != wsOpClose {
				t.Fatalf("frames=%v", frames)
			}
			if got := int(binary.BigEndian.Uint16(frames[0].payload)); got != tc.code {
				t.Fatalf("code=%d want %d", got, tc.code)
			}
			h.verifyConservation()
		})
	}
}

// close 帧恰好排在一个部分写的数据帧后面：FIFO 结构保证不插帧中间（W13）。
func TestW13_ClosePacksAfterPartialFrame(t *testing.T) {
	h := newWSHarness(t)
	msg := mkPayload(2000)
	_ = h.c.out.send(msg, flagZ|flagC|flagE)
	h.io.script = []ioStep{{accept: 100, err: errEAGAIN}, {accept: 0, err: errEAGAIN}}
	h.l.step() // dirty flush：写 100 字节后阻塞
	h.c.requestClose(nil)
	h.l.step() // close 帧排在链尾

	h.io.script = nil // 全收
	h.writable()
	frames := decodeServerWS(t, h.io.wrote.Bytes())
	if len(frames) != 2 || frames[0].op != wsOpBinary || frames[1].op != wsOpClose {
		t.Fatalf("frames=%d", len(frames))
	}
	got := decodeStream(t, frames[0].payload, nil, maxMessageSize)
	if len(got) != 1 || !bytes.Equal(got[0], msg) {
		t.Fatal("部分写的数据帧被 close 帧破坏")
	}
	if h.closed != 1 {
		t.Fatal("排空后应完成关闭")
	}
	h.verifyConservation()
}

// ─── 出站封帧边界（05「与 TCP 的语义对齐」：关压缩关合包）───

func TestWS_OutboundHeaderBoundaries(t *testing.T) {
	cases := []struct {
		bizLen  int // 业务长度
		hdrLen  int // 期望的 WS 帧头长
		wsPayld int // 期望的 WS 载荷（gate 帧头 + 业务长度）
	}{
		{123, 2, 125},      // 7-bit 上限
		{124, 4, 126},      // 切换到 16-bit
		{65531, 4, 65535},  // 16-bit 上限（gate 头此时已是 4 字节）
		{65532, 10, 65536}, // 切换到 64-bit
	}
	for _, tc := range cases {
		h := newWSHarness(t)
		_ = h.c.out.send(mkPayload(tc.bizLen), flagZ|flagC|flagE)
		h.l.step()
		wireB := h.io.wrote.Bytes()
		frames := decodeServerWS(t, wireB)
		if len(frames) != 1 || len(frames[0].payload) != tc.wsPayld {
			t.Fatalf("bizLen=%d: ws payload=%d want %d", tc.bizLen, len(frames[0].payload), tc.wsPayld)
		}
		if gotHdr := len(wireB) - tc.wsPayld; gotHdr != tc.hdrLen {
			t.Fatalf("bizLen=%d: ws hdr=%d want %d", tc.bizLen, gotHdr, tc.hdrLen)
		}
		h.c.requestClose(ErrBackpressure) // 不发 close 帧，保持流水干净
		h.l.step()
		h.verifyConservation()
	}
}

// ─── 握手 ───

func hsRequest(key, path string, extra ...string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", path, key)
	for _, h := range extra {
		b.WriteString(h)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

// hsHarness 起一个 Handshaking 状态的连接。
func hsHarness(t *testing.T, opts *WebSocketOptions) (*harness, func(raw []byte) *Handshake) {
	h := newHarnessNoOpen(t)
	wsHarnessOpt(&h.l.cfg)
	h.c.out.cfg = h.l.cfg.out
	h.c.state = stateHandshaking
	h.c.out.setFlushable(false)
	h.l.hsLRU.pushBack(&h.c.tnode, h.l.now())
	var seen *Handshake
	run := func(raw []byte) *Handshake {
		h.io.feed(raw)
		h.l.wsHsReadable(h.c, opts, func(hs *Handshake) { seen = hs })
		return seen
	}
	return h, run
}

func TestWS_HandshakeAcceptRFCVector(t *testing.T) {
	// RFC 6455 §1.3 的样例向量。
	if got := wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("accept=%q", got)
	}

	h, run := hsHarness(t, &WebSocketOptions{Path: "/game"})
	hs := run(hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/game", "X-Token: abc"))
	if hs == nil || hs.URI != "/game" || hs.Header.Get("X-Token") != "abc" {
		t.Fatalf("hs=%+v", hs)
	}
	if h.opened != 1 {
		t.Fatal("101 之后应进入 Open 并回调 OnOpen")
	}
	resp := h.io.wrote.String()
	if !strings.HasPrefix(resp, "HTTP/1.1 101 ") ||
		!strings.Contains(resp, "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
		t.Fatalf("resp=%q", resp)
	}
	// W11：绝不协商扩展与子协议。
	if strings.Contains(resp, "Sec-WebSocket-Extensions") || strings.Contains(resp, "Sec-WebSocket-Protocol") {
		t.Fatal("协商了不该协商的东西")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 抢跑客户端：升级请求和第一个 WS 帧同一次 read 到达，剩余字节直接进帧层。
func TestWS_PipelinedFrameAfterHandshake(t *testing.T) {
	h, run := hsHarness(t, &WebSocketOptions{})
	raw := hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/")
	raw = append(raw, wsClientFrame(true, wsOpBinary, gateWire(t, []byte("early")), testMask)...)
	run(raw)
	if len(h.msgs) != 1 || string(h.msgs[0]) != "early" {
		t.Fatalf("握手后剩余字节被吃掉: %q（05：不能把整个缓冲扔给解析器）", h.msgs)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

func TestWS_HandshakeRejections(t *testing.T) {
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	t.Run("OnUpgrade拒绝带状态码", func(t *testing.T) {
		h, run := hsHarness(t, &WebSocketOptions{
			OnUpgrade: func(hs *Handshake) error {
				return RejectUpgrade(http.StatusUnauthorized, "bad token")
			},
		})
		run(hsRequest(key, "/"))
		if h.opened != 0 || h.closed != 0 {
			t.Fatal("拒绝的连接不该 OnOpen/OnClose")
		}
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 401 ") {
			t.Fatalf("resp=%q", h.io.wrote.String())
		}
	})
	t.Run("路径不匹配404", func(t *testing.T) {
		h, run := hsHarness(t, &WebSocketOptions{Path: "/game"})
		run(hsRequest(key, "/other"))
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 404 ") {
			t.Fatalf("resp=%q", h.io.wrote.String())
		}
	})
	t.Run("头条数超限拒绝不截断", func(t *testing.T) {
		extra := make([]string, maxHandshakeHeaderLines+1)
		for i := range extra {
			extra[i] = fmt.Sprintf("X-Pad-%d: v", i)
		}
		h, run := hsHarness(t, &WebSocketOptions{})
		run(hsRequest(key, "/", extra...))
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 400 ") {
			t.Fatalf("resp=%q", h.io.wrote.String())
		}
	})
	t.Run("完整但超限的请求仍要拒", func(t *testing.T) {
		h, run := hsHarness(t, &WebSocketOptions{MaxHandshakeBytes: 128})
		run(hsRequest(key, "/", "X-Big: "+strings.Repeat("a", 200)))
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 431 ") {
			t.Fatalf("一次大 read 送来完整超限请求必须拒（W5）: %q", h.io.wrote.String())
		}
	})
	t.Run("非升级请求", func(t *testing.T) {
		h, run := hsHarness(t, &WebSocketOptions{})
		run([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 400 ") {
			t.Fatalf("resp=%q", h.io.wrote.String())
		}
	})
}

// 握手分多次读到（slowloris 形态的合法版）。
func TestWS_HandshakeAcrossReads(t *testing.T) {
	h, run := hsHarness(t, &WebSocketOptions{})
	req := hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/")
	h.io.feed(req[:10], req[10:25])
	h.l.wsHsReadable(h.c, &WebSocketOptions{}, nil)
	if h.opened != 0 {
		t.Fatal("请求未完整不该升级")
	}
	run(req[25:])
	if h.opened != 1 {
		t.Fatal("请求凑齐后应完成升级")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

var _ = time.Second
