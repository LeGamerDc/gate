//go:build darwin || linux

package gate

import (
	"encoding/binary"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// RFC 6455 §4.1：Sec-WebSocket-Key 必须是 16 字节随机数的 base64。
// 只查非空会接受无效握手。
func TestP1_HandshakeKeyMustBe16Bytes(t *testing.T) {
	for _, key := range []string{"", "short", "bm90LTE2LWJ5dGVz", "!!!not-base64!!!"} {
		h, run := hsHarness(t, &WebSocketOptions{})
		run(hsRequest(key, "/"))
		if !strings.HasPrefix(h.io.wrote.String(), "HTTP/1.1 400 ") {
			t.Fatalf("key=%q 应被拒: %q", key, firstLine(h.io.wrote.String()))
		}
	}
	// 合法的 16 字节 key（"the sample nonce"）照常升级。
	h, run := hsHarness(t, &WebSocketOptions{})
	run(hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/"))
	if h.opened != 1 {
		t.Fatal("合法 key 应升级成功")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 01 的回调表：OnUpgrade 返回普通 error 默认 401，panic 按 500。
func TestP1_OnUpgradeRejectStatusCodes(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*Handshake) error
		want string
	}{
		{"普通error默认401", func(*Handshake) error { return errors.New("nope") }, "HTTP/1.1 401 "},
		{"panic按500", func(*Handshake) error { panic("bug") }, "HTTP/1.1 500 "},
		{"显式状态码", func(*Handshake) error { return RejectUpgrade(http.StatusTeapot, "no") }, "HTTP/1.1 418 "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, run := hsHarness(t, &WebSocketOptions{OnUpgrade: tc.fn})
			run(hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/"))
			if !strings.HasPrefix(h.io.wrote.String(), tc.want) {
				t.Fatalf("got %q, want prefix %q", firstLine(h.io.wrote.String()), tc.want)
			}
		})
	}
}

// 05 校验清单：控制帧的长度/FIN 校验（第 5、6 步）必须早于扩展长度的
// 规范性检查（第 7 步）——错误分类与拒绝时机都不同。
func TestP1_ControlLengthCheckedBeforeExtendedLength(t *testing.T) {
	// ping 用 126 长度标记：第 5 步就该判「控制帧超 125」，
	// 而不是先解扩展长度再报 non-canonical。
	raw := []byte{wsFinBit | wsOpPing, wsMaskBit | 126, 0, 100}
	raw = append(raw, testMask[:]...)
	w := &wsState{}
	var col wsCollector
	if err := feedCollect(w, &col, raw); !errors.Is(err, errWSCtrlTooLong) {
		t.Fatalf("got %v, want errWSCtrlTooLong", err)
	}
	// 127 标记同理。
	raw = []byte{wsFinBit | wsOpClose, wsMaskBit | 127}
	var ext [8]byte
	binary.BigEndian.PutUint64(ext[:], 70000)
	raw = append(raw, ext[:]...)
	raw = append(raw, testMask[:]...)
	w = &wsState{}
	if err := feedCollect(w, &col, raw); !errors.Is(err, errWSCtrlTooLong) {
		t.Fatalf("got %v, want errWSCtrlTooLong", err)
	}
}

// PROXY v2：未定义的 command 与非 STREAM 的 transport 必须拒绝。
func TestP1_ProxyV2SemanticValidation(t *testing.T) {
	base := func(verCmd, fam byte) []byte {
		h := append([]byte(nil), proxyV2Sig...)
		h = append(h, verCmd, fam, 0x00, 12)
		h = append(h, 1, 2, 3, 4, 5, 6, 7, 8, 0x00, 0x50, 0x00, 0x50)
		return h
	}
	if _, _, err := parseProxy(base(0x23, 0x11), true); !errors.Is(err, errProxyMalformed) {
		t.Fatalf("未定义 command 应拒绝: %v", err)
	}
	if _, _, err := parseProxy(base(0x21, 0x12), true); !errors.Is(err, errProxyMalformed) {
		t.Fatalf("DGRAM 应在 TCP listener 上拒绝: %v", err)
	}
	src, n, err := parseProxy(base(0x21, 0x11), true) // PROXY + AF_INET|STREAM
	if err != nil || n != 28 || src.Addr().String() != "1.2.3.4" {
		t.Fatalf("合法 v2 头: src=%v n=%d err=%v", src, n, err)
	}
	// LOCAL：合法，但不带地址语义。
	if _, n, err := parseProxy(base(0x20, 0x11), true); err != nil || n != 28 {
		t.Fatalf("LOCAL: n=%d err=%v", n, err)
	}
}

// SendFunc 的 commit 按新 epoch 重查 MaxMessage：否则 Send 返回 nil，
// 编码期才因新 Overhead 越限，把整条连接关掉。
func TestP1_SendFuncRechecksLimitAfterCipherChange(t *testing.T) {
	o, _, _ := newTestOutbound(t, func(c *outConfig) { c.maxMessage = 110 })
	err := o.sendFunc(100, func(b []byte) (int, error) {
		o.setCipher(mustNewGCM()) // fill 期间换成 Overhead 16 的 AEAD
		return 100, nil
	})
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge（frameSize(100+16)=118 > 110）", err)
	}
	if reservedOf(o) != 0 {
		t.Fatal("拒绝后预算未退还")
	}
}

// Stats 的水位类字段必须真实反映当前占用，而不是恒为 0。
func TestP1_StatsWaterMarksAreLive(t *testing.T) {
	h := newHarness(t, func(c *loopConfig) {
		c.out.maxBuffer = 1 << 20
		c.out.highWater = 64
	})
	// 出站积压 + 高水位。
	h.io.script = []ioStep{{accept: 0, err: errEAGAIN}}
	_ = h.c.out.send(mkPayload(4000), flagE)
	h.l.step()
	st := h.l.stats.snapshot()
	if st.OutboundQueued <= 0 {
		t.Fatalf("OutboundQueued=%d, want > 0", st.OutboundQueued)
	}
	if st.ConnsOverHighWater != 1 {
		t.Fatalf("ConnsOverHighWater=%d, want 1", st.ConnsOverHighWater)
	}
	if st.WriteEAGAIN == 0 {
		t.Fatal("WriteEAGAIN 未计数")
	}

	// LoopLagNanos 用 loop 自己的时钟量（harness 下就是注入的假时钟）：
	// 在回调里推进时钟，本轮迭代的耗时就是可断言的。
	h.onMsg = func([]byte) error { h.advance(5 * time.Millisecond); return nil }
	h.feedFrames([]byte("tick"))
	h.readable()
	if got := h.l.stats.snapshot().LoopLagNanos; got < uint64(5*time.Millisecond) {
		t.Fatalf("LoopLagNanos=%d, want >= 5ms", got)
	}
	h.onMsg = nil

	// 入站残片。
	h.io.script = nil
	frame := gateWire(t, mkPayload(300))
	h.io.feed(frame[:20]) // 半个帧 → carry
	h.readable()
	if got := h.l.stats.snapshot().PendingInbound; got != 20 {
		t.Fatalf("PendingInbound=%d, want 20", got)
	}
	h.io.feed(frame[20:])
	h.readable()
	if got := h.l.stats.snapshot().PendingInbound; got != 0 {
		t.Fatalf("拼齐后 PendingInbound=%d, want 0", got)
	}

	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
	final := h.l.stats.snapshot()
	if final.OutboundQueued != 0 || final.ConnsOverHighWater != 0 {
		t.Fatalf("关闭后水位未归零: %+v", final)
	}
}

func TestP1_StatsPoolMissCounted(t *testing.T) {
	before := poolMisses.Load()
	poolPut(poolGet(1 << 27)) // 超过最大类：必然 miss
	if poolMisses.Load() <= before {
		t.Fatal("PoolMiss 未计数")
	}
}

func firstLine(s string) string {
	if i := strings.Index(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}
