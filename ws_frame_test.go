package gate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// ─── 测试侧构造器 ───

// wsClientFrame 拼一个客户端帧（带掩码）。
func wsClientFrame(fin bool, op byte, payload []byte, mask [4]byte) []byte {
	var b []byte
	b0 := op
	if fin {
		b0 |= wsFinBit
	}
	b = append(b, b0)
	n := len(payload)
	switch {
	case n < 126:
		b = append(b, wsMaskBit|byte(n))
	case n <= 65535:
		b = append(b, wsMaskBit|126, byte(n>>8), byte(n))
	default:
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		b = append(b, wsMaskBit|127)
		b = append(b, ext[:]...)
	}
	b = append(b, mask[:]...)
	for i, c := range payload {
		b = append(b, c^mask[i&3])
	}
	return b
}

var testMask = [4]byte{0xA1, 0xB2, 0xC3, 0xD4}

// gateWire 把消息编码成 gate 帧流（明文、不压缩）。
func gateWire(t *testing.T, msgs ...[]byte) []byte {
	t.Helper()
	var stream []byte
	for _, m := range msgs {
		bf, err := buildFrame(append([]byte(nil), m...), 0, nil, false, nil, maxMessageSize)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, wire(&bf)...)
	}
	return stream
}

// feedCollect 把 raw 直接喂进一个独立 wsState，收集 emit 段与控制帧。
type wsCollector struct {
	gate  []byte
	ctrls []struct {
		op      byte
		payload []byte
	}
}

func feedCollect(w *wsState, col *wsCollector, raw []byte) error {
	return w.feed(raw,
		func(seg []byte) error {
			col.gate = append(col.gate, seg...)
			return nil
		},
		func(op byte, payload []byte) error {
			// 镜像生产语义（wsCtrl）：close 的 payload 校验在控制帧收齐后。
			if op == wsOpClose {
				if _, _, err := parseClosePayload(payload); err != nil {
					return err
				}
			}
			col.ctrls = append(col.ctrls, struct {
				op      byte
				payload []byte
			}{op, append([]byte(nil), payload...)})
			return nil
		})
}

// ─── 校验清单逐条（05「校验清单」，编号即步骤号）───

func TestWSValidationChecklist(t *testing.T) {
	frame := func(mut func(b []byte)) []byte {
		b := wsClientFrame(true, wsOpBinary, []byte("abcd"), testMask)
		if mut != nil {
			mut(b)
		}
		return b
	}
	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{"2_RSV非零", frame(func(b []byte) { b[0] |= 0x40 }), errWSRsv},
		{"3_保留opcode", frame(func(b []byte) { b[0] = wsFinBit | 0x3 }), errWSOpcode},
		{"4_text帧", frame(func(b []byte) { b[0] = wsFinBit | wsOpText }), errWSText},
		{"5_控制帧超125", wsClientFrame(true, wsOpPing, mkPayload(126), testMask), errWSCtrlTooLong},
		{"6_控制帧分片", wsClientFrame(false, wsOpPing, []byte("x"), testMask), errWSCtrlFragment},
		{"7_126编码100", func() []byte {
			b := []byte{wsFinBit | wsOpBinary, wsMaskBit | 126, 0, 100}
			return append(b, testMask[:]...)
		}(), errWSBadLength},
		{"7_127编码70000以下", func() []byte {
			b := []byte{wsFinBit | wsOpBinary, wsMaskBit | 127}
			var ext [8]byte
			binary.BigEndian.PutUint64(ext[:], 65535)
			b = append(b, ext[:]...)
			return append(b, testMask[:]...)
		}(), errWSBadLength},
		{"8_64位长度MSB", func() []byte {
			b := []byte{wsFinBit | wsOpBinary, wsMaskBit | 127}
			var ext [8]byte
			binary.BigEndian.PutUint64(ext[:], 1<<63|70000)
			b = append(b, ext[:]...)
			return append(b, testMask[:]...)
		}(), errWSLenMSB},
		{"9_宽松上界", func() []byte {
			b := []byte{wsFinBit | wsOpBinary, wsMaskBit | 127}
			var ext [8]byte
			binary.BigEndian.PutUint64(ext[:], wsMaxFrameLen+1)
			b = append(b, ext[:]...)
			return append(b, testMask[:]...)
		}(), errWSFrameTooLong},
		{"10_未带掩码", func() []byte {
			b := wsClientFrame(true, wsOpBinary, []byte("abcd"), testMask)
			out := []byte{b[0], b[1] &^ wsMaskBit}
			return append(out, []byte("abcd")...) // 去掉掩码键，payload 不掩码
		}(), errWSUnmasked},
		{"12_无分片时continuation", wsClientFrame(true, wsOpContinuation, []byte("x"), testMask), errWSFragState},
		{"13_close长度1", wsClientFrame(true, wsOpClose, []byte{0x03}, testMask), errWSBadClose},
		{"13_close非法code", func() []byte {
			p := []byte{0x03, 0xEE} // 1006 不允许出现在线路上
			return wsClientFrame(true, wsOpClose, p, testMask)
		}(), errWSBadClose},
		{"13_close非UTF8", func() []byte {
			p := append([]byte{0x03, 0xE8}, 0xFF, 0xFE)
			return wsClientFrame(true, wsOpClose, p, testMask)
		}(), errWSBadClose},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &wsState{}
			var col wsCollector
			err := feedCollect(w, &col, tc.raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			w.release()
		})
	}
}

// 分片消息中间再来 binary → 违规。
func TestWSValidation_BinaryDuringFragment(t *testing.T) {
	w := &wsState{}
	var col wsCollector
	raw := wsClientFrame(false, wsOpBinary, []byte("p1"), testMask)
	raw = append(raw, wsClientFrame(true, wsOpBinary, []byte("p2"), testMask)...)
	if err := feedCollect(w, &col, raw); !errors.Is(err, errWSFragState) {
		t.Fatalf("got %v", err)
	}
}

// ─── W2/W8：分片 + 控制帧穿插 + 掩码跨事件续位 ───

func TestW2_FragmentsWithControlInterleaved(t *testing.T) {
	gate := gateWire(t, []byte("hello-fragmented-world"))
	p1, p2 := gate[:7], gate[7:]

	var raw []byte
	raw = append(raw, wsClientFrame(false, wsOpBinary, p1, testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpPing, []byte("ka"), testMask)...) // 分片中间穿插控制帧
	raw = append(raw, wsClientFrame(true, wsOpContinuation, p2, testMask)...)

	w := &wsState{}
	var col wsCollector
	if err := feedCollect(w, &col, raw); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(col.gate, gate) {
		t.Fatal("分片拼接后的 gate 字节流不一致")
	}
	if len(col.ctrls) != 1 || col.ctrls[0].op != wsOpPing || string(col.ctrls[0].payload) != "ka" {
		t.Fatalf("ctrls=%v", col.ctrls)
	}
	if w.fragmented {
		t.Fatal("FIN=1 后应退出分片状态")
	}
}

// FuzzWSFeedSplit 的确定性版：同一合法字节流按每个位置切两段喂，
// emit 序列与一次性喂逐字节相同——流式解帧 + maskOff 续位的直接证明。
func TestWSFeedSplitEveryPosition(t *testing.T) {
	gate := gateWire(t, []byte("msg-a"), []byte("msg-b"), mkPayload(300))
	var raw []byte
	raw = append(raw, wsClientFrame(false, wsOpBinary, gate[:11], testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpPing, []byte("p"), testMask)...)
	raw = append(raw, wsClientFrame(true, wsOpContinuation, gate[11:], testMask)...)

	wRef := &wsState{}
	var ref wsCollector
	if err := feedCollect(wRef, &ref, append([]byte(nil), raw...)); err != nil {
		t.Fatal(err) // 去掩码是原地的：参考轮也要用副本
	}
	for cut := 0; cut <= len(raw); cut++ {
		w := &wsState{}
		var col wsCollector
		src := append([]byte(nil), raw...) // 去掩码是原地的，每轮用新副本
		if err := feedCollect(w, &col, src[:cut]); err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if err := feedCollect(w, &col, src[cut:]); err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if !bytes.Equal(col.gate, ref.gate) || len(col.ctrls) != len(ref.ctrls) {
			t.Fatalf("cut=%d: 切分喂与一次性喂不一致", cut)
		}
	}
}

func FuzzWSFeedSplit(f *testing.F) {
	f.Add([]byte("hello websocket"), uint16(3), uint16(40))
	f.Add(mkPayload(300), uint16(0), uint16(1))
	f.Fuzz(func(t *testing.T, payload []byte, cutA, cutB uint16) {
		if len(payload) > 4<<10 {
			return
		}
		gate := gateWire(t, payload)
		split := int(cutA) % (len(gate) + 1)
		var raw []byte
		raw = append(raw, wsClientFrame(false, wsOpBinary, gate[:split], testMask)...)
		raw = append(raw, wsClientFrame(true, wsOpContinuation, gate[split:], testMask)...)

		wRef := &wsState{}
		var ref wsCollector
		if err := feedCollect(wRef, &ref, append([]byte(nil), raw...)); err != nil {
			t.Fatal(err)
		}
		cut := int(cutB) % (len(raw) + 1)
		w := &wsState{}
		var col wsCollector
		src := append([]byte(nil), raw...)
		if err := feedCollect(w, &col, src[:cut]); err != nil {
			t.Fatal(err)
		}
		if err := feedCollect(w, &col, src[cut:]); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(col.gate, ref.gate) {
			t.Fatal("任意切分下 emit 字节流必须一致")
		}
	})
}

// FuzzWebSocketFrames：任意字节进帧层，不 panic、不越界。
func FuzzWebSocketFrames(f *testing.F) {
	f.Add(wsClientFrame(true, wsOpBinary, []byte("ok"), testMask))
	f.Add(wsClientFrame(true, wsOpClose, []byte{0x03, 0xE8}, testMask))
	f.Add([]byte{0x82, 0xFF})
	f.Fuzz(func(t *testing.T, raw []byte) {
		w := &wsState{}
		var col wsCollector
		_ = feedCollect(w, &col, raw)
		// 帧层残片上界：半个帧头 + 收齐中的控制帧（W12）。
		if w.hdrLen > wsMaxHeader || len(w.ctrl) > wsMaxCtrlPayload {
			t.Fatal("WS 层残片越界")
		}
		w.release()
	})
}
