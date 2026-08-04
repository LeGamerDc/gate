package gate

import (
	"testing"

	"github.com/gobwas/ws"
)

/*
编码侧的 fuzz。

FuzzCodec（codec_test.go）覆盖的是**解码**：任意字节喂给 parse/deliver。
但 gate 复杂度最高的一段其实在**编码**侧——sender 要在压缩、合包、加密三个
开关和三种 permit 掩码之间做决策，再把结果拼成一个长度字段必须精确正确的帧。
那一侧此前一次随机化验证都没有。

差分 oracle 很干净：sender 编出来的东西，用 client 侧的 codec 解回来，
必须逐条等于送进去的消息。任何长度算错、分组算错、标记漏置的 bug 都会当场暴露。
*/

// FuzzSenderRoundTrip 用 fuzz 输入同时派生 sender 配置和消息序列，
// 断言 sender → 线路 → client codec 的 round-trip 恒等。
func FuzzSenderRoundTrip(f *testing.F) {
	f.Add([]byte("hello world"), uint16(0), uint16(0), uint8(0), uint8(1))
	f.Add([]byte("compress me, compress me, compress me"), uint16(8), uint16(4096), uint8(1), uint8(3))
	f.Add(make([]byte, 300), uint16(1), uint16(64), uint8(2), uint8(7))
	f.Add([]byte{}, uint16(0), uint16(16), uint8(3), uint8(1))
	f.Add(make([]byte, 5000), uint16(64), uint16(32768), uint8(1), uint8(2))

	f.Fuzz(func(t *testing.T, data []byte, threshold, cluster uint16, flags, chop uint8) {
		msgs := chopMessages(data, chop)
		if len(msgs) == 0 {
			return
		}

		modes := make([]sendMode, len(msgs))
		for i := range modes {
			// 用 flags 的高位配合下标派生每条消息的发送方式，让可合包与
			// 不可合包的消息在序列里交错出现——pushTcp 的分组扫描正是在这里出错的。
			switch (int(flags>>2) + i) % 4 {
			case 3:
				modes[i] = modeNoEncrypt
			case 2:
				modes[i] = modeStatic
			default:
				modes[i] = modeSend
			}
		}

		var cipher Cipher
		if flags&1 != 0 {
			cipher = xorCipher{key: 0xa7}
		}

		cfg := SenderConfig{
			CompressThreshold: int(threshold),
			MaxClusterSize:    int(cluster),
			CompressLevel:     CompressLevel(flags >> 6),
		}

		got, _ := senderRoundTrip(t, cfg, cipher, msgs, modes)
		requireRoundTrip(t, got, msgs, "fuzz round-trip")
	})
}

// chopMessages 把一段字节切成若干条消息。
//
// chop 为 0 时退化成"整段当成一条消息"。上限 64 条是为了让单次 fuzz 迭代
// 保持在毫秒量级；再多也不会覆盖到新的分支。
func chopMessages(data []byte, chop uint8) [][]byte {
	const maxMessages = 64

	if len(data) == 0 {
		// 空消息本身就是一个值得覆盖的用例：它在线路上仍然占 2 字节 header。
		return [][]byte{{}}
	}
	if chop == 0 {
		return [][]byte{data}
	}

	msgs := make([][]byte, 0, 8)
	step := int(chop)
	for len(data) > 0 && len(msgs) < maxMessages {
		n := step
		if n > len(data) {
			n = len(data)
		}
		msgs = append(msgs, data[:n])
		data = data[n:]
		// 让切片长度在序列里变化，而不是清一色等长。
		step = step*2 + 1
		if step > 4096 {
			step = int(chop) + 1
		}
	}
	if len(data) > 0 {
		msgs = append(msgs, data)
	}
	return msgs
}

// FuzzWebSocketInboundFrames 把任意字节当作已完成握手之后的 WebSocket 帧流。
//
// 不变式：decodeMessages 对任意输入都必须终止、不 panic；无论解析成功与否，
// 缓冲占用都不得超过配置的上限（否则一个畸形帧头就能变成内存放大攻击）。
func FuzzWebSocketInboundFrames(f *testing.F) {
	f.Add(maskedFuzzFrame(ws.OpBinary, gateFrame([]byte("hello"))))
	f.Add(maskedFuzzFrame(ws.OpPing, []byte("hb")))
	f.Add(maskedFuzzFrame(ws.OpClose, nil))
	f.Add(maskedFuzzFrame(ws.OpText, []byte("nope")))
	f.Add([]byte{0x82, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		const limit = 1 << 16

		g := &wsControlConn{}
		conn := &Conn{conn: g, codec: serverCodec(maxMessageSize)}
		conn.init()
		conn.markOpen()
		conn.handler = &funcHandler{fn: func([]byte) {}}

		state := &wsConnState{
			conn:             conn,
			upgraded:         true,
			maxBufferedBytes: limit,
		}
		state.buf.Write(data)

		// 允许返回任何错误（畸形输入本来就该被拒），但必须返回。
		_ = state.decodeMessages()

		// 断言 ensureBufferedLimit 真正负责的那两块：分片重组缓冲和 gate 残留缓冲。
		//
		// 不能拿 bufferedBytes() 去比 limit+len(data)——那个式子恒成立，是个假断言：
		// state.buf 里本来就装着我们自己写进去的 data，而另外两块只会更小。
		// 有意义的不变式是"畸形输入不能让 gate 自己再攒出超过上限的内存"。
		if n := state.fragmentBuf.payloadLen; n > limit {
			t.Fatalf("fragment buffer grew to %d bytes, past the %d-byte limit", n, limit)
		}
		if n := state.gateBuf.Len(); n > limit {
			t.Fatalf("gate remainder buffer grew to %d bytes, past the %d-byte limit", n, limit)
		}
	})
}

func maskedFuzzFrame(op ws.OpCode, payload []byte) []byte {
	f := ws.MaskFrameInPlace(ws.NewFrame(op, true, payload))
	b, err := ws.CompileFrame(f)
	if err != nil {
		panic(err)
	}
	return b
}
