package gate

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

/*
性质测试：对**生成的**输入断言不变式，而不是对手挑的例子断言结果。

sender 的压缩 / 合包 / 加密是三个互相影响的开关，再叠上"消息是否可合包"
（Send vs SendNoEncrypt vs SendStatic 给出三种不同的 permit 掩码），组合数远超
手写用例能覆盖的范围。而它们共同要保证的东西其实只有一句话：

	进去什么消息序列，出来就是什么消息序列。

把这句话写成 property，一次覆盖全部组合。
*/

// sendMode 决定一条消息走哪个发送接口，从而决定它的 permit 掩码，
// 也就决定它能不能进 compound。
type sendMode uint8

const (
	modeSend      sendMode = iota // 允许压缩/合包/加密
	modeNoEncrypt                 // 明文独立帧，不可合包
	modeStatic                    // 零拷贝独立帧，不可合包
)

func (m sendMode) String() string {
	switch m {
	case modeNoEncrypt:
		return "no-encrypt"
	case modeStatic:
		return "static"
	default:
		return "send"
	}
}

func (m sendMode) apply(s *sender, data []byte) error {
	switch m {
	case modeNoEncrypt:
		return s.SendNoEncrypt(data)
	case modeStatic:
		return s.SendStatic(data, false)
	default:
		return s.Send(data)
	}
}

// decodeGateWire 用 client 侧最宽松的策略把一段线路字节还原成业务消息序列。
func decodeGateWire(tb testing.TB, wire []byte, cipher Cipher) [][]byte {
	tb.Helper()

	c := clientCodec(maxMessageSize, maxMessageSize)
	dec, err := newZstdDecoder(maxMessageSize)
	if err != nil {
		tb.Fatal(err)
	}
	defer dec.Close()

	msgs := make([][]byte, 0, 8)
	for len(wire) > 0 {
		f, n, ok, err := c.parse(wire)
		if err != nil {
			tb.Fatalf("parse wire: %v", err)
		}
		if !ok {
			tb.Fatalf("truncated frame on the wire, %d byte(s) left", len(wire))
		}
		if err = c.deliver(f, cipher, dec, func(msg []byte, _ bool) error {
			msgs = append(msgs, append([]byte(nil), msg...))
			return nil
		}); err != nil {
			tb.Fatalf("deliver frame: %v", err)
		}
		wire = wire[n:]
	}
	return msgs
}

// wireStats 记录一次 flush 在线路上实际用到了哪些编码路径。
//
// property 测试自己也需要被证明"真的走到了有意思的分支"：一个恒等于
// pushSeparate + 明文的 round-trip 断言，无论跑多少轮都发现不了压缩或合包的 bug。
type wireStats struct {
	frames     int
	compressed int
	compound   int
	encrypted  int
	extHeader  int // 用了 4 字节扩展头的帧
}

func (s *wireStats) add(o wireStats) {
	s.frames += o.frames
	s.compressed += o.compressed
	s.compound += o.compound
	s.encrypted += o.encrypted
	s.extHeader += o.extHeader
}

// observeWire 只解析帧头收集标记，不做解密/解压——decodeGateWire 会就地解密，
// 统计必须在那之前做。
func observeWire(tb testing.TB, wire []byte) wireStats {
	tb.Helper()

	c := clientCodec(maxMessageSize, maxMessageSize)
	var st wireStats
	for len(wire) > 0 {
		f, n, ok, err := c.parse(wire)
		if err != nil || !ok {
			tb.Fatalf("observe wire: err=%v ok=%t", err, ok)
		}
		st.frames++
		if f.z {
			st.compressed++
		}
		if f.c {
			st.compound++
		}
		if f.e {
			st.encrypted++
		}
		if n-len(f.payload) == 4 {
			st.extHeader++
		}
		wire = wire[n:]
	}
	return st
}

// senderRoundTrip 把一组消息喂进 sender、触发一次 flush、再解回来。
//
// MaxBufferSize 恒为 0（不限）：这里测的是编码的正确性，背压是另一组用例的事。
// 留着上限会让 property 的判据从"完全相等"退化成"相等或被拒"，也就没法再发现
// 丢消息的 bug 了。
func senderRoundTrip(tb testing.TB, cfg SenderConfig, cipher Cipher, msgs [][]byte, modes []sendMode) ([][]byte, wireStats) {
	tb.Helper()

	cfg.MaxBufferSize = 0
	s, w, g := newTestSender(tb, &cfg)
	if cipher != nil {
		s.conn.UpdateCipher(cipher)
	}

	for i, m := range msgs {
		if err := modes[i].apply(s, m); err != nil {
			tb.Fatalf("send message %d (%d bytes, %s): %v", i, len(m), modes[i], err)
		}
	}
	g.fire(nil)

	wire := w.wire()
	st := observeWire(tb, wire)
	return decodeGateWire(tb, wire, cipher), st
}

// requireRoundTrip 断言解回来的消息序列与发出去的逐条相等。
func requireRoundTrip(tb testing.TB, got, want [][]byte, ctx string) {
	tb.Helper()

	if len(got) != len(want) {
		tb.Fatalf("%s: got %d message(s), want %d", ctx, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			tb.Fatalf("%s: message %d mismatch (got %d bytes, want %d bytes)",
				ctx, i, len(got[i]), len(want[i]))
		}
	}
}

// P2：任意配置 + 任意消息序列，round-trip 必须逐条相等。
//
// 这是这一组里最有价值的一条：它一次性覆盖压缩阈值、合包上限、加密、
// 以及三种 permit 掩码的全部交叉组合。
func TestPropertySenderRoundTripPreservesMessages(t *testing.T) {
	rng := rand.New(rand.NewSource(20260804))
	var total wireStats

	for iter := 0; iter < 300; iter++ {
		cfg := SenderConfig{
			// 0 表示关闭；其余取值让阈值有时高于、有时低于实际消息大小。
			CompressThreshold: []int{0, 1, 64, 512, 4096}[rng.Intn(5)],
			MaxClusterSize:    []int{0, 8, 64, 1024, 32 * 1024}[rng.Intn(5)],
			CompressLevel:     CompressLevel(rng.Intn(4)),
		}
		var cipher Cipher
		if rng.Intn(2) == 0 {
			cipher = xorCipher{key: byte(rng.Intn(255) + 1)}
		}

		n := rng.Intn(40) + 1
		msgs := make([][]byte, n)
		modes := make([]sendMode, n)
		for i := range msgs {
			// 尺寸分布刻意混进 0 和跨过 4096 的值：header 在那里从 2 字节切到 4 字节。
			size := []int{0, 1, 7, 100, 4095, 4096, 4097, 9000}[rng.Intn(8)]
			msgs[i] = randomPayload(rng, size)
			// 偏向 modeSend：只有它是可合包的，均匀三选一会让"连续两条都可合包"
			// 只剩约 1/9 的概率，compound 路径几乎走不到。
			switch rng.Intn(5) {
			case 3:
				modes[i] = modeNoEncrypt
			case 4:
				modes[i] = modeStatic
			default:
				modes[i] = modeSend
			}
		}

		ctx := fmt.Sprintf("iter=%d threshold=%d cluster=%d cipher=%t n=%d",
			iter, cfg.CompressThreshold, cfg.MaxClusterSize, cipher != nil, n)
		got, st := senderRoundTrip(t, cfg, cipher, msgs, modes)
		requireRoundTrip(t, got, msgs, ctx)
		total.add(st)
	}

	// 证明这一轮 property 真的走到了每一条编码路径。少了这一段，一个
	// "所有分支都退化成明文独立帧"的回归会让上面的断言全部通过。
	switch {
	case total.compressed == 0:
		t.Fatal("no compressed frame was produced; the compression path was never exercised")
	case total.compound == 0:
		t.Fatal("no compound frame was produced; the compounding path was never exercised")
	case total.encrypted == 0:
		t.Fatal("no encrypted frame was produced; the encryption path was never exercised")
	case total.extHeader == 0:
		t.Fatal("no 4-byte-header frame was produced; the >=4096 path was never exercised")
	}
	t.Logf("wire coverage: %d frames, %d compressed, %d compound, %d encrypted, %d extended-header",
		total.frames, total.compressed, total.compound, total.encrypted, total.extHeader)
}

// randomPayload 造一段部分可压缩的数据：纯随机不可压缩，全同字节压得过好，
// 两者都会让"压缩后反而更大"这类分支永远走不到。
func randomPayload(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		if rng.Intn(4) == 0 {
			b[i] = byte(rng.Intn(256))
		} else {
			b[i] = byte('a' + i%5)
		}
	}
	return b
}

// P5：队列计量必须精确等于 Σ frameSize，否则 MaxBufferSize 形同虚设。
func TestPropertyQueueAccountingIsExact(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	for iter := 0; iter < 200; iter++ {
		s, _, _ := newTestSender(t, &SenderConfig{})

		want := 0
		n := rng.Intn(20) + 1
		for i := 0; i < n; i++ {
			size := []int{0, 1, 4095, 4096, 5000}[rng.Intn(5)]
			data := make([]byte, size)
			if err := s.Send(data); err != nil {
				t.Fatalf("iter=%d send %d: %v", iter, i, err)
			}
			want += frameSize(size)
		}

		s.mu.Lock()
		got := s.queued
		s.mu.Unlock()
		if got != want {
			t.Fatalf("iter=%d: queued = %d, want %d", iter, got, want)
		}
	}
}

// P4：cluster() 报出来的 size 必须恒等于实际编码出的 compound body 字节数。
//
// 这两个数必须是同一个：它既是和 MaxClusterSize 比较的口径，也是最终写进外层
// header 长度字段的值。对不上就意味着帧长度写错，对端一定拒收。
func TestPropertyClusterSizeMatchesEncodedBody(t *testing.T) {
	rng := rand.New(rand.NewSource(99))

	for iter := 0; iter < 300; iter++ {
		maxCluster := []int{8, 64, 1024, 32 * 1024}[rng.Intn(4)]
		s, _, _ := newTestSender(t, &SenderConfig{MaxClusterSize: maxCluster})

		n := rng.Intn(20) + 1
		buf := make([]sendMsg, n)
		for i := range buf {
			size := []int{0, 1, 100, 4095, 4096}[rng.Intn(5)]
			buf[i] = compoundMsg(make([]byte, size))
		}

		j, size := s.cluster(buf, 0)
		if got := compoundBody(buf[0:j]); got != size {
			t.Fatalf("iter=%d: cluster reported %d, encoded body is %d", iter, size, got)
		}
		if size > s.maxCluster && j-0 > 1 {
			t.Fatalf("iter=%d: cluster of %d message(s) is %d bytes, over the %d limit",
				iter, j, size, s.maxCluster)
		}
	}
}

// P1：header 编解码在整个可表达范围内必须往返一致，且长度不得污染标记位。
func TestPropertyHeaderRoundTrip(t *testing.T) {
	sizes := []int{0, 1, 2, 4094, 4095, 4096, 4097, 65535, 65536, 1 << 20, maxMessageSize}
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 500; i++ {
		sizes = append(sizes, rng.Intn(maxMessageSize+1))
	}

	for _, size := range sizes {
		var b [4]byte
		n := encodeHeader(b[:], size)
		if n != frameOverhead(size) {
			t.Fatalf("size %d: encodeHeader wrote %d bytes, frameOverhead says %d",
				size, n, frameOverhead(size))
		}

		got, m, z, c, e := parseHeader([2]byte{b[0], b[1]})
		if z || c || e {
			t.Fatalf("size %d: length bits leaked into the z/c/e flags", size)
		}
		if m {
			got = parseHeader2([2]byte{b[2], b[3]}, got)
		}
		if got != size {
			t.Fatalf("header round-trip: got %d, want %d", got, size)
		}
		if m != (n == 4) {
			t.Fatalf("size %d: m flag %t disagrees with header length %d", size, m, n)
		}
	}
}

// P6：三档策略必须是单调收紧的 sub ⊂ server ⊂ client。
//
// 任何一档接受的标记组合，都必须被更宽的那一档接受；反过来被严档拒绝的，
// 宽档不一定拒绝。这条不变式是"compound 子消息不得带任何标记"的结构性来源。
func TestPropertyCodecPolicyIsMonotonic(t *testing.T) {
	var (
		client = clientCodec(maxMessageSize, maxMessageSize)
		server = serverCodec(maxMessageSize)
		sub    = server.subCodec()
	)

	for bits := 0; bits < 8; bits++ {
		z, c, e := bits&1 != 0, bits&2 != 0, bits&4 != 0

		subOK := sub.checkFlags(z, c, e) == nil
		serverOK := server.checkFlags(z, c, e) == nil
		clientOK := client.checkFlags(z, c, e) == nil

		if subOK && !serverOK {
			t.Fatalf("z=%t c=%t e=%t accepted by sub policy but rejected by server", z, c, e)
		}
		if serverOK && !clientOK {
			t.Fatalf("z=%t c=%t e=%t accepted by server policy but rejected by client", z, c, e)
		}
		// sub 策略必须只接受"三个标记全 0"这一种。
		if subOK != (!z && !c && !e) {
			t.Fatalf("sub policy accepted z=%t c=%t e=%t", z, c, e)
		}
	}
}
