package gate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// ─── 构造原始帧的小工具（测试侧独立拼字节，不走 putHeader）─────────────────

// rawFrame 手工拼一个帧：hdr4 = true 强制 4 字节头（可构造非规范编码）。
func rawFrame(flags byte, size int, hdr4 bool, payload []byte) []byte {
	var b []byte
	if hdr4 {
		b = append(b, flagM|flags|byte(size>>24), byte(size>>16), byte(size>>8), byte(size))
	} else {
		b = append(b, flags|byte(size>>8), byte(size))
	}
	return append(b, payload...)
}

func mkPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i * 7)
	}
	return p
}

// ─── parse：校验规格逐步骤（02「校验规格」，编号即步骤号）───────────────────

func TestCodecStep1_NeedTwoBytes(t *testing.T) {
	c := serverCodec(maxMessageSize)
	for _, src := range [][]byte{nil, {}, {0x00}} {
		_, n, ok, err := c.parse(src)
		if ok || err != nil || n != 0 {
			t.Fatalf("src=%v: want 数据不足, got n=%d ok=%v err=%v", src, n, ok, err)
		}
	}
}

func TestCodecStep3_LargeHeaderNeedsFourBytes(t *testing.T) {
	c := serverCodec(maxMessageSize)
	full := rawFrame(0, 5000, true, nil)
	for cut := 2; cut < 4; cut++ {
		_, _, ok, err := c.parse(full[:cut])
		if ok || err != nil {
			t.Fatalf("cut=%d: want 数据不足, got ok=%v err=%v", cut, ok, err)
		}
	}
}

func TestCodecStep5_NonCanonical(t *testing.T) {
	c := clientCodec(maxMessageSize, maxMessageSize)
	for _, size := range []int{0, 100, 4095} {
		src := rawFrame(0, size, true, mkPayload(size))
		_, _, _, err := c.parse(src)
		if !errors.Is(err, ErrNonCanonicalHeader) {
			t.Fatalf("m=1 size=%d: want ErrNonCanonicalHeader, got %v", size, err)
		}
	}
}

// 扩展空间：m=1 且 高12=0 且 低16<4096 是唯一预留的不可达编码，
// 现行实现必须把它当协议违规拒掉——明确失败，不是静默误解。
func TestCodecStep5_EscapeSpaceRejected(t *testing.T) {
	c := clientCodec(maxMessageSize, maxMessageSize)
	src := []byte{flagM, 0x00, 0x00, 0x00} // 高12=0，低16=0
	if _, _, _, err := c.parse(src); !errors.Is(err, ErrNonCanonicalHeader) {
		t.Fatalf("escape encoding: want ErrNonCanonicalHeader, got %v", err)
	}
}

// 第 6 步的位置是硬要求：长度校验必须早于任何按长度进行的分配或切片。
// 一个 4 字节的头谎报 200MB，parse 必须原样报错且 0 分配。
func TestCodecStep6_SizeBeforeAlloc(t *testing.T) {
	c := serverCodec(1 << 10)
	src := rawFrame(flagE, 200<<20, true, nil)
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, err := c.parse(src); !errors.Is(err, ErrMaxMessageSize) {
			t.Fatalf("want ErrMaxMessageSize, got %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("parse 拒超长帧分配了 %v 次", allocs)
	}
}

func TestCodecStep6_MaxMessageBoundary(t *testing.T) {
	const limit = 5000
	c := codec{maxMessage: limit, allow: flagAll}
	okSrc := rawFrame(0, limit, true, mkPayload(limit))
	if _, _, ok, err := c.parse(okSrc); !ok || err != nil {
		t.Fatalf("size=maxMessage: want ok, got ok=%v err=%v", ok, err)
	}
	badSrc := rawFrame(0, limit+1, true, mkPayload(limit+1))
	if _, _, _, err := c.parse(badSrc); !errors.Is(err, ErrMaxMessageSize) {
		t.Fatalf("size=maxMessage+1: want ErrMaxMessageSize, got %v", err)
	}
}

func TestCodecStep7_AllowMask(t *testing.T) {
	server := serverCodec(maxMessageSize) // allow = e
	for _, flags := range []byte{flagZ, flagC, flagZ | flagC} {
		src := rawFrame(flags, 3, false, []byte("abc"))
		if _, _, _, err := server.parse(src); !errors.Is(err, ErrFlagNotAllowed) {
			t.Fatalf("server flags=%02x: want ErrFlagNotAllowed, got %v", flags, err)
		}
	}
	sub := server.subCodec() // allow = 0：包括 e 在内全拒
	for _, flags := range []byte{flagZ, flagC, flagE} {
		src := rawFrame(flags, 3, false, []byte("abc"))
		if _, _, _, err := sub.parse(src); !errors.Is(err, ErrFlagNotAllowed) {
			t.Fatalf("sub flags=%02x: want ErrFlagNotAllowed, got %v", flags, err)
		}
	}
}

// W3 双向：配了 Cipher 但 e=0 ⇒ ErrCipherRequired（AAD 管不到不解密的帧，
// 必须把所有帧逼进认证路径）；没配但 e=1 ⇒ ErrCipherUnavailable（堵 nil 解引用）。
func TestCodecStep8_CipherConsistency(t *testing.T) {
	withCipher := serverCodec(maxMessageSize)
	withCipher.requireEncrypt = true
	plain := rawFrame(0, 3, false, []byte("abc"))
	if _, _, _, err := withCipher.parse(plain); !errors.Is(err, ErrCipherRequired) {
		t.Fatalf("配了 Cipher 收到 e=0: want ErrCipherRequired, got %v", err)
	}

	noCipher := serverCodec(maxMessageSize)
	enc := rawFrame(flagE, 3, false, []byte("abc"))
	if _, _, _, err := noCipher.parse(enc); !errors.Is(err, ErrCipherUnavailable) {
		t.Fatalf("没配 Cipher 收到 e=1: want ErrCipherUnavailable, got %v", err)
	}
}

// 任何合法帧的每一个真前缀都必须返回「数据不足」，而不是错误。
func TestCodecStep9_EveryPrefixRetryable(t *testing.T) {
	c := serverCodec(maxMessageSize)
	for _, size := range []int{0, 1, 100, 4095, 4096, 65535, 65536} {
		full := rawFrame(0, size, size >= smallSizeLimit, mkPayload(size))
		for _, cut := range []int{0, 1, 2, 3, len(full) / 2, len(full) - 1} {
			if cut >= len(full) {
				continue
			}
			_, _, ok, err := c.parse(full[:cut])
			if ok || err != nil {
				t.Fatalf("size=%d cut=%d: want 数据不足, got ok=%v err=%v", size, cut, ok, err)
			}
		}
		f, n, ok, err := c.parse(full)
		if !ok || err != nil || n != len(full) || len(f.payload) != size {
			t.Fatalf("size=%d 整帧: got n=%d ok=%v err=%v len=%d", size, n, ok, err, len(f.payload))
		}
	}
}

// 三索引切片封 cap：对 payload 做 append 不得覆盖后续兄弟字节。
func TestCodecStep10_ThreeIndexSlice(t *testing.T) {
	c := serverCodec(maxMessageSize)
	src := append(rawFrame(0, 3, false, []byte("abc")), []byte("XYZ")...)
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if cap(f.payload) != len(f.payload) || cap(f.header) != len(f.header) {
		t.Fatalf("cap 未封住: payload %d/%d header %d/%d",
			len(f.payload), cap(f.payload), len(f.header), cap(f.header))
	}
	_ = append(f.payload, '!', '!') //nolint —— 故意 append，必须触发复制
	if !bytes.Equal(src[5:8], []byte("XYZ")) {
		t.Fatalf("append 覆盖了相邻字节: %q", src[5:8])
	}
}

// 帧头必须原样保留在 frame 里作为 Open 的 aad。
func TestCodecParse_KeepsHeaderForAAD(t *testing.T) {
	c := clientCodec(maxMessageSize, maxMessageSize)
	small := rawFrame(0, 3, false, []byte("abc"))
	f, _, _, _ := c.parse(small)
	if !bytes.Equal(f.header, small[:2]) {
		t.Fatalf("2 字节头未保留: %x", f.header)
	}
	big := rawFrame(0, 5000, true, mkPayload(5000))
	f, _, _, _ = c.parse(big)
	if !bytes.Equal(f.header, big[:4]) {
		t.Fatalf("4 字节头未保留: %x", f.header)
	}
}

// ─── 边界表（02「测试要求」）────────────────────────────────────────────────

func TestCodecBoundary_HeaderSwitch(t *testing.T) {
	c := clientCodec(maxMessageSize, maxMessageSize)
	cases := []struct {
		size   int
		hdrLen int
	}{
		{4095, 2}, {4096, 4}, // 2/4 字节头切换
		{65535, 4}, {65536, 4}, // 低 16 位进位
		{0, 2}, // 空消息
	}
	for _, tc := range cases {
		var h [maxHeaderSize]byte
		n := putHeader(h[:], tc.size, 0)
		if n != tc.hdrLen {
			t.Fatalf("size=%d: putHeader 头长 %d, want %d", tc.size, n, tc.hdrLen)
		}
		src := append(h[:n:n], mkPayload(tc.size)...)
		f, consumed, ok, err := c.parse(src)
		if !ok || err != nil || consumed != n+tc.size || len(f.payload) != tc.size {
			t.Fatalf("size=%d: n=%d ok=%v err=%v", tc.size, consumed, ok, err)
		}
	}
}

// ─── deliver ────────────────────────────────────────────────────────────────

func collect(t *testing.T, c codec, src []byte, ci Cipher, dec *zstd.Decoder) [][]byte {
	t.Helper()
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	var got [][]byte
	if err := c.deliver(f, ci, dec, func(msg []byte, _ bool) error {
		got = append(got, append([]byte(nil), msg...)) // sink 约定：留住必须复制
		return nil
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	return got
}

func TestDeliver_PlainPassthrough(t *testing.T) {
	c := serverCodec(maxMessageSize)
	got := collect(t, c, rawFrame(0, 5, false, []byte("hello")), nil, nil)
	if len(got) != 1 || !bytes.Equal(got[0], []byte("hello")) {
		t.Fatalf("got %q", got)
	}
}

// Overhead()==0：原地解密，msg 必须落在原 payload 的底层数组里（零拷贝零分配）。
func TestDeliver_InPlaceDecrypt(t *testing.T) {
	sender := &xorCipher{key: 0x5a}
	receiver := &xorCipher{key: 0x5a}
	c := serverCodec(maxMessageSize)
	c.requireEncrypt = true

	plain := []byte("in-place")
	var h [maxHeaderSize]byte
	hn := putHeader(h[:], len(plain), flagE)
	body := sender.Seal(make([]byte, 0, len(plain)), plain, h[:hn])
	src := append(append([]byte{}, h[:hn]...), body...)

	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	var sawOwned bool
	var sawMsg []byte
	if err := c.deliver(f, receiver, nil, func(msg []byte, owned bool) error {
		sawOwned, sawMsg = owned, msg
		if &msg[0] != &f.payload[0] {
			t.Fatal("原地解密的 msg 不在原 payload 内")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if sawOwned {
		t.Fatal("原地解密借用调用方内存，owned 应为 false")
	}
	if !bytes.Equal(sawMsg, plain) {
		t.Fatalf("got %q", sawMsg)
	}
}

func TestDeliver_AEADRoundTripAndOwned(t *testing.T) {
	enc, dec := pairGCM([16]byte{1, 2, 3})
	c := serverCodec(maxMessageSize)
	c.requireEncrypt = true

	plain := []byte("sealed payload")
	var h [maxHeaderSize]byte
	hn := putHeader(h[:], len(plain)+enc.Overhead(), flagE)
	body := enc.Seal(nil, plain, h[:hn])
	src := append(append([]byte{}, h[:hn]...), body...)

	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	var count int
	if err := c.deliver(f, dec, nil, func(msg []byte, owned bool) error {
		count++
		if !bytes.Equal(msg, plain) {
			t.Fatalf("got %q", msg)
		}
		if !owned {
			t.Fatal("AEAD 输出来自分级池，owned 应为 true")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal(count)
	}
}

// AAD：帧头翻任何一位（不改变帧定界的那些位），Open 必须失败。
func TestDeliver_AADHeaderFlipFailsOpen(t *testing.T) {
	key := [16]byte{9}
	plain := []byte("aad protected")

	// e 位之外再翻 z / c：这两位在 allow=z|c|e 的客户端策略下解析仍会通过，
	// 静默与否只能靠 AAD 认证兜住——这正是 W9 关掉的那类攻击。
	for _, flip := range []byte{flagZ, flagC} {
		enc, dec := pairGCM(key)
		var h [maxHeaderSize]byte
		hn := putHeader(h[:], len(plain)+enc.Overhead(), flagE)
		body := enc.Seal(nil, plain, h[:hn])

		tampered := append(append([]byte{}, h[:hn]...), body...)
		tampered[0] ^= flip

		c := clientCodec(maxMessageSize, maxMessageSize)
		c.requireEncrypt = true
		f, _, ok, err := c.parse(tampered)
		if !ok || err != nil {
			t.Fatalf("flip=%02x: parse 应通过（翻位不改变定界）, ok=%v err=%v", flip, ok, err)
		}
		zdec, _ := zstd.NewReader(nil)
		defer zdec.Close()
		err = c.deliver(f, dec, zdec, func([]byte, bool) error {
			t.Fatalf("flip=%02x: 篡改帧被投递", flip)
			return nil
		})
		if !errors.Is(err, ErrDecryptFailed) {
			t.Fatalf("flip=%02x: want ErrDecryptFailed, got %v", flip, err)
		}
	}
}

func TestDeliver_CiphertextShorterThanOverhead(t *testing.T) {
	_, dec := pairGCM([16]byte{7})
	c := serverCodec(maxMessageSize)
	c.requireEncrypt = true
	src := rawFrame(flagE, 4, false, []byte{1, 2, 3, 4}) // 4 < Overhead(16)
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err := c.deliver(f, dec, nil, nil); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("want ErrDecryptFailed, got %v", err)
	}
}

// ─── compound ───────────────────────────────────────────────────────────────

func compoundBody(msgs ...[]byte) []byte {
	var body []byte
	for _, m := range msgs {
		body = appendSubFrame(body, m)
	}
	return body
}

func TestDeliver_CompoundIterates(t *testing.T) {
	msgs := [][]byte{[]byte("a"), {}, mkPayload(4096), []byte("tail")} // 含空消息与 4 字节子头
	body := compoundBody(msgs...)
	c := clientCodec(maxMessageSize, maxMessageSize)
	src := rawFrame(flagC, len(body), len(body) >= smallSizeLimit, body)
	got := collect(t, c, src, nil, nil)
	if len(got) != len(msgs) {
		t.Fatalf("投递 %d 条, want %d", len(got), len(msgs))
	}
	for i := range msgs {
		if !bytes.Equal(got[i], msgs[i]) {
			t.Fatalf("第 %d 条不一致", i)
		}
	}
}

// 空消息在 compound 里的计量：每条空消息贡献 2 字节子头（W7 的 frameSize 口径）。
func TestDeliver_CompoundEmptyMessagesCost(t *testing.T) {
	body := compoundBody([]byte{}, []byte{}, []byte{})
	if len(body) != 3*frameSize(0) {
		t.Fatalf("3 条空消息 body=%d, want %d", len(body), 3*frameSize(0))
	}
}

func TestDeliver_CompoundTrailingBytes(t *testing.T) {
	body := append(compoundBody([]byte("ok")), 0x00) // 残留 1 字节（不足一个子头）
	c := clientCodec(maxMessageSize, maxMessageSize)
	src := rawFrame(flagC, len(body), false, body)
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	err = c.deliver(f, nil, nil, func([]byte, bool) error { return nil })
	if !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("want ErrTrailingBytes, got %v", err)
	}
}

// 子消息带任何标记位（包括嵌套 compound）都在校验阶段被拒：展开是迭代不是递归。
func TestDeliver_NestedCompoundRejected(t *testing.T) {
	inner := compoundBody([]byte("x"))
	var body []byte
	var h [maxHeaderSize]byte
	n := putHeader(h[:], len(inner), flagC) // 手工构造带 c 位的子帧
	body = append(append(body, h[:n]...), inner...)

	c := clientCodec(maxMessageSize, maxMessageSize)
	src := rawFrame(flagC, len(body), false, body)
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	err = c.deliver(f, nil, nil, func([]byte, bool) error { return nil })
	if !errors.Is(err, ErrFlagNotAllowed) {
		t.Fatalf("want ErrFlagNotAllowed, got %v", err)
	}
}

// sink 返回 error：同一批里剩下的消息不再投递（01 OnMessage 的硬语义）。
func TestDeliver_SinkErrorStopsBatch(t *testing.T) {
	body := compoundBody([]byte("1"), []byte("2"), []byte("3"))
	c := clientCodec(maxMessageSize, maxMessageSize)
	src := rawFrame(flagC, len(body), false, body)
	f, _, _, _ := c.parse(src)

	stop := errors.New("business reject")
	var delivered int
	err := c.deliver(f, nil, nil, func(msg []byte, _ bool) error {
		delivered++
		if delivered == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || delivered != 2 {
		t.Fatalf("delivered=%d err=%v", delivered, err)
	}
}

// ─── 解压两道闸 ─────────────────────────────────────────────────────────────

func TestDeliver_DecompressSecondGate(t *testing.T) {
	// 解码器故意放得很宽（模拟「上限因构造方式不同而失配」），
	// codec.maxDecompressed 这道闸必须独立兜住。
	zenc, _ := zstd.NewWriter(nil)
	defer zenc.Close()
	zdec, _ := zstd.NewReader(nil) // 未设 WithDecoderMaxMemory
	defer zdec.Close()

	blob := bytes.Repeat([]byte{'A'}, 1<<16) // 高度可压
	compressed := zenc.EncodeAll(blob, nil)

	c := clientCodec(maxMessageSize, 1<<10) // 只允许解出 1KB
	src := rawFrame(flagZ, len(compressed), len(compressed) >= smallSizeLimit, compressed)
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	err = c.deliver(f, nil, zdec, func([]byte, bool) error {
		t.Fatal("超限输出被投递")
		return nil
	})
	if !errors.Is(err, ErrDecompressLimit) {
		t.Fatalf("want ErrDecompressLimit, got %v", err)
	}
}

func TestDeliver_DecompressCorruptRejected(t *testing.T) {
	zdec, _ := zstd.NewReader(nil)
	defer zdec.Close()
	c := clientCodec(maxMessageSize, maxMessageSize)
	src := rawFrame(flagZ, 8, false, []byte("notzstd!"))
	f, _, ok, err := c.parse(src)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	err = c.deliver(f, nil, zdec, func([]byte, bool) error { return nil })
	if err == nil {
		t.Fatal("损坏的压缩流未被拒")
	}
}
