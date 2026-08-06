package gate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// ─── FuzzCodecParse：任意字节 → 不 panic、不越界、错误分类只有两种 ────────────

func FuzzCodecParse(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x80, 0x00, 0x10, 0x00})
	f.Add(rawFrame(0, 5, false, []byte("hello")))
	f.Add(rawFrame(flagE, 20, false, mkPayload(20)))
	f.Add(rawFrame(flagZ|flagC, 8, false, mkPayload(8)))
	f.Add(rawFrame(0, 4096, true, mkPayload(4096)))

	zdec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(1<<20))
	if err != nil {
		f.Fatal(err)
	}
	defer zdec.Close()

	codecs := []codec{
		serverCodec(1 << 16),
		clientCodec(1<<16, 1<<16),
		{maxMessage: 1 << 16, allow: flagAll, requireEncrypt: true},
	}
	xor := &xorCipher{key: 0x3c} // Open 永不失败，让 deliver 的 z/c 分支吃到垃圾输入

	f.Fuzz(func(t *testing.T, src []byte) {
		for i, c := range codecs {
			fr, n, ok, err := c.parse(src)
			if ok && err != nil {
				t.Fatalf("codec[%d]: ok 与 err 同时为真", i)
			}
			if !ok {
				continue
			}
			if n < minHeaderSize || n > len(src) {
				t.Fatalf("codec[%d]: n=%d 越界", i, n)
			}
			if len(fr.payload) > c.maxMessage || cap(fr.payload) != len(fr.payload) {
				t.Fatalf("codec[%d]: payload 长度/cap 违规", i)
			}
			var ci Cipher
			if c.requireEncrypt {
				ci = xor
			}
			payload := append([]byte(nil), fr.payload...) // deliver 可能原地覆写
			fr.payload = payload[:len(payload):len(payload)]
			_ = c.deliver(fr, ci, zdec, func(msg []byte, _ bool) error {
				_ = msg
				return nil
			})
		}
	})
}

// ─── FuzzRoundTrip：fuzz 派生配置 + 消息序列，双侧编解码 + 独立 oracle ────────

// takeMsgs 从 fuzz 输入派生一串消息（长度前缀 + 重复填充），总量有上界。
func takeMsgs(data []byte) [][]byte {
	var msgs [][]byte
	total := 0
	for len(data) >= 2 && len(msgs) < 24 && total < 96<<10 {
		n := int(data[0])<<8 | int(data[1])
		n %= 8 << 10 // 单条 ≤ 8KB
		data = data[2:]
		m := make([]byte, n)
		if len(data) > 0 {
			for i := range m {
				m[i] = data[i%len(data)]
			}
		}
		if len(data) > 4 {
			data = data[4:]
		}
		msgs = append(msgs, m)
		total += n
	}
	return msgs
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(byte(0), []byte("\x00\x05hello\x00\x00\x00\x03abc"))
	f.Add(byte(1), []byte("\x00\x40aaaaaaaa\x00\x40bbbbbbbb"))
	f.Add(byte(2), bytes.Repeat([]byte("\x01\x00zzzz"), 8))
	f.Add(byte(7), bytes.Repeat([]byte("\x20\x00gate"), 4))

	f.Fuzz(func(t *testing.T, cfg byte, raw []byte) {
		const limit = 1 << 20
		msgs := takeMsgs(raw)
		if len(msgs) == 0 {
			return
		}

		cipherKind := cfg & 0x03      // 0/3=无, 1=xor, 2=gcm
		compress := cfg&0x04 != 0     // 压缩阈值 64B
		clusterN := int(cfg>>3) & 0x3 // 每组子消息数-1：0 ⇒ 全部独立帧

		var sender, prodRecv, oracleRecv Cipher
		switch cipherKind {
		case 1:
			sender, prodRecv, oracleRecv = &xorCipher{key: 9}, &xorCipher{key: 9}, &xorCipher{key: 9}
		case 2:
			key := [16]byte{cfg, 1, 2}
			s, r1 := pairGCM(key)
			_, r2 := pairGCM(key)
			sender, prodRecv, oracleRecv = s, r1, r2
		}

		zenc, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			t.Fatal(err)
		}
		defer zenc.Close()
		zdec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(limit))
		if err != nil {
			t.Fatal(err)
		}
		defer zdec.Close()

		// ── 编码（生产 buildFrame / appendSubFrame）──
		var stream []byte
		queue := msgs
		for len(queue) > 0 {
			group := 1
			if clusterN > 0 && len(queue) >= 2 {
				group = min(clusterN+1, len(queue))
			}
			var bf builtFrame
			var err error
			if group == 1 {
				m := queue[0]
				data := append([]byte(nil), m...) // buildFrame 可能原地覆写，喂副本
				bf, err = buildFrame(data, 0, zenc, compress && len(m) > 64, sender, limit)
			} else {
				var body []byte
				for _, m := range queue[:group] {
					body = appendSubFrame(body, m)
				}
				bf, err = buildFrame(body, flagC, zenc, compress && len(body) > 64, sender, limit)
			}
			if err != nil {
				t.Fatal(err)
			}
			stream = append(append(stream, bf.hdr[:bf.hdrLen]...), bf.body...)
			bf.release()
			queue = queue[group:]
		}

		// ── 解码 A：生产 codec ──
		c := clientCodec(limit, limit)
		c.requireEncrypt = sender != nil
		var got [][]byte
		rest := stream
		for len(rest) > 0 {
			_, n, ok, err := c.parse(rest)
			if err != nil || !ok {
				t.Fatalf("生产 codec 解自家编码失败: ok=%v err=%v", ok, err)
			}
			// deliver 可能原地覆写（xor 路径），喂副本以免污染 oracle 的输入。
			cp := append([]byte(nil), rest[:n]...)
			fr2, _, _, _ := c.parse(cp)
			if err := c.deliver(fr2, prodRecv, zdec, func(msg []byte, _ bool) error {
				got = append(got, append([]byte(nil), msg...))
				return nil
			}); err != nil {
				t.Fatalf("deliver: %v", err)
			}
			rest = rest[n:]
		}

		// ── 解码 B：独立 oracle ──
		want, err := oracleDecodeAll(stream, oracleConfig{
			cipher: oracleRecv, allowZ: true, allowC: true, allowE: oracleRecv != nil,
			maxSize: limit, maxDecomp: limit,
		})
		if err != nil {
			t.Fatalf("oracle 解生产编码失败: %v", err)
		}

		if len(got) != len(msgs) || len(want) != len(msgs) {
			t.Fatalf("条数: prod=%d oracle=%d want=%d", len(got), len(want), len(msgs))
		}
		for i := range msgs {
			if !bytes.Equal(got[i], msgs[i]) {
				t.Fatalf("生产解码第 %d 条不一致", i)
			}
			if !bytes.Equal(want[i], msgs[i]) {
				t.Fatalf("oracle 第 %d 条不一致", i)
			}
		}

		// ── AAD：篡改帧头任何一位，AEAD 下必须失败，绝不投递原文 ──
		if cipherKind == 2 && len(stream) >= 2 {
			hdrLen := 2
			if stream[0]&flagM != 0 {
				hdrLen = 4
			}
			bit := int(cfg) % (hdrLen * 8)
			tampered := append([]byte(nil), stream...)
			tampered[bit/8] ^= 1 << (bit % 8)

			_, r3 := pairGCM([16]byte{cfg, 1, 2})
			tc := clientCodec(limit, limit)
			tc.requireEncrypt = true
			fr, _, ok, err := tc.parse(tampered)
			if err == nil && ok {
				err = tc.deliver(fr, r3, zdec, func(msg []byte, _ bool) error {
					if bytes.Equal(msg, msgs[0]) {
						t.Fatal("帧头被翻位后仍投递了原始消息")
					}
					return nil
				})
				if err == nil {
					t.Fatal("帧头被翻位后 deliver 竟然成功")
				}
			}
		}
	})
}

// ─── 覆盖自证：线路上每类帧都必须出现过，任何一类为 0 直接失败 ────────────────

func TestWireCoverageSelfProof(t *testing.T) {
	zenc, _ := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	defer zenc.Close()

	var nZ, nC, nE, nM, nBig int
	scan := func(stream []byte) {
		c := clientCodec(maxMessageSize, maxMessageSize)
		for len(stream) > 0 {
			b0 := stream[0]
			// 只看帧头计数，不投递（cipher/e 组合各测各的，这里数分类）。
			cc := c
			cc.requireEncrypt = b0&flagE != 0
			fr, n, ok, err := cc.parse(stream)
			if !ok || err != nil {
				t.Fatalf("scan: ok=%v err=%v", ok, err)
			}
			if fr.z() {
				nZ++
			}
			if fr.c() {
				nC++
			}
			if fr.e() {
				nE++
			}
			if b0&flagM != 0 {
				nM++
			}
			if len(fr.payload) > 64<<10 {
				nBig++
			}
			stream = stream[n:]
		}
	}

	sender, _ := pairGCM([16]byte{11})

	// 场景 1：可压大消息 + 加密 → z、e、m
	big := bytes.Repeat([]byte("swordfish"), 20000) // 180KB，> 64KB rbuf
	bf, err := buildFrame(append([]byte(nil), big...), 0, zenc, true, sender, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	scan(wire(&bf))

	// 场景 2：小消息合包（不压不加密）→ c
	body := compoundBody([]byte("a"), []byte("bb"), []byte("ccc"))
	bf, err = buildFrame(body, flagC, nil, false, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	scan(wire(&bf))

	// 场景 3：高熵大帧（放弃压缩）直发 → m、大帧。
	// xorshift 伪随机字节保证 zstd 压不动，压缩结果被丢弃。
	raw := make([]byte, 100<<10)
	s := uint64(0x9E3779B97F4A7C15)
	for i := range raw {
		s ^= s << 13
		s ^= s >> 7
		s ^= s << 17
		raw[i] = byte(s)
	}
	bf, err = buildFrame(raw, 0, zenc, true, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	scan(wire(&bf))

	for name, n := range map[string]int{"z": nZ, "c": nC, "e": nE, "m4": nM, "big": nBig} {
		if n == 0 {
			t.Errorf("线路覆盖自证失败：%s 类帧计数为 0", name)
		}
	}
}

// 错误分类封闭性：parse 的所有错误值都在 02 的错误表内。
func TestParseErrorTaxonomyClosed(t *testing.T) {
	known := []error{
		ErrMaxMessageSize, ErrNonCanonicalHeader, ErrFlagNotAllowed,
		ErrCipherRequired, ErrCipherUnavailable,
	}
	inputs := [][]byte{
		rawFrame(0, 100, true, mkPayload(100)),   // 非规范
		rawFrame(0, 1<<20, true, nil),            // 超长
		rawFrame(flagZ, 3, false, []byte("abc")), // 服务端不许 z
		rawFrame(0, 3, false, []byte("abc")),     // requireEncrypt 但 e=0
		rawFrame(flagE, 3, false, []byte("abc")), // 无 cipher 但 e=1
		{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},     // 各位全置
	}
	codecs := []codec{
		serverCodec(1 << 10),
		{maxMessage: 1 << 10, allow: flagE, requireEncrypt: true},
	}
	for _, src := range inputs {
		for _, c := range codecs {
			_, _, ok, err := c.parse(src)
			if ok || err == nil {
				continue
			}
			matched := false
			for _, k := range known {
				if errors.Is(err, k) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("parse 返回了错误表之外的错误: %v (src=%x)", err, src)
			}
		}
	}
}
