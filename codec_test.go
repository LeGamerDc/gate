package gate

import (
	"bytes"
	"errors"
	"testing"
)

// mkFrame 拼一个线路帧，供测试构造任意（含非法）输入。
func mkFrame(payload []byte, flags byte) []byte {
	var hdr [4]byte
	n := encodeHeader(hdr[:], len(payload))
	hdr[0] |= flags
	out := make([]byte, 0, n+len(payload))
	out = append(out, hdr[:n]...)
	return append(out, payload...)
}

func collect(t *testing.T, c codec, f frame, cipher Cipher) ([][]byte, error) {
	t.Helper()
	dec, err := newZstdDecoder(c.maxDecompressed)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	var got [][]byte
	err = c.deliver(f, cipher, dec, func(msg []byte, _ bool) error {
		got = append(got, append([]byte(nil), msg...))
		return nil
	})
	return got, err
}

func TestCodecRejectsFlagsByPolicy(t *testing.T) {
	tests := []struct {
		name  string
		codec codec
		flags byte
		want  error
	}{
		{"server rejects compressed uplink", serverCodec(maxMessageSize), maskZ, ErrFlagNotAllowed},
		{"server rejects compound uplink", serverCodec(maxMessageSize), maskC, ErrFlagNotAllowed},
		{"server accepts encrypted uplink", serverCodec(maxMessageSize), maskE, nil},
		{"server accepts plain uplink", serverCodec(maxMessageSize), 0, nil},
		{"client accepts compressed", clientCodec(maxMessageSize, maxMessageSize), maskZ, nil},
		{"client accepts compound", clientCodec(maxMessageSize, maxMessageSize), maskC, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := mkFrame([]byte("payload"), tt.flags)
			_, _, _, err := tt.codec.parse(src)
			if !errors.Is(err, tt.want) {
				t.Fatalf("parse err = %v, want %v", err, tt.want)
			}
		})
	}
}

// 这是旧实现里 client 无限递归的那条路径：compound 的子消息又是 compound。
func TestCodecRejectsNestedCompound(t *testing.T) {
	inner := mkFrame([]byte("x"), 0)
	nested := mkFrame(inner, maskC) // 子消息带 c 标记
	top := mkFrame(nested, maskC)

	c := clientCodec(maxMessageSize, maxMessageSize)
	f, _, ok, err := c.parse(top)
	if err != nil || !ok {
		t.Fatalf("parse top: err=%v ok=%v", err, ok)
	}
	if _, err = collect(t, c, f, nil); !errors.Is(err, ErrFlagNotAllowed) {
		t.Fatalf("deliver err = %v, want ErrFlagNotAllowed", err)
	}
}

func TestCodecRejectsFlaggedSubFrames(t *testing.T) {
	for name, flag := range map[string]byte{"z": maskZ, "c": maskC, "e": maskE} {
		t.Run(name, func(t *testing.T) {
			body := mkFrame([]byte("sub"), flag)
			top := mkFrame(body, maskC)
			c := clientCodec(maxMessageSize, maxMessageSize)
			f, _, _, err := c.parse(top)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = collect(t, c, f, nil); !errors.Is(err, ErrFlagNotAllowed) {
				t.Fatalf("deliver err = %v, want ErrFlagNotAllowed", err)
			}
		})
	}
}

func TestCodecExpandsWellFormedCompound(t *testing.T) {
	want := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	var body []byte
	for _, m := range want {
		body = append(body, mkFrame(m, 0)...)
	}
	top := mkFrame(body, maskC)

	c := clientCodec(maxMessageSize, maxMessageSize)
	f, n, ok, err := c.parse(top)
	if err != nil || !ok {
		t.Fatalf("parse: err=%v ok=%v", err, ok)
	}
	if n != len(top) {
		t.Fatalf("consumed %d, want %d", n, len(top))
	}
	got, err := collect(t, c, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("msg[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCodecRejectsTruncatedCompound(t *testing.T) {
	body := append(mkFrame([]byte("ok"), 0), 0x00, 0x09) // 声称 9 字节但没有内容
	top := mkFrame(body, maskC)
	c := clientCodec(maxMessageSize, maxMessageSize)
	f, _, _, err := c.parse(top)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = collect(t, c, f, nil); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("deliver err = %v, want ErrTrailingBytes", err)
	}
}

// 旧实现里 8KB 的帧能解出 256MB。
func TestCodecCapsDecompression(t *testing.T) {
	const limit = 1 << 20
	bomb := enc.EncodeAll(make([]byte, 64<<20), nil)
	t.Logf("wire frame = %d bytes, decompresses to %d bytes", len(bomb), 64<<20)

	c := clientCodec(maxMessageSize, limit)
	_, err := collect(t, c, frame{payload: bomb, z: true}, nil)
	if err == nil {
		t.Fatal("expected the decompression bomb to be rejected")
	}
	t.Logf("rejected with: %v", err)
}

func TestCodecRejectsOversizeBeforeAllocating(t *testing.T) {
	c := serverCodec(1024)
	// 4 字节头声称 100000 字节，但后面一个字节都没有
	var hdr [4]byte
	encodeHeader(hdr[:], 100000)
	if _, _, _, err := c.parse(hdr[:]); !errors.Is(err, ErrMaxMessageSize) {
		t.Fatalf("parse err = %v, want ErrMaxMessageSize", err)
	}
	if _, err := c.readFrame(bytes.NewReader(hdr[:])); !errors.Is(err, ErrMaxMessageSize) {
		t.Fatalf("readFrame err = %v, want ErrMaxMessageSize", err)
	}
}

func TestCodecPartialFrameIsRetryable(t *testing.T) {
	full := mkFrame([]byte("hello world"), 0)
	c := serverCodec(maxMessageSize)
	for i := range full {
		f, n, ok, err := c.parse(full[:i])
		if err != nil {
			t.Fatalf("prefix len %d: unexpected err %v", i, err)
		}
		if ok {
			t.Fatalf("prefix len %d: parsed a frame from an incomplete buffer (n=%d, %q)", i, n, f.payload)
		}
	}
	if _, n, ok, err := c.parse(full); !ok || err != nil || n != len(full) {
		t.Fatalf("full frame: ok=%v err=%v n=%d", ok, err, n)
	}
}

func TestCodecEncryptedFrameWithoutCipher(t *testing.T) {
	c := serverCodec(maxMessageSize)
	f, _, _, err := c.parse(mkFrame([]byte("secret"), maskE))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = collect(t, c, f, nil); !errors.Is(err, ErrCipherRequired) {
		t.Fatalf("deliver err = %v, want ErrCipherRequired", err)
	}
}

func TestCodecHeaderRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 65535, 65536, 100000, maxMessageSize} {
		var b [4]byte
		n := encodeHeader(b[:], size)
		got, m, _, _, _ := parseHeader([2]byte{b[0], b[1]})
		if m {
			got = parseHeader2([2]byte{b[2], b[3]}, got)
		}
		if got != size {
			t.Errorf("size=%d n=%d round-tripped to %d (raw % x)", size, n, got, b[:n])
		}
	}
}

// FuzzCodec 是把三处解析合并成一个 codec 的主要收益：
// 协议层现在有单一入口，可以被持续性地随机化验证。
//
// 不变式：parse 对任意输入都必须终止、不 panic、不越界；成功时 n 必须落在
// (0, len(data)] 区间内；deliver 必须在有限步内返回。
func FuzzCodec(f *testing.F) {
	f.Add(mkFrame([]byte("hello"), 0))
	f.Add(mkFrame([]byte("hello"), maskE))
	f.Add(mkFrame(mkFrame([]byte("sub"), 0), maskC))
	f.Add(mkFrame(mkFrame(mkFrame([]byte("x"), 0), maskC), maskC))
	f.Add(mkFrame(enc.EncodeAll([]byte("compress me"), nil), maskZ))
	f.Add(mkFrame(make([]byte, 5000), 0))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x80, 0x00})

	dec, err := newZstdDecoder(1 << 20)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(dec.Close)

	codecs := map[string]codec{
		"server": serverCodec(1 << 16),
		"client": clientCodec(1<<16, 1<<20),
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		for name, c := range codecs {
			fr, n, ok, err := c.parse(data)
			if err != nil {
				continue
			}
			if !ok {
				if n != 0 {
					t.Fatalf("%s: incomplete parse returned n=%d", name, n)
				}
				continue
			}
			if n <= 0 || n > len(data) {
				t.Fatalf("%s: parse consumed %d of %d bytes", name, n, len(data))
			}
			if len(fr.payload) > c.maxMessage {
				t.Fatalf("%s: payload %d exceeds maxMessage %d", name, len(fr.payload), c.maxMessage)
			}
			// deliver 允许返回错误，但绝不允许 panic 或不终止。
			total := 0
			_ = c.deliver(fr, nopFuzzCipher{}, dec, func(msg []byte, _ bool) error {
				total += len(msg)
				if total > c.maxDecompressed+c.maxMessage {
					t.Fatalf("%s: delivered %d bytes, beyond any configured limit", name, total)
				}
				return nil
			})
		}
	})
}

type nopFuzzCipher struct{}

func (nopFuzzCipher) Encrypt([]byte) {}
func (nopFuzzCipher) Decrypt([]byte) {}
