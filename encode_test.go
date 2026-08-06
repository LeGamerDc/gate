package gate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func mustEncoder(t *testing.T, opts ...zstd.EOption) *zstd.Encoder {
	t.Helper()
	enc, err := zstd.NewWriter(nil, append(opts,
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = enc.Close() })
	return enc
}

func mustDecoder(t *testing.T) *zstd.Decoder {
	t.Helper()
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxMessageSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dec.Close)
	return dec
}

// wire 把 builtFrame 拼成线路字节并 release。
func wire(f *builtFrame) []byte {
	out := append(append([]byte{}, f.hdr[:f.hdrLen]...), f.body...)
	f.release()
	return out
}

func TestBuildFrame_PlainSmall(t *testing.T) {
	f, err := buildFrame([]byte("abc"), 0, nil, false, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	got := wire(&f)
	want := rawFrame(0, 3, false, []byte("abc"))
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
}

// 压完不比原来小就丢弃压缩结果：高熵输入必须走明文，z 位不置。
func TestBuildFrame_CompressionDiscardedWhenLarger(t *testing.T) {
	enc := mustEncoder(t)
	data := mkPayload(64) // mkPayload 的 i*7 序列基本不可压
	f, err := buildFrame(data, 0, enc, true, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if f.hdr[0]&flagZ != 0 {
		t.Fatal("高熵输入不该置 z 位")
	}
	if f.bodyPooled {
		t.Fatal("放弃压缩后 body 应指向原 data")
	}
	f.release()
}

func TestBuildFrame_CompressionKeptWhenSmaller(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	data := bytes.Repeat([]byte("gate!"), 400) // 高度可压
	f, err := buildFrame(data, 0, enc, true, nil, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if f.hdr[0]&flagZ == 0 || !f.bodyPooled {
		t.Fatalf("z=%v pooled=%v", f.hdr[0]&flagZ != 0, f.bodyPooled)
	}
	if f.wireSize() >= frameSize(len(data)) {
		t.Fatalf("压缩后 %d >= 原 %d", f.wireSize(), frameSize(len(data)))
	}
	src := wire(&f)
	c := clientCodec(maxMessageSize, maxMessageSize)
	got := collect(t, c, src, nil, dec)
	if len(got) != 1 || !bytes.Equal(got[0], data) {
		t.Fatal("压缩帧 round-trip 失败")
	}
}

// 编码顺序：先算最终长度 → 拼帧头 → Seal(dst, data, header)。
// 帧头写的长度必须是 seal 之后的线路字节数。
func TestBuildFrame_AEADLengthAndAAD(t *testing.T) {
	enc, dec := pairGCM([16]byte{42})
	plain := []byte("hello aead")
	f, err := buildFrame(plain, 0, nil, false, enc, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.body) != len(plain)+enc.Overhead() {
		t.Fatalf("body=%d want %d", len(f.body), len(plain)+enc.Overhead())
	}
	src := wire(&f)

	c := serverCodec(maxMessageSize)
	c.requireEncrypt = true
	got := collect(t, c, src, dec, nil)
	if len(got) != 1 || !bytes.Equal(got[0], plain) {
		t.Fatalf("got %q", got)
	}
}

// Overhead()==0 时原地覆写：body 指向传入的 data 本身。
func TestBuildFrame_InPlaceSeal(t *testing.T) {
	ci := &xorCipher{key: 0x77}
	data := []byte("mutate me")
	f, err := buildFrame(data, 0, nil, false, ci, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if &f.body[0] != &data[0] {
		t.Fatal("Overhead()==0 应原地覆写调用方缓冲")
	}
	if f.bodyPooled {
		t.Fatal("原地路径不该标 pooled")
	}
}

// compound 分支由调用方拼 body 后传入 flagC；子头计入 body 长度（frameSize 口径）。
func TestBuildFrame_CompoundRoundTrip(t *testing.T) {
	enc, dec := pairGCM([16]byte{5})
	zdec := mustDecoder(t)
	msgs := [][]byte{[]byte("alpha"), {}, mkPayload(300)}
	body := compoundBody(msgs...)
	f, err := buildFrame(body, flagC, nil, false, enc, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	src := wire(&f)

	c := clientCodec(maxMessageSize, maxMessageSize)
	c.requireEncrypt = true
	got := collect(t, c, src, dec, zdec)
	if len(got) != len(msgs) {
		t.Fatalf("%d msgs", len(got))
	}
	for i := range msgs {
		if !bytes.Equal(got[i], msgs[i]) {
			t.Fatalf("第 %d 条不一致", i)
		}
	}
}

func TestBuildFrame_TooLargeRejected(t *testing.T) {
	enc, _ := pairGCM([16]byte{1})
	data := mkPayload(100)
	// 100 + 16 > 110
	if _, err := buildFrame(data, 0, nil, false, enc, 110); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
	if _, err := buildFrame(data, 0, nil, false, nil, 99); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
}
