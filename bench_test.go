package gate

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

var (
	benchHeaderSink int
	benchFrameSink  inboundFrame
	benchErrSink    error
	benchByteSink   int
)

type benchClientHandler struct {
	total int
}

func (h *benchClientHandler) OnConnect(*Client) {}

func (h *benchClientHandler) OnMessage(_ *Client, msg []byte) {
	h.total += len(msg)
}

func (h *benchClientHandler) OnClose(*Client, error) {}

func BenchmarkEncodeHeader(b *testing.B) {
	for _, size := range []int{128, 8 * 1024} {
		b.Run(headerBenchmarkName(size), func(b *testing.B) {
			var header [4]byte
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchHeaderSink = encodeHeader(header[:], size)
			}
		})
	}
}

func BenchmarkConsumeFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("a"), 1024)
	frame := buildFrame(payload, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchFrameSink, benchHeaderSink, benchErrSink = consumeFrame(frame, maxMessageSize)
		if benchErrSink != nil {
			b.Fatal(benchErrSink)
		}
	}
}

func BenchmarkReadFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("b"), 8*1024)
	frame := buildFrame(payload, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(frame)
		benchFrameSink, benchErrSink = readFrame(r, maxMessageSize)
		if benchErrSink != nil {
			b.Fatal(benchErrSink)
		}
	}
}

func BenchmarkClientEncodeOutbound(b *testing.B) {
	for _, tc := range []struct {
		name    string
		payload []byte
		cipher  Cipher
	}{
		{name: "plain", payload: bytes.Repeat([]byte("x"), 256)},
		{name: "encrypt", payload: bytes.Repeat([]byte("y"), 1024), cipher: xorCipher{key: 0x5a}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			client := &Client{
				maxMessageSize: maxMessageSize,
				cipher:         tc.cipher,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := outboundMsg{
					data:    append([]byte(nil), tc.payload...),
					encrypt: tc.cipher != nil,
				}
				frame, err := client.encodeOutbound(msg)
				if err != nil {
					b.Fatal(err)
				}
				benchByteSink += len(frame)
			}
		})
	}
}

func BenchmarkClientHandleFrame(b *testing.B) {
	for _, tc := range []struct {
		name   string
		frame  inboundFrame
		cipher Cipher
	}{
		{
			name:  "plain",
			frame: inboundFrame{payload: bytes.Repeat([]byte("p"), 256)},
		},
		{
			name: "encrypt",
			frame: func() inboundFrame {
				payload := bytes.Repeat([]byte("e"), 1024)
				cipher := xorCipher{key: 0x33}
				cipher.Encrypt(payload)
				return inboundFrame{payload: payload, e: true}
			}(),
			cipher: xorCipher{key: 0x33},
		},
		{
			name: "compress",
			frame: inboundFrame{
				payload: enc.EncodeAll(bytes.Repeat([]byte("z"), 8*1024), nil),
				z:       true,
			},
		},
		{
			name: "compound_compress_encrypt",
			frame: func() inboundFrame {
				msg1 := bytes.Repeat([]byte("a"), 128)
				msg2 := bytes.Repeat([]byte("b"), 256)
				msg3 := bytes.Repeat([]byte("c"), 512)
				payload := buildCompoundPayload(msg1, msg2, msg3)
				payload = enc.EncodeAll(payload, nil)
				cipher := xorCipher{key: 0x66}
				cipher.Encrypt(payload)
				return inboundFrame{payload: payload, z: true, c: true, e: true}
			}(),
			cipher: xorCipher{key: 0x66},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer dec.Close()

			handler := &benchClientHandler{}
			client := &Client{
				handler:        handler,
				maxMessageSize: maxMessageSize,
				dec:            dec,
				cipher:         tc.cipher,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				frame := tc.frame
				frame.payload = append([]byte(nil), tc.frame.payload...)
				if err := client.handleFrame(frame); err != nil {
					b.Fatal(err)
				}
			}
			benchByteSink += handler.total
		})
	}
}

func headerBenchmarkName(size int) string {
	if size < moreHeaderSize {
		return "short_header"
	}
	return "extended_header"
}
