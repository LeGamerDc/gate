package gate

import (
	"bytes"
	"testing"

	"github.com/gobwas/ws"
)

var (
	benchHeaderSink int
	benchFrameSink  frame
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

type benchConnHandler struct {
	total int
}

func (h *benchConnHandler) Handle(raw []byte) {
	h.total += len(raw)
}

func (h *benchConnHandler) Close() {}

func BenchmarkEncodeHeader(b *testing.B) {
	for _, size := range []int{128, 8 * 1024} {
		b.Run(headerBenchmarkName(size), func(b *testing.B) {
			var header [4]byte
			b.ReportAllocs()
			for b.Loop() {
				benchHeaderSink = encodeHeader(header[:], size)
			}
		})
	}
}

// 协议解析现在只有 codec 一个入口，基准也跟着收敛到它身上。
func BenchmarkCodecParse(b *testing.B) {
	payload := bytes.Repeat([]byte("a"), 1024)
	wire := mkFrame(payload, 0)
	c := serverCodec(maxMessageSize)

	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		benchFrameSink, benchHeaderSink, ok, benchErrSink = c.parse(wire)
		if benchErrSink != nil || !ok {
			b.Fatalf("parse: ok=%v err=%v", ok, benchErrSink)
		}
	}
}

func BenchmarkCodecReadFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("b"), 8*1024)
	wire := mkFrame(payload, 0)
	c := clientCodec(maxMessageSize, maxMessageSize)

	b.ReportAllocs()
	for b.Loop() {
		r := bytes.NewReader(wire)
		benchFrameSink, benchErrSink = c.readFrame(r)
		if benchErrSink != nil {
			b.Fatal(benchErrSink)
		}
	}
}

func BenchmarkCodecDeliver(b *testing.B) {
	for _, tc := range []struct {
		name   string
		frame  frame
		cipher Cipher
	}{
		{
			name:  "plain",
			frame: frame{payload: bytes.Repeat([]byte("p"), 256)},
		},
		{
			name: "encrypt",
			frame: func() frame {
				payload := bytes.Repeat([]byte("e"), 1024)
				xorCipher{key: 0x33}.Encrypt(payload)
				return frame{payload: payload, e: true}
			}(),
			cipher: xorCipher{key: 0x33},
		},
		{
			name: "compress",
			frame: frame{
				payload: enc.EncodeAll(bytes.Repeat([]byte("z"), 8*1024), nil),
				z:       true,
			},
		},
		{
			name: "compound_compress_encrypt",
			frame: func() frame {
				payload := buildCompoundPayload(
					bytes.Repeat([]byte("a"), 128),
					bytes.Repeat([]byte("b"), 256),
					bytes.Repeat([]byte("c"), 512),
				)
				payload = enc.EncodeAll(payload, nil)
				xorCipher{key: 0x66}.Encrypt(payload)
				return frame{payload: payload, z: true, c: true, e: true}
			}(),
			cipher: xorCipher{key: 0x66},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := clientCodec(maxMessageSize, maxMessageSize)
			dec, err := newZstdDecoder(maxMessageSize)
			if err != nil {
				b.Fatal(err)
			}
			defer dec.Close()

			handler := &benchClientHandler{}
			sink := func(msg []byte, _ bool) error {
				handler.total += len(msg)
				return nil
			}

			b.ReportAllocs()
			for b.Loop() {
				// deliver 会就地解密，所以每轮都要给一份新的 payload。
				f := tc.frame
				f.payload = append([]byte(nil), tc.frame.payload...)
				if err := c.deliver(f, tc.cipher, dec, sink); err != nil {
					b.Fatal(err)
				}
			}
			benchByteSink += handler.total
		})
	}
}

func BenchmarkWebSocketDecodeMessages(b *testing.B) {
	for _, tc := range []struct {
		name   string
		frames [][]byte
	}{
		{
			name: "binary",
			frames: [][]byte{
				mkFrame(bytes.Repeat([]byte("w"), 1024), 0),
			},
		},
		{
			name: "fragmented_binary",
			frames: [][]byte{
				mkFrame(bytes.Repeat([]byte("f"), 1024), 0)[:128],
				mkFrame(bytes.Repeat([]byte("f"), 1024), 0)[128:],
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			wire := buildWebSocketWireFrames(b, tc.frames...)
			handler := &benchConnHandler{}
			conn := &Conn{handler: handler, codec: serverCodec(maxMessageSize)}
			conn.init()
			conn.markOpen()
			state := &wsConnState{conn: conn}

			b.ReportAllocs()
			for b.Loop() {
				handler.total = 0
				state.buf.Reset()
				state.gateBuf.Reset()
				state.fragmentBuf.reset()
				_, _ = state.buf.Write(wire)
				if err := state.decodeMessages(); err != nil {
					b.Fatal(err)
				}
				benchByteSink += handler.total + state.gateBuf.Len()
			}
		})
	}
}

func headerBenchmarkName(size int) string {
	if size < moreHeaderSize {
		return "short_header"
	}
	return "extended_header"
}

func buildWebSocketWireFrames(b *testing.B, frames ...[]byte) []byte {
	b.Helper()

	var wire bytes.Buffer
	for i, payload := range frames {
		op := ws.OpBinary
		fin := true
		if len(frames) > 1 {
			if i == 0 {
				fin = false
			} else {
				op = ws.OpContinuation
			}
			if i != len(frames)-1 {
				fin = false
			}
		}
		frame := ws.MaskFrame(ws.NewFrame(op, fin, payload))
		if err := ws.WriteFrame(&wire, frame); err != nil {
			b.Fatal(err)
		}
	}
	return wire.Bytes()
}
