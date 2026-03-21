package common

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	MaskM byte = 1 << 7
	MaskZ byte = 1 << 6
	MaskC byte = 1 << 5
	MaskE byte = 1 << 4

	maskS          = uint16(4095)
	offset         = 1 << 16
	moreHeaderSize = 4096

	MaxMessageSize = 32 * 1024 * 1024
)

var ErrMaxMessageSize = errors.New("message size > 32MB")

type Frame struct {
	Payload   []byte
	Z         bool
	C         bool
	E         bool
	WireBytes int
}

type XORCipher struct {
	Key byte
}

func (c XORCipher) Encrypt(data []byte) {
	for i := range data {
		data[i] ^= c.Key
	}
}

func (c XORCipher) Decrypt(data []byte) {
	c.Encrypt(data)
}

func ParseHeader(b [2]byte) (size int, m, z, c, e bool) {
	m, z, c, e = b[0]&MaskM > 0, b[0]&MaskZ > 0, b[0]&MaskC > 0, b[0]&MaskE > 0
	size = int(binary.BigEndian.Uint16(b[:]) & maskS)
	return
}

func ParseHeader2(b [2]byte, size int) int {
	return int(binary.BigEndian.Uint16(b[:])) + size*offset
}

func EncodeHeader(b []byte, size int) int {
	if size < moreHeaderSize {
		binary.BigEndian.PutUint16(b[:2], uint16(size))
		return 2
	}
	binary.BigEndian.PutUint32(b[:4], uint32(size))
	b[0] |= MaskM
	return 4
}

func EncodedFrameSize(payloadLen int) int {
	if payloadLen < moreHeaderSize {
		return 2 + payloadLen
	}
	return 4 + payloadLen
}

func EncodeFrame(dst, payload []byte, flag byte) (int, error) {
	if len(payload) > MaxMessageSize {
		return 0, ErrMaxMessageSize
	}
	if len(dst) < EncodedFrameSize(len(payload)) {
		return 0, io.ErrShortBuffer
	}

	var header [4]byte
	n := EncodeHeader(header[:], len(payload))
	header[0] |= flag
	copy(dst[:n], header[:n])
	copy(dst[n:n+len(payload)], payload)
	return n + len(payload), nil
}

func BuildFrame(payload []byte, flag byte) []byte {
	frame := make([]byte, EncodedFrameSize(len(payload)))
	n, _ := EncodeFrame(frame, payload, flag)
	return frame[:n]
}

func ReadFrame(r io.Reader, maxSize int) (Frame, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:2]); err != nil {
		return Frame{}, err
	}

	size, m, z, c, e := ParseHeader([2]byte{header[0], header[1]})
	headSize := 2
	if m {
		if _, err := io.ReadFull(r, header[2:4]); err != nil {
			return Frame{}, err
		}
		size = ParseHeader2([2]byte{header[2], header[3]}, size)
		headSize = 4
	}
	if size > maxSize {
		return Frame{}, ErrMaxMessageSize
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}

	return Frame{
		Payload:   payload,
		Z:         z,
		C:         c,
		E:         e,
		WireBytes: headSize + size,
	}, nil
}

func ConsumeFrame(data []byte, maxSize int) (Frame, int, error) {
	if len(data) < 2 {
		return Frame{}, 0, io.ErrUnexpectedEOF
	}

	size, m, z, c, e := ParseHeader([2]byte{data[0], data[1]})
	headSize := 2
	if m {
		if len(data) < 4 {
			return Frame{}, 0, io.ErrUnexpectedEOF
		}
		size = ParseHeader2([2]byte{data[2], data[3]}, size)
		headSize = 4
	}
	if size > maxSize {
		return Frame{}, 0, ErrMaxMessageSize
	}
	if len(data) < headSize+size {
		return Frame{}, 0, io.ErrUnexpectedEOF
	}

	return Frame{
		Payload:   data[headSize : headSize+size],
		Z:         z,
		C:         c,
		E:         e,
		WireBytes: headSize + size,
	}, headSize + size, nil
}

func WriteAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
