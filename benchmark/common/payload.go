package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
)

const (
	PayloadMetaSize = 24

	payloadMagic = "GATE"
)

var payloadMagicBytes = [4]byte{'G', 'A', 'T', 'E'}

var ErrInvalidPayload = errors.New("invalid benchmark payload")

func BuildPayloadTemplate(size int, mode string, seed int64) ([]byte, error) {
	if size < PayloadMetaSize {
		return nil, ErrInvalidPayload
	}

	payload := make([]byte, size)
	copy(payload[:4], payloadMagicBytes[:])

	body := payload[PayloadMetaSize:]
	switch mode {
	case "repeat":
		fill := byte('a' + seed%26)
		for i := range body {
			body[i] = fill
		}
	case "random":
		rng := rand.New(rand.NewSource(seed))
		if _, err := rng.Read(body); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("payload mode must be repeat or random")
	}

	return payload, nil
}

func FillPayloadMeta(payload []byte, connID uint32, seq uint64, sentNano int64) {
	copy(payload[:4], payloadMagicBytes[:])
	binary.BigEndian.PutUint32(payload[4:8], connID)
	binary.BigEndian.PutUint64(payload[8:16], seq)
	binary.BigEndian.PutUint64(payload[16:24], uint64(sentNano))
}

func ParsePayloadMeta(payload []byte) (connID uint32, seq uint64, sentNano int64, err error) {
	if len(payload) < PayloadMetaSize {
		return 0, 0, 0, ErrInvalidPayload
	}
	if payload[0] != payloadMagicBytes[0] || payload[1] != payloadMagicBytes[1] || payload[2] != payloadMagicBytes[2] || payload[3] != payloadMagicBytes[3] {
		return 0, 0, 0, ErrInvalidPayload
	}
	connID = binary.BigEndian.Uint32(payload[4:8])
	seq = binary.BigEndian.Uint64(payload[8:16])
	sentNano = int64(binary.BigEndian.Uint64(payload[16:24]))
	return
}

func VerifyPayloadBody(payload, template []byte) bool {
	if len(payload) != len(template) {
		return false
	}
	return bytes.Equal(payload[PayloadMetaSize:], template[PayloadMetaSize:])
}
