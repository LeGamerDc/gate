package gate

import (
	"errors"
	"io"
)

func consumeServerMessage(data []byte, cipher Cipher) (payload []byte, n int, ok bool, err error) {
	frame, n, err := consumeFrame(data, maxMessageSize)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	if frame.e && cipher != nil {
		cipher.Decrypt(frame.payload)
	}
	return frame.payload, n, true, nil
}
