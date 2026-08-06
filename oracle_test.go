package gate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// oracle：按 02 的文字规格独立实现的解析器（T-D4）。
// 允许慢、允许分配，刻意不复用生产代码里的任何常量与函数——
// 它和生产 codec 的分歧就是 bug 信号，无论错在哪一边。

var (
	errOracleShort = errors.New("oracle: short input")
	errOracleProto = errors.New("oracle: protocol violation")
)

type oracleConfig struct {
	cipher    Cipher
	allowZ    bool
	allowC    bool
	allowE    bool
	maxSize   int
	maxDecomp int
}

// oracleNext 解出 src 头部的一个帧，返回展开后的消息列表与消耗字节数。
func oracleNext(src []byte, cfg oracleConfig) ([][]byte, int, error) {
	if len(src) < 2 {
		return nil, 0, errOracleShort
	}
	first := src[0]
	mBit := first&0x80 != 0
	zBit := first&0x40 != 0
	cBit := first&0x20 != 0
	eBit := first&0x10 != 0

	var size, hdrLen int
	if mBit {
		if len(src) < 4 {
			return nil, 0, errOracleShort
		}
		size = int(binary.BigEndian.Uint32(src[:4]) & 0x0FFFFFFF)
		hdrLen = 4
		if size < 4096 {
			return nil, 0, fmt.Errorf("%w: non-canonical", errOracleProto)
		}
	} else {
		size = int(binary.BigEndian.Uint16(src[:2]) & 0x0FFF)
		hdrLen = 2
	}
	if size > cfg.maxSize {
		return nil, 0, fmt.Errorf("%w: oversize", errOracleProto)
	}
	if (zBit && !cfg.allowZ) || (cBit && !cfg.allowC) || (eBit && !cfg.allowE) {
		return nil, 0, fmt.Errorf("%w: flag not allowed", errOracleProto)
	}
	if hasCipher := cfg.cipher != nil; eBit != hasCipher {
		return nil, 0, fmt.Errorf("%w: cipher/e mismatch", errOracleProto)
	}
	if len(src) < hdrLen+size {
		return nil, 0, errOracleShort
	}
	header := append([]byte(nil), src[:hdrLen]...)
	data := append([]byte(nil), src[hdrLen:hdrLen+size]...)

	if eBit {
		plain, err := cfg.cipher.Open(nil, data, header)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: open: %w", errOracleProto, err)
		}
		data = plain
	}
	if zBit {
		// 刻意走 io.Reader 流式路径，与生产侧的 DecodeAll 不同代码路径。
		zr, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, 0, fmt.Errorf("%w: zstd: %w", errOracleProto, err)
		}
		out, err := io.ReadAll(io.LimitReader(zr, int64(cfg.maxDecomp)+1))
		zr.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("%w: decompress: %w", errOracleProto, err)
		}
		if len(out) > cfg.maxDecomp {
			return nil, 0, fmt.Errorf("%w: decompress limit", errOracleProto)
		}
		data = out
	}

	if !cBit {
		return [][]byte{data}, hdrLen + size, nil
	}
	var msgs [][]byte
	for len(data) > 0 {
		sub, n, err := oracleNext(data, oracleConfig{maxSize: cfg.maxSize})
		if err != nil {
			return nil, 0, fmt.Errorf("%w: compound: %w", errOracleProto, err)
		}
		msgs = append(msgs, sub...)
		data = data[n:]
	}
	return msgs, hdrLen + size, nil
}

// oracleDecodeAll 解完整个流，要求不剩任何残字节。
func oracleDecodeAll(src []byte, cfg oracleConfig) ([][]byte, error) {
	var msgs [][]byte
	for len(src) > 0 {
		m, n, err := oracleNext(src, cfg)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m...)
		src = src[n:]
	}
	return msgs, nil
}
