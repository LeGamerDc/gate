package gate

import (
	"encoding/binary"

	"github.com/gobwas/ws"
)

// parseWSHeader 从 data 头部解析一个 WebSocket 帧头，返回帧头和它占用的字节数。
//
// ok == false 且 err == nil 是唯一的"可重试"返回：data 还不足以放下一个完整的
// 帧头，调用方应当等更多数据。err != nil 表示帧头本身违反协议。
//
// 这是 ws.ReadHeader 的等价实现，唯一的区别是它直接吃 []byte。ws.ReadHeader 只
// 接受 io.Reader，于是每个帧都要包一个 bytes.NewReader（指针逃逸进接口，一次
// 分配），而它内部还要 make 一块 12 字节的暂存区（第二次分配）。实测 WebSocket
// 入站路径上每帧固定 2 次分配、64 字节，全部来自这两处——一条 64 字节消息的
// 处理成本里，光是"为了读 2~14 字节帧头"就占了约 30ns。
//
// 帧头的字节布局（RFC 6455 §5.2）：
//
//	byte0: FIN(1) RSV(3) OPCODE(4)
//	byte1: MASK(1) LEN(7)
//	LEN==126 -> 随后 2 字节大端长度；LEN==127 -> 随后 8 字节大端长度
//	MASK==1  -> 长度之后再跟 4 字节掩码
func parseWSHeader(data []byte) (h ws.Header, n int, ok bool, err error) {
	if len(data) < ws.MinHeaderSize {
		return h, 0, false, nil
	}

	b0, b1 := data[0], data[1]
	h.Fin = b0&0x80 != 0
	h.Rsv = (b0 & 0x70) >> 4
	h.OpCode = ws.OpCode(b0 & 0x0f)
	h.Masked = b1&0x80 != 0

	n = ws.MinHeaderSize
	switch length := b1 & 0x7f; {
	case length < 126:
		h.Length = int64(length)
	case length == 126:
		if len(data) < n+2 {
			return ws.Header{}, 0, false, nil
		}
		h.Length = int64(binary.BigEndian.Uint16(data[n : n+2]))
		n += 2
	default: // length == 127，7 位字段装不下别的值
		if len(data) < n+8 {
			return ws.Header{}, 0, false, nil
		}
		// 最高位必须为 0：RFC 要求 64 位长度是无符号且 MSB 为 0，
		// 与 ws.ReadHeader 的 ErrHeaderLengthMSB 保持一致。
		if data[n]&0x80 != 0 {
			return ws.Header{}, 0, false, ws.ErrHeaderLengthMSB
		}
		h.Length = int64(binary.BigEndian.Uint64(data[n : n+8]))
		n += 8
	}

	if h.Masked {
		if len(data) < n+4 {
			return ws.Header{}, 0, false, nil
		}
		copy(h.Mask[:], data[n:n+4])
		n += 4
	}
	return h, n, true, nil
}
