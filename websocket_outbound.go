package gate

import (
	"encoding/binary"
	"math"

	"github.com/gobwas/ws"
)

// wsMaxServerHeaderSize 是服务端帧头的最大长度：2 字节基础头 + 8 字节扩展长度。
// 服务端帧从不带掩码，所以用不到 ws.MaxHeaderSize 的 14 字节。
const wsMaxServerHeaderSize = 10

// wsWriter 是 wsOutbound 需要的底层写能力，gnet.Conn 天然满足。
type wsWriter interface {
	Write([]byte) (int, error)
	Writev([][]byte) (int, error)
}

// wsOutbound 把出站字节包成 WebSocket 二进制帧。
//
// 之前它走 wsutil.WriteServerBinary -> ws.WriteFrame，而 ws.WriteFrame 是
// WriteHeader(w, h) 之后再 w.Write(payload)，也就是每条消息两次写；writev 那条
// 路径更糟，先把所有分片拷进一块池化缓冲，再按同样的方式写两次。
//
// 现在帧头自己编码，和 payload 一起在一次 Writev 里发出去：write 是 2 个
// iovec，writev 是 1+n 个，都只对应一次内核写，并且 payload 一次都不再拷贝。
type wsOutbound struct {
	conn wsWriter

	// hdr / iov 是每连接常驻的临时缓冲，避免每条消息都分配。
	//
	// 所有出站写都发生在该连接自己的事件循环 goroutine 上（sender 通过
	// conn.Wake 把 flush 排到事件循环里），因此这里不需要额外同步。
	hdr [wsMaxServerHeaderSize]byte
	iov [][]byte
}

func (w *wsOutbound) write(data []byte) error {
	n := encodeWSBinaryHeader(w.hdr[:], len(data))
	w.iov = append(w.iov[:0], w.hdr[:n], data)
	return w.flush()
}

func (w *wsOutbound) writev(vb [][]byte) error {
	if len(vb) == 0 {
		return nil
	}
	total := 0
	for _, b := range vb {
		total += len(b)
	}
	n := encodeWSBinaryHeader(w.hdr[:], total)
	w.iov = append(w.iov[:0], w.hdr[:n])
	w.iov = append(w.iov, vb...)
	return w.flush()
}

func (w *wsOutbound) flush() error {
	_, err := w.conn.Writev(w.iov)
	// 不要跨调用持有调用方的分片：sender 的数据来自 mcache，flush 之后就会
	// 被归还并复用。
	clear(w.iov)
	w.iov = w.iov[:0]
	return err
}

// encodeWSBinaryHeader 写入一个 FIN=1、RSV=0、未掩码的二进制帧头，返回头长度。
// dst 至少要有 wsMaxServerHeaderSize 字节。
func encodeWSBinaryHeader(dst []byte, length int) int {
	// 0x80 = FIN；RSV 三位全 0；低四位是 opcode。
	dst[0] = 0x80 | byte(ws.OpBinary)
	switch {
	case length < 126:
		dst[1] = byte(length)
		return 2
	case length <= math.MaxUint16:
		dst[1] = 126
		binary.BigEndian.PutUint16(dst[2:4], uint16(length))
		return 4
	default:
		dst[1] = 127
		binary.BigEndian.PutUint64(dst[2:10], uint64(length))
		return 10
	}
}
