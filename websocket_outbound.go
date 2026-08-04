package gate

import (
	"encoding/binary"
	"math"
	"sync"

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
//
// 这个结构体本身不再持有任何暂存空间，见 wsScratch。
type wsOutbound struct {
	conn wsWriter
}

// wsScratch 是组一帧 WebSocket 出站数据需要的全部暂存空间。
//
// hdr 和 vec 此前是 wsOutbound 的常驻字段，也就是每条 WebSocket 连接长期扛着一条
// 最多 1+2*separateChunk 项的 iovec 切片（约 1.5KB）。它们只在一次 flush 期间
// 有用，十万连接就是白占约 150MB。
//
// 池化的前提和 sender 的 separateScratch 完全一样：所有出站写都发生在连接自己的
// 事件循环上（sender 通过 conn.Wake 把 flush 排进事件循环），一次调用期间不会
// 让出，所以池里同时在外的对象数等于事件循环数，不随连接数增长。
type wsScratch struct {
	hdr [wsMaxServerHeaderSize]byte
	vec [][]byte
}

// 池里存指针：存值会在每次 Put 上装箱分配一次（staticcheck SA6002）。
var wsScratchPool = sync.Pool{New: func() interface{} {
	return &wsScratch{vec: make([][]byte, 0, 1+2*separateChunk)}
}}

func getWSScratch() *wsScratch {
	return wsScratchPool.Get().(*wsScratch)
}

func putWSScratch(s *wsScratch) {
	// 不要跨调用持有调用方的分片：sender 的数据来自 mcache，写完就会被归还并复用。
	//
	// 这里清到 len 就够，不必像 putSeparateScratch 那样清到 cap：每次借用都从
	// len 0 开始、只 append 一段、然后清掉自己写过的全部长度，所以归还时索引
	// >= len 的位置必然已经是上一次借用清干净的。清到 cap 反而要在每次 WS 写上
	// 多付一次按历史高水位计的 memset。
	//
	// pushSeparate 那边不成立，是因为它按 chunk 循环、每个 chunk 都从 [:0] 重填，
	// 归还时的 len 只是最后一个 chunk 的长度（见 putSeparateScratch）。
	clear(s.vec)
	s.vec = s.vec[:0]
	wsScratchPool.Put(s)
}

func (w *wsOutbound) write(data []byte) error {
	s := getWSScratch()
	defer putWSScratch(s)

	n := encodeWSBinaryHeader(s.hdr[:], len(data))
	s.vec = append(s.vec, s.hdr[:n], data)
	_, err := w.conn.Writev(s.vec)
	return err
}

func (w *wsOutbound) writev(vb [][]byte) error {
	if len(vb) == 0 {
		return nil
	}
	s := getWSScratch()
	defer putWSScratch(s)

	total := 0
	for _, b := range vb {
		total += len(b)
	}
	n := encodeWSBinaryHeader(s.hdr[:], total)
	s.vec = append(s.vec, s.hdr[:n])
	s.vec = append(s.vec, vb...)
	_, err := w.conn.Writev(s.vec)
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
