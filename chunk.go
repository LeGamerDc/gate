package gate

import "github.com/klauspost/compress/zstd"

// stage 2 的唯一载体：一段最终线路字节（06「出站的精确模型」）。
//
// 三条不变式：
//   - stage 2 里的每一个字节都是最终线路字节，入链之后不再变换（含 WS 帧头）。
//   - off 是唯一的进度表示，部分写只推进 off，永不重新生成任何字节。
//   - 严格 FIFO，只在尾部追加。
type chunkKind uint8

const (
	chunkInline chunkKind = iota // 帧头（gate 2~4 字节 / WS 2~10 字节），内联存储
	chunkPooled                  // payload，来自分级池
	chunkFrame                   // payload，属于某个 *Frame
)

type chunk struct {
	kind  chunkKind
	off   int32    // 本 chunk 已写出的字节数，唯一的进度表示
	n     int32    // 有效字节数
	hdr   [10]byte // kind == chunkInline
	buf   []byte   // kind == chunkPooled | chunkFrame
	frame *Frame   // kind == chunkFrame：持有引用防 GC
	next  *chunk
}

// bytes 返回本 chunk 的完整线路字节（未按 off 截断）。
func (c *chunk) bytes() []byte {
	if c.kind == chunkInline {
		return c.hdr[:c.n]
	}
	return c.buf[:c.n]
}

// release 在 chunk 完全写出（或被丢弃）后归还其持有的内存。
func (c *chunk) release() {
	switch c.kind {
	case chunkPooled:
		poolPut(c.buf)
	case chunkFrame:
		c.frame = nil // 放开引用，GC 接手
	}
	c.buf = nil
}

// chunkSlab 是每 loop 的 chunk 节点自由链，只在 loop 线程上取放，无锁。
type chunkSlab struct {
	free        *chunk
	outstanding int // 已借出未归还的节点数（配平断言用；非原子——只在 loop 上动）
}

func (s *chunkSlab) get() *chunk {
	s.outstanding++
	if c := s.free; c != nil {
		s.free = c.next
		c.next = nil
		return c
	}
	return &chunk{}
}

func (s *chunkSlab) put(c *chunk) {
	s.outstanding--
	*c = chunk{next: s.free}
	s.free = c
}

// loopEnv 是每事件循环一套的编码暂存：iovec、chunk slab、懒初始化的 zstd 编码器。
// 跨调用的暂存空间一律按事件循环池化，不挂成连接级字段（01「内存与 GC 预算」）。
type loopEnv struct {
	iov    [][]byte
	slab   chunkSlab
	stats  *loopStats          // 与 loop.stats 同一对象；outbound 经它计数
	quota  *quotaLease         // 全局出站预算的本 loop 租约（06「全局预算是近似的」）
	aadHdr [maxHeaderSize]byte // buildFrameAAD 的 scratch：AAD 头的长命暂存

	enc     *zstd.Encoder
	encInit bool
	encOpts encoderOptions
}

type encoderOptions struct {
	level CompressLevel
	dict  []byte
}

func newLoopEnv(opts encoderOptions) *loopEnv {
	return &loopEnv{
		iov:     make([][]byte, 0, iovMax),
		encOpts: opts,
		stats:   &loopStats{},
		quota:   newQuotaLease(newServerBudget(0)), // 默认不限；server 装配时替换
	}
}

func zstdLevel(l CompressLevel) zstd.EncoderLevel {
	switch l {
	case CompressBalanced:
		return zstd.SpeedDefault
	case CompressBetter:
		return zstd.SpeedBetterCompression
	case CompressBest:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedFastest
	}
}

// encoder 懒初始化本 loop 的 zstd 编码器（concurrency = 1）。
// CompressThreshold <= 0 的 gate 永远走不到这里，一分钱都不付。
func (e *loopEnv) encoder() *zstd.Encoder {
	if !e.encInit {
		opts := []zstd.EOption{
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstdLevel(e.encOpts.level)),
		}
		if len(e.encOpts.dict) > 0 {
			opts = append(opts, zstd.WithEncoderDict(e.encOpts.dict))
		}
		enc, err := zstd.NewWriter(nil, opts...)
		if err != nil {
			// 只有非法选项组合才可能失败，而选项在构造时已定死。
			panic("gate: zstd encoder init: " + err.Error())
		}
		e.enc, e.encInit = enc, true
	}
	return e.enc
}
