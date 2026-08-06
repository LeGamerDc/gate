package gate

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// codec 描述一侧对入站帧的解析策略。零值不可用。
// 一份实现被三个角色复用，差异只体现为 allow 掩码（02「策略：allow 掩码」）。
type codec struct {
	maxMessage      int  // payload 字节上限（线路长度口径）
	maxDecompressed int  // 解压输出上限；<= 0 表示不接受压缩帧
	allow           byte // 允许对端设置的标记位
	requireEncrypt  bool // 连接配了 Cipher ⇒ 每帧必须 e = 1（W3，双向）
}

// serverCodec 解析上行：只接受 e，压缩与合包一律拒绝并断开。
// 服务端因此根本不存在解压/展开的攻击面（W2）。
func serverCodec(maxMessage int) codec {
	return codec{maxMessage: maxMessage, allow: flagE}
}

// clientCodec 解析下行：z / c / e 都可能出现。
func clientCodec(maxMessage, maxDecompressed int) codec {
	return codec{maxMessage: maxMessage, maxDecompressed: maxDecompressed, allow: flagAll}
}

// subCodec 解析 compound 子消息：任何标记位都不允许。
// 嵌套 compound 在校验阶段被拒，展开因此是迭代而不是递归（W6）。
func (c codec) subCodec() codec {
	return codec{maxMessage: c.maxMessage}
}

// frame 是 parse 的输出。payload 与 header 都借用 src 的底层数组，
// 生命周期由调用方负责；header 保留原始的 2/4 字节帧头，作为 deliver 里 Open 的 aad。
type frame struct {
	header  []byte // 原始帧头（2 或 4 字节）
	payload []byte // 三索引切片，cap 封在末尾
	flags   byte   // 首字节的 m/z/c/e 位
}

func (f frame) z() bool { return f.flags&flagZ != 0 }
func (f frame) c() bool { return f.flags&flagC != 0 }
func (f frame) e() bool { return f.flags&flagE != 0 }

// parse 从 src 头部定位一个完整帧，不做任何解密/解压/展开。
//
// 校验顺序是规格的一部分（02「校验规格」1~10 步），每一步都保护着后面的步骤；
// 尤其第 6 步：长度校验必须早于任何按长度进行的分配或切片。
//
// 返回值只有两种非成功形态，类型上不可混淆（W8）：
//
//	ok = false, err = nil ⇒ 数据不足，保留字节等下一次读事件（唯一可重试的返回）。
//	                        此时若帧头已完整（1~8 步已通过），n 是凑齐该帧所需的
//	                        总字节数——读路径靠它决定走 rbuf 还是大帧直读；
//	                        帧头本身不全时 n = 0。
//	err != nil            ⇒ 协议违规，关闭连接
func (c codec) parse(src []byte) (f frame, n int, ok bool, err error) {
	if len(src) < minHeaderSize { // 1. 数据不足
		return
	}
	b0 := src[0] // 2. 取出 m/z/c/e 与 size12
	size := int(b0&0x0f)<<8 | int(src[1])
	hdr := minHeaderSize
	if b0&flagM != 0 {
		if len(src) < maxHeaderSize { // 3. 数据不足
			return
		}
		size = size<<16 | int(src[2])<<8 | int(src[3]) // 4. 拼出完整 size
		hdr = maxHeaderSize
		if size < smallSizeLimit { // 5. 规范编码：m = 1 ⟺ size >= 4096
			err = ErrNonCanonicalHeader
			return
		}
	}
	if size > c.maxMessage { // 6. 必须早于任何按长度切片/分配
		err = ErrMaxMessageSize
		return
	}
	if b0&flagAll&^c.allow != 0 { // 7. 标记位不在 allow 之内
		err = ErrFlagNotAllowed
		return
	}
	if e := b0&flagE != 0; e != c.requireEncrypt { // 8. Cipher 配置与 e 位强制一致（W3）
		if c.requireEncrypt {
			err = ErrCipherRequired
		} else {
			err = ErrCipherUnavailable
		}
		return
	}
	if len(src) < hdr+size { // 9. 数据不足；带回总帧长供大帧路径决策
		n = hdr + size
		return
	}
	// 10. 三索引切片封住 cap：sink 里的 append 不可能覆盖相邻字节
	f = frame{
		header:  src[:hdr:hdr],
		payload: src[hdr : hdr+size : hdr+size],
		flags:   b0,
	}
	return f, hdr + size, true, nil
}

// deliver 完成 解密 → 解压 → 展开 的全过程，逐条交给 sink（02 解码顺序）。
//
// owned = true 表示 msg 所在内存是本次解码新借出的（不属于调用方的 src）；
// 无论 owned 与否，msg 都只在本次 sink 调用期间有效——deliver 返回时归还
// 全部临时缓冲（06 字节所有权表：解密/解压输出的生命周期是「deliver 期间」）。
//
// dec 只在 maxDecompressed > 0 的策略下需要；服务端传 nil。
func (c codec) deliver(f frame, ci Cipher, dec *zstd.Decoder, sink func(msg []byte, owned bool) error) error {
	data := f.payload
	owned := false

	if f.e() {
		if ci == nil {
			// parse 第 8 步已挡住这条路径；这里是纵深防御，不给 nil 解引用留门。
			return ErrCipherUnavailable
		}
		if ov := ci.Overhead(); ov == 0 {
			p, err := ci.Open(data[:0], data, f.header) // 原地
			if err != nil {
				return fmt.Errorf("%w: %w", ErrDecryptFailed, err)
			}
			data = p
		} else {
			if len(data) < ov {
				return ErrDecryptFailed
			}
			buf := poolGet(len(data) - ov)
			defer poolPut(buf)
			p, err := ci.Open(buf[:0], data, f.header)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrDecryptFailed, err)
			}
			data, owned = p, true
		}
	}

	if f.z() {
		if dec == nil || c.maxDecompressed <= 0 {
			return ErrFlagNotAllowed
		}
		buf := poolGet(min(len(data)*4, c.maxDecompressed))
		defer poolPut(buf) // DecodeAll 换过底层数组时归还的仍是原 buf，长出来的交给 GC
		out, err := dec.DecodeAll(data, buf[:0])
		if err != nil {
			if errors.Is(err, zstd.ErrDecoderSizeExceeded) {
				// 第一道闸：解码器构造时的 WithDecoderMaxMemory。
				return fmt.Errorf("%w: %w", ErrDecompressLimit, err)
			}
			return fmt.Errorf("gate: decompress: %w", err)
		}
		// 第二道闸：上限可能因解码器构造方式不同而失配，
		// 而放大攻击的代价太低，值得重复校验。
		if len(out) > c.maxDecompressed {
			return ErrDecompressLimit
		}
		data, owned = out, true
	}

	if !f.c() {
		return sink(data, owned)
	}

	// compound：迭代展开，子策略 allow = 0。
	// body 必须正好切分干净——它是一个已经完整收齐的 payload，不存在「后续数据」。
	sub := c.subCodec()
	for len(data) > 0 {
		g, k, ok, err := sub.parse(data)
		if err != nil {
			return err
		}
		if !ok {
			return ErrTrailingBytes
		}
		if err := sink(g.payload, owned); err != nil {
			return err
		}
		data = data[k:]
	}
	return nil
}
