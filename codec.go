package gate

import (
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

/*
codec 是 gate 线路协议唯一的解析入口。

在此之前，同一份 wire protocol 被三处代码分别解析（TCP server 的 conn.read、
WS server 的 consumeServerMessage、client 的 readFrame），三处对 z/c/e 标记
采取了三种不同的策略，于是同一个协议缺陷会在不同传输上以不同形式出现。

现在所有调用方共用 codec，差异只体现为策略字段：

	server:  Allow = maskE                 // 只允许对端加密，不接受上行压缩/合包
	client:  Allow = maskZ | maskC | maskE // server 下行三种都可能用

compound 内部的子消息按协议约定不得再带任何标记（见 header.go 顶部注释），
codec 用一个 Allow=0 的子策略强制这一点，因此 compound 的展开是迭代而非递归，
结构上不存在嵌套，也就不存在栈溢出。
*/

var (
	// ErrMaxMessageSize 帧长度超过 codec 允许的上限。
	ErrMaxMessageSize = errors.New("gate: message size exceeds limit")
	// ErrFlagNotAllowed 对端设置了当前策略不接受的标记位。
	ErrFlagNotAllowed = errors.New("gate: frame flag not permitted")
	// ErrTrailingBytes compound 内部存在无法整除的残留字节。
	ErrTrailingBytes = errors.New("gate: trailing bytes in compound frame")
	// ErrDecompressLimit 解压后的数据超过允许的上限。
	ErrDecompressLimit = errors.New("gate: decompressed size exceeds limit")
	// ErrCipherRequired 收到加密帧但没有配置 Cipher。
	ErrCipherRequired = errors.New("gate: encrypted frame but no cipher configured")
	// ErrDecompressUnsupported 当前策略不接受压缩帧。
	ErrDecompressUnsupported = errors.New("gate: compressed frame not supported by this codec")
)

// frame 是一个已定位、但尚未解密/解压/展开的线路帧。
// payload 借用调用方传入的底层数组，生命周期由调用方负责。
type frame struct {
	payload []byte
	z, c, e bool
}

// codec 描述一侧对入站帧的解析策略。零值不可用，请用 serverCodec/clientCodec 构造。
type codec struct {
	// maxMessage 单个帧 payload 的字节上限。
	maxMessage int
	// maxDecompressed 解压后允许的字节上限；<=0 表示不接受压缩帧。
	maxDecompressed int
	// allow 允许对端设置的标记位，取值为 maskZ|maskC|maskE 的子集。
	// maskM 是结构性标记，永远允许。
	allow byte
}

// serverCodec 构造服务端策略：只接受对端加密，不接受上行压缩或合包。
func serverCodec(maxMessage int) codec {
	return codec{maxMessage: maxMessage, allow: maskE}
}

// clientCodec 构造客户端策略：服务端下行可能压缩、合包、加密。
func clientCodec(maxMessage, maxDecompressed int) codec {
	if maxDecompressed <= 0 {
		maxDecompressed = maxMessage
	}
	return codec{
		maxMessage:      maxMessage,
		maxDecompressed: maxDecompressed,
		allow:           maskZ | maskC | maskE,
	}
}

// subCodec 返回用于解析 compound 子消息的策略：子消息不允许携带任何标记。
func (c codec) subCodec() codec {
	return codec{maxMessage: c.maxMessage, allow: 0}
}

// parse 从 src 头部定位一个完整帧。
//
// ok=false 且 err=nil 表示 src 中的数据还不足以构成一个完整帧，调用方应等待更多数据；
// 这是唯一一种"可重试"的返回，其余任何 err != nil 都表示对端违反协议，连接应当关闭。
func (c codec) parse(src []byte) (f frame, n int, ok bool, err error) {
	if len(src) < 2 {
		return frame{}, 0, false, nil
	}

	size, m, z, cp, e := parseHeader([2]byte(src[:2]))
	header := 2
	if m {
		if len(src) < 4 {
			return frame{}, 0, false, nil
		}
		size = parseHeader2([2]byte(src[2:4]), size)
		header = 4
	}

	// 长度校验必须早于任何按长度分配/切片的动作。
	if size > c.maxMessage {
		return frame{}, 0, false, ErrMaxMessageSize
	}
	if err = c.checkFlags(z, cp, e); err != nil {
		return frame{}, 0, false, err
	}
	if len(src) < header+size {
		return frame{}, 0, false, nil
	}

	return frame{
		// 三索引切片把 cap 封在 payload 末尾。少了这一层，compound 的子消息拿到的
		// 切片 cap 会一直延伸到父 buffer 末尾，sink 里一次 append 就能悄悄覆盖掉
		// 紧随其后的兄弟消息。它是免费的，而且让"子消息之间互不可见"成为结构性
		// 事实，而不是依赖每个 sink 各自记得拷贝。
		payload: src[header : header+size : header+size],
		z:       z,
		c:       cp,
		e:       e,
	}, header + size, true, nil
}

// readFrame 从流式数据源读取一个完整帧，payload 为新分配的独占内存。
// 与 parse 共用同一套校验策略。
//
// 内部走 readFrameInto，自带一块局部暂存区。稳态读循环应当直接用 readFrameInto
// 并复用暂存区：header 会被传进 io.Reader 这个接口调用，逃逸分析救不了它，
// 于是每帧固定多一次 4 字节的堆分配。
func (c codec) readFrame(r io.Reader) (frame, error) {
	var header [4]byte
	return c.readFrameInto(r, &header)
}

// readFrameInto 用调用方提供的暂存区读一个完整帧。
// header 只在本次调用内使用，调用方可以在自己的读循环里长期复用同一块。
func (c codec) readFrameInto(r io.Reader, header *[4]byte) (frame, error) {
	if _, err := io.ReadFull(r, header[:2]); err != nil {
		return frame{}, err
	}

	size, m, z, cp, e := parseHeader([2]byte{header[0], header[1]})
	if m {
		if _, err := io.ReadFull(r, header[2:4]); err != nil {
			return frame{}, err
		}
		size = parseHeader2([2]byte{header[2], header[3]}, size)
	}

	// 先校验再分配：否则对端只要谎报长度就能让我们立刻申请 maxMessage 字节。
	if size > c.maxMessage {
		return frame{}, ErrMaxMessageSize
	}
	if err := c.checkFlags(z, cp, e); err != nil {
		return frame{}, err
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, err
	}
	return frame{payload: payload, z: z, c: cp, e: e}, nil
}

func (c codec) checkFlags(z, cp, e bool) error {
	if z && c.allow&maskZ == 0 {
		return ErrFlagNotAllowed
	}
	if cp && c.allow&maskC == 0 {
		return ErrFlagNotAllowed
	}
	if e && c.allow&maskE == 0 {
		return ErrFlagNotAllowed
	}
	return nil
}

// deliver 完成 解密 → 解压 → 展开 compound 的全过程，并把每条业务消息交给 sink。
//
// 传给 sink 的切片仅在该次调用期间有效；owned 为 true 表示这块内存是本次解码新分配的
// 独占内存，sink 可以直接接管而无需再拷贝一次。
//
// compound 的展开是迭代的：子消息用 allow=0 的策略解析，任何嵌套 compound 都会被
// checkFlags 拒掉，因此这里不存在递归深度问题。
func (c codec) deliver(f frame, cipher Cipher, dec *zstd.Decoder, sink func(msg []byte, owned bool) error) error {
	data := f.payload
	owned := false

	if f.e {
		if cipher == nil {
			return ErrCipherRequired
		}
		cipher.Decrypt(data)
	}

	if f.z {
		if c.maxDecompressed <= 0 || dec == nil {
			return ErrDecompressUnsupported
		}
		out, err := dec.DecodeAll(data, nil)
		if err != nil {
			return err
		}
		// 解码器本身已经用 WithDecoderMaxMemory 设了上限，这里是第二道闸：
		// 上限可能因构造方式不同而失配，而放大攻击的代价太高，值得重复校验。
		if len(out) > c.maxDecompressed {
			return ErrDecompressLimit
		}
		data, owned = out, true
	}

	if !f.c {
		return sink(data, owned)
	}
	return c.expandCompound(data, sink)
}

// expandCompound 迭代展开 compound payload。
func (c codec) expandCompound(data []byte, sink func(msg []byte, owned bool) error) error {
	sub := c.subCodec()
	for len(data) > 0 {
		f, n, ok, err := sub.parse(data)
		if err != nil {
			return err
		}
		if !ok {
			// compound 是一个已完整收齐的 payload，内部必须正好切分干净。
			// 切不干净说明对端构造有误，而不是"再等等就好"。
			return ErrTrailingBytes
		}
		// 子 payload 借用 compound 的底层数组，所有权不移交。
		if err = sink(f.payload, false); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// newZstdDecoder 构造带输出上限的解码器。
// 不设上限时 klauspost/compress 默认允许 64GB，一个几 KB 的帧即可解出数百 MB。
func newZstdDecoder(maxDecompressed int) (*zstd.Decoder, error) {
	if maxDecompressed <= 0 {
		maxDecompressed = maxMessageSize
	}
	return zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxDecompressed)))
}
