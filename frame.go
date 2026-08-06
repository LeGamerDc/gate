package gate

import "github.com/klauspost/compress/zstd"

// Frame 是预编码帧：编码一次、分发 N 次（广播）。
// 不可变，由 GC 管理生命周期——出站队列写出前持有引用，写完放手（01 D20）。
type Frame struct {
	wire []byte // 完整线路字节：帧头 + payload（可能已压缩）。永不改写。
	raw  int    // 原始 payload 长度：BytesOutRaw 的口径（压缩率的分母）
}

type frameOptions struct {
	preEncrypted bool
}

// FrameOption 配置 NewFrame 的编码行为。
type FrameOption func(*frameOptions)

// PreEncrypted 声明 payload 是业务自理的密文：gate 跳过压缩（压密文只浪费 CPU），
// 但**不**设置线路 e 位——e 位属于连接级 Cipher 的职权，解密由对端业务完成，
// gate 视 payload 为不透明字节（01 D26）。
func PreEncrypted() FrameOption {
	return func(o *frameOptions) { o.preEncrypted = true }
}

// newFrame 编码一次：帧头 + 按 cfg 压缩。不加密——per-connection 密钥与
// 「编码一次分发多次」根本不兼容，SendFrame 在配了 Cipher 的连接上返回
// ErrCipherConflict（01「与加密的关系」）。
//
// wire 是一次普通堆分配，不入池：Frame 的分配频率是每次广播一次，不是每连接一次。
func newFrame(payload []byte, cfg Outbound, enc *zstd.Encoder, opts ...FrameOption) (*Frame, error) {
	var fo frameOptions
	for _, opt := range opts {
		opt(&fo)
	}
	tryCompress := !fo.preEncrypted && enc != nil &&
		cfg.CompressThreshold > 0 && len(payload) > cfg.CompressThreshold
	bf, err := buildFrame(payload, 0, enc, tryCompress, nil, maxMessageSize)
	if err != nil {
		return nil, err
	}
	wire := make([]byte, 0, bf.wireSize())
	wire = append(wire, bf.hdr[:bf.hdrLen]...)
	wire = append(wire, bf.body...)
	bf.release()
	return &Frame{wire: wire, raw: len(payload)}, nil
}
