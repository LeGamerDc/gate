package gate

import (
	"github.com/klauspost/compress/zstd"
)

// 编码侧的纯函数核心（02「附：编解码伪码」的 encode 半边）。
// 03 的 stage-2 编码器负责分组与 chunk 链，这里只做单帧的三个变换：
//
//	[z] 整体 zstd 压缩 → [e] Seal（帧头做 AAD）→ 写外层帧头
//
// 顺序是协议的一部分：先拼包后压缩由调用方保证（body 传进来时已是
// compound body 或单条 payload），先压缩后加密在本文件内保证。

// appendSubFrame 把一条子消息追加进 compound body：2/4 字节子头 + payload。
// 子消息的标记位必须全为 0（W6），所以这里根本不接受 flags。
func appendSubFrame(body []byte, msg []byte) []byte {
	var h [maxHeaderSize]byte
	n := putHeader(h[:], len(msg), 0)
	body = append(body, h[:n]...)
	return append(body, msg...)
}

// builtFrame 是 buildFrame 的输出：hdr[:hdrLen] + body 即完整线路帧。
//
// body 的所有权：bodyPooled = true 时 body 来自分级池，写完后归还；
// 否则 body 就是调用方传入的 data（可能已被 Overhead()==0 的 Seal 原地覆写）。
type builtFrame struct {
	hdr        [maxHeaderSize]byte
	hdrLen     int
	body       []byte
	bodyPooled bool
}

func (b *builtFrame) wireSize() int { return b.hdrLen + len(b.body) }

// release 归还池借的 body。写完（或编码中途放弃）后必须恰好调用一次。
func (b *builtFrame) release() {
	if b.bodyPooled {
		poolPut(b.body[:0])
		b.body, b.bodyPooled = nil, false
	}
}

// buildFrame 把一个已完成合包决策的 data 变成线路帧。
//
//   - flags 是调用方已经定下的标记位（compound 的 c 位）；z / e 由本函数按需置入。
//   - tryCompress = true 时先压缩；压完不比原来小就丢弃压缩结果——既省对端一次
//     无谓解压，也保证贴着上限的 body 不会因 zstd 膨胀越过协议上限。
//   - ci != nil 时 Seal，帧头做 AAD：Overhead() 是常量、Seal 输出长度精确，
//     所以先算最终长度 → 生成帧头 → Seal(dst, data, header)，不存在循环依赖（W9/D24）。
//     Overhead() == 0 时原地覆写 data。
//   - maxWire 是线路长度上限（调用方的 Limits.MaxMessage，已 ≤ 协议上限）。
//     Send 在准入时已按 frameSize(len+Overhead) 检查过，这里越限说明 gate
//     自身有 bug，返回 ErrMessageTooLarge 由调用方关连接，绝不写错帧上线路。
func buildFrame(data []byte, flags byte, enc *zstd.Encoder, tryCompress bool, ci Cipher, maxWire int) (builtFrame, error) {
	var scratch [maxHeaderSize]byte
	return buildFrameAAD(data, flags, enc, tryCompress, ci, maxWire, &scratch)
}

// buildFrameAAD 是热路径变体：aad 头先写进调用方提供的长命 scratch
// （outbound 传每 loop 的 env.aadHdr）。把 f.hdr 直接切给接口方法 Seal
// 会让整个 builtFrame 按逃逸分析上堆——每帧一次 48B 分配。
func buildFrameAAD(data []byte, flags byte, enc *zstd.Encoder, tryCompress bool, ci Cipher, maxWire int, scratch *[maxHeaderSize]byte) (builtFrame, error) {
	var f builtFrame
	body := data

	if tryCompress && enc != nil {
		out := enc.EncodeAll(body, poolGet(len(body))[:0])
		if len(out) < len(body) {
			body, flags = out, flags|flagZ
			f.bodyPooled = true
		} else {
			poolPut(out[:0])
		}
	}

	if ci != nil {
		flags |= flagE
		wire := len(body) + ci.Overhead()
		if wire > maxWire {
			f.body = body
			f.release()
			return builtFrame{}, ErrMessageTooLarge
		}
		f.hdrLen = putHeader(scratch[:], wire, flags)
		f.hdr = *scratch
		if ci.Overhead() == 0 {
			body = ci.Seal(body[:0], body, scratch[:f.hdrLen])
		} else {
			dst := poolGet(wire)[:0]
			sealed := ci.Seal(dst, body, scratch[:f.hdrLen])
			if f.bodyPooled {
				poolPut(body[:0])
			}
			body, f.bodyPooled = sealed, true
		}
	} else {
		if len(body) > maxWire {
			f.body = body
			f.release()
			return builtFrame{}, ErrMessageTooLarge
		}
		f.hdrLen = putHeader(f.hdr[:], len(body), flags)
	}

	f.body = body
	return f, nil
}
