package gate

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

// WebSocket 帧层（05）：从原始字节里切出 WS 帧、校验、去掩码、处理控制帧，
// 把数据帧 payload 一段一段喂给 gate 帧循环。**数据帧完全流式**（W12/W-D9）：
// 一个 WS 帧合法地装着一整批 gate 帧，攒齐再处理的缓冲上界不受任何配置约束。
// WS 层常驻残片上界 = 14 字节帧头 + 125 字节控制帧暂存。

const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1
	wsOpBinary       = 0x2
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA

	wsFinBit  = 0x80
	wsRsvMask = 0x70
	wsMaskBit = 0x80

	wsMaxCtrlPayload = 125
	wsMaxHeader      = 14 // 2 基础 + 8 扩展长度 + 4 掩码

	// WS 帧长的宽松上界（W14/W-D10）：MaxMessage 不约束 WS 帧长——gate 自己
	// 出站的一批就能超过它；这里只防 64 位长度字段被滥用。
	wsMaxFrameLen = 1 << 30
)

// WS 侧的协议违规错误值（都会被 wrapProtocol 包进 ErrProtocol）。
var (
	errWSRsv          = errors.New("gate/ws: non-zero RSV")
	errWSOpcode       = errors.New("gate/ws: reserved opcode")
	errWSText         = errors.New("gate/ws: text frame not supported")
	errWSCtrlTooLong  = errors.New("gate/ws: control payload > 125")
	errWSCtrlFragment = errors.New("gate/ws: fragmented control frame")
	errWSBadLength    = errors.New("gate/ws: non-canonical extended length")
	errWSLenMSB       = errors.New("gate/ws: 64-bit length MSB set")
	errWSFrameTooLong = errors.New("gate/ws: frame exceeds loose bound")
	errWSUnmasked     = errors.New("gate/ws: client frame not masked")
	errWSFragState    = errors.New("gate/ws: fragmentation state violation")
	errWSBadClose     = errors.New("gate/ws: malformed close payload")
)

// wsState 是每连接的 WS 帧层状态（06「WebSocket 的入站是流式的」）。
// 只在 loop 线程访问。
type wsState struct {
	remaining  int64 // 当前数据帧还剩多少 payload
	mask       [4]byte
	masked     bool
	maskOff    uint8 // 跨读事件的掩码相位
	fragmented bool  // 是否处在一条分片消息中间
	deliver    bool  // 当前数据帧是否投递（CLOSE_RECEIVED 后不投递）

	hdrBuf [wsMaxHeader]byte
	hdrLen int

	ctrl    []byte // 控制帧暂存（分级池，≤125B，稳态不存在）
	ctrlOp  byte
	ctrlLen int

	closeSent bool // 本地 close 帧已入出站链：之后不再排数据帧（W13）
	closeRecv bool // 收到对端 close 帧：之后入站数据帧不再投递
}

func (w *wsState) release() {
	if w.ctrl != nil {
		poolPut(w.ctrl)
		w.ctrl = nil
	}
}

// unmask 原地去掩码，维护跨读事件的相位（W8）。
func (w *wsState) unmask(b []byte) {
	if !w.masked {
		return
	}
	off := w.maskOff
	for i := range b {
		b[i] ^= w.mask[(off+uint8(i))&3]
	}
	w.maskOff = (off + uint8(len(b))) & 3
}

// feed 把本轮读到的原始字节转换成若干段 gate 字节流，逐段调用 emit。
// emit 返回 error 表示上层要求停止（关闭 / 协议错误）；暂停不在此列——
// 暂停时 gate 层把字节收进 carry，feed 继续推进（内核已读出的字节不能丢）。
// ctrl 回调处理收齐的控制帧。返回的 error 是协议违规或 emit/ctrl 的错误。
func (w *wsState) feed(raw []byte, emit func([]byte) error, ctrl func(op byte, payload []byte) error) error {
	for {
		// ① 处在一个数据帧中间：流式去掩码 + 立刻喂给 gate 帧循环。
		if w.remaining > 0 {
			if len(raw) == 0 {
				return nil
			}
			take := min(int(w.remaining), len(raw))
			seg := raw[:take]
			w.unmask(seg)
			w.remaining -= int64(take)
			raw = raw[take:]
			if w.deliver {
				if err := emit(seg); err != nil {
					return err
				}
			}
			continue
		}
		// ② 收齐中的控制帧（跨读事件用 ctrl 暂存）。
		if w.ctrl != nil {
			take := min(w.ctrlLen-len(w.ctrl), len(raw))
			if take > 0 {
				seg := raw[:take]
				w.unmask(seg)
				w.ctrl = append(w.ctrl, seg...)
				raw = raw[take:]
			}
			if len(w.ctrl) < w.ctrlLen {
				return nil // 等更多数据
			}
			op, payload := w.ctrlOp, w.ctrl
			err := ctrl(op, payload)
			poolPut(w.ctrl)
			w.ctrl = nil
			if err != nil {
				return err
			}
			continue
		}
		// ③ 解析帧头（经 hdrBuf 累积，可跨读事件）。
		if len(raw) == 0 {
			return nil
		}
		need := w.headerNeed()
		take := min(need-w.hdrLen, len(raw))
		copy(w.hdrBuf[w.hdrLen:], raw[:take])
		w.hdrLen += take
		raw = raw[take:]
		if w.hdrLen < w.headerNeed() { // 长度字段/掩码会把 need 撑大
			continue // raw 还有字节就继续取；耗尽时循环顶部返回
		}
		if err := w.beginFrame(); err != nil {
			return err
		}
	}
}

// headerNeed 返回按当前已累积的头字节推断的完整头长。
func (w *wsState) headerNeed() int {
	if w.hdrLen < 2 {
		return 2
	}
	n := 2
	switch w.hdrBuf[1] & 0x7F {
	case 126:
		n += 2
	case 127:
		n += 8
	}
	if w.hdrBuf[1]&wsMaskBit != 0 {
		n += 4
	}
	return n
}

// beginFrame 在整头收齐后执行校验清单 2~10 步并进入 payload 阶段。
func (w *wsState) beginFrame() error {
	b0, b1 := w.hdrBuf[0], w.hdrBuf[1]
	fin := b0&wsFinBit != 0
	op := b0 & 0x0F
	masked := b1&wsMaskBit != 0

	if b0&wsRsvMask != 0 { // 2. 我们不协商任何扩展
		return errWSRsv
	}
	switch op {
	case wsOpContinuation, wsOpBinary, wsOpClose, wsOpPing, wsOpPong:
	case wsOpText: // 4. gate 的 payload 是二进制
		return errWSText
	default: // 3. 保留 opcode
		return errWSOpcode
	}

	plen := int64(b1 & 0x7F)
	off := 2
	switch plen {
	case 126:
		v := int64(binary.BigEndian.Uint16(w.hdrBuf[2:4]))
		if v <= 125 { // 7. 扩展长度必须规范编码
			return errWSBadLength
		}
		plen, off = v, 4
	case 127:
		u := binary.BigEndian.Uint64(w.hdrBuf[2:10])
		if u&(1<<63) != 0 { // 8. 64 位长度最高位必须为 0
			return errWSLenMSB
		}
		if u <= 65535 { // 7.
			return errWSBadLength
		}
		plen, off = int64(u), 10
	}

	isCtrl := op >= wsOpClose
	if isCtrl {
		if plen > wsMaxCtrlPayload { // 5. 必须早于收 payload
			return errWSCtrlTooLong
		}
		if !fin { // 6. 控制帧不得分片
			return errWSCtrlFragment
		}
	} else if plen > wsMaxFrameLen { // 9. 宽松上界，不是 MaxMessage（W14）
		return errWSFrameTooLong
	}
	if !masked { // 10. RFC 硬要求：客户端帧必须带掩码
		return errWSUnmasked
	}
	copy(w.mask[:], w.hdrBuf[off:off+4])
	w.masked = true
	w.maskOff = 0
	w.hdrLen = 0

	if isCtrl {
		w.ctrlOp = op
		w.ctrlLen = int(plen)
		w.ctrl = poolGet(int(plen))[:0] // plen 为 0 也要非 nil：标记「处理控制帧中」
		return nil
	}

	// 12. 分片状态校验（控制帧穿插不影响分片状态）。
	switch {
	case op == wsOpBinary && !w.fragmented:
		w.fragmented = !fin
	case op == wsOpContinuation && w.fragmented:
		if fin {
			w.fragmented = false
		}
	default:
		return errWSFragState
	}
	w.remaining = plen
	w.deliver = !w.closeRecv // CLOSE_RECEIVED 后数据帧不再投递（W13）
	return nil
}

// ─── close 帧 ───

// wsValidCloseCode 按 RFC 6455 §7.4 判断线路上允许出现的状态码。
func wsValidCloseCode(code int) bool {
	switch {
	case code >= 3000 && code <= 4999:
		return true
	case code == 1000 || code == 1001 || code == 1002 || code == 1003:
		return true
	case code >= 1007 && code <= 1011:
		return true
	}
	return false
}

// parseClosePayload 校验并解出对端 close 帧（校验清单第 13 步）。
func parseClosePayload(p []byte) (code int, reason string, err error) {
	switch {
	case len(p) == 0:
		return 1005, "", nil // 无状态码
	case len(p) == 1:
		return 0, "", errWSBadClose
	}
	code = int(binary.BigEndian.Uint16(p[:2]))
	if !wsValidCloseCode(code) {
		return 0, "", fmt.Errorf("%w: code %d", errWSBadClose, code)
	}
	if !utf8.Valid(p[2:]) {
		return 0, "", fmt.Errorf("%w: reason not utf-8", errWSBadClose)
	}
	return code, string(p[2:]), nil
}

// wsServerHeader 生成服务端（不带掩码）的帧头，返回头长。dst 容量 ≥ 10。
func wsServerHeader(dst []byte, op byte, n int) int {
	dst[0] = wsFinBit | op
	switch {
	case n < 126:
		dst[1] = byte(n)
		return 2
	case n <= 65535:
		dst[1] = 126
		binary.BigEndian.PutUint16(dst[2:], uint16(n))
		return 4
	default:
		dst[1] = 127
		binary.BigEndian.PutUint64(dst[2:], uint64(n))
		return 10
	}
}

// wsBinaryWrap 是 outbound 的编码期封帧钩子（O12/W7）：
// 一次编码的一整批 gate 帧装进一个 WS 二进制帧。
func wsBinaryWrap(total int, dst []byte) int {
	return wsServerHeader(dst, wsOpBinary, total)
}

// wsControlFrame 拼一个完整的服务端控制帧（≤ 2+125 字节）。
func wsControlFrame(op byte, payload []byte) []byte {
	buf := make([]byte, 0, 2+len(payload))
	var h [wsMaxHeader]byte
	n := wsServerHeader(h[:], op, len(payload))
	return append(append(buf, h[:n]...), payload...)
}

// wsCloseFrame 按状态码与原因拼 close 帧（reason 超长截断到控制帧上限）。
func wsCloseFrame(code int, reason string) []byte {
	if len(reason) > wsMaxCtrlPayload-2 {
		reason = reason[:wsMaxCtrlPayload-2]
	}
	p := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(p, uint16(code))
	copy(p[2:], reason)
	return wsControlFrame(wsOpClose, p)
}

// wsCloseCodeFor 把关闭原因映射到 close code（05「本地主动关闭」的表）。
// ok = false 表示不发 close 帧（背压 / 写失败：出站方向已经写不动了）。
func wsCloseCodeFor(reason error) (code int, ok bool) {
	switch {
	case reason == nil:
		return 1000, true
	case errors.Is(reason, ErrBackpressure):
		return 0, false
	case errors.Is(reason, ErrServerClosed),
		errors.Is(reason, ErrIdleTimeout),
		errors.Is(reason, ErrPauseTimeout),
		errors.Is(reason, ErrHandshakeTimeout):
		return 1001, true
	case errors.Is(reason, ErrProtocol):
		return 1002, true
	case errors.Is(reason, ErrPendingOverflow),
		errors.Is(reason, ErrMessageTooLarge):
		return 1009, true
	case errors.Is(reason, ErrHandlerPanic):
		return 1011, true
	case errors.Is(reason, ErrPeerClosed), errors.Is(reason, ErrPeerReset):
		return 0, false // 对端已关/已断：无处可发
	default:
		// 业务自定义 error → 1000。写失败的底层错误也落到这里，但那时
		// outbound 已经 discard，入队会失败——结构上不会写进有洞的流。
		return 1000, true
	}
}
