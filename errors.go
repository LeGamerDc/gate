package gate

import (
	"errors"
	"fmt"
)

// 关闭原因：OnClose 的 reason 用 errors.Is 判断。见 01「错误与关闭原因」。
var (
	ErrPeerClosed       = errors.New("gate: peer closed")        // 对端正常关闭（TCP FIN / WS close 帧）
	ErrPeerReset        = errors.New("gate: peer reset")         // 连接被重置或读写失败
	ErrIdleTimeout      = errors.New("gate: idle timeout")       // Limits.Idle 到期
	ErrBackpressure     = errors.New("gate: outbound stalled")   // 积压非空且 StallTimeout 内一个字节都没写出去
	ErrPendingOverflow  = errors.New("gate: pending overflow")   // Pause 期间入站积压越过 Limits.MaxPending
	ErrPauseTimeout     = errors.New("gate: pause timeout")      // 单次 Pause 超过 Limits.MaxPause
	ErrHandshakeTimeout = errors.New("gate: handshake timeout")  // 握手未在 Limits.HandshakeTimeout 内完成
	ErrProtocol         = errors.New("gate: protocol violation") // 线路协议违规（包装具体错误）
	ErrHandlerPanic     = errors.New("gate: handler panic")      // 业务回调 panic，已被恢复屏障接住
	ErrConnLimit        = errors.New("gate: connection limit")   // 越过 Limits.MaxConns
	ErrServerClosed     = errors.New("gate: server closed")      // Shutdown
)

// 发送侧错误。见 01「发送 / 背压」。
var (
	ErrConnClosed      = errors.New("gate: connection closed")    // Close 之后的发送
	ErrSendQueueFull   = errors.New("gate: send queue full")      // 积压达到 MaxBuffer，消息从未入队
	ErrMessageTooLarge = errors.New("gate: message too large")    // frameSize(len+Overhead) > Limits.MaxMessage
	ErrInvalidLength   = errors.New("gate: invalid fill length")  // SendFunc 的 fill 返回 k < 0 或 k > n
	ErrAsyncBusy       = errors.New("gate: async task in flight") // 该连接已有一次 AsyncDo 在途
	ErrCipherConflict  = errors.New("gate: cipher conflict")      // 对配了 Cipher 的连接调用 SendFrame
)

// 协议违规的具体错误值，都会被包进 ErrProtocol。见 02「错误分类」。
var (
	ErrMaxMessageSize     = errors.New("gate: message exceeds max size")     // 长度超过 maxMessage
	ErrNonCanonicalHeader = errors.New("gate: non-canonical header")         // m = 1 但 size < 4096
	ErrFlagNotAllowed     = errors.New("gate: flag not allowed")             // 标记位不在 allow 之内
	ErrCipherRequired     = errors.New("gate: cipher required")              // 配了 Cipher 却收到 e = 0 的帧
	ErrCipherUnavailable  = errors.New("gate: cipher unavailable")           // 没配 Cipher 却收到 e = 1 的帧
	ErrDecryptFailed      = errors.New("gate: decrypt failed")               // Open 返回错误
	ErrDecompressLimit    = errors.New("gate: decompressed size over limit") // 解压输出超过上限
	ErrTrailingBytes      = errors.New("gate: compound trailing bytes")      // compound 切不干净
)

// wrapProtocol 把 codec / ws 层的具体错误包进 ErrProtocol，
// 使 errors.Is(reason, ErrProtocol) 与 errors.Is(reason, 具体错误) 同时成立。
// 只在关闭连接的冷路径上调用。
func wrapProtocol(cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrProtocol, cause)
}
