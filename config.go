package gate

import "time"

// Unlimited 显式关闭某项保护性上限。
// 保护性选项（MaxBuffer、StallTimeout、MaxOutboundBytes …）的零值是默认值而不是
// 「关闭」——忘了配压缩只是慢一点，忘了配出站上限是 OOM，保护必须是选择退出的。
const Unlimited = -1

// DisableKeepAlive 关闭 SO_KEEPALIVE（Limits.KeepAlive 的哨兵值）。
const DisableKeepAlive time.Duration = -1

// CompressLevel 决定 zstd 编码等级，同时决定每 loop 编码器的常驻内存（量级差 16 倍）。
type CompressLevel uint8

const (
	CompressFastest CompressLevel = iota // 零值，也是推荐值
	CompressBalanced
	CompressBetter
	CompressBest
)

// Outbound 是发送侧配置。见 01「配置」与 03。
type Outbound struct {
	CompressThreshold int           // <=0 不压缩；独立帧按 payload 判定，合并帧按整批字节数判定
	CompressLevel     CompressLevel // 零值 CompressFastest
	Dict              []byte        // 可选 zstd 训练字典
	MaxCluster        int           // <=0 不合包；单个合并帧上限（含子 header），大于协议上限会被夹住
	MaxBuffer         int           // 出站积压准入上限；0 用默认 1MB，Unlimited 关闭
	HighWater         int           // 软上限，越过后 Writable() 返回 false；0 ⇒ MaxBuffer/4
	StallTimeout      time.Duration // 积压非空且这么久没写出一个字节 ⇒ 关闭；0 用默认 30s，Unlimited 关闭
	CloseLinger       time.Duration // Close 之后为排空出站队列最多再等多久；0 用默认 1s，Unlimited 表示不等待、立即关闭
}

// DefaultOutbound 返回推荐配置。是函数而非可变全局量——包级变量可以被任意一个
// import 了 gate 的包改掉。
func DefaultOutbound() Outbound {
	return Outbound{
		MaxBuffer:    1 << 20,
		HighWater:    1 << 18,
		StallTimeout: 30 * time.Second,
		CloseLinger:  time.Second,
	}
}

// Limits 是保护性上限。见 01「配置」。
type Limits struct {
	MaxMessage       int           // 单条入站消息上限；0 用协议上限 32MB
	MaxPending       int           // Pause 期间已读出未投递的字节上限；0 ⇒ MaxMessage + 64KB
	MaxConns         int           // 每 server 并发连接上限；0 不限
	MaxOutboundBytes int           // 每 server 出站积压总预算（近似口径）；0 用默认 1GB，Unlimited 关闭
	MaxHandshaking   int           // 握手阶段连接上限；0 ⇒ MaxConns/16
	HandshakeTimeout time.Duration // 0 用默认 10s
	MaxPause         time.Duration // 单次 Pause 最长时长；0 用默认 60s
	Idle             time.Duration // 0 不启用
	KeepAlive        time.Duration // 0 用默认 60s；DisableKeepAlive 关闭
}

// Socket 是 setsockopt 层配置。
type Socket struct {
	RecvBuffer int  // 0 不设置（保留内核自动调优）
	SendBuffer int  // 0 不设置
	Nagle      bool // 零值即 TCP_NODELAY；置 true 才打开 Nagle
}
