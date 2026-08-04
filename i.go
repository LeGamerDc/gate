package gate

import "errors"

var (
	// ErrConnClosed 连接已经关闭，消息未能入队。
	//
	// 在此之前 Conn.Send 在连接关闭后仍然返回 nil，业务层拿不到任何断线信号，
	// 并且消息会无限堆积在一个永远不会被 flush 的队列里。
	ErrConnClosed = errors.New("gate: connection closed")
	// ErrSendQueueFull 连接的待发送缓冲已达上限，消息被拒绝。
	ErrSendQueueFull = errors.New("gate: send queue full")
)

// SenderI is the per-connection outbound sender abstraction.
type SenderI interface {
	// Send copies data before enqueueing it. Callers may reuse data after
	// Send returns.
	Send([]byte) error
	// SendNoEncrypt copies data before enqueueing it and always sends
	// plaintext standalone frames.
	SendNoEncrypt([]byte) error
	// SendStatic keeps the caller-owned buffer by reference and never mutates
	// it: no compression, no compounding, no encryption.
	//
	// 契约是"data 必须永久不可变"，而不是"直到 flush 完成"——gate 没有任何
	// 完成回调，调用方无从得知 flush 何时发生，所以只有不可变的数据
	// （例如预先编码好的固定回包）才适合走这个接口。
	SendStatic(data []byte, alreadyCompressed bool) error

	// Close 释放 sender 持有的资源，并丢弃尚未 flush 的消息。
	// 由 gate 在连接关闭时调用，业务层不应直接调用。
	Close()
}

type SenderBuilder interface {
	Build(*Conn) SenderI
}

type Cipher interface {
	// Encrypt data, must not panic
	Encrypt([]byte)
	// Decrypt data, must not panic
	Decrypt([]byte)
}

type ConnHandler interface {
	// Handle 处理 client 发送的消息，Handle内部不应该阻塞。
	// 如果存在阻塞性任务（如rpc访问其他服务）应当调用 conn.AsyncDo
	// 在 Handle 结束后不允许再持有 raw
	// raw 可以直接传给 conn.Send/conn.SendNoEncrypt，这两个接口会立即复制数据；
	// 如果要传给 conn.SendStatic，则业务层需要自行持有一份永久只读的副本。
	//
	// Handle 内部的 panic 会被 gate 捕获并只关闭当前连接，不会影响其他连接。
	Handle(raw []byte)
	Close()
}

// ReadyHandler 是 ConnHandler 的可选扩展。实现了它的 handler 会在连接真正可以
// 发送数据时收到 OnReady：TCP 下是连接建立之后立即触发，WebSocket 下是握手
// 完成之后触发。
//
// 需要在连接建立时主动下发数据（握手包、欢迎消息等）的 handler 必须用 OnReady，
// 而不能在 ConnHandlerBuilder.Build 里直接发：WebSocket 模式下那时握手响应还
// 没写完，一个 WS 数据帧插进去会直接让连接作废。
type ReadyHandler interface {
	OnReady()
}

type ConnHandlerBuilder interface {
	Build(*Conn) ConnHandler
}
