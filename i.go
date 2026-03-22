package gate

import "errors"

var (
	ErrMaxMessageSize = errors.New("message size > 32MB")
)

// SenderI is the per-connection outbound sender abstraction.
type SenderI interface {
	// Send copies data before enqueueing it. Callers may reuse data after
	// Send returns.
	Send([]byte) error
	// SendNoEncrypt copies data before enqueueing it and always sends
	// plaintext standalone frames.
	SendNoEncrypt([]byte) error
	// SendShared keeps the caller-owned buffer by reference. The payload must
	// remain read-only until it has been flushed.
	SendShared(data []byte, alreadyCompressed bool) error

	//OnTraffic()
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
	// 如果要传给 conn.SendShared，则业务层需要自行持有只读副本。
	Handle(raw []byte)
	Close()
}

type ConnHandlerBuilder interface {
	Build(*Conn) ConnHandler
}
