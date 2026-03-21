package gate

import "errors"

var (
	ErrMaxMessageSize = errors.New("message size > 32MB")
)

type SenderI interface {
	Send([]byte) error
	SendNoEncrypt([]byte) error
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
	Handle(raw []byte)
	Close()
}

type ConnHandlerBuilder interface {
	Build(*Conn) ConnHandler
}
