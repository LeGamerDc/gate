package gate

// Cipher 是可选的逐连接加密接口。实现契约见 01「Cipher」：
//
//   - Overhead() 是常量，不随调用变化；0 表示长度保持。
//   - Seal 返回长度必须恰好是 len(plaintext) + Overhead()。
//   - Open 返回长度 <= len(ciphertext) - Overhead()；认证失败返回 error。
//   - aad 是该帧最终的 2/4 字节帧头。AEAD 实现必须把它纳入认证
//     （照 crypto/cipher.AEAD 的语义）；Overhead() == 0 的实现可以忽略它，
//     代价是得不到帧头认证。
//   - dst 由 gate 保证容量足够。Overhead() == 0 时 gate 传 dst = src[:0]，
//     允许原地覆写；除此之外 dst 与 src 不重叠。
//   - 收发方向必须使用各自独立的 nonce 序列。
//   - 同一实例只被一个事件循环触碰，实现内部不需要同步。
type Cipher interface {
	Overhead() int
	Seal(dst, plaintext, aad []byte) []byte
	Open(dst, ciphertext, aad []byte) ([]byte, error)
}
