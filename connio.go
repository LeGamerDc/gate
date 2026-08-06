package gate

// connIO 是连接级 syscall 的接缝（07 第 0 层）：生产实现直通 read/writev/close，
// 测试实现可编程——部分写可以停在任意字节位置、可以注入 EAGAIN / 真错误 / EOF。
// 接口分派的纳秒级成本包在微秒级的 syscall 外面，在噪声里。
type connIO interface {
	read(fd int, p []byte) (int, error)
	writev(fd int, vec [][]byte) (int, error)
	close(fd int) error
}
