package gate

type outboundWriter interface {
	write([]byte) error
	writev([][]byte) error
}

// writeOutbound / writevOutbound 是所有出站数据离开 gate 的唯一两个出口。
//
// writable() 检查放在这里而不是放在 sender 里，是为了让"连接已关闭就不能再写"
// 成为结构性保证：sender（以及未来任何自定义 SenderI 实现）无论怎么写，都不可能
// 绕过这个闸口把数据送到一个已经被回收、并且可能已被新连接复用的 fd 上。
func (c *Conn) writeOutbound(data []byte) error {
	if !c.writable() {
		return ErrConnClosed
	}
	if c.outbound != nil {
		return c.outbound.write(data)
	}
	_, err := c.conn.Write(data)
	return err
}

func (c *Conn) writevOutbound(vb [][]byte) error {
	if !c.writable() {
		return ErrConnClosed
	}
	if c.outbound != nil {
		return c.outbound.writev(vb)
	}
	_, err := c.conn.Writev(vb)
	return err
}

// outboundBuffered 返回底层连接尚未写出的字节数；连接不可写时返回 0，
// 避免在已释放的 outboundBuffer 上取值。
func (c *Conn) outboundBuffered() int {
	if !c.writable() {
		return 0
	}
	return c.conn.OutboundBuffered()
}
