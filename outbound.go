package gate

type outboundWriter interface {
	write([]byte) error
	writev([][]byte) error
}

func (c *Conn) writeOutbound(data []byte) error {
	if c.outbound != nil {
		return c.outbound.write(data)
	}
	_, err := c.conn.Write(data)
	return err
}

func (c *Conn) writevOutbound(vb [][]byte) error {
	if c.outbound != nil {
		return c.outbound.writev(vb)
	}
	_, err := c.conn.Writev(vb)
	return err
}
