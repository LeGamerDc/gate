package gate

import "github.com/gobwas/ws/wsutil"

type wsOutbound struct {
	conn interface {
		Write([]byte) (int, error)
	}
}

func (w *wsOutbound) write(data []byte) error {
	return wsutil.WriteServerBinary(w.conn, data)
}

func (w *wsOutbound) writev(vb [][]byte) error {
	if len(vb) == 0 {
		return nil
	}
	buf := getBuffer()
	defer putBuffer(buf)
	for _, b := range vb {
		buf.Write(b)
	}
	return wsutil.WriteServerBinary(w.conn, buf.Bytes())
}
