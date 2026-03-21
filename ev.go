package gate

import (
	"net"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

var (
	_   gnet.EventHandler = (*ev)(nil)
	log logging.Logger
)

type ev struct {
	c     *Config
	en    gnet.Engine
	ready chan serverReady
}

func StartEventLoop(c *Config) error {
	s, err := StartServer(c)
	if err != nil {
		return err
	}
	return s.Wait()
}

func (e *ev) OnBoot(en gnet.Engine) (action gnet.Action) {
	e.en = en
	if e.ready != nil {
		addr, err := engineListenAddr(en)
		e.ready <- serverReady{
			engine: en,
			addr:   addr,
			err:    err,
		}
	}
	return gnet.None
}

func (e *ev) OnShutdown(eng gnet.Engine) {}

func (e *ev) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	conn := &Conn{
		conn:       c,
		remoteIp:   c.RemoteAddr().(*net.TCPAddr).IP,
		remotePort: c.RemoteAddr().(*net.TCPAddr).Port,
	}
	conn.sender = e.c.SB.Build(conn)
	conn.handler = e.c.CHB.Build(conn)
	c.SetContext(conn)
	return nil, gnet.None
}

func (e *ev) OnClose(c gnet.Conn, _ error) (action gnet.Action) {
	conn := c.Context().(*Conn)
	conn.handler.Close()
	return gnet.None
}

func (e *ev) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := c.Context().(*Conn)
	conn.onTraffic()
	return gnet.None
}

func (e *ev) OnTick() (delay time.Duration, action gnet.Action) {
	return 0, gnet.None
}
