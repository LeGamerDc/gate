package gate

import (
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
)

const (
	maxMessageSize = 32 * 1024 * 1024
)

type Conn struct {
	conn gnet.Conn

	remoteIp   net.IP
	remotePort int
	sender     SenderI
	cipher     Cipher
	handler    ConnHandler

	blocking atomic.Bool
}

func (c *Conn) onTraffic() {
	var (
		msg   []byte
		clean func()
		ok    = true
	)
	for ok {
		if c.blocking.Load() {
			break
		}
		if msg, clean, ok = c.read(); ok {
			func() {
				defer clean()
				c.handler.Handle(msg)
			}()
		}
	}
}

func (c *Conn) read() (data []byte, clean func(), ok bool) {
	var (
		buf    []byte
		e      error
		header = 2
	)
	if buf, e = c.conn.Peek(2); e != nil {
		return
	}
	// 接收侧协议只支持解帧和可选解密，client 不允许上行 z/c 标记。
	size, m, _, _, en := parseHeader([2]byte(buf))
	if m {
		if buf, e = c.conn.Peek(4); e != nil {
			return
		}
		size = parseHeader2([2]byte(buf[2:]), size)
		header = 4
	}
	if size > maxMessageSize {
		logErr(c.conn.Close())
		return
	}
	if buf, e = c.conn.Peek(header + size); e != nil {
		return
	}
	data = buf[header : header+size]
	clean = func() {
		c.conn.Discard(header + size)
	}
	if en && c.cipher != nil {
		c.cipher.Decrypt(data)
	}
	return data, clean, true
}

// AsyncDo 阻塞 connection 继续处理消息，直到 f 完成。
// 对于一些有限制串行的消息有用。阻塞期间数据仍保留在 gnet 的连接缓冲区中，不会丢失；
// f 完成后会通过 Wake 继续消费这些消息。
func (c *Conn) AsyncDo(f func()) {
	c.blocking.Store(true)
	go func() {
		defer func() {
			c.blocking.Store(false)
			logErr(c.conn.Wake(nil))
			if r := recover(); r != nil {
				fmt.Printf("[gate] AsyncDo panic: %v\n%s\n", r, debug.Stack())
			}
		}()
		f()
	}()
}

func (c *Conn) Send(data []byte) error {
	return c.sender.Send(data)
}

// SendNoEncrypt 不启用加密
func (c *Conn) SendNoEncrypt(data []byte) error {
	return c.sender.SendNoEncrypt(data)
}

// SendShared 发送共享只读数据，不允许后续压缩、合包或加密。
// alreadyCompressed=true 时仅携带压缩标记，gate 不会改写 data。
func (c *Conn) SendShared(data []byte, alreadyCompressed bool) error {
	return c.sender.SendShared(data, alreadyCompressed)
}

func (c *Conn) UpdateCipher(cipher Cipher) {
	c.cipher = cipher
}

func (c *Conn) RemoteIp() string {
	return c.remoteIp.String()
}

func (c *Conn) RemotePort() int {
	return c.remotePort
}

func (c *Conn) Remote() string {
	return c.remoteIp.String() + ":" + strconv.Itoa(c.remotePort)
}

func (c *Conn) Close() {
	logErr(c.conn.Close())
}

func logErr(err error) {
	if err != nil {
		log.Errorf("[gate] %v", err)
	}
}
