package gate

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	ErrClientClosed   = errors.New("client closed")
	ErrCipherRequired = errors.New("cipher required")
)

// ClientConfig controls how a Client connects and processes messages.
type ClientConfig struct {
	Addr           string
	Handler        ClientHandler
	Cipher         Cipher
	MaxMessageSize int // default: 32MB
	SendQueueSize  int // default: 1024
}

func (c *ClientConfig) purge() error {
	if c.Addr == "" {
		return errors.New("client addr must set")
	}
	if c.Handler == nil {
		return errors.New("client handler must set")
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = maxMessageSize
	}
	if c.SendQueueSize <= 0 {
		c.SendQueueSize = 1024
	}
	return nil
}

// ClientHandler receives connection lifecycle callbacks and decoded messages.
type ClientHandler interface {
	OnConnect(*Client)
	OnMessage(*Client, []byte)
	OnClose(*Client, error)
}

type outboundMsg struct {
	data    []byte
	encrypt bool
}

// Client is a simple reference gate client built on top of net.Conn.
//
// Outbound messages are always sent as standalone frames: no compression and
// no compound packing. Inbound messages fully support decrypt/decompress/
// uncompound before being delivered to the handler.
type Client struct {
	conn           net.Conn
	handler        ClientHandler
	maxMessageSize int
	sendCh         chan outboundMsg
	closed         chan struct{}
	done           chan struct{}
	dec            *zstd.Decoder

	cipherMu sync.RWMutex
	cipher   Cipher

	closeOnce sync.Once
	closeMu   sync.Mutex
	closeErr  error
	wg        sync.WaitGroup
}

// DialContext creates a client connection and starts background read/write
// loops.
func DialContext(ctx context.Context, cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	if err := cfg.purge(); err != nil {
		return nil, err
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	client, err := newClientWithConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func newClientWithConn(conn net.Conn, cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("client config must set")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn:           conn,
		handler:        cfg.Handler,
		maxMessageSize: cfg.MaxMessageSize,
		sendCh:         make(chan outboundMsg, cfg.SendQueueSize),
		closed:         make(chan struct{}),
		done:           make(chan struct{}),
		dec:            dec,
		cipher:         cfg.Cipher,
	}

	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	go c.waitLoop()

	c.handler.OnConnect(c)
	return c, nil
}

func (c *Client) waitLoop() {
	c.wg.Wait()
	c.dec.Close()
	c.handler.OnClose(c, c.err())
	close(c.done)
}

func (c *Client) readLoop() {
	defer c.wg.Done()

	for {
		frame, err := readFrame(c.conn, c.maxMessageSize)
		if err != nil {
			if c.isClosed() {
				return
			}
			c.closeWithError(err)
			return
		}
		if err := c.handleFrame(frame); err != nil {
			c.closeWithError(err)
			return
		}
	}
}

func (c *Client) writeLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closed:
			return
		case msg := <-c.sendCh:
			if c.isClosed() {
				return
			}
			frame, err := c.encodeOutbound(msg)
			if err != nil {
				c.closeWithError(err)
				return
			}
			if err := writeAll(c.conn, frame); err != nil {
				if c.isClosed() {
					return
				}
				c.closeWithError(err)
				return
			}
		}
	}
}

// Send sends exactly one gate frame. The client never compresses or compounds
// outbound messages.
func (c *Client) Send(data []byte) error {
	return c.send(data, true)
}

// SendNoEncrypt sends exactly one plaintext gate frame.
func (c *Client) SendNoEncrypt(data []byte) error {
	return c.send(data, false)
}

func (c *Client) send(data []byte, encrypt bool) error {
	if len(data) > c.maxMessageSize {
		return ErrMaxMessageSize
	}
	if c.isClosed() {
		return ErrClientClosed
	}

	msg := outboundMsg{
		data:    append([]byte(nil), data...),
		encrypt: encrypt,
	}

	select {
	case <-c.closed:
		return ErrClientClosed
	case c.sendCh <- msg:
		return nil
	}
}

// UpdateCipher swaps the cipher used by subsequent inbound/outbound encrypted
// frames.
func (c *Client) UpdateCipher(cipher Cipher) {
	c.cipherMu.Lock()
	c.cipher = cipher
	c.cipherMu.Unlock()
}

func (c *Client) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the connection and stops background loops.
func (c *Client) Close() error {
	c.closeWithError(nil)
	return nil
}

// Wait blocks until the client has fully stopped.
func (c *Client) Wait() error {
	<-c.done
	return c.err()
}

func (c *Client) handleFrame(frame inboundFrame) error {
	data := frame.payload
	if frame.e {
		cipher := c.getCipher()
		if cipher == nil {
			return ErrCipherRequired
		}
		cipher.Decrypt(data)
	}
	if frame.z {
		var err error
		data, err = c.dec.DecodeAll(data, nil)
		if err != nil {
			return err
		}
	}
	if !frame.c {
		c.handler.OnMessage(c, append([]byte(nil), data...))
		return nil
	}

	for len(data) > 0 {
		sub, n, err := consumeFrame(data, c.maxMessageSize)
		if err != nil {
			return err
		}
		if err := c.handleFrame(sub); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (c *Client) encodeOutbound(msg outboundMsg) ([]byte, error) {
	if len(msg.data) > c.maxMessageSize {
		return nil, ErrMaxMessageSize
	}

	flag := byte(0)
	if msg.encrypt {
		if cipher := c.getCipher(); cipher != nil {
			cipher.Encrypt(msg.data)
			flag |= maskE
		}
	}

	var header [4]byte
	n := encodeHeader(header[:], len(msg.data))
	header[0] |= flag

	frame := make([]byte, n+len(msg.data))
	copy(frame, header[:n])
	copy(frame[n:], msg.data)
	return frame, nil
}

func (c *Client) getCipher() Cipher {
	c.cipherMu.RLock()
	defer c.cipherMu.RUnlock()
	return c.cipher
}

func (c *Client) closeWithError(err error) {
	c.closeOnce.Do(func() {
		c.setErr(err)
		close(c.closed)
		if closeErr := c.conn.Close(); err == nil && closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			c.setErr(closeErr)
		}
	})
}

func (c *Client) setErr(err error) {
	c.closeMu.Lock()
	c.closeErr = err
	c.closeMu.Unlock()
}

func (c *Client) err() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closeErr
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

type inboundFrame struct {
	payload []byte
	z       bool
	c       bool
	e       bool
}

func readFrame(r io.Reader, maxSize int) (inboundFrame, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:2]); err != nil {
		return inboundFrame{}, err
	}

	size, m, z, c, e := parseHeader([2]byte{header[0], header[1]})
	if m {
		if _, err := io.ReadFull(r, header[2:4]); err != nil {
			return inboundFrame{}, err
		}
		size = parseHeader2([2]byte{header[2], header[3]}, size)
	}
	if size > maxSize {
		return inboundFrame{}, ErrMaxMessageSize
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return inboundFrame{}, err
	}

	return inboundFrame{payload: payload, z: z, c: c, e: e}, nil
}

func consumeFrame(data []byte, maxSize int) (inboundFrame, int, error) {
	if len(data) < 2 {
		return inboundFrame{}, 0, io.ErrUnexpectedEOF
	}

	size, m, z, c, e := parseHeader([2]byte{data[0], data[1]})
	header := 2
	if m {
		if len(data) < 4 {
			return inboundFrame{}, 0, io.ErrUnexpectedEOF
		}
		size = parseHeader2([2]byte{data[2], data[3]}, size)
		header = 4
	}
	if size > maxSize {
		return inboundFrame{}, 0, ErrMaxMessageSize
	}
	if len(data) < header+size {
		return inboundFrame{}, 0, io.ErrUnexpectedEOF
	}

	return inboundFrame{
		payload: data[header : header+size],
		z:       z,
		c:       c,
		e:       e,
	}, header + size, nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
