package gate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketClientConfig controls how a Client connects to a websocket gate
// server.
type WebSocketClientConfig struct {
	URL     string
	Handler ClientHandler
	Cipher  Cipher
	// MaxMessageSize 同 ClientConfig.MaxMessageSize：<=0 时取默认值 32MB，
	// 超过 32MB 会被拒绝（校验统一由 ClientConfig.purge 完成）。
	MaxMessageSize int
	SendQueueSize  int // default: 1024
	Header         http.Header
	Dialer         *websocket.Dialer
}

// purge 把校验完全委托给 ClientConfig.purge，这样两种传输不会出现两套上限。
func (c *WebSocketClientConfig) purge() error {
	if c.URL == "" {
		return errors.New("websocket client url must set")
	}
	clientCfg := ClientConfig{
		Addr:           c.URL,
		Handler:        c.Handler,
		Cipher:         c.Cipher,
		MaxMessageSize: c.MaxMessageSize,
		SendQueueSize:  c.SendQueueSize,
	}
	if err := clientCfg.purge(); err != nil {
		return err
	}
	c.MaxMessageSize = clientCfg.MaxMessageSize
	c.SendQueueSize = clientCfg.SendQueueSize
	return nil
}

// DialWebSocketContext creates a websocket-backed gate client and starts the
// same background read/write loops as DialContext.
func DialWebSocketContext(ctx context.Context, cfg *WebSocketClientConfig) (*Client, error) {
	if cfg == nil {
		cfg = &WebSocketClientConfig{}
	}
	if err := cfg.purge(); err != nil {
		return nil, err
	}

	dialer := cloneWebSocketDialer(cfg.Dialer)
	conn, resp, err := dialer.DialContext(ctx, cfg.URL, cfg.Header)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	client, err := newClientWithConn(newWebSocketNetConn(conn), &ClientConfig{
		Addr:           cfg.URL,
		Handler:        cfg.Handler,
		Cipher:         cfg.Cipher,
		MaxMessageSize: cfg.MaxMessageSize,
		SendQueueSize:  cfg.SendQueueSize,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func cloneWebSocketDialer(dialer *websocket.Dialer) *websocket.Dialer {
	if dialer == nil {
		clone := *websocket.DefaultDialer
		return &clone
	}
	clone := *dialer
	return &clone
}

type webSocketNetConn struct {
	conn   *websocket.Conn
	reader io.Reader
}

func newWebSocketNetConn(conn *websocket.Conn) net.Conn {
	return &webSocketNetConn{conn: conn}
}

func (c *webSocketNetConn) Read(p []byte) (int, error) {
	for {
		if c.reader == nil {
			messageType, reader, err := c.conn.NextReader()
			if err != nil {
				return 0, normalizeWebSocketReadError(err)
			}
			if messageType != websocket.BinaryMessage {
				if _, discardErr := io.Copy(io.Discard, reader); discardErr != nil {
					return 0, discardErr
				}
				return 0, fmt.Errorf("unsupported websocket message type %d", messageType)
			}
			c.reader = reader
		}

		n, err := c.reader.Read(p)
		if err == io.EOF {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *webSocketNetConn) Write(p []byte) (int, error) {
	if err := c.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *webSocketNetConn) Close() error {
	return c.conn.Close()
}

func (c *webSocketNetConn) LocalAddr() net.Addr {
	return c.conn.UnderlyingConn().LocalAddr()
}

func (c *webSocketNetConn) RemoteAddr() net.Addr {
	return c.conn.UnderlyingConn().RemoteAddr()
}

func (c *webSocketNetConn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}

func (c *webSocketNetConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *webSocketNetConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func normalizeWebSocketReadError(err error) error {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway:
			return io.EOF
		}
	}
	return err
}
