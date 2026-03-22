package gate

import (
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/panjf2000/gnet/v2/pkg/logging"
)

const (
	defaultWebSocketPath             = "/"
	defaultMaxWebSocketHandshakeSize = 16 * 1024
	defaultMaxWebSocketBufferedSize  = maxMessageSize + 64*1024
)

// ServerTransport selects the outer transport used by the server.
type ServerTransport uint8

const (
	ServerTransportTCP ServerTransport = iota
	ServerTransportWebSocket
)

// Config controls how a gate server is created.
type Config struct {
	LoopCount                  int                // 设置几个epoll loop用于处理网络消息，[1, runtime.NumCPU()]
	Addr                       string             // 设置监听地址，例如 127.0.0.1:8081；为空时回退到 Port
	Port                       int                // 兼容旧接口，Addr 为空时使用 0.0.0.0:<Port>
	Transport                  ServerTransport    // 传输封装，默认 TCP；设置为 WebSocket 时启动 WS 兼容模式
	WebSocketPath              string             // WebSocket 握手路径，默认 "/"
	MaxWebSocketHandshakeBytes int                // WebSocket 握手阶段允许缓冲的最大字节数，<=0 时使用默认值 16KB
	MaxWebSocketBufferedBytes  int                // WebSocket 单连接允许缓冲的最大原始/解帧字节数，<=0 时使用默认值 32MB+64KB
	CHB                        ConnHandlerBuilder // 设置消息处理逻辑的Builder，不能为空
	SB                         SenderBuilder      // 设置发送逻辑，建议使用DefaultSenderBuilder，不能为空
	Logger                     logging.Logger     // 给gate以及gnet提供日志接口，不能为空
}

func (c *Config) purge() error {
	if c == nil {
		return errors.New("config must set")
	}
	if c.LoopCount < 1 {
		c.LoopCount = 1
	}
	if c.LoopCount > runtime.NumCPU() {
		c.LoopCount = runtime.NumCPU()
	}

	if c.CHB == nil {
		return errors.New("conn handler builder must set")
	}
	if c.SB == nil {
		return errors.New("sender builder must set")
	}
	if c.Logger == nil {
		return errors.New("logger must set")
	}
	if c.Transport == ServerTransportWebSocket {
		if c.WebSocketPath == "" {
			c.WebSocketPath = defaultWebSocketPath
		}
		if !strings.HasPrefix(c.WebSocketPath, "/") {
			c.WebSocketPath = "/" + c.WebSocketPath
		}
		if c.MaxWebSocketHandshakeBytes <= 0 {
			c.MaxWebSocketHandshakeBytes = defaultMaxWebSocketHandshakeSize
		}
		if c.MaxWebSocketBufferedBytes <= 0 {
			c.MaxWebSocketBufferedBytes = defaultMaxWebSocketBufferedSize
		}
		if c.MaxWebSocketBufferedBytes < c.MaxWebSocketHandshakeBytes {
			c.MaxWebSocketBufferedBytes = c.MaxWebSocketHandshakeBytes
		}
	}
	return nil
}

func (c *Config) listenAddr() string {
	if c.Addr != "" {
		return c.Addr
	}
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func (c *Config) protoAddr() string {
	return "tcp://" + c.listenAddr()
}
