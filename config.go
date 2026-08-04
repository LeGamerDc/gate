package gate

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/panjf2000/gnet/v2/pkg/logging"
)

const (
	defaultWebSocketPath             = "/"
	defaultMaxWebSocketHandshakeSize = 16 * 1024
	defaultMaxWebSocketBufferedSize  = maxMessageSize + 64*1024
	defaultTCPKeepAlive              = time.Minute
	minIdleCheckInterval             = time.Second
	maxIdleCheckInterval             = 30 * time.Second

	// minTCPKeepAlive 是 TCPKeepAlive 允许的最小值。
	//
	// gnet 在未显式设置 TCPKeepInterval 时会取 TCPKeepAlive/5 作为 TCP_KEEPINTVL，
	// 再用 int(d.Seconds()) 截断成秒。任何小于 5s 的值都会算出 0，而 socket 层
	// 明确拒绝 intvl<=0，结果是 keep-alive 静默不生效（darwin/BSD 上还会打一条
	// setsockopt 错误日志）。与其让它悄悄失效，不如在配置校验阶段直接报错。
	minTCPKeepAlive = 5 * time.Second
)

// DisableKeepAlive 赋给 Config.TCPKeepAlive 可以关闭 TCP keep-alive。
// 默认是开启的（60s）：没有 keep-alive 时，被 NAT 静默丢弃的半开连接会一直
// 占着缓冲区和 handler，永远不会触发 OnClose。
const DisableKeepAlive = time.Duration(-1)

// ServerTransport selects the outer transport used by the server.
type ServerTransport uint8

const (
	ServerTransportTCP ServerTransport = iota
	ServerTransportWebSocket
)

// Config controls how a gate server is created.
type Config struct {
	LoopCount int             // 设置几个epoll loop用于处理网络消息，[1, runtime.NumCPU()]
	Addr      string          // 设置监听地址，例如 127.0.0.1:8081；为空时回退到 Port
	Port      int             // 兼容旧接口，Addr 为空时使用 0.0.0.0:<Port>
	Transport ServerTransport // 传输封装，默认 TCP；设置为 WebSocket 时启动 WS 兼容模式

	// MaxMessageSize 单条入站消息的字节上限，<=0 时使用默认值 32MB。
	//
	// 这个值决定了单个连接能让服务端缓冲多少数据。绝大多数业务的单条消息远小于
	// 32MB，把它收紧到实际需要的量级（例如 64KB）可以显著降低被慢速大包打爆内存
	// 的风险。
	//
	// 注意它还兼了第二个用途：AsyncDo 挂起期间，整条连接的入站积压上限是
	// MaxMessageSize + 64KB，超过就关连接（见 Conn.onTraffic）。挂起时 gate 不
	// 消费任何数据，而 gnet 会继续往连接缓冲里追加，没有上限的话就是一个可远程
	// 触发的 OOM。所以如果业务的 AsyncDo 较慢、同时客户端还在持续发小包，
	// 这个值要按"阻塞期间可能积压多少"来配，而不只是按单条消息的大小。
	MaxMessageSize int
	// MaxConnections 最大并发连接数，<=0 表示不限制。超过后新连接会被立即关闭。
	MaxConnections int
	// TCPKeepAlive 开启 SO_KEEPALIVE 并设置空闲探测时间，<=0 时使用默认值 60s；
	// 传 DisableKeepAlive 关闭。
	TCPKeepAlive time.Duration
	// IdleTimeout 超过该时长没有**交付过完整消息**的连接会被主动关闭，
	// <=0 表示不启用。
	//
	// 判据是"最后一次把一条完整消息交给 handler 的时刻"，不是"最后一次收到
	// 字节"。两个直接后果：只发半个帧吊着连接的 slowloris 会被回收；反过来，
	// 服务端单向下推不会让连接显得活跃。
	//
	// 与 TCPKeepAlive 互补：keep-alive 处理的是对端消失，IdleTimeout 处理的是
	// 对端还在但已经不再是有效会话。
	IdleTimeout time.Duration

	WebSocketPath              string // WebSocket 握手路径，默认 "/"
	MaxWebSocketHandshakeBytes int    // WebSocket 握手阶段允许缓冲的最大字节数，<=0 时使用默认值 16KB
	MaxWebSocketBufferedBytes  int    // WebSocket 单连接允许缓冲的最大原始/解帧字节数，<=0 时使用默认值 32MB+64KB
	// OnWebSocketUpgrade 在 WebSocket 握手完成前被调用，可以读取请求 URI 和
	// HTTP 头做鉴权、取 X-Forwarded-For 等；返回非 nil 会拒绝这次握手。
	// 只在 Transport 为 WebSocket 时生效。
	OnWebSocketUpgrade func(*Handshake) error

	CHB    ConnHandlerBuilder // 设置消息处理逻辑的Builder，不能为空
	SB     SenderBuilder      // 设置发送逻辑，建议使用DefaultSenderBuilder，不能为空
	Logger logging.Logger     // 给gate以及gnet提供日志接口，不能为空
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

	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = maxMessageSize
	}
	if c.MaxMessageSize > maxMessageSize {
		return fmt.Errorf("max message size %d exceeds protocol ceiling %d", c.MaxMessageSize, maxMessageSize)
	}
	if c.TCPKeepAlive == 0 {
		c.TCPKeepAlive = defaultTCPKeepAlive
	}
	if c.TCPKeepAlive > 0 && c.TCPKeepAlive < minTCPKeepAlive {
		return fmt.Errorf("tcp keep-alive %v is below the minimum %v; use DisableKeepAlive to turn it off",
			c.TCPKeepAlive, minTCPKeepAlive)
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
			c.MaxWebSocketBufferedBytes = c.MaxMessageSize + 64*1024
		}
		if c.MaxWebSocketBufferedBytes < c.MaxWebSocketHandshakeBytes {
			c.MaxWebSocketBufferedBytes = c.MaxWebSocketHandshakeBytes
		}
	}
	return nil
}

// keepAlive 返回传给 gnet 的 SO_KEEPALIVE 时长，0 表示不启用。
func (c *Config) keepAlive() time.Duration {
	if c.TCPKeepAlive < 0 {
		return 0
	}
	return c.TCPKeepAlive
}

// idleCheckInterval 返回空闲连接的扫描周期。取 IdleTimeout 的 1/4 并夹在
// [1s, 30s]，这样最坏情况下连接会在 IdleTimeout 之后再多活四分之一个周期，
// 同时避免在 IdleTimeout 很大时把扫描拉得过稀。
func (c *Config) idleCheckInterval() time.Duration {
	if c.IdleTimeout <= 0 {
		return 0
	}
	d := c.IdleTimeout / 4
	if d < minIdleCheckInterval {
		d = minIdleCheckInterval
	}
	if d > maxIdleCheckInterval {
		d = maxIdleCheckInterval
	}
	return d
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
