package gate

import (
	"fmt"
	"runtime"

	"github.com/panjf2000/gnet/v2/pkg/logging"
)

type Config struct {
	LoopCount int                // 设置几个epoll loop用于处理网络消息，[1, runtime.NumCPU()]
	Addr      string             // 设置监听地址，例如 127.0.0.1:8081；为空时回退到 Port
	Port      int                // 兼容旧接口，Addr 为空时使用 0.0.0.0:<Port>
	CHB       ConnHandlerBuilder // 设置消息处理逻辑的Builder，不能为空
	SB        SenderBuilder      // 设置发送逻辑，建议使用DefaultSenderBuilder，不能为空
	Logger    logging.Logger     // 给gate以及gnet提供日志接口，不能为空
}

func (c *Config) purge() {
	if c.LoopCount < 1 {
		c.LoopCount = 1
	}
	if c.LoopCount > runtime.NumCPU() {
		c.LoopCount = runtime.NumCPU()
	}

	if c.CHB == nil || c.SB == nil {
		panic("builder must set")
	}
	if c.Logger == nil {
		panic("logger must set")
	}
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
