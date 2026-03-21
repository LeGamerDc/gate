# gate

`gate` 是一个基于 `gnet` 的轻量 TCP 网关库。它当前做的事情不是业务协议本身，而是把连接管理、拆包、可选加密、发送侧压缩/合包这些通用能力抽成一个可复用组件，让业务只需要实现自己的 `ConnHandler`。

仓库同时带了一套 `benchmark` 示例，用来验证吞吐、延迟、压缩率和合包效果。

## 当前项目做了什么

这个项目目前主要提供 5 类能力：

1. 提供基于事件循环的 TCP 服务端入口。
2. 为每个连接构建统一的 `Conn` 抽象，封装远端地址、发送方法和连接级状态。
3. 定义一套简单的消息头协议，用于表示消息长度，以及是否加密、压缩、合包。
4. 在发送侧实现消息队列、批量发送、可选压缩、可选加密和背压保护。
5. 提供 `benchmark/server` 和 `benchmark/client`，用于回显压测和协议验证。

换句话说，`gate` 更像是“TCP 会话层 / 传输层增强库”，而不是带完整鉴权、路由、协议编解码的成品网关。

## 如何做的

### 1. 启动方式

入口是 `StartEventLoop`。它会：

- 校验 `Config`
- 把调用方传入的 logger 注册给 `gate` 和 `gnet`
- 使用 `gnet.Run` 启动 `tcp://0.0.0.0:<port>` 监听
- 通过 `LoopCount` 控制事件循环数量

对应实现见 [ev.go](/Users/dongcheng/Project/legamerdc/gate/ev.go)。

### 2. 连接生命周期

每次新连接建立时，`OnOpen` 会创建一个 `Conn`：

- 保存底层 `gnet.Conn`
- 记录远端 IP 和端口
- 通过 `SenderBuilder` 构建发送器
- 通过 `ConnHandlerBuilder` 构建业务处理器
- 把 `Conn` 放进 `gnet.Conn` 的上下文

连接关闭时，会调用业务处理器的 `Close()` 做清理。

这部分在 [ev.go](/Users/dongcheng/Project/legamerdc/gate/ev.go) 和 [conn.go](/Users/dongcheng/Project/legamerdc/gate/conn.go)。

### 3. 收包路径

收到网络数据后，`OnTraffic` 会进入 `Conn.onTraffic()`，持续读取当前连接缓冲区中的完整消息：

- 先读 2 字节基础消息头
- 如果消息长度超过 4KB，则继续读额外 2 字节扩展长度
- 校验单条消息最大不能超过 32MB
- 按消息头裁出 payload
- 如果设置了 `Cipher` 且消息带加密标记，则先解密
- 把 payload 直接交给 `ConnHandler.Handle`

这里有一个很重要的边界：

- 当前接收路径内置支持的是“解帧 + 可选解密”
- 当前协议不允许 client 上行压缩消息或 compound message；也就是说接收侧不处理 `z/c` 标记
- `Handle(raw []byte)` 拿到的是连接读缓冲上的借用数据，如需跨 `Handle` 生命周期使用，业务层需要自行拷贝

对应实现见 [conn.go](/Users/dongcheng/Project/legamerdc/gate/conn.go) 和 [header.go](/Users/dongcheng/Project/legamerdc/gate/header.go)。

### 4. 发包路径

发送路径是这个项目的重点。

业务代码调用：

- `Conn.Send(data)`：允许自动压缩、合包、加密
- `Conn.SendNoEncrypt(data)`：直接发送，不压缩、不加密
- `Conn.SendShared(data, alreadyCompressed)`：发送共享只读数据，`gate` 只负责挂消息头，不会再压缩、合包或加密这块 payload

内部流程如下：

1. 业务 goroutine 调用发送接口，把消息放入连接级队列。
2. 队列首次从空变为非空时，通过 `Wake` 唤醒事件循环。
3. 真正的 TCP 写出在事件循环线程中执行，避免并发直接写 socket。
4. 写出前根据配置决定是否：
   - 单条压缩
   - 多条合并成 compound message 再整体压缩
   - 对 payload 做加密
5. 如果连接的 `OutboundBuffered()` 超过 `MaxBufferSize`，会停止继续发送并记录日志，避免慢连接无限堆积。

对应实现主要在 [sender.go](/Users/dongcheng/Project/legamerdc/gate/sender.go)。

### 5. 为什么能减少系统调用和带宽

发送器通过几个手段提升吞吐：

- `CompressThreshold`：消息超过阈值时才压缩，避免小包压缩得不偿失。
- `MaxClusterSize`：把多条允许合包的消息合成一条 compound message，再统一压缩和发送。
- `sync.Pool` 和 `mcache`：复用队列、buffer 和压缩缓冲区，减少分配。

### 6. 如何处理“当前消息必须串行，且处理过程会阻塞”的场景

`ConnHandler.Handle` 的约定是不要阻塞事件循环。如果某条消息必须串行处理，并且中间会做 RPC、数据库访问或其他阻塞操作，可以调用：

```go
conn.AsyncDo(func() {
    // 阻塞逻辑
})
```

`AsyncDo` 会：

- 暂停该连接继续读后续消息
- 在新 goroutine 中执行业务逻辑
- 逻辑结束后再唤醒连接，继续消费网络数据

这里的“暂停处理”并不意味着丢消息。`gnet` 仍然会把网络数据保留在该连接的读缓冲中，`Wake` 后会继续从缓冲区里往下消费。

这样可以保证“单连接内顺序”不被打乱，同时避免阻塞整个 event loop。

## 消息头协议

消息头在 [header.go](/Users/dongcheng/Project/legamerdc/gate/header.go)。

### 基础格式

- 小于 4KB 的消息使用 2 字节头
- 大于等于 4KB 的消息使用 4 字节头
- 单条消息最大 32MB

### 标记位

- `m`：是否存在扩展长度头
- `z`：payload 是否压缩
- `c`：payload 是否为 compound message
- `e`：payload 是否加密

### compound message

当发送器决定合包时，会把多条子消息按“头 + payload”的形式拼起来，然后把整个 compound payload 统一压缩/加密后发送。

项目里约定：

- compound message 内部的子消息本身不再带压缩、加密、合包标记

## 核心接口

### `Config`

见 [config.go](/Users/dongcheng/Project/legamerdc/gate/config.go)。

| 字段 | 说明 |
| --- | --- |
| `LoopCount` | 事件循环数量，最终会被限制在 `[1, runtime.NumCPU()]` |
| `Port` | 监听端口 |
| `CHB` | `ConnHandlerBuilder`，负责为每个连接创建业务 handler |
| `SB` | `SenderBuilder`，负责为每个连接创建发送器 |
| `Logger` | `gate` 和 `gnet` 共用的 logger |

### `SenderConfig`

见 [sender.go](/Users/dongcheng/Project/legamerdc/gate/sender.go)。

| 字段 | 说明 |
| --- | --- |
| `CompressThreshold` | 压缩阈值，`<= 0` 表示关闭自动压缩 |
| `MaxBufferSize` | 单连接最大待发送缓冲，超过后会触发丢弃保护 |
| `MaxClusterSize` | 合包后的最大大小，`<= 0` 表示关闭合包 |

默认发送器 `DefaultSenderBuilder` 的配置为：

- 压缩阈值 `1KB`
- 最大缓冲 `2MB`
- 最大合包 `32KB`

### 业务接口

见 [i.go](/Users/dongcheng/Project/legamerdc/gate/i.go)。

```go
type ConnHandler interface {
    Handle(raw []byte)
    Close()
}

type ConnHandlerBuilder interface {
    Build(*Conn) ConnHandler
}

type Cipher interface {
    Encrypt([]byte)
    Decrypt([]byte)
}
```

设计思路很直接：

- `gate` 负责连接和收发通道
- 业务层负责协议编解码、状态机和业务逻辑
- 加密策略通过 `Cipher` 注入

## 最小使用示例

下面这个示例和 `benchmark/server` 一样，做的是一个简单回显服务：

```go
package main

import (
    "fmt"

    "github.com/LeGamerDc/gate"
)

type logger struct{}

func (l *logger) Debugf(format string, args ...interface{}) { fmt.Printf(format+"\n", args...) }
func (l *logger) Infof(format string, args ...interface{})  { fmt.Printf(format+"\n", args...) }
func (l *logger) Warnf(format string, args ...interface{})  { fmt.Printf(format+"\n", args...) }
func (l *logger) Errorf(format string, args ...interface{}) { fmt.Printf(format+"\n", args...) }
func (l *logger) Fatalf(format string, args ...interface{}) { fmt.Printf(format+"\n", args...) }

type handlerBuilder struct{}

func (hb *handlerBuilder) Build(conn *gate.Conn) gate.ConnHandler {
    return &handler{conn: conn}
}

type handler struct {
    conn *gate.Conn
}

func (h *handler) Handle(raw []byte) {
    _ = h.conn.Send(raw)
}

func (h *handler) Close() {}

func main() {
    _ = gate.StartEventLoop(&gate.Config{
        LoopCount: 1,
        Port:      8081,
        CHB:       &handlerBuilder{},
        SB:        gate.DefaultSenderBuilder,
        Logger:    &logger{},
    })
}
```

## Benchmark 目录说明

### `benchmark/server`

- 启动一个基于 `gate` 的回显服务
- 通过 flag 控制 `LoopCount`、压缩阈值、合包大小、是否加密
- 可选开启 `pprof`

运行：

```bash
go run ./benchmark/server \
  -addr 127.0.0.1:8081 \
  -loops 4 \
  -compress-threshold 1024 \
  -max-cluster-size 32768 \
  -pprof-addr 127.0.0.1:8520
```

常用 flag：

- `-compress-threshold <= 0`：关闭压缩
- `-max-cluster-size <= 0`：关闭 compound 合包
- `-encrypt`：开启请求/回包加密

### `benchmark/client`

- 启动多个 TCP 客户端并持续发送请求
- 校验 server 回包内容
- 每秒打印一组统计信息：
  - `tx/rx msg/s`
  - `raw MiB/s`
  - `wire MiB/s`
  - `wire/raw ratio`
  - `compound` 聚合度（业务消息数 / 外层 frame 数）
  - `avg/p50/p95/p99` RTT

运行：

```bash
go run ./benchmark/client \
  -addr 127.0.0.1:8081 \
  -connections 1000 \
  -payload-size 256 \
  -duration 15s \
  -warmup 3s \
  -payload-mode repeat
```

常用 flag：

- `-rate 0`：打满发送
- `-inflight`：限制每个连接的在途请求数，避免无限堆积
- `-payload-mode repeat|random`：分别用于观察高压缩率和低压缩率场景
- `-encrypt`：与 server 侧加密配置配套使用

### `go test -bench`

仓库还提供了协议和 client 关键路径的微基准，可直接运行：

```bash
go test -run '^$' -bench . -benchmem
```

## 当前实现边界

基于当前代码，这个项目已经适合做高性能 TCP 服务的底层连接层，但仍有一些明确边界：

- 没有内置上层协议编解码，业务拿到的是 `[]byte`
- 没有内置鉴权、路由、心跳、连接注册或房间广播语义
- 接收侧协议不支持 client 上行压缩消息或 compound message
- 仓库中的示例主要是回显和压测，不是完整业务服务

如果后续继续演进，这个库最自然的方向通常是：

- 增加连接级元数据和 session 管理
- 抽象统一的 codec 层
- 补充生产环境示例和测试用例

## 验证

当前仓库已通过：

```bash
go test ./...
```
