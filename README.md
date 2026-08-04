# gate

`gate` 是一个基于 `gnet` 的轻量 TCP / WebSocket 网关库。它做的不是业务协议本身，而是把连接管理、拆包、可选加密、发送侧压缩/合包这些通用能力抽成一个可复用组件，让业务只需要实现自己的 `ConnHandler`。

仓库同时带了一套 `benchmark` 示例，用来验证吞吐、延迟、压缩率和合包效果；这套逻辑已经抽成可复用的 `benchmark/runner`，可以直接被自动化测试调用。

## 核心设计

### 发送侧：Wake 批量聚合

这是这个库最主要的价值点。

```
业务 goroutine ──Send()──> 连接级队列 ──首次入队时 Wake()──┐
                                                          ↓
                                event loop 线程：整批取出
                                    → pushSeparate  (writev 批量写)
                                    → pushCompound  (多条拼包 → 可选压缩 → 单帧)
```

业务侧只做「入队 + 一次 Wake」，真正的 write 收敛到 event loop 线程执行。这样一次 epoll 唤醒就摊薄了 N 条消息的系统调用开销，同时因为写操作天然串行，不需要任何 socket 写锁。

合并帧是**整体压缩**而不是逐条压缩：100 条相似的小消息拼成 30KB 再压，压缩比远高于压 100 次（zstd 可以跨消息复用上下文）。

### 收包侧：单一 codec

协议解析只有 `codec.go` 一个入口。之前 TCP server、WebSocket server、client 各有一份解析实现，三者对 `z`/`c`/`e` 标记采取了三种不同的策略，导致同一个协议缺陷在不同传输上以不同形式出现。现在差异只体现为策略字段：

| 侧 | `allow` | 含义 |
| --- | --- | --- |
| server | `maskE` | 只接受对端加密，上行压缩/合包一律拒绝并断开 |
| client | `maskZ｜maskC｜maskE` | 服务端下行三种都可能用 |
| compound 子消息 | `0` | 子消息不得携带任何标记 |

compound 子消息用 `allow = 0` 的子策略解析，所以嵌套 compound 会被直接拒掉，展开过程是**迭代而非递归**——结构上不存在栈深度问题。

协议层有 fuzz 覆盖：

```bash
go test -run '^$' -fuzz FuzzCodec
```

### 连接生命周期

`Conn` 有显式状态机：`handshaking → open → closed`。

所有出站数据只经过 `writeOutbound` / `writevOutbound` 两个出口，它们会先检查 `writable()`。这让「连接已关闭就不能再写」成为结构性保证：任何 `SenderI` 实现都不可能绕过闸口，把数据写到一个已被 gnet 回收、并且可能已被新连接复用的 fd 上。

- TCP：`OnOpen` 之后立即进入 `open`
- WebSocket：握手完成后才进入 `open`
- `OnClose` 里第一件事就是落 `closed` 状态，之后排队的任何 Wake 回调都会被 `ErrConnClosed` 挡下

### panic 隔离

`gnet` 核心没有任何 `recover`，任何从事件循环逃逸的 panic 都会终止整个进程。因此 gate 在每一个回调业务代码的边界都加了 recover barrier。

服务端：`Handle`、`ConnHandler.Close`、`ConnHandlerBuilder.Build`、`SenderBuilder.Build`、`SenderI.Close`、`OnReady`、`OnWebSocketUpgrade`、`AsyncDo`。一条能让业务解码 panic 的畸形消息只会关闭它自己那条连接。

客户端：`OnMessage` 和 `OnClose` 同样被隔离（`OnClose` 尤其关键——逃逸的 panic 会跳过 `close(done)`，让所有 `Wait()` 永久挂起）。`OnConnect` 的 panic 会传播给 `DialContext` 的调用方，但 gate 会先把 client 拆干净，不留半构造状态。

## 消息头协议

见 [header.go](header.go)。

- 小于 4KB 的消息使用 2 字节头，大于等于 4KB 使用 4 字节头
- 长度字段为 12 位或 12+16 位；协议上限 32MB
- 标记位：`m` 扩展长度头、`z` 压缩、`c` compound、`e` 加密

```
|-h-|-------- message --------|
    |-----decrypt message-----|                          if e = 1
    |------------ uncompressed message -------------|    if z = 1
    |-h-|--message--|-h-|--message--|-h-|--message--|    if c = 1
```

约定：compound 内部的子消息永远不带压缩、加密、合包标记（`z=0 c=0 e=0`），codec 会强制校验这一点。

## 核心接口

### `Config`

见 [config.go](config.go)。

| 字段 | 说明 |
| --- | --- |
| `LoopCount` | 事件循环数量，最终被限制在 `[1, runtime.NumCPU()]` |
| `Addr` / `Port` | 监听地址；`Addr` 为空时回退到 `0.0.0.0:<Port>` |
| `Transport` | 传输封装，默认 TCP，可切到 WebSocket |
| `MaxMessageSize` | 单条入站消息上限，`<=0` 时默认 32MB |
| `MaxConnections` | 最大并发连接数，`<=0` 不限制（原子占位，多 loop 下不会超发） |
| `TCPKeepAlive` | `SO_KEEPALIVE` 空闲探测时长，`0` 时默认 60s，传 `DisableKeepAlive` 关闭。**最小 5s** |
| `IdleTimeout` | 超时未收到任何完整消息的连接会被主动关闭，`<=0` 不启用 |
| `WebSocketPath` | WebSocket 握手路径，默认 `/` |
| `MaxWebSocketHandshakeBytes` | 握手阶段缓冲上限，默认 16KB |
| `MaxWebSocketBufferedBytes` | 单连接缓冲上限，默认 `MaxMessageSize + 64KB` |
| `OnWebSocketUpgrade` | 握手完成前的回调，可读 URI / HTTP 头做鉴权，返回错误即拒绝握手 |
| `CHB` | `ConnHandlerBuilder`，不能为空 |
| `SB` | `SenderBuilder`，不能为空 |
| `Logger` | gate 与 gnet 共用的 logger，不能为空 |

> `MaxMessageSize` 值得按业务实际收紧。它决定了单个连接能让服务端缓冲多少数据，绝大多数业务的单条消息远小于 32MB。

> `TCPKeepAlive` 不能小于 5s：gnet 在未显式设置探测间隔时取 `TCPKeepAlive/5` 并截断成整秒，小于 5s 会算出 0，socket 层直接拒绝，keep-alive 会静默失效。低于这个值 `StartServer` 会直接返回错误而不是让它悄悄不生效。

> `IdleTimeout` 的判据是「最后一次**交付完整消息**的时间」，不是「最后一次收到字节」。因此它也会回收只发半个帧吊着连接的 slowloris；反过来，服务端单向下推不会让连接显得活跃。

> 日志是进程级全局的（gnet 的限制），同进程启动多个 server 时只有第一个的 `Logger` 生效。

### `SenderConfig`

见 [sender.go](sender.go)。

| 字段 | 说明 |
| --- | --- |
| `CompressThreshold` | 压缩阈值，`<=0` 关闭压缩。独立帧按 payload 判定，合并帧按整批字节数判定 |
| `MaxBufferSize` | 单连接积压上限，`<=0` 不限制 |
| `MaxClusterSize` | 单个合并帧上限，`<=0` 关闭合并 |

**合并与压缩是相互独立的开关。** 关掉压缩不会连带关掉合并——大量小消息正是「该合包但不值得压缩」的典型场景，合包本身就能省下系统调用和小 TCP 段。

`MaxBufferSize` 同时约束两处：sender 内部尚未 flush 的队列（超出时 `Send` 返回 `ErrSendQueueFull`），以及底层连接的出站缓冲（超出时**关闭连接**而不是丢消息——从一条有状态的帧流中间抽掉几条，只会让对端状态机静默错乱，业务层什么都察觉不到）。

默认 `DefaultSenderBuilder`：压缩阈值 1KB、最大缓冲 2MB、最大合包 32KB。

### 业务接口

见 [i.go](i.go)。

```go
type ConnHandler interface {
    Handle(raw []byte)   // 不要阻塞；panic 只会关闭当前连接
    Close()
}

// 可选扩展：连接真正可以发送数据时触发
type ReadyHandler interface {
    OnReady()
}

type Cipher interface {
    Encrypt([]byte)
    Decrypt([]byte)
}
```

需要在连接建立时主动下发数据（握手包、欢迎消息）**必须**用 `OnReady`，不能在 `Build` 里直接发：WebSocket 模式下那时握手响应还没写完，插一个 WS 数据帧进去会让连接作废。

### 发送接口

```go
conn.Send(data)                          // 复制后入队，允许压缩/合包/加密
conn.SendNoEncrypt(data)                 // 复制后入队，明文独立帧
conn.SendStatic(data, alreadyCompressed) // 零拷贝，gate 不改写 data
```

`SendStatic` 的契约是 **data 必须永久保持不可变**，而不是「直到 flush 完成」——gate 没有 flush 完成回调，调用方无从得知何时可以安全复用这块内存。只有生命周期长于连接的数据（预编码的固定回包、广播帧）才适合走这个接口。

连接关闭后所有发送接口返回 `ErrConnClosed`，积压超限返回 `ErrSendQueueFull`。

### 阻塞任务

```go
conn.AsyncDo(func() {
    // rpc / db 等阻塞逻辑
})
```

`AsyncDo` 会暂停该连接继续读后续消息，在新 goroutine 中执行，结束后唤醒连接继续消费。数据仍保留在 gnet 的连接缓冲区中，不会丢失；支持重入。

**只应在 `Handle` 内部调用**——从其他 goroutine 调用时「单连接内串行」的保证不成立，因为事件循环可能已经越过了阻塞检查。

### WebSocket 握手信息

```go
cfg := &gate.Config{
    Transport: gate.ServerTransportWebSocket,
    OnWebSocketUpgrade: func(h *gate.Handshake) error {
        if h.Header.Get("Authorization") != wantToken {
            return errors.New("unauthorized")   // 拒绝握手
        }
        return nil
    },
}
```

握手完成后，业务层可以在 `OnReady` / `Handle` 里通过 `conn.Handshake()` 拿到 `URI` 和 `Header`（TCP 传输下返回 nil）。挂在 LB 后面时用它读 `X-Forwarded-For` / `X-Real-IP` 取真实来源。

握手头最多记录 64 条，**超出直接拒绝握手**而不是截断。HTTP 头没有「重要的排在前面」这种保证：静默丢弃意味着只要在前面塞满 64 条无关头，`Authorization` 就对钩子不可见，把「头不存在」当成匿名放行的鉴权逻辑会被绕过。

## 最小使用示例

```go
package main

import (
    "fmt"

    "github.com/LeGamerDc/gate"
)

type logger struct{}

func (l *logger) Debugf(f string, a ...interface{}) { fmt.Printf(f+"\n", a...) }
func (l *logger) Infof(f string, a ...interface{})  { fmt.Printf(f+"\n", a...) }
func (l *logger) Warnf(f string, a ...interface{})  { fmt.Printf(f+"\n", a...) }
func (l *logger) Errorf(f string, a ...interface{}) { fmt.Printf(f+"\n", a...) }
func (l *logger) Fatalf(f string, a ...interface{}) { fmt.Printf(f+"\n", a...) }

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
        LoopCount:      4,
        Port:           8081,
        MaxMessageSize: 64 * 1024,
        IdleTimeout:    5 * time.Minute,
        CHB:            &handlerBuilder{},
        SB:             gate.DefaultSenderBuilder,
        Logger:         &logger{},
    })
}
```

## Benchmark

### `benchmark/server` / `benchmark/client`

```bash
go run ./benchmark/server -addr 127.0.0.1:8081 -loops 4 \
  -compress-threshold 1024 -max-cluster-size 32768

go run ./benchmark/client -addr 127.0.0.1:8081 \
  -connections 1000 -payload-size 256 -duration 15s -payload-mode repeat
```

client 每秒打印 `tx/rx msg/s`、`raw MiB/s`、`wire MiB/s`、`wire/raw ratio`、compound 聚合度、`avg/p50/p95/p99` RTT。

常用 flag：`-compress-threshold <= 0` 关压缩、`-max-cluster-size <= 0` 关合包、`-rate 0` 打满、`-payload-mode repeat|random` 分别观察高/低压缩率场景。

### 微基准与 fuzz

```bash
go test -run '^$' -bench . -benchmem
go test -run '^$' -fuzz FuzzCodec -fuzztime 60s
```

## 当前边界

- 没有内置上层协议编解码，业务拿到的是 `[]byte`
- 没有内置鉴权、路由、心跳、连接注册或房间广播语义
- 接收侧协议不接受 client 上行压缩消息或 compound message（会直接断开）
- 不支持 TLS / wss，需要前置代理终结
- 广播场景目前仍是逐连接编码；一份 payload 编码一次再分发给 N 个连接尚未实现（注意它与逐连接加密在根本上不兼容）

## 验证

```bash
go vet ./...
go test -race ./...
```
