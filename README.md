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
| `CompressLevel` | 压缩等级，零值保持历史默认。它同时决定 zstd encoder 池的常驻内存 |

**合并与压缩是相互独立的开关。** 关掉压缩不会连带关掉合并——大量小消息正是「该合包但不值得压缩」的典型场景，合包本身就能省下系统调用和小 TCP 段。

`MaxBufferSize` 同时约束两处：sender 内部尚未 flush 的队列（超出时 `Send` 返回 `ErrSendQueueFull`），以及底层连接的出站缓冲（超出时**关闭连接**而不是丢消息——从一条有状态的帧流中间抽掉几条，只会让对端状态机静默错乱，业务层什么都察觉不到）。

默认 `DefaultSenderBuilder`：压缩阈值 1KB、最大缓冲 2MB、最大合包 32KB。

#### 压缩等级与内存

zstd 在第一次压缩时会按并发度（`GOMAXPROCS`）造出对应数量的 encoder，每个 encoder 自带一整套固定大小的哈希表。10 核机器实测（首次 `EncodeAll` 之后的堆增量 ÷ 并发度）：

| `CompressLevel` | 每 encoder | GOMAXPROCS=10 合计 |
| --- | --- | --- |
| `CompressFastest` | 约 0.27 MB | 约 3 MB |
| `CompressBalanced` | 约 1.32 MB | 约 13 MB |
| `CompressBetter`（零值） | 约 4.27 MB | 约 60 MB |
| `CompressBest` | 更高 | 更高 |

encoder 按等级在进程内共享，所以多个 `SenderBuilder` 用同一等级只付一份；从不压缩的 gate 一分钱都不付（zstd 是懒初始化的）。

同一份「游戏协议 batch」样本上 `CompressFastest` 比 `CompressBetter` 快 24% 且压缩率一样（`BenchmarkZstdLevel`）。真实数据上 `Better` 通常确实更小，所以默认值没有变；对 CPU 或常驻内存敏感的话先试 `CompressFastest`。

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

> 阻塞期间入站缓冲有上限：`MaxMessageSize + 64KB`，超过就关连接。挂起时 gate 一个字节都不消费，而 gnet 会继续把新到的数据追加进连接缓冲——没有上限的话，一个在慢速 `AsyncDo` 期间持续灌数据的客户端可以把单条连接的内存一路撑下去。如果业务确实需要在长时间 `AsyncDo` 期间容纳大量在途数据，把 `MaxMessageSize` 调大。

### 关闭语义

`conn.Close()` 之后，**同一批里剩下的消息不会再投递给 handler**。一个 TCP 读事件（或一个 WebSocket 二进制消息）里可以塞进多条消息，「第一条鉴权失败就关连接」必须真的能挡住后面那几条，否则把 `[伪造的登录包, 想执行的命令]` 拼在一个包里发出来就能绕过判定。

反过来，**已经入队的出站消息仍然会被刷出去**：`Close()` 只停入站投递，不关出站闸口。所以「回一条拒绝消息，然后关连接」是可靠的。

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

`perf_bench_test.go` 装的是整条链路级别的基准（`ConnOnTraffic` / `WSOnTraffic` / `SenderSend` / `ClientEncodeOutbound`），`perf_test.go` 里每条用例都对应一处性能改动，用来钉住「优化没有改变可观察行为」。

### 热路径的分配情况

稳态下这几条路径都是 **0 分配**（`-benchmem` 可验证）：

- TCP 入站：`Peek` → `codec.parse` → `deliver` → handler → `Discard`
- WebSocket 入站：帧头解析、去掩码、内层 gate 帧解析与投递
- 出站 flush：`pushSeparate`（含 writev 的 iovec 与 header 暂存）与 `pushCompound`
- `Send` 入队：只有 `mcache` 那次借用，且走的是池

客户端每条出站消息 1 次分配（帧缓冲本身）。

跨调用的暂存空间一律**按事件循环池化**，不挂成连接级字段：`separateScratch`（sender 的 iovec + header）和 `wsScratch`（WebSocket 帧头 + iovec）都只在一次 flush 期间有用，而所有出站写都发生在连接自己的事件循环上且中途不让出，因此池里同时在外的对象数等于事件循环数，不随连接数增长。挂成连接级字段的话，十万连接光这两块就要白占约 300MB。

### 这些优化在什么场景下才看得见

`benchmark/runner` 那套 loopback 端到端基准**量不出**上面这些改动：交替跑 6 轮 A/B，四个场景的差值都在 ±1% 以内，而运行间离散度本身有 3.5~5%。

原因不是优化没效果，而是那个 harness 根本不在 CPU 上。对它做 CPU profile：

```
92.85%  syscall.rawsyscalln
 3.86%  syscall.RawSyscall
 0.99%  runtime.kevent
```

gate 自身热路径的**平坦**耗时是 0%——`sender.pushSeparate` 的 3.86% 累计耗时全部是它内部那一次 `writev`，`Conn.onTraffic` 的 0.57% 全部是读。客户端和服务端跑在同一个进程、同样 10 个核上，时间几乎全花在系统调用上。

所以这些改动的收益出现在**系统调用不再是瓶颈**的地方：

- 单条 epoll 事件携带多条消息时（入站每消息固定成本从 31ns 降到 8ns）
- CPU 受限的容器里（gate 少占的那部分 CPU 直接还给业务逻辑）
- 海量连接下的常驻内存（暂存空间从按连接改成按事件循环，见上）
- GC 压力（出站路径零分配意味着不再向堆里灌垃圾）

反过来说：如果你的网关就是 syscall-bound 的，别指望这些改动能提高吞吐——该去调的是批量大小、`LoopCount` 和内核参数。

### 一个关于取舍的说明

活跃时间（`IdleTimeout` 的判据）是**每轮事件**刷新一次，而不是每条消息一次：`time.Now()` 要同时取墙上时钟和单调时钟，实测 31ns，而整条 TCP 入站路径处理一条消息本来只要 31ns。语义没有变——仍然只有真正交付了完整消息才算活跃，只是精度从「每条消息」降到「每次事件」，而 `IdleTimeout` 的量级是秒。

## 当前边界

- 没有内置上层协议编解码，业务拿到的是 `[]byte`
- 没有内置鉴权、路由、心跳、连接注册或房间广播语义
- 接收侧协议不接受 client 上行压缩消息或 compound message（会直接断开）
- 不支持 TLS / wss，需要前置代理终结
- WebSocket 本地主动关闭（业务调 `Close`、空闲回收、背压、协议错误）不发 close 帧，直接关 TCP，所以对端看到的是 1006 abnormal closure 而不是状态码。收到对端的 close 帧时会正常回一个 close 完成握手。
- `Cipher.Encrypt/Decrypt` 是唯一没有 recover 保护的业务回调（`i.go` 要求它 must not panic）。它在入站解密和出站加密的热路径上被逐帧调用，加一层 recover 的代价不值得——但这也意味着一个会 panic 的 Cipher 实现能打死进程。
- 广播场景目前仍是逐连接编码；一份 payload 编码一次再分发给 N 个连接尚未实现（注意它与逐连接加密在根本上不兼容）

## 测试工程

测试按**被测能力**组织，而不是按写它的那次改动组织（此前是一批 `*_audit_test.go`，
文件名记录的是"这批测试是哪次审查写的"，找不到东西也没地方放新用例）。分五层：

| 层 | 文件 | 覆盖什么 |
| --- | --- | --- |
| harness | `testutil_test.go` | 共享的起 server / 连 client / logger / handler |
| 协议单元 | `codec_test.go` `websocket_frame_test.go` `wscontrol_test.go` | 纯函数：头编解码、codec 策略、WS 帧头与控制帧 |
| 组件流程 | `conn_close_test.go` `wsstate_test.go` `inbound_bound_test.go` `sender_test.go` `sender_writefail_test.go` `server_conn_test.go` `client_test.go` `client_protocol_test.go` | 单个组件的状态机，用可控假连接驱动 |
| 端到端流程 | `flow_roundtrip_test.go` `flow_lifecycle_test.go` `flow_matrix_test.go` `flow_limits_test.go` `flow_panic_test.go` `flow_failure_test.go` `flow_concurrent_test.go` `flow_handshake_test.go` `websocket_handshake_test.go` `websocket_e2e_test.go` | 真实 socket，两种传输跑同一份断言 |
| 性质与 fuzz | `property_test.go` `fuzz_test.go` | 对生成的输入断言不变式 |
| 性能护栏 | `perf_test.go` `perf_bench_test.go` `bench_test.go` | 钉住热路径的分配数与结构体大小 |

几条值得说明的设计：

**同一份用例在两种传输上各跑一遍。** TCP 和 WebSocket 是两套完全独立的入站状态机
（`conn.onTraffic` 与 `wsConnState.onTraffic`），却承诺对业务层呈现相同语义。
`flow_matrix_test.go` 里的 `transportCase` 把"起 server + 连 client"抽象掉，
让一致性成为持续被验证的东西，而不是一句注释。

**尺寸边界按各自协议的切换点取，而不是按业务长度取。** WS 帧头的宽度切换点是按
**WS 载荷**算的，而 WS 载荷 = gate header + 业务长度，中间差 2 或 4 字节。所以
覆盖 WS 的 126 边界要用业务长度 123/124，覆盖 65536 边界要用 65531/65532。
这组用例还必须关掉压缩——否则线路上的长度由 zstd 决定，精心挑出来的边界值一个都落不到。

**property 测试自己要证明走到了有意思的分支。** `TestPropertySenderRoundTripPreservesMessages`
在断言 round-trip 之外还统计线路上到底出现了多少压缩帧、合并帧、加密帧、4 字节头帧，
任何一类为 0 就直接失败。少了这一段，一个"所有分支都退化成明文独立帧"的回归会让
round-trip 断言全部通过。

**编码侧也有 fuzz。** `FuzzCodec` 覆盖解码；`FuzzSenderRoundTrip` 用 fuzz 输入同时派生
sender 配置和消息序列，以 client codec 作为独立 oracle 验证 sender 编出来的东西能原样解回来。
`FuzzWebSocketInboundFrames` 把任意字节当作握手后的帧流喂给状态机。

```bash
go test -run '^$' -fuzz FuzzSenderRoundTrip -fuzztime 60s
go test -run '^$' -fuzz FuzzWebSocketInboundFrames -fuzztime 60s
go test -run '^$' -fuzz FuzzCodec -fuzztime 60s
```

## 验证

```bash
go vet ./...
go test ./...                      # 分配断言只在这一趟生效
go test -race ./...
go test -count=2 -shuffle=on ./...  # 用例之间不得有顺序依赖
```

前两趟都要跑。`perf_test.go` 里那些"稳态零分配"的断言在 `-race` 下会自动跳过——竞态插桩会改变逃逸分析并自带簿记分配（实测同一条 Send+flush 路径不带 -race 是 0 次、带 -race 稳定多 1 次），那个数不代表被测代码。只跑 `-race` 的话，这类性能回退完全不会被检查到。
