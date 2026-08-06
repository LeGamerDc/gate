# 服务端接口设计

本文定义业务开发者看到的**全部**公共 API：怎么用、每个决策为什么这么定、参考了谁。

阅读顺序建议：先看「五分钟上手」和「API 全景」拿到形状，用到哪一块再翻对应小节，
最后看「设计决策记录」——那里才是这份设计真正的内容。

---

## 目录

- [五分钟上手](#五分钟上手)
- [API 全景](#api-全景)
- [Handler：业务只实现这一个接口](#handler业务只实现这一个接口)
- [Conn：连接句柄](#conn连接句柄)
- [发送](#发送)
- [Frame：预编码帧与广播](#frame预编码帧与广播)
- [并发与关闭语义](#并发与关闭语义)
- [背压](#背压)
- [阻塞任务：AsyncDo 与 Pause](#阻塞任务asyncdo-与-pause)
- [内存与 GC 预算](#内存与-gc-预算)
- [配置](#配置)
- [Server 生命周期](#server-生命周期)
- [错误与关闭原因](#错误与关闭原因)
- [Cipher](#cipher)
- [WebSocket](#websocket)
- [日志与统计](#日志与统计)
- [平台无关性：为什么 API 里没有 epoll 的影子](#平台无关性为什么-api-里没有-epoll-的影子)
- [设计决策记录](#设计决策记录)
- [参考与借鉴对照](#参考与借鉴对照)
- [非目标](#非目标)
- [从旧 API 迁移](#从旧-api-迁移)

---

## 五分钟上手

```go
package main

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/LeGamerDc/gate"
)

// 每连接的业务状态。它会被**内联**进 gate 的连接对象，不产生额外分配。
type player struct {
	id       uint64
	loggedIn bool
}

type handler struct{ /* 全局依赖：注册表、指标、DB 句柄… */ }

func (h *handler) OnOpen(c *gate.Conn[*player]) (*player, error) {
	return &player{}, nil // 返回 error 即拒绝这条连接
}

func (h *handler) OnMessage(c *gate.Conn[*player], msg []byte) error {
	if !c.State.loggedIn {
		id, ok := parseLogin(msg)
		if !ok {
			_ = c.Send(rejectPacket)  // 这条仍然会被送出
			return gate.ErrProtocol   // 然后关闭；同一批剩下的消息不再投递
		}
		c.State.id, c.State.loggedIn = id, true
		return nil
	}
	return c.Send(handle(msg))
}

func (h *handler) OnClose(c *gate.Conn[*player], reason error) {
	if c.State.loggedIn {
		unregister(c.State.id)
	}
}

func main() {
	_ = gate.Run(context.Background(), gate.Options[*player]{
		Addr:     ":8081",
		Handler:  &handler{},
		Outbound: gate.DefaultOutbound(),
		Limits: gate.Limits{
			MaxMessage: 64 << 10,
			Idle:       5 * time.Minute,
		},
		Log: zap.L(),
	})
}
```

换成 WebSocket 只加一个字段，业务代码一行不改：

```go
WebSocket: &gate.WebSocketOptions{Path: "/ws"},
```

---

## API 全景

这是公共 API 的**全部**。刻意压到一屏之内——一个网关库该暴露的概念不该比这更多。

```go
package gate

// ── 启动与生命周期 ───────────────────────────────────────────────
func Listen[S any](opts Options[S]) (*Server[S], error)   // 返回时已在监听
func Run[S any](ctx context.Context, opts Options[S]) error // = Listen + Wait，ctx 取消即优雅关闭

type Server[S any] struct{ /* opaque */ }
func (s *Server[S]) Addr() netip.AddrPort
func (s *Server[S]) Stats() Stats
func (s *Server[S]) Shutdown(ctx context.Context) error
func (s *Server[S]) Wait() error

// ── 业务只实现这一个接口 ─────────────────────────────────────────
type Handler[S any] interface {
	OnOpen(c *Conn[S]) (S, error)
	OnMessage(c *Conn[S], msg []byte) error
	OnClose(c *Conn[S], reason error)
}

// ── 连接句柄（永不复用，见 D17）─────────────────────────────────
type Conn[S any] struct {
	State S // 业务状态；**只允许在事件循环上访问**：回调内 或 Post 内
}
// 发送（可从任意 goroutine 调用）
func (c *Conn[S]) Send(b []byte) error       // 复制入队，允许压缩 / 合包 / 加密
func (c *Conn[S]) SendAlone(b []byte) error  // 复制入队，独立帧不压缩不合包；加密照旧
func (c *Conn[S]) SendFrame(f *Frame) error  // 零拷贝，广播用
func (c *Conn[S]) SendFunc(n int, fill func(b []byte) (int, error)) error // 直接写进出站缓冲
func (c *Conn[S]) Writable() bool            // 出站积压是否低于高水位

// 调度
func (c *Conn[S]) Post(f func(*Conn[S]))     // 把 f 排进该连接的串行域执行
func (c *Conn[S]) AsyncDo(f func()) error    // 在别的 goroutine 上跑 f，但仍属串行域
func (c *Conn[S]) Pause() (resume func())    // 低级原语：只暂停，不提供独占保证

// 其它
func (c *Conn[S]) Close(reason error)        // reason 会原样传给 OnClose
func (c *Conn[S]) SetCipher(Cipher)          // **只能在事件循环线程上调用**；出站经队列 barrier 生效
func (c *Conn[S]) Handshake() *Handshake     // 非 WebSocket 返回 nil
func (c *Conn[S]) Remote() netip.AddrPort
func (c *Conn[S]) ID() uint64                // 进程内唯一且永不复用

// ── 预编码帧：一份数据发给 N 条连接 ──────────────────────────────
type Frame struct{ /* opaque, immutable, GC 管理 */ }
func (s *Server[S]) NewFrame(payload []byte, opts ...FrameOption) (*Frame, error)
func PreEncrypted() FrameOption // payload 是业务自理的密文：gate 跳过压缩，不设线路 e 位

// ── 配置 ─────────────────────────────────────────────────────────
type Options[S any] struct {
	Addr      string
	Loops     int
	Handler   Handler[S]
	Outbound  Outbound
	Limits    Limits
	Socket    Socket
	WebSocket *WebSocketOptions // nil ⇒ 裸 TCP
	Proxy     ProxyMode         // PROXY protocol，默认 Off
	Log       Logger
}

type Outbound struct {
	CompressThreshold int
	CompressLevel     CompressLevel
	Dict              []byte // 可选：zstd 训练字典
	MaxCluster        int
	MaxBuffer         int           // 准入上限；<=0 用默认，gate.Unlimited 关闭
	HighWater         int           // 软上限，越过后 Writable() 转 false
	StallTimeout      time.Duration // 积压非空且这么久一个字节都没写出去 ⇒ 关闭
	CloseLinger       time.Duration // Close 之后为排空出站队列最多再等多久
}
func DefaultOutbound() Outbound // 函数而非可变全局量

type Limits struct {
	MaxMessage       int           // 单条入站消息上限
	MaxPending       int           // Pause 期间已读出但未投递的字节上限
	MaxConns         int           // 每 server 并发连接上限
	MaxOutboundBytes int           // 每 server 出站积压总预算（跨全部连接；近似口径见 06）
	MaxHandshaking   int           // 处于握手阶段的连接上限（WebSocket / PROXY）
	HandshakeTimeout time.Duration // 握手必须在这么久内完成
	MaxPause         time.Duration // 单次 Pause 的最长时长，超过即关闭
	Idle             time.Duration
	KeepAlive        time.Duration
}

const Unlimited = -1 // 显式关闭某项保护

type Socket struct {
	RecvBuffer int
	SendBuffer int
	Nagle      bool
}

type WebSocketOptions struct {
	Path              string
	MaxHandshakeBytes int
	OnUpgrade         func(*Handshake) error
}
type Handshake struct {
	URI    string
	Header http.Header
}
func RejectUpgrade(status int, reason string) error

// ── 其它 ─────────────────────────────────────────────────────────
type Cipher interface {
	Overhead() int
	Seal(dst, plaintext, aad []byte) []byte          // aad = 该帧最终的 2/4 字节帧头
	Open(dst, ciphertext, aad []byte) ([]byte, error)
}

type Field = zap.Field                  // 类型别名，零成本
type Logger interface {                 // *zap.Logger 直接满足
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}
func SlogLogger(*slog.Logger) Logger    // slog 用户的适配器

type CompressLevel uint8
const (
	CompressFastest CompressLevel = iota // 零值，也是推荐值
	CompressBalanced
	CompressBetter
	CompressBest
)

type Stats struct{ /* 见「日志与统计」 */ }

// 不需要每连接状态时用它：gate.Options[gate.None]
type None = struct{}
```

数一下：**1 个必须实现的接口（3 个方法）**、2 个可选实现的接口（`Cipher`、`Logger`）、
1 个连接句柄、1 个帧对象、5 个配置结构体。没有 builder，没有工厂，没有可选扩展接口。

### 线程模型速查

这是整套 API 里最容易写错的地方，先给结论：

| | 可以从哪调用 | 备注 |
| --- | --- | --- |
| `Send` / `SendAlone` / `SendFrame` / `SendFunc` | **任意 goroutine** | |
| `Writable` / `Remote` / `ID` / `Handshake` | 任意 goroutine | |
| `Close` / `Post` | 任意 goroutine | |
| `resume`（`Pause` 的返回值） | 任意 goroutine | 幂等 |
| **`c.State`** | **只能在串行域内**：`OnOpen` / `OnMessage` / `OnClose` / `Post` / **`AsyncDo`** 的函数体 | `Pause` 后自己起的 goroutine **不算** |
| **`SetCipher`** / `AsyncDo` / `Pause` | **只能在事件循环上**：回调与 `Post` 的函数体 | 比 `State` 窄一格——**`AsyncDo` 的函数体不算**，见下 |

一句话记法：**要碰 `State`，就得在串行域里；不在的话用 `Post` 排进去。**

「串行域」= 这条连接上所有由 gate 排定顺序的执行体。`AsyncDo` 的函数体虽然跑在另一个
goroutine 上，但 gate 保证它与其余成员不重叠，见[阻塞任务](#阻塞任务asyncdo-与-pause)。

**但改 gate 自己的连接状态（`SetCipher` / `Pause` / `AsyncDo`）要求更严一格：
必须在事件循环线程上。** 这三个动作会碰 cipher、读闸深度、LRU 归属、poller 注册——
它们是 loop 私有的，而 `AsyncDo` 的函数体跑在别的 goroutine 上，同一时刻 loop 仍可能
因超时或 `Shutdown` 在拆这条连接。在 `AsyncDo` 里需要它们时用 `Post` 排回去：

```go
c.AsyncDo(func() {
    key := negotiate()            // 慢调用
    c.State.key = key             // 业务状态：这里直接改是安全的
    c.Post(func(c *gate.Conn[*player]) {
        c.SetCipher(newCipher(key)) // gate 状态：排回事件循环
    })
})
```

Go 里没有可靠的「我在哪个 goroutine 上」，所以这是一条契约而不是运行时检查，
由 `-race` 兜底。

---

## Handler：业务只实现这一个接口

```go
type Handler[S any] interface {
	OnOpen(c *Conn[S]) (S, error)
	OnMessage(c *Conn[S], msg []byte) error
	OnClose(c *Conn[S], reason error)
}
```

**一个 `Handler` 实例服务所有连接**，不是每连接一个。连接私有的东西全部放在
`S` 里；`Handler` 自己只放全局依赖（注册表、指标、下游客户端）。因此
`Handler` 的方法会被多个事件循环并发调用，它自身的字段必须是只读的或自带同步。

### `OnOpen`

在连接**真正可以发送数据**的时刻调用，返回值成为 `c.State`。

- TCP：三次握手完成、socket 就绪之后。
- WebSocket：**101 响应已经写进 socket 之后**。

这个时机保证是硬承诺：`OnOpen` 里可以直接 `c.Send(welcome)`，两种传输都安全。

返回 `error` 表示拒绝这条连接：gate 立刻关闭它，并且**不会**调用 `OnClose`
（`OnClose` 只与成功返回的 `OnOpen` 配对，业务不必写「我到底初始化到哪一步了」的
防御代码）。

### `OnMessage`

每收到一条**完整业务消息**调用一次。解密、解压、compound 展开都已经完成，
`msg` 就是对端 `Send` 时给的那些字节。

- `msg` 的生命周期**仅限本次调用**。要留下来必须自己复制。
  传给 `c.Send` / `c.SendAlone` 是安全的——它们立即复制。
- 返回 `nil`：继续投递下一条。
- 返回非 `nil`：**同一批里剩下的消息不再投递**，连接关闭，`OnClose` 收到这个 error。

第二条是有安全含义的，必须是硬语义：一个 TCP 读事件（或一个 WebSocket 二进制消息）
里可以塞进多条 gate 消息。把 `[伪造的登录包, 想执行的命令]` 拼在一起发出来，
如果「第一条判定失败」挡不住后面那条，命令就会在一条已经被判死的连接上执行。

**返回 error 之前 `Send` 的消息仍然会被送出。** 「回一条拒绝消息，然后断开」是可靠的：
错误只停**入站**投递，不关**出站**闸口。

### `OnClose`

连接关闭时**恰好调用一次**，`reason` 说明为什么。见[错误与关闭原因](#错误与关闭原因)。

调用之后 `c` 上的所有发送接口返回 `ErrConnClosed`。`c.State` 在 `OnClose` 期间仍然有效，
这是业务做反注册、落盘、结算的地方。

---

## Conn：连接句柄

### `State`：为什么是泛型字段

```go
type Conn[S any] struct {
	State S
}
```

`S` 直接内联在连接对象里。三种典型用法的成本：

| `S` | 每连接额外分配 | 访问成本 |
| --- | --- | --- |
| `*player`（指针） | 1 次（`player` 本身） | 一次解引用 |
| `player`（值） | **0 次** | 直接字段访问 |
| `gate.None` | 0 次 | — |

10 万连接的场景下，「每连接一次 handler 对象分配 + 一次接口装箱 + 每条消息一次类型
断言」不是可以忽略的开销，而泛型把它们全部消掉，同时还换来了编译期类型安全——
不必写 `c.State().(*player)`，也不会在半夜因为断言失败 panic。

代价是 `Options` / `Server` / `Conn` / `Handler` 都带类型参数。这个传染是有边界的：
`Frame`、`Outbound`、`Limits`、`Cipher`、`Logger`、`Stats`、所有 error 都不带。

**线程约束**：`State` 只能在这条连接的**串行域**内访问——`OnOpen` / `OnMessage` /
`OnClose` / `Post` / `AsyncDo` 的函数体。gate 保证这五者对同一条连接不重叠。

唯一的例外是 `Pause` 之后业务自己起的 goroutine：那段代码 gate 排不了序，
在其中访问 `State` 需要业务自己同步，或者用 `Post` 排回串行域。
见[阻塞任务](#阻塞任务asyncdo-与-pause)。

### `ID` 与连接对象的生命周期

`ID()` 是进程内唯一、**永不复用**的 64 位标识。

**`*Conn[S]` 本身也永不复用。** gate 池化连接内部的资源（读残片缓冲、出站 chunk 节点、
poller 槽位），但**绝不池化 `Conn` 对象本身**——它会逃逸到业务的注册表、定时任务、
`resume` 闭包里，而 gate 无法知道业务什么时候放手。复用它意味着一个持有旧指针的
goroutine 调用 `Send` 会把数据发给**另一个玩家**，这不是丢包或竞态，是跨会话的数据泄漏。

代价是每条连接一次堆分配（`Conn` + 内联的 `State`）。连接建立本来就要走若干次系统调用，
这一次分配无关紧要——**「稳态零分配」说的是消息路径，不是连接建立路径。**

`ID()` 仍然值得用作注册表的 key：它可比较、可打日志、可跨进程传递，而 `Remote()`
在 NAT 后面会重复。

---

## 发送

```go
c.Send(b)             // 复制 b；允许 gate 压缩、合包、加密
c.SendAlone(b)        // 复制 b；独立帧，不压缩不合包；**加密照旧**
c.SendFrame(f)        // 不复制；发送一个预编码好的帧
c.SendFunc(n, fill)   // 业务直接写进 gate 的出站缓冲，零复制
```

全部是**入队**语义，立刻返回，真正的 `writev` 由事件循环合批执行。
返回 `nil` 只表示「已入队」，不表示「已上线路」。

`Send` 和 `SendAlone` 返回之后调用方可以立即复用 `b`。

所有发送接口都可以从**任意 goroutine** 调用。

### `SendAlone`：不压缩不合包，但仍然加密

用途有二：

1. **规避压缩侧信道。** 一条含敏感内容的消息不该和攻击者可影响的内容进同一个压缩单元，
   见 [02 的 CRIME 类分析](02-wire-protocol.md#压缩侧信道crime-类)。
2. **对端按固定 offset 解析**的消息，不希望被合进 compound。

> **为什么没有「明文发送」接口。** 上一版有 `SendNoEncrypt`，它同时关掉了压缩、合包
> **和加密**。那是一个明文旁路：一条配置了 `Cipher` 的连接上，任何一处业务代码
> （或一次重构失误）都能把明文送上线路，而且**不会有任何地方报错**。
> 这一版把两件事分开：`SendAlone` 只关掉压缩与合包，加密由连接的 `Cipher` 统一决定，
> **没有任何接口能绕过它**。握手阶段的明文消息不需要特殊接口——那时 `Cipher` 本来就是 nil。

### 发送顺序

- **同一个 goroutine 连续调用**，或调用之间存在 happens-before 关系（锁、channel、
  `Post`）：**严格保持 FIFO**。有状态协议、序号、握手流程依赖这一条。
- **真正并发的调用之间顺序未定义**，但每个调用各自是原子的（不会有两条消息交错成
  半条）。需要顺序的业务自己建立 happens-before。
- `Close` 与 `SetCipher` 参与同一个顺序：`SetCipher` 之前入队的消息用旧密钥，
  之后的用新密钥——这由出站队列里的 **cipher barrier** 保证（加密发生在 flush，
  「在哪个线程调用」本身创造不了这个边界），见 [Cipher](#cipher) 与
  [03](03-outbound.md#setcipher队列里的-barrier)。

### `SendFunc`：零复制路径

```go
err := c.SendFunc(m.SizeVT(), func(b []byte) (int, error) {
	return m.MarshalToSizedBufferVT(b)   // 直接序列化进 gate 的出站缓冲
})
```

`fill` 拿到一块长度为 `n` 的缓冲，返回实际写入的字节数。gate 全程持有所有权。
内部是三阶段协议（完整规格见 [06](06-connection-state-machine.md#sendfunc-的两阶段协议)）：

1. **reserve**：加锁检查状态与预算、扣减、取池内存——这**不是**线性化点，消息还没入队；
2. **fill**：在锁外调用业务的 `fill`——持锁跑业务代码会把所有 `Send` / `Close`
   挡在锁上，`fill` 里重入 `Send` 还会死锁；
3. **commit**：重新加锁。若 `fill` 期间连接已关闭，归还内存、退还预算、返回
   `ErrConnClosed`（消息**未**入队）；否则此刻才真正入队、线性化。

`fill` 的每种返回都定死：

| `fill` | 结果 |
| --- | --- |
| `(k, nil)`，`0 <= k <= n` | 提交前 `k` 字节 |
| `k < 0` 或 `k > n` | 业务 bug：不入队，返回 `ErrInvalidLength` |
| `(_, err)` | 不入队，原样返回该 error |
| panic | 恢复屏障接住，缓冲照常回收，按 `OnMessage` panic 处理 |
| 永不返回 | gate 不介入；`Close` **不等待** `fill`，这块预留等同业务自己的泄漏 |

设计成回调而不是 `Reserve` / `Commit` 一对方法，是为了让下面这些误用**在结构上不可能**：

| 误用 | 回调形态下为什么不可能 |
| --- | --- |
| 借了不还 | 没有「还」这个动作 |
| 还两次 | 同上 |
| `fill` 里 panic 或提前 return | gate 的 `defer` 兜住，缓冲照常回收 |
| `append` 换掉了底层数组 | `fill` 只返回长度，不返回切片 |
| 提交到另一条连接上 | 缓冲从来没离开过 `SendFunc` |
| 未提交的缓冲不计入预算 | 预算在 `SendFunc` 入口就已扣减 |

`fill` 不会被 gate 保存，所以闭包通常能栈上分配。`n` 不足以容纳数据时由 `fill` 自己
返回错误——gate 不会重新分配（那会破坏「长度即预算」）。

### 复制的成本，以及怎么绕开它

`Send` / `SendAlone` 会复制 `b`，因为调用方在它返回之后就可以复用那块内存。
这次复制走**分级 size-class 池**，因此：

| | |
| --- | --- |
| 堆分配 | 预热之后、尺寸命中池的分档时为 **0**（池 miss 或 GC 清池后会重新分配） |
| GC 压力 | 不向堆里灌**新**垃圾；但池中对象仍属于活跃集，仍会被扫描 |
| 实际成本 | 一次 memcpy。量级上与整条入站路径处理一条消息相当（这是观测值，不是接口契约） |

真正昂贵的是「每次 `Send` 一次 `make([]byte, n)`」——那才是 GC 灾难，而复制正是为了避开它。

**为什么不是每连接一个 ring buffer。** 10 万连接下任何「按连接预留」的方案都不成立：
哪怕每连接 4KB 的环形缓冲也是 400MB，而且环的尺寸必须按最大消息来定，否则大消息进不去。
分级池的常驻成本只与**在途字节数**成正比，稳态下队列几乎是空的，而 `Outbound.MaxBuffer`
已经给在途量封了顶。这是「按连接数付费」和「按在途量付费」的区别，在这个量级上是决定性的。

想彻底省掉这次 memcpy 有两条路：`SendFunc`（业务直接写进出站缓冲）和
`SendFrame`（一份数据发给 N 条连接，只编码一次）。两者见下。

诚实的补充：如果这条消息最终被**合包**，它在 flush 时还会被拷进 compound body 一次。
`SendFunc` 在独立帧路径上省得干净，在合包路径上是把 2 次复制变成 1 次。

### 错误

| 返回 | 含义 | 业务通常怎么做 |
| --- | --- | --- |
| `nil` | 已入队 | — |
| `ErrConnClosed` | 连接已关闭 | 停止推送，清理会话 |
| `ErrSendQueueFull` | 积压达到 `Outbound.MaxBuffer` | 丢弃非关键推送，或提前用 `Writable()` 判断 |
| `ErrMessageTooLarge` | 超过协议上限 32MB | 业务 bug |
| `ErrInvalidLength` | `SendFunc` 的 `fill` 返回越界长度（`k < 0` 或 `k > n`） | 业务 bug |
| `ErrAsyncBusy` | 该连接已有一次 `AsyncDo` 在途 | 见[阻塞任务](#这个保证是怎么来的) |
| `ErrCipherConflict` | 对一条配了 `Cipher` 的连接调用 `SendFrame` | 见 [Frame](#frame预编码帧与广播) |

---

## Frame：预编码帧与广播

广播的正确形状是**编码一次、分发 N 次**，而不是让 N 条连接各自压缩加密一遍。

```go
f, err := srv.NewFrame(payload)    // 编码一次：帧头 + 按 server 的 Outbound 配置压缩
if err != nil {
	return err
}
for _, c := range room.Conns() {
	_ = c.SendFrame(f)             // 不复制、不改写
}
// 不需要做任何释放动作
```

`Frame` 是**不可变**的，由 GC 管理生命周期：gate 的出站队列在写出去之前一直持有它的
引用，写完就放手，最后一个引用消失时由 GC 回收。

**`NewFrame` 是 `Server` 的方法而不是包级函数**，因为它必须拿到这个 server 的
`Outbound` 配置——压缩等级、阈值和字典。包级函数编不出和普通 `Send` 一致的帧。

### 为什么不做公开的引用计数

上一版的 `SendStatic(data, alreadyCompressed)` 契约是「`data` 必须永久保持不可变」，
把一个生命周期问题转嫁给了用户的纪律。一个自然的修法是引用计数（Netty `ByteBuf` 的做法），
但那会把另一类问题搬进公共 API：少一次 `Release` 是泄漏，多一次是**仍在发送的连接读到
一块已经回池并被覆盖的内存**——后者的后果和上一版的「永久不可变」违约一样严重，
只是换了个触发方式。

而且它优化错了对象：`Frame` 的分配频率是**每次广播一次**（每个 tick 一条），
不是每连接一次。让 GC 管理它一年也回收不了多少 CPU。

**v1 用 GC 管理 `Frame`，不公开 `Retain` / `Release`。** 少两个方法，少一整类
双重释放的缺陷。将来 profile 真的指向这里，再在内部加引用计数——那是实现细节，
不必出现在 API 上。

### 与加密的关系

per-connection 独立密钥和「编码一次分发多次」在根本上不兼容。gate 不假装能同时做到：

- 对一条设置了 `Cipher` 的连接调用 `SendFrame` 返回 `ErrCipherConflict`——**显式失败，
  不静默降级成明文**。
- 房间级共享密钥的场景：业务自己加密 payload，然后
  `srv.NewFrame(sealed, gate.PreEncrypted())`。

**`PreEncrypted` 不复用线路的 `e` 位。** 它只声明「payload 已是密文」，效果是 gate
跳过压缩（压密文只浪费 CPU）；解密由对端**业务**完成，gate 视 payload 为不透明字节。
初稿让它设置 `e` 位——但 `e` 位属于连接级 `Cipher` 的职权：配了 Cipher 的对端会用
**连接**密钥去 `Open` 一个用**房间**密钥封的帧（必然失败），没配 Cipher 的对端按
[02 的 W3](02-wire-protocol.md#w3-cipher-配置与-e-位强制一致) 根本不接受 `e = 1`。
两头都走不通，所以业务自理的加密必须整体待在 payload 里，不碰 gate 的标记位。

---

## 并发与关闭语义

发送接口可以从任意 goroutine 调用，`Close` 也是。这一节把两者相遇时的行为定死。

### 线性化点

`Send` 和 `Close` 都在**获取同一把出站锁**的那一刻线性化：

- `Send` 的线性化点 = 扣减预算并把消息接进队列。
- `Close` 的线性化点 = 关掉入队闸口（同一把锁）。

由此得到唯一需要的那条不变式：

> **先线性化的 `Send`，一定会被后续的排空看到。**

不是「一次原子存储」就能兑现的——`Send` 可能先读到 open、随后 `Close` 置位并让事件
循环排空、最后 `Send` 才把节点接进队列，那条消息就既错过了排空又返回了 `nil`。
把两者放进同一把锁的临界区，这个窗口在结构上不存在。

### `Send` 与 `Close` 的竞态

| 时刻 | `Send` 返回 | 已入队的消息 |
| --- | --- | --- |
| `Send` 完全早于 `Close`（有 happens-before） | `nil` | 参与排空 |
| 与 `Close` 并发（无 happens-before） | `nil` **或** `ErrConnClosed`，两者都正确 | 若返回 `nil`，参与排空 |
| `Send` **调用**晚于 `Close` **返回** | **一定**是 `ErrConnClosed` | — |
| `OnClose` 期间及之后 | `ErrConnClosed` | — |

第三行是硬保证，业务可以依赖它。注意它说的是**调用**晚于 `Close` 返回；
一个**调用**早于、但**返回**晚于 `Close` 的 `Send` 仍然可以返回 `nil`。

中间那一行做不到确定，**也不打算做到**：要确定就得让 `Send` 阻塞等事件循环确认，
而「不阻塞、只入队」正是这套设计的前提。需要确定性的业务应当用自己的锁把
`Close` 和 `Send` 串起来——那本来就是业务的会话状态该管的事。

两条附带保证：

- **返回 `nil` 的 `Send` 绝不泄漏内存。** 那块池化缓冲要么被写上线路，要么在拆连接时
  被回收，没有第三种结局。
- **`OnClose` 至多被调用一次**，且在业务归还暂停令牌、进程仍存活时**恰好**一次。
  `Close` 可以重复调用，第一次的 `reason` 生效。推迟的情形见
  [阻塞任务的代价](#代价)。

### 关闭时已入队消息的处置

不同的关闭原因下处置**不一样**，这是刻意的：

| 关闭原因 | 已入队消息 | 为什么 |
| --- | --- | --- |
| 业务 `Close(reason)`、`OnMessage` 返回 error | **尽力 flush** | 「回一条拒绝消息再断开」必须可靠 |
| `ErrIdleTimeout`、`ErrServerClosed` | 尽力 flush | 正常关闭，出站方向没坏 |
| `ErrProtocol`、`ErrHandlerPanic` | 尽力 flush | 坏的是入站方向 |
| `ErrBackpressure`、底层写失败 | **丢弃** | 帧流已经有洞，继续写只会让对端在错位的流上解析 |

### 「尽力 flush」到底承诺了什么

**承诺：** 已入队的消息不会**因为你调用了 `Close`** 而被丢弃。gate 会在关闭前继续
尝试把它们写出去，最多再等 `Outbound.CloseLinger`（零值默认 1s，`Unlimited` 表示不等待、立即关闭）。

**不承诺：** 它们一定到达对端。一个不读数据的慢客户端会让 socket 缓冲一直是满的，
`CloseLinger` 到期后 gate 照常关闭连接并丢弃剩余字节——**否则一条卡死的连接可以无限期
拖住 fd 和内存**。

所以 `OnMessage` 里「回一条拒绝消息再返回 error」这个模式的准确表述是：

> gate 保证这条拒绝消息**排在关闭动作之前**被尝试写出，而不是保证它送达。

真需要确认送达，那是业务层 ack 该做的事——gate 没有、也不打算有 flush 完成回调。

### 回调的串行性

对**同一条连接**，跑在事件循环上的这四处严格串行，且 `OnOpen` 早于任何 `OnMessage`、
`OnClose` 晚于任何 `OnMessage`：

```
OnOpen → OnMessage* / Post* （交错，但串行）→ OnClose
```

因此 `c.State` 在这四处不需要任何同步。

**`AsyncDo` 的函数体也在这个串行域里**，尽管它跑在另一个 goroutine 上。
gate 用三个措施保证它不与其余成员重叠（拒绝重入、延迟启动、推迟 `OnClose`），
见[阻塞任务](#这个保证是怎么来的)。

**`Pause` 之后业务自己起的 goroutine 不在串行域里**——那段代码由业务提供，
gate 无从知道它何时开始。在其中访问 `State` 是一个 `-race` 才抓得到的数据竞争。

对**不同连接**，回调可能在不同事件循环上并发。`Handler` 自身的字段必须是只读的
或自带同步。

### 回调的状态迁移与 panic

每个回调失败/panic 之后的配对规则不同，列全：

| 事件 | `State` 是否已建立 | 是否调用 `OnClose` | 备注 |
| --- | --- | --- | --- |
| `OnUpgrade` 返回 error | 否 | **否** | 握手被拒，连接直接关闭 |
| `OnUpgrade` panic | 否 | **否** | 同上，按 500 拒绝 |
| `OnOpen` 返回 error | 否（返回值被丢弃） | **否** | 业务自己清理它在 `OnOpen` 里申请的东西 |
| `OnOpen` panic | 否 | **否** | 同上 |
| `OnMessage` 返回 error | 是 | **是**，`reason` = 该 error | |
| `OnMessage` panic | 是 | **是**，`reason` = `ErrHandlerPanic` | |
| `Post` 的函数体 panic | 是 | **是**，`ErrHandlerPanic` | |
| `AsyncDo` 的函数体 panic | 是 | **是**，`reason` = `ErrHandlerPanic` | 恢复屏障接住，归还暂停令牌，连接关闭——与 `Post` panic 同一策略：`State` 可能改到一半，这条连接不能再要了 |
| `OnClose` panic | 是 | 已在其中 | 记日志，清理继续 |

一句话：**`OnClose` 只与成功返回的 `OnOpen` 配对。** 业务因此不必写「我到底初始化到
哪一步了」的防御代码。

多个关闭原因并发时（例如业务正在 `Close`，同时对端 reset），**第一个线性化的 reason 生效**。

---

## 背压

三条线，语义完全不同：

```go
Outbound{
	HighWater:    512 << 10,        // 软：越过之后 c.Writable() 返回 false
	MaxBuffer:    2 << 20,          // 准入：越过之后 Send 返回 ErrSendQueueFull
	StallTimeout: 30 * time.Second, // 卡死：一个字节都写不出去这么久 ⇒ 关闭
}
```

加上一条全局的：`Limits.MaxOutboundBytes` 约束**整个 server 所有连接**的出站积压总和。
只有每连接上限是不够的——`2MB × 10 万连接 = 200GB`，分布式慢客户端不需要让任何一条
连接越线就能拖垮进程。它的默认值是**有限**的（1GB），要放开必须显式写
`gate.Unlimited`——保护性选项的零值是默认值，这条原则对它同样适用（初稿写
「默认不限」，恰好违反了自己定的原则）。它的口径是**近似**的：为了不让一条全局
cache line 被所有发送核心争抢，预算按 loop 租约划拨，瞬时总量可能超出一个租约粒度，
见 [06 的预算模型](06-connection-state-machine.md#预算模型)。

**准入上限不关连接。** `Send` 返回 `ErrSendQueueFull` 时消息**从未进入队列**，
帧流上没有洞，业务知道是**哪一条**没发出去。这比上一版好：上一版是在 flush 时发现
超限，然后关闭连接并丢弃队列里数量未知的消息，业务只能从 `OnClose` 推断出
「有东西没发出去」，却不知道是哪些。

核心不变式：**gate 从不静默丢消息——要么送出，要么明确告诉你没送出。**

**卡死检测才关连接。** 判据是「有没有写出去哪怕一个字节」，而不是「积压有没有超限」：
一条长期贴着上限但在稳定排空的连接是健康的（`ErrSendQueueFull` 已经在提醒业务了）；
一条一个字节都写不出去的连接是死的。`Limits.Idle` 救不了它——空闲判据是「交付过完整
消息」，而一个卡死的对端确实不再发消息。

`StallTimeout` 只抓「零进度」，有一个已知盲区：每 29 秒读走 1 字节的对端可以永久
重置计时，同时占着 `MaxBuffer`。完整保护需要「最老消息年龄」类判据，v1 不加第三个
旋钮，把盲区写明，总代价由 `MaxOutboundBytes` 兜底——见
[03](03-outbound.md#为什么需要-stalltimeout)。

`Writable()` 让业务可以**主动降级**而不是被动断线：

```go
func (h *handler) pushWorldState(c *gate.Conn[*player], snap []byte) {
	if !c.Writable() {
		return // 这条连接跟不上了，丢掉这一帧状态同步，等它追上来
	}
	_ = c.Send(snap)
}
```

关键位置消息照发不误——它们会一直入队到 `MaxBuffer`；真的顶到上限时 `Send` 会明确
报错，业务自己决定是丢弃还是 `c.Close(...)`。

### 内核缓冲占满时会发生什么

两个方向的正确行为**完全不同**，这是理解整套背压设计的关键。

```
对端读得慢 / 网络拥塞
  └─ SO_SNDBUF 满 → writev 返回 EAGAIN 或部分写
       └─ gate 保留未写完的部分，注册可写事件，等内核腾出空间续写   ← 从不阻塞
            └─ 出站队列增长
                 ├─ > HighWater  → Writable() = false          业务可主动降级
                 ├─ > MaxBuffer  → Send 返回 ErrSendQueueFull   消息未入队，连接照常
                 └─ 一个字节都写不出去超过 StallTimeout
                                 → 关闭，OnClose(ErrBackpressure)

对端发得快 / 我们 Pause 了
  └─ 我们不读 → SO_RCVBUF 满 → TCP 通告零窗口
       └─ 对端 TCP 停发，对端 send() 阻塞或 EAGAIN
            └─ 服务端这边：什么都不发生
```

**收方向占满是正常状态，不是故障。** 用户态零内存增长，背压由 TCP 流控完成，
不需要任何应用层机制。这正是 `Pause()` 定义成「一个字节都不从内核读」而不是
「读进来先存着」的原因——后者等于在用户态重新实现内核已经做好的事，而且要为此
再发明一个积压上限。

**发方向占满才是需要处理的。** gate 从不阻塞在 `write` 上，也从不从队列中间丢消息。
三道防线各司其职：软水位让业务降级（`Writable()`），准入线让 `Send` 明确报错
（消息未入队，连接照常活着），**只有卡死检测才关连接**（`StallTimeout`）。

两个实操要点：

- **`Pause()` 期间不计入 `Limits.Idle` 的空闲时长。** 否则一个跑 6 分钟 RPC 的
  `AsyncDo` 会被 5 分钟的空闲判定干掉。恢复投递之后重新开始计时。
- **Linux 上显式 `setsockopt(SO_RCVBUF)` 会关掉内核的接收缓冲自动调优**
  （`tcp_moderate_rcvbuf`），而且内核会把设定值翻倍用于记账。小包网关设成几十 KB 是
  对的；有大消息的业务要按带宽时延积算，往小了拍会直接损失吞吐。`Socket` 的两个字段
  零值都是「不设置」，也就是保留系统默认与自动调优。

> 参考 Netty 的 `WriteBufferWaterMark` + `isWritable()`。Netty 还有一个
> `channelWritabilityChanged` 事件回调；gate 这一版只做轮询式的 `Writable()`，
> 因为它覆盖了绝大多数场景（推送前问一句）而不需要往 `Handler` 上加第四个方法。
> 如果将来出现了真的需要边沿触发的场景，再以 `Options.OnWritable` 这种**可选字段**
> 的形式加进来，不动 `Handler`。

---

## 阻塞任务：AsyncDo 与 Pause

`OnMessage` 跑在事件循环上，**不允许阻塞**。要做 RPC / DB 这类事情，用 `AsyncDo`：

```go
func (h *handler) OnMessage(c *gate.Conn[*player], msg []byte) error {
	c.AsyncDo(func() {
		profile, err := rpc.Load(c.State.id)   // ← 可以直接读写 State
		if err != nil {
			c.Close(err)
			return
		}
		c.State.profile = profile
		_ = c.Send(encode(profile))
	})
	return nil
}
```

**`f` 属于这条连接的串行域**：它不会与 `OnMessage`、`OnClose`、`Post` 的闭包或另一个
`AsyncDo` 并发。因此在 `f` 里访问 `c.State` 是安全的，不需要任何同步，也不需要把结果
`Post` 回去。

### 这个保证是怎么来的

`f` 跑在另一个 goroutine 上，天然有三个并发源。gate 逐个消掉：

| 并发源 | 消除方式 |
| --- | --- |
| ① 同一次回调里调用两次 `AsyncDo` | **拒绝重入**：已有一次在途时，再次调用返回 `ErrAsyncBusy` |
| ② `f` 与**当次回调的剩余部分** | `AsyncDo` 只登记，**等回调返回之后**才启动 goroutine |
| ③ `f` 与 `OnClose` | socket 立刻拆除，但 **`OnClose` 回调推迟到 `resume` 之后** |

外加一条：暂停期间 `Post` 的闭包也**一起挂起**（见下）。

①之所以够，是因为 `AsyncDo` 只能在事件循环上调用，而它一调用连接就暂停，
**不会再有新的 `OnMessage` 被投递**——跨回调的并发本来就不可能，只剩同一次回调里
调两次这一种，拒绝掉即可。

②是把「什么时候 `go f()`」这个决定权收回 gate 自己手里。顺带的好处：`OnMessage`
返回 error（连接要关）时干脆不启动 `f`，省掉一次注定没用的 RPC。

③在语义上其实比不推迟更对——你不会想在一个正在回写玩家状态的 RPC 中途，
就把这个玩家反注册掉。

### 代价

- **`Conn` 与 `State` 会活到 `f` 结束**，比 socket 长。fd、缓冲、poller 槽位在关闭时
  立刻回收，但业务泄漏一个 goroutine 就会跟着泄漏一个 `Conn`。
- **`Shutdown` 不等待在途的 `AsyncDo`**。那些连接的 `OnClose` 会在各自的 `f` 结束后
  才触发；进程先退出的话就不触发了。所以准确表述是：**`OnClose` 至多调用一次；
  业务归还了暂停令牌且进程仍存活时，恰好一次。**

### `Pause`：不带保证的低级原语

```go
resume := c.Pause()   // 从这一刻起不再投递新消息，也不再执行 Post
go myPool.Submit(func() {
	defer resume()
	// ⚠️ 这里**不能**碰 c.State：gate 不知道这段代码什么时候开始、是否独占
})
```

`Pause` 只做一件事：暂停这条连接的串行域。它**不提供** `AsyncDo` 的独占保证——
执行体由业务自己提供，gate 无从知道它何时开始、是否与别的东西重叠。

用它的理由只有一个：**业务想用自己的 worker pool**（限流、复用 goroutine、带 tracing
的执行器），而不是每次 `AsyncDo` 起一个新 goroutine。这种场景下 `State` 的串行性由
业务自己负责，或者用 `Post` 回到事件循环再碰。

### `Post`

```go
func (c *Conn[S]) Post(f func(*Conn[S]))
```

把 `f` 排进该连接所属事件循环的队列执行。`f` 在串行域内，可以安全访问 `State`。

它是给**真正外部的触发源**用的：定时器、广播调度器、管理接口、以及上面那种
自带 worker pool 的 `Pause` 用法。

两条语义：

- 连接已经关闭时 `f` **不会被执行**（也不会报错）。异步回来发现会话没了是常态，
  不该让每个调用点都写一遍判断。
- **连接处于暂停状态时 `f` 排队等待**，`resume` 之后按投递顺序执行。
  这是「暂停 = 整个串行域暂停」的一部分——否则 `Post` 的闭包会和 `AsyncDo` 的 `f`
  在 `State` 上撞车。排队的闭包数量有上限，超过即关闭连接（`ErrPendingOverflow`）。

语义：

- **生效点**：`Pause` 线性化之后，gate 不再对这条连接**发起新的 read 系统调用**。
  它**不能**撤销已经读进用户态的字节——那些留在 gate 的入站残片里，
  上限由 `Limits.MaxPending` 约束。
- 可重入：多次 `Pause` 需要同样多次 `resume` 才恢复。
- `resume` 幂等；在已关闭的连接上调用是 no-op。
- `Pause` 只能在**事件循环线程上**调用（与速查表一致）：回调与 `Post` 的函数体，
  **不含 `AsyncDo` 的函数体**。它要改读闸深度、LRU 归属与 poller 注册，这些是
  loop 私有状态；从别的 goroutine 调用时「单连接内串行」的保证也不成立——
  事件循环可能已经越过了暂停检查。在 `AsyncDo` 里需要它时用 `Post` 排回去。
- **暂停期间不计入 `Limits.Idle` 的空闲时长**，`resume` 之后重新开始计时。
  否则一个跑 6 分钟 RPC 的 `AsyncDo` 会被 5 分钟的空闲判定干掉。
- **但暂停本身有上限**：单次暂停超过 `Limits.MaxPause` 即关闭连接
  （`ErrPauseTimeout`）。漏掉一次 `resume`、或一个永不返回的 RPC，否则会让这条连接
  永久绕过空闲回收——一个业务 bug 变成一条永远不会被释放的 fd。
- 暂停期间对端**仍在发**的数据留在内核 socket 缓冲里，用户态零增长，
  对端被 TCP 流控拖住。
- **代价**：每次 `Pause` / `resume` 各需要一次 poller 的注册变更（`epoll_ctl` /
  `kevent`），量级是微秒。它是给「偶尔一次的阻塞调用」用的，**不适合每条消息都走一遍**。

`Limits.MaxPending` 约束的只是一小块东西：**暂停发生在一批数据中间时，已经从内核读出来、
但还没投递的那部分**。它的上界是「一次读批次 + 一条最大消息」，默认
`MaxMessage + 64KB` 正是按这个算的。它不是「暂停期间对端能发多少」的上限——
那个由内核缓冲和 TCP 流控负责，不需要应用层封顶。

> **为什么原语是 `Pause` 而不是 `AsyncDo`。** 上一版只有 `AsyncDo`，名字讲的是
> 「异步执行一个函数」，而它真正做的事是「暂停读侧」。名实不符导致两个后果：
> 业务无法把执行体交给自己的池，以及「只能在 Handle 内调用」这条约束看起来像是
> 随意的限制而不是它本来的样子。改名之后约束变得自明：暂停/恢复当然要成对出现在
> 同一个投递上下文里。
>
> 这个形状还直接对应底层实现：`Pause` 在 epoll 上是摘掉 `EPOLLIN`，在 kqueue 上是
> `EV_DISABLE` 掉 `EVFILT_READ`——之后数据留在**内核** socket 缓冲里，
> 对端被 TCP 流控自动拖住，用户态一个字节都不占。见
> [平台无关性](#平台无关性为什么-api-里没有-epoll-的影子)。
>
> 这个映射也带来一个必须写下来的**副作用**：读事件被摘掉之后，对端的 FIN / RST
> 也观察不到了（kqueue 尤其明显）。也就是说**暂停期间的断线要等 `resume` 之后才会
> 被发现**。`Limits.MaxPause` 同时兜住了这一条。

---

## 内存与 GC 预算

10 万连接是这个库的存在理由，所以内存必须是**可以被算出来**的，而不是跑起来再看。
这一节列出全部内存项以及它们随什么增长。

### 稳态零堆分配的路径

`go test -benchmem` 可验证，这几条路径稳态下是 0 allocs/op：

- 入站：读 → 解析 → 投递 → handler
- 出站入队：`Send` 只有一次从**分级池**的借用
- 出站 flush：拼包、压缩、加密、`writev` 的 iovec 与 header 暂存
- 加密：`Overhead() == 0` 时全程原地

「零堆分配」不等于「零内存」，而是**不向堆里灌垃圾**：所有短生命周期缓冲都来自池，
用完归还。这直接决定了 GC 的扫描量和 STW 频率——十万连接的网关最怕的不是分配本身，
是分配出来的对象进入 GC 的活跃集。

### 按连接增长的内存

| 项 | 量级 | 备注 |
| --- | --- | --- |
| 连接对象 + `State S` | 与 `S` 有关 | 值类型 `S` 内联，不额外分配 |
| 未投递完的入站残片 | 稳态 **0** | carry ≤ 64KB；大帧缓冲 ≤ `MaxMessage`；`Pause` 时整体受 `Limits.MaxPending` 封顶 |
| 出站队列 | 稳态 **0** | 按在途量付费，上界 `Outbound.MaxBuffer` |
| 内核 socket 缓冲 | **几十 KB ~ 几百 KB** | 由 `Socket.RecvBuffer` / `SendBuffer` 决定 |
| `Cipher` 实现的状态 | 取决于实现 | 见下 |

**内核 socket 缓冲是这张表里最大的一项。** Linux 默认 rmem/wmem 各百来 KB，
10 万连接真吃满就是几十 GB。任何在应用层抠每连接几 KB 的优化，在这一项面前都是零头。
把它按业务实际收紧，是 10 万连接场景下**收益最大的单个动作**。

**per-connection AEAD 状态值得单独量一次。** `crypto/aes` 在有 AES-NI 的机器上 GCM
状态不大，但走通用实现时预计算表能到几 KB——10 万条独立密钥就是几百 MB。
用 per-conn 独立密钥之前先测这个数；房间级共享密钥 + `SendFrame` 是另一条路。

### 按事件循环增长的内存（不随连接数增长）

跨调用的暂存空间一律**按事件循环**池化，不挂成连接级字段：

| 项 | 量级 |
| --- | --- |
| 读缓冲 | 每 loop 一块（64KB 量级） |
| flush 暂存（iovec、header、拼包缓冲） | 每 loop 一套（KB 量级） |
| zstd 编码器 | 见下表 |

出站写全部发生在连接自己的事件循环上且中途不让出，所以池里同时在外的对象数
**等于事件循环数**。挂成连接级字段的话，十万连接光暂存空间就要白占几百 MB。

### 压缩的内存

编码器是**每事件循环一个**（`concurrency = 1`），不是全局共享池。所有压缩都发生在
连接所属的事件循环上，因此不需要任何交接或争用，常驻内存从 `GOMAXPROCS ×`
变成 `Loops ×`——而 `Loops` 是你可配的，`GOMAXPROCS` 不是。

实测每 encoder（首次压缩后的堆增量）：

| `CompressLevel` | 每 encoder | `Loops = 8` |
| --- | --- | --- |
| `CompressFastest`（零值） | 约 0.27 MB | 约 2 MB |
| `CompressBalanced` | 约 1.32 MB | 约 11 MB |
| `CompressBetter` | 约 4.27 MB | 约 34 MB |
| `CompressBest` | 更高 | 更高 |

- **懒初始化**：`CompressThreshold <= 0` 的 gate 一分钱都不付。
- **压缩输出走同一个分级池**，稳态零堆分配。
- **压完反而更大就丢弃压缩结果**：既省对端一次无谓解压，也保证一个刚好贴着上限的
  body 不会因 zstd 膨胀越过协议上限。
- **服务端零解压内存、零解压攻击面**：线路协议不接受上行压缩帧，服务端连解码器都
  不存在，也就不存在解压炸弹。
- `Outbound.Dict` 可以挂一个用协议样本训出来的 zstd 字典。对「几百种消息、每种都
  长得差不多」的游戏协议，它让**单条小消息**压缩也变得划算，而不必依赖合包攒够体积。

---

## 配置

`Options` 按**关注点**分组，而不是拍平成一个几十字段的结构体。分组本身就是文档：
看到 `Socket` 就知道那些字段是 setsockopt，看到 `WebSocket` 是 nil 就知道当前是裸 TCP。

### `Options`

| 字段 | 零值 | 说明 |
| --- | --- | --- |
| `Addr` | 必填 | 监听地址，如 `":8081"` / `"127.0.0.1:0"`（0 表示随机端口，用 `Server.Addr()` 取回） |
| `Loops` | `NumCPU()` | 事件循环数量，夹在 `[1, NumCPU()]` |
| `Handler` | 必填 | 见上 |
| `Outbound` | 见下 | 要推荐行为请显式写 `gate.DefaultOutbound()` |
| `Limits` | 见下 | |
| `Socket` | 见下 | |
| `WebSocket` | `nil` | nil ⇒ 裸 TCP；非 nil ⇒ WebSocket |
| `Proxy` | `ProxyOff` | PROXY protocol，见[真实来源地址](#真实来源地址) |
| `Log` | 必填 | `zap.L()` 即可 |

**`Listen` 会深拷贝并冻结整个 `Options`**（包括 `Outbound.Dict` 和 `WebSocket`
指向的结构）。返回之后再改原来那个结构体不会影响已启动的 server，也不会产生
data race。同理 `DefaultOutbound()` 是**函数**而不是可变的包级变量——后者可以被
任意一个 import 了 gate 的包改掉。

### `Outbound`

| 字段 | 零值 | 说明 |
| --- | --- | --- |
| `CompressThreshold` | 不压缩 | 独立帧按 payload 判定，合并帧按整批字节数判定 |
| `CompressLevel` | `CompressFastest` | 同时决定编码器常驻内存，量级差 16 倍。见[压缩的内存](#压缩的内存) |
| `Dict` | 无 | 可选 zstd 训练字典，让单条小消息压缩也划算 |
| `MaxCluster` | 不合包 | 单个合并帧上限（含子 header）。大于协议上限的取值会被夹住 |
| `MaxBuffer` | **1MB** | 出站积压准入上限，越过时 `Send` 返回 `ErrSendQueueFull`。`gate.Unlimited` 关闭 |
| `HighWater` | `MaxBuffer / 4` | 软上限，越过后 `Writable()` 返回 false |
| `StallTimeout` | **30s** | 积压非空且这么久一个字节都没写出去 ⇒ 关闭。`gate.Unlimited` 关闭 |
| `CloseLinger` | **1s** | `Close` 之后为排空出站队列最多再等多久；`gate.Unlimited` 表示不等待、立即关闭（linger 的「保护」是给排空封顶，关掉上限即不等） |

**合并与压缩是相互独立的开关。** 关掉压缩不会连带关掉合并——大量小消息正是
「该合包但不值得压缩」的典型场景，合包本身就能省下系统调用和小 TCP 段。

**注意零值语义在这里不是「关闭」。** 性能选项（压缩、合包）的零值是关闭，
**保护性选项**（`MaxBuffer`、`StallTimeout`）的零值是**默认值**，要关掉必须显式写
`gate.Unlimited`。理由很简单：忘了配压缩只是慢一点，忘了配出站上限是 OOM。
保护应该是选择退出的，不是选择加入的。

### `Limits`

| 字段 | 零值 | 说明 |
| --- | --- | --- |
| `MaxMessage` | 32MB（协议上限） | 单条入站消息上限。**值得按业务收紧**，它决定单条连接能让服务端缓冲多少数据 |
| `MaxPending` | `MaxMessage + 64KB` | `Pause` 期间「已从内核读出、尚未投递」的字节上限 |
| `MaxConns` | 不限 | 超过后新连接立即关闭（`ErrConnLimit`），`OnOpen` 不会被调用 |
| `MaxOutboundBytes` | **1GB** | 整个 server 所有连接的出站积压总预算；近似口径（loop 配额租约，见 06）；`gate.Unlimited` 关闭 |
| `MaxHandshaking` | `MaxConns / 16` | 处于握手阶段（WebSocket / PROXY）的连接上限 |
| `HandshakeTimeout` | **10s** | 握手必须在这么久内完成，否则关闭（`ErrHandshakeTimeout`） |
| `MaxPause` | **60s** | 单次 `Pause` 的最长时长，超过即关闭（`ErrPauseTimeout`） |
| `Idle` | 不启用 | 超时未**交付过完整消息**的连接会被主动关闭 |
| `KeepAlive` | 60s | `SO_KEEPALIVE` 空闲探测时长；`gate.DisableKeepAlive` 关闭 |

`MaxHandshaking` 与 `HandshakeTimeout` 堵的是同一个洞：**握手阶段的连接还没进入
`Idle` 的管辖范围**（它一条完整消息都没交付过），一个慢速发 HTTP 头的客户端可以在
任何回收机制注意到它之前就把 fd 耗光。这是 slowloris 在 WebSocket 上的形态。

`Idle` 的判据是「最后一次交付完整消息的时刻」，不是「最后一次收到字节」。两个直接后果：
只发半个帧吊着连接的 slowloris 会被回收；反过来，服务端单向下推**不会**让连接显得活跃。

`Idle` 与 `KeepAlive` 互补：keep-alive 处理的是**对端消失**（被 NAT 静默丢弃的半开连接
永远不会触发关闭事件），`Idle` 处理的是**对端还在但已经不是有效会话**。

`MaxMessage` 与 `MaxPending` 分开是这一版的修正。上一版 `MaxMessageSize` 兼了两个职责，
文档里要解释两大段「如果你的 AsyncDo 较慢、客户端还在持续发小包，这个值要按阻塞期间
可能积压多少来配，而不只是按单条消息大小」——那说明它本来就是两个旋钮。

### `Socket`

| 字段 | 零值 | 说明 |
| --- | --- | --- |
| `RecvBuffer` | **不设置**（系统默认 + 自动调优） | `SO_RCVBUF` |
| `SendBuffer` | **不设置**（系统默认） | `SO_SNDBUF` |
| `Nagle` | `false`（即 `TCP_NODELAY`） | 置 true 才打开 Nagle |

**10 万连接下内核 socket 缓冲才是内存大头。** Linux 默认 rmem/wmem 各百来 KB，
真吃满就是几十 GB。一个只跑小消息的网关应该显式把它压到几十 KB。上一版完全没有
透出这两个旋钮，却在应用层花大力气把每连接几 KB 的暂存空间池化掉——数量级差了两个。

但**不要拍脑袋往小了设**：Linux 上显式 `setsockopt(SO_RCVBUF)` 会关掉内核的接收
缓冲自动调优（`tcp_moderate_rcvbuf`），而且内核把设定值翻倍用于记账。有大消息或
高带宽时延积的业务要按 BDP 算，设小了直接损失吞吐。零值（不设置）保留自动调优，
这是安全的默认。占满时的行为见[背压](#内核缓冲占满时会发生什么)。

`Nagle` 用「零值即推荐值」的取反命名，避免 `NoDelay bool` 那种零值等于错误默认的形状。
`KeepAlive` 用 `DisableKeepAlive` 哨兵值也是同一个原则。

---

## Server 生命周期

```go
srv, err := gate.Listen(opts)   // 返回时监听套接字已就绪，可以接受连接
addr := srv.Addr()              // 随机端口场景取回真实地址
...
err = srv.Shutdown(ctx)         // 优雅关闭
```

`Listen` 返回即可用——不需要 sleep 或轮询等待就绪，测试里可以直接连。

`Shutdown` 的顺序：

1. 停止 accept；
2. 对每条连接：停止入站投递 → 尽力 flush 已入队的出站消息 → `OnClose(c, ErrServerClosed)`；
3. 全部关闭后返回。`ctx` 到期则放弃剩余的 flush，直接关闭。

**返回值**：`ctx` 到期、走了强拆路径时返回 `ctx.Err()`——**优雅的那一半没做完**，
可能有已入队的消息没写上线路。socket 无论如何都已经拆干净（或已尽最大努力，
见日志）。正常排空完成返回 `nil`；重复调用等到收尾后返回 `nil`，它没有参与那次
关闭，报不出它的结果。

`Wait()` 目前恒返回 `nil`：事件循环没有能让整个 server 失败的错误路径。返回值留在
签名里是给将来那种错误用的，**不要**拿它当「没出问题」的判据。

`Run(ctx, opts)` = `Listen` + `Wait`，且 `ctx` 取消时自动 `Shutdown`。给 `main()` 用。

### `ctx` 约束的是什么，不约束什么

**约束**：gate 自己的排空与回收——每条连接还愿意为 flush 等多久、整体等多久。

**不约束**：业务代码。Go 没有办法中断一个正在跑的函数。具体地：

- **所有事件循环回调（`OnOpen` / `OnMessage` / `OnClose` / `Post`）都必须是非阻塞的。**
  一个阻塞的 `OnClose` 会卡住整个事件循环，连带成千上万条连接无法回收——
  `ctx` 到期也救不了。要在关闭时做落盘、结算这类耗时清理，**在 `OnClose` 里取一份
  `State` 的快照，把它交给业务自己的 worker，然后立刻返回。**
- **在途的 `AsyncDo` 不会被等待。** `Shutdown` 不知道它们要跑多久，也不能保证它们
  一定会结束。这些连接的 socket 会被正常拆除（fd、缓冲、槽位都回收），
  但它们的 `OnClose` 要等各自的 `f` 结束后才触发；进程先退出的话就不触发了。
  业务需要自己的 `context` 来收敛这些 goroutine。

这两条是 `Shutdown` 能给出确定时限的前提。

> **日志不再是进程级全局的。** 上一版因为底座的 logger 是进程单例，同进程启动多个
> server 时只有第一个的 `Logger` 生效，这个怪癖被原样转嫁给了用户。这一版
> `Log` 是每 server 的。

---

## 错误与关闭原因

`OnClose` 的 `reason` 用 `errors.Is` 判断。这是这一版新增的能力：上一版
`ConnHandler.Close()` 什么都拿不到，而「对端关的 / 空闲回收 / 背压 / 协议错误 /
业务 panic / 服务器下线」在运维上是完全不同的信号。

| reason | 触发 |
| --- | --- |
| `ErrPeerClosed` | 对端正常关闭（TCP FIN / WS close 帧） |
| `ErrPeerReset` | 连接被重置或读写失败 |
| `ErrIdleTimeout` | `Limits.Idle` 到期 |
| `ErrBackpressure` | 出站积压非空且 `Outbound.StallTimeout` 内一个字节都没写出去 |
| `ErrPendingOverflow` | `Pause` 期间入站积压越过 `Limits.MaxPending` |
| `ErrPauseTimeout` | 单次 `Pause` 超过 `Limits.MaxPause` |
| `ErrHandshakeTimeout` | 握手未在 `Limits.HandshakeTimeout` 内完成 |
| `ErrProtocol` | 线路协议违规（包装了 codec / WebSocket 的具体错误） |
| `ErrHandlerPanic` | 业务回调 panic，已被恢复屏障接住 |
| `ErrConnLimit` | 越过 `Limits.MaxConns`（此时 `OnOpen` 都不会被调用） |
| `ErrServerClosed` | `Shutdown` |
| 业务自己的 error | `OnMessage` 返回的、或 `Close(reason)` 传入的 |

注意 `ErrBackpressure` **不是**「积压超过 `MaxBuffer`」——那种情况下 `Send` 返回
`ErrSendQueueFull` 而连接照常活着，见[背压](#背压)。

典型用法：

```go
func (h *handler) OnClose(c *gate.Conn[*player], reason error) {
	switch {
	case errors.Is(reason, gate.ErrIdleTimeout):
		metrics.IdleKick.Inc()
	case errors.Is(reason, gate.ErrBackpressure):
		metrics.SlowClient.Inc()   // 这个数一涨就该去看下行带宽了
	case errors.Is(reason, gate.ErrProtocol):
		metrics.BadClient.Inc()
		log.Warn("protocol violation",
			zap.Stringer("remote", c.Remote()), zap.Error(reason))
	}
	unregister(c.State.id)
}
```

`Close(reason)` 允许业务自定义原因，`nil` 表示正常关闭：

```go
c.Close(errKickedByAdmin)
```

### panic 隔离

`OnOpen` / `OnMessage` / `OnClose` / `OnUpgrade` / `Post` / `AsyncDo` 的函数体，
每一处都有恢复屏障。一条能让业务解码 panic 的畸形消息只会关闭它自己那条连接，
其余 99999 条不受影响。配对规则见[上面那张表](#回调的状态迁移与-panic)。

`Cipher` 的 `Seal` / `Open` 也在屏障之内，但屏障的位置是**每批一次**（一次读事件、
一次 flush），不是每帧一次。理由见 [Cipher](#cipher)。

---

## Cipher

```go
type Cipher interface {
	Overhead() int
	Seal(dst, plaintext, aad []byte) []byte
	Open(dst, ciphertext, aad []byte) ([]byte, error)
}

c.SetCipher(myCipher)  // 只能在事件循环线程上调用；nil 表示关闭加密
```

### 契约

实现必须满足，gate 也依赖这些做长度计算：

| | |
| --- | --- |
| `Overhead()` | 常量，不随调用变化。`0` 表示长度保持 |
| `Seal` 返回长度 | **恰好** `len(plaintext) + Overhead()`。少一个字节帧头就写错了 |
| `Open` 返回长度 | `<= len(ciphertext) - Overhead()`；认证失败返回 error |
| `aad` | gate 传入**该帧最终的 2/4 字节帧头**。AEAD 实现必须把它纳入认证（照 `crypto/cipher.AEAD` 的语义）；`Overhead() == 0` 的实现可以忽略它，代价是得不到帧头认证 |
| `dst` | gate 保证容量足够。`Overhead() == 0` 时 gate 传 `dst = src[:0]`，允许**原地**覆写 |
| aliasing | 除上面那种原地情形外，`dst` 与 `src` 不重叠 |
| nonce | **收发必须使用各自独立的序列**，见下 |
| 并发 | 同一实例只被一个事件循环触碰，实现内部不需要同步 |

### 收发方向必须分开

`Seal` 与 `Open` 由同一个实例服务，但**绝不能共用一个 nonce 计数器**。

两端的收发交错顺序天然不同（服务端先收后发，客户端先发后收），共用计数器会让两侧对
「这是第几个 nonce」得出不同答案，解密直接失败；更糟的是如果两个方向用同一个 key
又撞上同一个 nonce，那是 AEAD 的灾难性失效——同一 key/nonce 加密两段不同明文，
密钥流可被直接还原。

实现内部保持两个计数器（或两套 key）即可，gate 保证 `Seal` 严格按出站帧顺序调用、
`Open` 严格按入站帧顺序调用，两者都在同一个事件循环上。

### 长度上限

`Overhead() > 0` 时密文比明文长，所以准入检查用的是**线路长度**：

```
frameSize(len(data) + Overhead()) <= Limits.MaxMessage
```

也就是说一条贴着 `MaxMessage` 的明文消息在配了 AEAD 之后会被 `Send` 拒绝
（`ErrMessageTooLarge`），而不是编码到一半才发现越限。

### `SetCipher`：事件循环线程上调用，出站经队列 barrier 生效

密钥切换必须**在帧流里有一个确定的位置**——切换点之前的帧用旧密钥，之后的用新密钥，
否则对端无从解密。它一次做两件事：

- **入站**：cipher 是只在 loop 线程上读写的普通字段，切换对下一个被解析的帧生效。
  这就是「只能在事件循环线程上调用」（回调与 `Post` 的函数体）的原因，
  顺带消掉一个原子指针。
  **`AsyncDo` 的函数体不算**——它属于**业务状态**意义上的串行域，却跑在另一个
  goroutine 上，而 cipher 是 loop 私有状态（见[线程模型速查](#线程模型速查)与
  [06](06-connection-state-machine.md#入站串行域内的即时切换)）。
- **出站**：往出站队列追加一个 **cipher barrier**。加密发生在 **flush 时**，不是
  入队时——初稿以为「在事件循环上调用」就足够了，但已入队未编码的消息照样会被
  新密钥追上（第二轮评审的 P0）。编码器遇到 barrier 才切换，并结束当前的 compound
  分组。见 [03](03-outbound.md#setcipher队列里的-barrier)。

异步握手完成后换密钥的典型写法——**从 `AsyncDo` 里必须经 `Post` 排回 loop**，
不能直接调：

```go
c.AsyncDo(func() {
	negotiated := kdf(...)          // 慢活：在别的 goroutine 上做
	c.Post(func(c *gate.Conn[*player]) {
		c.SetCipher(negotiated)     // 回到 loop 线程才碰 cipher
		_ = c.Send(handshakeDone)   // 这条以及之后的都用新密钥
	})
})
```

在 `AsyncDo` 的函数体里直接 `c.SetCipher(...)` 是数据竞争：同一时刻 loop 可能正因
超时、`Shutdown` 或写失败在执行拆除。gate 检测不到这类误用（Go 里没有可靠的
「我在哪个 goroutine 上」），由 `-race` 兜底。

### 分配

1. **`Overhead() == 0`（流式 / CTR 类）走原地路径。** gate 传 `dst = src[:0]`，
   全程零拷贝零分配——与上一版 `Encrypt([]byte)` 成本相同。
2. **`Overhead() > 0`（AEAD）时 `dst` 由 gate 从分级池提供**，容量保证是
   `len(src) + Overhead()`。实现只要 `append` 进去就不会分配——这正是
   `crypto/cipher.AEAD.Seal` 的契约。
3. nonce 由实现自己的计数器产生，不需要每帧分配。

### panic 屏障放在批边界

`Seal` / `Open` 在热路径上被逐帧调用，每帧一次 `defer recover()` 太贵。
但「一个会 panic 的 `Cipher` 实现能打死整个进程」也不能接受——那是上一版留下的、
唯一一条「你必须遵守否则进程会死」的约定。

这一版把屏障放在**批边界**：一次读事件一个、一次 flush 一个。成本是每批约 2ns
（不是每帧），而爆炸半径仍然收敛到单条连接——批处理到一半 panic 时那条连接本来
就要关掉。

代价：同一批里 panic 之前已经处理的消息**已经投递给业务了**，之后的不会。
这与 `OnMessage` 返回 error 时的行为一致，业务不需要理解第二套语义。

### 帧头在认证范围内（AAD）

`aad` 参数就是**该帧最终的帧头**（长度 + `m`/`z`/`c`/`e`）。任何被中间人翻过标记位
或长度的帧在 `Open` 时认证失败——堵住的是 `z`/`c`/`e` **清位**那一类静默误解释
（逐位、逐方向的分析见 [02 的降级攻击一节](02-wire-protocol.md#降级攻击翻转标记位)）。
「配了 Cipher 就要求每帧 `e = 1`」的检查仍然保留：AAD 只保护走进 `Open` 的帧，
`e` 被清成 0 的帧根本不解密，必须由这条规则逼进认证路径。

一句话定位：**gate 的 `Cipher` 是防嗅探、防外挂的纵深防御，不是用来对抗主动中间人的**
——那是 TLS 的职责，而 gate 明确要求前置 TLS 终结。AAD 只是让「一次位翻转导致
静默误解释」在这道纵深防御里也不成立。

> **为什么要改掉 `Encrypt([]byte)` / `Decrypt([]byte)`。** 原地、不改长度、无返回值的
> 形状排除了所有 AEAD——而游戏网关要做防篡改迟早会撞上。更重要的是 `Decrypt`
> 没有错误通道：认证失败是**正常的协议错误**，不该只能靠 panic 表达。
> 现在 `Open` 返回 error，连接以 `ErrProtocol` 关闭，业务在 `OnClose` 里就能看到。

---

## WebSocket

```go
WebSocket: &gate.WebSocketOptions{
	Path: "/ws",
	OnUpgrade: func(h *gate.Handshake) error {
		if h.Header.Get("Authorization") != wantToken {
			return gate.RejectUpgrade(http.StatusUnauthorized, "unauthorized")
		}
		return nil
	},
},
```

- `WebSocket == nil` 是裸 TCP。**一个字段决定传输**，不存在「设了 WS 参数但忘了改
  Transport 枚举」这种矛盾状态。
- `OnUpgrade` 在写出 101 之前调用，可以读 URI 和全部请求头做鉴权、取
  `X-Forwarded-For` 里的真实来源。返回任意非 nil error 即拒绝握手（默认 401）；
  要自选状态码用 `RejectUpgrade`。
- 握手完成后，`c.Handshake()` 在 `OnOpen` / `OnMessage` 里都能拿到同一份信息。
- 请求头最多记录 64 条，**超出直接拒绝握手而不是截断**。HTTP 头没有「重要的排在前面」
  这种保证：静默丢弃意味着攻击者只要在前面塞满 64 条无关头，`Authorization` 就对
  钩子不可见，把「头不存在」当成匿名放行的鉴权逻辑会被直接绕过。
- 握手必须在 `Limits.HandshakeTimeout` 内完成，同时并发握手数受 `Limits.MaxHandshaking`
  约束。见 [`Limits`](#limits)。

### 鉴权放在 `OnUpgrade`，不要放在 `OnOpen`

`OnOpen` 发生在 **101 已经写出去之后**。此时返回 error 只能把一条已经升级成功的连接
异常断掉——对端会看到一个莫名其妙的断线，而不是一个带状态码的 HTTP 拒绝。

所以分工是：**`OnUpgrade` 做鉴权（可以返回 HTTP 状态码），`OnOpen` 只做连接级初始化。**
需要把鉴权结果带进 `OnOpen`，从 `c.Handshake()` 读——`OnUpgrade` 校验过的那份
`Handshake` 就挂在连接上。

### 本地主动关闭会发送 close 帧

`Close(reason)`、空闲回收、`Shutdown` 这些**正常关闭**会先发一个 WebSocket close 帧
再关 TCP，对端因此能拿到一个状态码而不是 1006（abnormal closure）。**协议错误也发**
（1002——坏的是入站方向，出站还能写）；只有背压和写失败直接关 TCP，
那时出站方向根本写不动或已经有洞。

具体的 reason → close code 映射见 [05-websocket](05-websocket.md#关闭语义)。

> `RejectUpgrade` 的存在是为了不泄漏底层 WebSocket 库的类型。上一版要求业务
> 返回 `ws.RejectConnectionError(...)`（gobwas/ws 的类型）才能自选状态码——那意味着
> 换库就是一次破坏性升级。

### 与 TCP 的语义对齐

WebSocket 模式下业务代码**一行都不用改**。gate 承诺两种传输对业务呈现相同语义，
包括边界情况：一条 WS 二进制消息里可以承载多条 gate 消息（合包），跨 WS 分片的 gate
消息会被重组，`OnMessage` 看到的永远是完整的单条业务消息。

这条承诺由端到端测试矩阵持续验证——同一份用例在两种传输上各跑一遍，
而不是靠一句注释。

---

## 真实来源地址

挂在四层 LB 后面时 `Remote()` 是 LB 的地址，做封禁、限流、审计都用不了。
gate 支持 PROXY protocol（v1 与 v2）在第一个 gate 帧之前解出真实来源：

```go
Proxy: gate.ProxyRequired,   // ProxyOff（默认）/ ProxyOptional / ProxyRequired
```

- `ProxyRequired`：必须有 PROXY 头，否则拒绝连接。**挂在 LB 后面时用这个。**
- `ProxyOptional`：有就解析，没有就用 socket 地址。仅用于灰度切换。
- 解析成功后 `Remote()` 返回的就是真实来源，业务无需改动。

**这个能力不能推给「以后再说」**：一旦 LB 开了 PROXY protocol 而 gate 不认，
那个头会被当成第一个 gate 帧解析，连接立刻按协议错误断开。要么支持，要么部署方式受限。

> WebSocket 场景下也可以从 `X-Forwarded-For` 取。但**不要无条件信任它**——
> 那个头是客户端可以自己伪造的。只有确认对端是可信代理（用 `ProxyRequired`，
> 或在 `OnUpgrade` 里校验 `Remote()` 落在代理网段内）之后，读它才有意义。

---

## 日志与统计

### `Logger`

```go
type Field = zap.Field   // 类型别名，零成本、完全互换

type Logger interface {
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}
```

`*zap.Logger` **直接满足**这个接口，`Log: zap.L()` 即可。用 slog 的项目传
`gate.SlogLogger(slog.Default())`。

只有三级、没有 `Debug` / `Fatalf`、也没有 `printf` 风格：

- 没有 `printf`：结构化字段才能被采集和聚合，`"[gate] %s: inbound protocol error: %v"`
  这种日志在 10 万连接的量级上是不可查询的。
- 没有 `Debug`：gate 的原则是**每一条日志都应该是可执行的信号**。需要连接级追踪时
  用 `Stats()` 和 `OnClose(reason)`，它们是结构化的、可聚合的、且没有采样问题。
- 没有 `Fatalf`：库不该决定进程的生死。

> **为什么 `zap.Field` 出现在公共 API 里，而 D7 说底座类型不该出现。** 界线是
> **谁选的**：`gnet/pkg/logging.Logger` 和 `ws.RejectConnectionError` 是 gate 的
> **实现选型**，换底座它们就变，泄漏出去意味着换底座那天所有用户的编译都会挂；
> `zap.Field` 是**调用方**的选型，gate 换 reactor 不会导致它变。
>
> gate 的日志全部在错误路径上，所以 zap 的零分配优势对 gate 本身不是决定性的——
> 选它的真正理由是让 gate 的日志能直接进业务已有的 sink 和字段规范。
> 类型别名（而不是直接写 `zap.Field`）是留的一道口子：真要迁移，gate 侧改一行别名
> 加一个适配器就够了。

### `Stats`

```go
func (s *Server[S]) Stats() Stats        // 全 server 汇总
func (s *Server[S]) LoopStats() []Stats  // 逐 loop，用来看倾斜

type Stats struct {
	// 连接
	ConnsOpen        int64  // 当前
	ConnsHandshaking int64  // 当前处于握手阶段
	ConnsPaused      int64  // 当前被 Pause
	ConnsOverHighWater int64 // 当前 Writable() == false
	ConnsAccepted    uint64 // 累计
	ConnsRejected    uint64

	// 流量
	MessagesIn  uint64
	MessagesOut uint64
	BytesIn     uint64
	BytesOut    uint64 // 线路字节（压缩后）
	BytesOutRaw uint64 // 压缩前
	FramesOut   uint64 // 线路帧数

	// 资源水位
	OutboundQueued int64  // 当前出站积压总字节
	PendingInbound int64  // 当前入站残片总字节
	PoolMiss       uint64 // 分级池取不到、走了新分配

	// 压力信号
	WriteEAGAIN   uint64 // writev 写不完的次数
	SendQueueFull uint64 // Send 因准入上限被拒的次数
	LoopLagNanos  uint64 // 一轮迭代耗时的累计值，除以迭代数即平均循环延迟

	// 关闭原因分布（完整覆盖 OnClose 的 reason 表）
	ClosedPeer, ClosedIdle, ClosedBackpressure, ClosedProtocol,
	ClosedPanic, ClosedPauseTimeout, ClosedHandshakeTimeout,
	ClosedPendingOverflow, ClosedByServer, ClosedByHandler uint64
}
```

两个比值是这个库最该被观察的东西：

- `BytesOut / BytesOutRaw` —— 压缩率
- `MessagesOut / FramesOut` —— 合包聚合度（每个线路帧平均携带多少条业务消息）

而 10 万连接下最常见的三类故障，各自对应一组指标：

| 故障 | 看什么 |
| --- | --- |
| 单个 loop 倾斜 | `LoopStats()` 里 `ConnsOpen` 与 `LoopLagNanos` 的分布 |
| 慢客户端拖垮内存 | `OutboundQueued`、`ConnsOverHighWater`、`WriteEAGAIN`、`SendQueueFull` |
| 内存预算逼近上限 | `OutboundQueued` vs `Limits.MaxOutboundBytes`、`PendingInbound`、`PoolMiss` |

计数器是每事件循环独立的（同一条 cache line 上不会有跨核往返），`Stats()` 读的时候
求和。热路径上没有任何接口调用。

> **为什么没有 `Observer` 回调接口。** 上一版把 `SenderI` 设计成可替换的，
> 于是业务做埋点的正统方式是「用 wrapper 包住内置 sender」——而这引出了一整个
> 内部机制（`flushHook`）来绕开「wrapper 会让类型断言失败」的问题。
> 这一版把埋点需求拆成两半：连接级事件由 `OnClose(reason)` 承担，
> 线路级计数由 `Stats()` 承担。两者都不需要在热路径上做接口分派，
> 也不需要业务实现任何东西。第三条路（`Observer` 接口）会与它们重叠，所以不做。

---

## 平台无关性：为什么 API 里没有 epoll 的影子

gate 要同时支持 Linux（epoll）和 macOS（kqueue）。两者的能力**不等价**，因此有一条
硬性约束：**公共 API 里不允许出现任何只在一边成立的概念。**

上面的每个 API 原语在两个平台上都有落点：

| API | Linux / epoll | macOS / kqueue |
| --- | --- | --- |
| `Pause()` / `resume()` | `EPOLL_CTL_MOD` 去掉 `EPOLLIN` | `EV_DISABLE` / `EV_ENABLE` on `EVFILT_READ` |
| `Post` / 跨 goroutine `Send` 唤醒 | `eventfd` + MPSC 队列 | `EVFILT_USER` + MPSC 队列 |
| 各种超时扫描 | `epoll_wait` 的 timeout 参数 + per-loop LRU 链表 | `kevent` 的 timespec + 同一套 LRU |
| 出站合批 | `writev` | `writev` |
| `Writable()` | 用户态积压记账 | 同左（与 poller 无关） |
| accept 分发 | `SO_REUSEPORT`，内核哈希到各 loop | Darwin 没有对应的内核级 accept 负载均衡（FreeBSD 是 `SO_REUSEPORT_LB`），退化为单 acceptor + 轮转投递 |

定时器那一行值得单独说：gate **不用** `timerfd` / `EVFILT_TIMER`。每轮迭代本来就要
算出「下一个截止时刻」，把它当作 `epoll_wait` / `kevent` 的超时参数传下去就够了——
少一个 fd、少一处平台差异、少一类「定时器 fd 泄漏」的故障模式。毫秒精度对秒级的
空闲判定绰绰有余。

最后一行是唯一一处平台行为有实质差异的地方，而它**完全不可见**：`Options` 里没有
`ReusePort` 这个字段，因为它在 macOS 上没有 Linux 的语义。gate 在 Linux 上用它、
在 macOS 上不用，业务不需要知道。

同理，`Options` 里也没有 `EdgeTriggered`。触发模式是实现细节，而且这一版的
`Pause()` 语义（暂停期间**一个字节都不从内核读**，让 TCP 流控替我们做背压）在
水平触发下才自然成立——把它做成用户可配的旋钮等于把一个内部不变式交给用户来维护。

> 这一条约束也是先写接口文档、后写 reactor 文档的原因：只要公共 API 不泄漏 poller 的
> 概念，换底座（gnet → 自建 epoll/kqueue → io_uring）就是一次内部重构；泄漏了，
> 就是一次破坏性升级。上一版 `Config.Logger` 用的是底座库的类型，
> 这意味着换底座那天所有用户的编译都会挂——这正是要避免的事。

---

## 设计决策记录

每条记录的格式：**问题 → 选择 → 代价 → 参考**。

### D1. Builder + 可选接口 → 单一 `Handler`

**问题。** 上一版是 `ConnHandlerBuilder.Build(*Conn) ConnHandler`，每连接构造一个
handler 对象；`Build` 发生在连接建立时，但 WebSocket 此刻握手响应还没写完，
在 `Build` 里发数据会把一个 WS 数据帧插进未完成的 HTTP 响应里，连接直接作废。
于是补了一个可选接口 `ReadyHandler.OnReady`，并在文档里加粗警告「**必须**用 OnReady」。

这是**用文档兜住一个本可以结构性消除的陷阱**：一个正确的 API 不该有「这个回调里
不能做那件事」这种规则。

**选择。** 一个 `Handler` 服务所有连接，`OnOpen` 的调用时机就是「连接真正可写」。
两种传输都成立，于是没有陷阱可踩，也不需要第二个接口。

**代价。** `Handler` 变成并发调用，业务不能再把连接私有状态放在 handler 字段里——
必须放进 `S`。这是好事：上一版那种写法在多事件循环下本来就只是碰巧安全。

**参考。** Netty 的 `ChannelHandler` 是每 Channel 一个实例（对应上一版），
但它同时提供 `@Sharable` 的单实例模式，而高性能场景一律用后者。
evio 更彻底：回调全是全局函数，连接状态由用户自己按 id 索引。gate 取中间路径——
单 handler + 类型化的每连接状态。

### D2. `Handle` 无返回值 → `OnMessage() error`

**问题。** 上一版业务要拒绝连接只能调 `conn.Close()`，而底座的 Close 是异步的，
于是 gate 必须额外维护一个 `closing` 标记，让入站投递循环能立刻停手而不是等到
真正关闭——每条消息多一次原子读，只为了让「第一条鉴权失败挡住同批后面的消息」成立。

**选择。** `OnMessage` 返回 error。「停止本批投递」从一个需要跨状态同步维护的不变式，
变成一个 `return err` 的本地控制流。

**代价。** 业务必须区分「这条消息处理失败但连接可以继续」（返回 nil，自己记日志）
和「这条连接不能要了」（返回 error）。这是应该被显式表达的区别。

**参考。** evio 的回调返回 `action`（`None` / `Close` / `Shutdown`）。gate 用 error
而不是枚举，因为 error 顺带携带了**原因**，正好喂给 D3。

### D3. `Close()` → `OnClose(c, reason)`

**问题。** 上一版 `ConnHandler.Close()` 没有任何参数。10 万连接的网关上，
「对端关的 / 空闲回收 / 背压关的 / 协议错误 / 业务 panic / 服务器下线」是完全不同的
运维信号，而它们全都只存在于 gate 自己的日志里——业务想做「慢客户端比例」这种
基础指标都做不到。

**选择。** `reason error` + 一组可 `errors.Is` 的哨兵。

**代价。** 几乎没有。不关心的业务写 `func (h) OnClose(c, _ error) {}`。

### D4. `SenderI` / `SenderBuilder` → `Outbound` 值 + 内部实现

**问题。** 上一版把出站策略做成了可替换接口 `SenderI`，但内置实现依赖一个**不在
接口里**的私有方法 `flush()`，于是不得不引入 `Conn.flushHook` 让内置 sender
自己登记——因为业务用 wrapper 包住内置 sender 之后，类型断言必然失败。

这就是症状：`SenderI` 名义上是扩展点，实际上没有人能正确实现它。任何第三方实现
都要重新发明唤醒不变式、字节计量口径、内存归属、关闭闸口配合。它是**内部策略被
误提升成了公共接口**。

**选择。** 出站策略降为纯数据（`Outbound` 结构体），实现不再暴露。业务包 wrapper
真正想做的三件事——埋点、限流、审计——分别由 `Stats()`、`Writable()` + `Send` 的
返回值、`OnClose(reason)` 承担。

**代价。** 失去了「完全自定义出站编码」的能力。这个能力上一版也只是名义上存在。

**参考。** Netty 的 `ChannelOutboundBuffer` 同样是内部类，暴露给用户的是水位线配置
和 `isWritable()`，不是「你可以换掉整个出站缓冲实现」。

### D5. `SendStatic` 的永久不可变契约 → `Frame`

见 [Frame](#frame预编码帧与广播)。顺带把「广播编码一次」从上一版的已知缺口变成能力。

**参考。** Netty `ByteBuf` 的 `retain()` / `release()`。

### D6. 泛型 `S` 而不是 `any` 状态槽

**问题。** 单 `Handler` 之后，每连接状态需要一个去处。`any` 字段最简单，
但每条消息要一次类型断言，值类型状态还要装箱分配。

**选择。** `Conn[S]`，`S` 内联。

**代价。** `Options` / `Server` / `Conn` / `Handler` 带类型参数，签名变长。
传染范围止步于这四个类型。

**权衡依据。** 这个库的存在理由就是 10 万连接下的常驻内存和每消息成本。
在这两件事上，「省掉每连接一次分配 + 每消息一次断言」正好落在它该关心的地方。
用 `any` 会显得更「简洁」，但那是把成本推给了每一个用户。

### D7. 底座类型不出现在公共 API

**问题。** 上一版 `Config.Logger` 的类型是 `gnet/pkg/logging.Logger`，
`OnWebSocketUpgrade` 自选状态码要返回 `gobwas/ws` 的类型。**这两处都意味着换底层
库的那天，所有用户的编译都会挂。**

**选择。** gate 自己定义 `Logger` 和 `RejectUpgrade`，底座类型只出现在内部适配层。

**代价。** 多两个类型声明和一层内部适配。

**界线在哪。** 这条原则针对的是**实现选型**的类型——换底座它们就变。调用方选型的
类型（例如 `zap.Field`）不在此列，见 D16。

### D8. `AsyncDo` → `Pause` 为原语

见 [阻塞任务](#阻塞任务asyncdo-与-pause)。名字讲清楚它真正做的事，同时让业务能接管执行体。

### D9. 拆开 `MaxMessageSize` 的两个职责

上一版这一个字段既是「单条消息上限」又是「阻塞期间入站积压上限」，文档要为此解释
两大段。两个职责就是两个字段：`Limits.MaxMessage` 和 `Limits.MaxPending`。

### D10. 透出 socket 缓冲配置

10 万连接下内核 socket 缓冲是内存大头，上一版完全不可配。见 [`Socket`](#socket)。

### D11. `Cipher` 改成 `Seal` / `Open`

见 [Cipher](#cipher)。为 AEAD 留出空间，同时给解密失败一个合法的错误通道。

### D12. `Transport` 枚举 + 并列 WS 字段 → `WebSocket *WebSocketOptions`

一个决策而不是两个，不存在互相矛盾的配置状态。零值（nil）落在更常见的裸 TCP 上。

### D13. `Send` 保留复制语义，另开一条零拷贝路径

**问题。** 复制看起来像是白付的开销，尤其在「业务刚序列化完一块内存，转手就交给
`Send`」这个最常见的场景里——那是第二次复制。

**选择。** 默认路径保留复制，但让它落在**分级 size-class 池**上；
另开 `SendFunc` 让业务直接序列化进出站缓冲（形态的选择见 D19）。

**为什么不换成默认所有权移交。** 那会让最常用的接口带上一条「调用后不能再碰这块内存」
的隐性契约，而这类契约的违反是**静默的**：数据被改写、发出去的是半新半旧的字节，
没有任何地方会报错。默认接口应该是**误用了也只是慢一点**的那个。

**为什么不是每连接 ring buffer。** 10 万连接下按连接预留不成立，且环的尺寸必须
按最大消息定。分级池按在途量付费，而在途量已经被 `MaxBuffer` 封顶。

**代价。** 两个额外的方法，以及它们自己的所有权规则——但这条规则只在你主动选择
这条路径时才需要遵守。

### D14. zstd 编码器改为每事件循环一个，默认等级改为 `CompressFastest`

**问题。** 上一版按压缩等级共享一个 `concurrency = GOMAXPROCS` 的编码器，
`EncodeAll` 内部要从 channel 里抢一个 encoder。常驻内存由 `GOMAXPROCS` 决定，
而它在容器里未必是你想要的数，也不可配。

**选择。** 每事件循环一个 `concurrency = 1` 的编码器。所有压缩本来就发生在连接所属的
事件循环上，所以不需要任何交接；常驻内存变成 `Loops ×`，而 `Loops` 是配置项。

默认等级从 `CompressBetter` 改成 `CompressFastest`（并让它成为零值）：在游戏协议
batch 样本上实测 Fastest 比 Better 快 24% 且压缩率相同，而常驻内存差 16 倍
（0.27MB vs 4.27MB 每 encoder）。真实数据上 Better 通常确实更小，需要的人显式配。

**代价。** 默认压缩率在某些负载上会略降。这是可配的，而 16 倍的内存差不是。

### D15. `Close` 之后的 `Send` 行为写成三档，而不是承诺一个确定值

见[并发与关闭语义](#并发与关闭语义)。**「`Close` 返回之后 `Send` 一定报错」是硬保证；
与 `Close` 并发的 `Send` 返回什么则不保证。**

后者做不到确定，也不打算做到：要确定就得让 `Send` 阻塞等事件循环确认，
而「不阻塞、只入队」是这套设计的前提。把它写清楚，比假装它是确定的、
或者用一把锁把所有 `Send` 串起来要诚实。

同一条记录还定死了**已入队消息在不同关闭原因下的处置**：正常关闭尽力 flush，
背压和写失败一律丢弃——因为那两种情况下帧流已经有洞了。

### D16. `Logger` 采用 zap 形状

见[日志与统计](#日志与统计)。这条与 D7 看似冲突，界线在于**类型是谁选的**：
实现选型的类型不能泄漏，调用方选型的类型可以。用类型别名 `type Field = zap.Field`
留一道迁移口子。

### D17. `*Conn[S]` 永不复用

**问题（这一版最严重的一个）。** 初稿一面承诺「`Close` 之后所有发送接口永远返回
`ErrConnClosed`」，一面说连接对象会被池化复用，同时又允许任意 goroutine 长期持有它。
这三条不可能同时成立：业务的注册表、延迟任务、`resume` 闭包里都可能留着旧指针，
对象一旦被新连接复用，旧 goroutine 的一次 `Send` 就把数据发给了**另一个玩家**。

这不是丢包或普通竞态，是**跨会话的数据泄漏**，而且没有任何一层会报错。
`ID()` 也救不了——它能让注册表 key 不串，但挡不住方法调用。

**选择。** 池化连接**内部**的资源（读残片缓冲、出站 chunk 节点、poller 槽位），
**绝不池化 `Conn` 对象本身**。每条连接一次堆分配。

**代价。** 连接建立多一次分配。连接建立本来就要走若干次系统调用，这一次无关紧要——
「稳态零分配」说的是消息路径，不是连接建立路径。

**替代方案为什么不行。** 「池化 + 每个公共方法校验 generation」看起来可行，
但 `State` 是一个**字段**，字段访问没有地方插校验。要么放弃泛型内联状态，
要么放弃池化。放弃池化便宜得多。

### D18. 把 `AsyncDo` 的函数体拉进串行域

**问题。** 初稿写「`AsyncDo` 的函数体与回调互斥」，但没有任何机制保证它。
实际上有三个并发源：`AsyncDo` 一返回 goroutine 就可能开始跑而 `OnMessage` 还没结束；
同一次回调里两个 `AsyncDo` 之间；以及 `OnClose`（对端 reset、`Shutdown`、写失败随时
可能触发）。按初稿的说法去写业务，`c.State` 上就是一个数据竞争。

**第一版修法（已放弃）**：承认并发，禁止在 `f` 里访问 `State`，用 `Post` 回写。
安全，但把每个异步逻辑都劈成两段，而且「不能碰 `State`」是一条**违反了只会静默出错**
的软契约。

**最终选择：把三个并发源逐个消掉**，让 `f` 真的进入串行域。

| 并发源 | 消除方式 | 代价 |
| --- | --- | --- |
| 同一次回调里调两次 | 拒绝重入，返回 `ErrAsyncBusy` | 无 |
| 与当次回调的剩余部分 | 只登记，回调返回后才 `go f()` | 无（还省掉一次注定没用的 RPC） |
| 与 `OnClose` | socket 立刻拆，`OnClose` 推迟到 `resume` 之后 | `Conn` 活得比 socket 长 |

**跨回调的并发本来就不可能**——`AsyncDo` 只能在事件循环线程上调用（回调与 `Post`
的函数体），而它一调用连接就暂停，不会再有新的 `OnMessage` 被投递。所以只需要处理
「同一次回调里调两次」。

**代价。** `Conn` 与 `State` 活到 `f` 结束；`Shutdown` 不等待在途的 `AsyncDo`，
那些连接的 `OnClose` 推迟触发甚至不触发（进程先退出时）。因此
「`OnClose` 恰好一次」被精确成「至多一次；归还了暂停令牌且进程存活时恰好一次」。

**`Pause` 保持为不带保证的低级原语**：执行体由业务提供，gate 排不了序。
它存在的理由是让业务能用自己的 worker pool。

**与 Netty / Seastar 的差别。** 它们的答案是「异步逻辑劈两段，结果 `submit_to` 回
loop」，也就是被放弃的第一版。gate 能做得更进一步，是因为它对异步任务的形态做了更强的
假设：**一条连接同时至多一个在途任务，且必须从串行域内发起**。这个假设对网关业务成立
（一条连接的消息本来就要串行处理），对通用框架不成立，所以它们没法这么做。

### D19. `Reserve` / `Commit` → `SendFunc`（回调形态）

**问题。** 裸 `[]byte` 承载不了所有权移交的契约：`append` 换掉底层数组、忘记提交、
重复提交、跨连接提交、`fill` 中途 panic——每一种都是静默失败，而且未提交的缓冲
不受预算约束。

**选择。** 改成 `SendFunc(n, fill)`。所有上述误用在结构上都不可能，
[对照表见上](#sendfunc零复制路径)。

**代价。** 一个闭包。`fill` 不被 gate 保存，通常能栈上分配。

### D20. `Frame` 由 GC 管理，不公开引用计数

**问题。** 公开的 `Retain` / `Release` 把「少一次 = 泄漏、多一次 = 仍在发送的连接读到
已回池并被覆盖的内存」这一整类缺陷搬进了 API。

**选择。** `Frame` 不可变、由 GC 管理，gate 的队列自然持有引用。API 少两个方法。

**权衡依据。** 它优化错了对象：`Frame` 的分配频率是**每次广播一次**（每 tick 一条），
不是每连接一次。将来 profile 真指向这里，在**内部**加引用计数即可，那是实现细节。

顺带把 `NewFrame` 从包级函数改成 `Server` 的方法——它必须拿到该 server 的压缩等级、
阈值和字典，否则编不出和普通 `Send` 一致的帧。

### D21. 删掉明文旁路，`SendNoEncrypt` → `SendAlone`

`SendNoEncrypt` 在一条配了 `Cipher` 的连接上同时关掉了压缩、合包**和加密**，
任何一处业务代码都能把明文送上线路且不会报错。`SendAlone` 只关掉压缩与合包，
**没有任何接口能绕过连接的 `Cipher`**。

同时它修好了另一件事：[02](02-wire-protocol.md#压缩侧信道crime-类) 里对 CRIME 类
侧信道的规避建议需要的是「不压缩不合包」，而不是「不加密」——旧接口给的是错的那个。

### D22. 背压：准入控制 + 卡死检测，而不是「超限即关闭」

见[背压](#背压)。准入控制让业务知道**是哪一条**消息没发出去（消息从未入队，帧流没有洞），
卡死检测用「有没有写出去哪怕一个字节」而不是「积压有没有超限」做判据。
另加 `Limits.MaxOutboundBytes` 约束全 server 总量——`2MB × 10 万 = 200GB`，
只有每连接上限是挡不住分布式慢客户端的。

同时确立一条零值原则：**性能选项的零值是关闭，保护性选项的零值是默认值**，
要关掉必须显式写 `gate.Unlimited`。忘了配压缩只是慢一点，忘了配出站上限是 OOM。

复核后把这条原则贯彻到 `MaxOutboundBytes` 自己头上：初稿给它的默认值是「不限」，
恰好违反本条——现改为有限默认（1GB），并明确其近似口径（loop 配额租约，见 06）。

### D23. 补齐三个「无人管辖」的时间窗口

每一个都是「连接存在，但现有的回收机制都管不到它」：

| 窗口 | 为什么现有机制管不到 | 新增 |
| --- | --- | --- |
| 握手阶段 | 一条完整消息都没交付过，`Idle` 不认它 | `HandshakeTimeout` + `MaxHandshaking` |
| `Pause` 期间 | 刻意不计入 `Idle` | `MaxPause` |
| 出站卡死 | 对端不发消息也算「没超时」，`Idle` 不认 | `StallTimeout` |

三者都搭在 reactor 已有的每轮超时扫描上，不引入新的时间源。

### D24. `Cipher` 带上 AAD——推翻初版的「不做 AAD」论证

初版拒绝 AAD 的两条理由（长度循环依赖、4 个 bit 不值一个参数）都被第二轮评审推翻：
`Overhead()` 是常量、`Seal` 长度精确，先算长度拼头再 seal 即可，不存在循环依赖；
而初版的翻位分析漏了 `1→0` 方向——`z`/`c` 清位在下行方向是**静默**的，
「其余翻转都可检测」这个前提本身就错了。完整分析见
[02 的 W9](02-wire-protocol.md#w9-完整帧头进入-aead-认证范围aad)。
`aad` = 该帧最终帧头，由 gate 生成并传入；实现照 `crypto/cipher.AEAD` 语义处理。

### D25. `SetCipher` 的出站生效点是队列 barrier

「只能在事件循环上调用」是必要条件，不是充分条件：加密发生在 flush，已入队未编码
的消息会被新密钥追上。出站生效点必须表达为**队列里的 barrier**（编码器遇到才切换、
并结束当前 compound 分组）；入站则是串行域字段的即时切换。调用约束从「事件循环上」
精确化为「串行域内」——`AsyncDo` 的函数体也是合法调用点（它属于串行域，见 D18）。

### D26. `PreEncrypted` 不复用线路 `e` 位

`e` 位属于**连接级** Cipher 的职权。共享帧若带 `e = 1`：配了 Cipher 的对端会用连接
密钥去解房间密钥封的帧（必然失败），没配的对端按 02 的 W3 直接拒收——两头都走不通。
所以 `PreEncrypted` 重定义为「payload 是业务自理的密文」：gate 只跳过压缩、
不碰标记位，解密由对端业务完成。见 [Frame 的与加密的关系](#与加密的关系)。


---

## 参考与借鉴对照

| 来源 | 借鉴 | gate 里的落点 |
| --- | --- | --- |
| **Netty** | `ByteBuf` 的不可变共享缓冲 | `Frame`，广播编码一次（但**不**照搬它的公开引用计数，见 D20） |
| **Netty** | `WriteBufferWaterMark` + `isWritable()` | `Outbound.HighWater` + `Conn.Writable()`，业务可主动降级 |
| **Netty** | `ChannelOutboundBuffer` 是内部类 | `SenderI` 降级为内部实现（D4） |
| **Netty** | `@Sharable` 单 handler 实例 | `Handler` 服务所有连接（D1） |
| **Netty** | `inEventLoop()` 快路径 | 内部优化：已在事件循环上时直接入队并在事件末尾 flush，不做跨线程唤醒 |
| **evio** | 回调返回 `action` | `OnMessage() error`（D2），并让 error 顺带携带原因 |
| **evio** | 极小的 API 表面 | 全部公共 API 压到一屏 |
| **Seastar** | shard-per-core、shared-nothing | 连接不跨事件循环迁移；`Stats` 与 zstd 编码器每 loop 独立（D14） |
| **zap** | 结构化字段的日志签名 | `Logger` 接口不发明新形状（D16） |
| **Go stdlib** | `cipher.AEAD.Seal(dst, ...)` 的 dst 容量契约 | `Cipher.Seal`，实现 append 进 gate 给的缓冲即零分配 |
| **Go stdlib** | `net.Listen` 的命名与就绪语义 | `Listen` 返回即在监听 |

**明确不借鉴的：** Netty 的 `ChannelPipeline`（运行时可插拔的 handler 链）。
gate 的传输层只有 TCP 和 WS 两种、编解码只有一种，为这点灵活性付出每层一次虚调用
和每层一次缓冲传递不划算。gate 内部的分层是编译期确定的。

---

## 非目标

- **上层协议编解码。** 业务拿到 `[]byte`。
- **鉴权、路由、心跳、连接注册、房间广播语义。** gate 提供广播所需的**机制**
  （`Frame` 预编码帧），不提供**策略**。
- **TLS / wss 终结。** 前置 nginx / envoy / LB。理由：TLS 在网关进程内终结会把
  证书轮转、会话票据、握手 CPU 尖峰、以及一整套 OpenSSL 状态机塞进事件循环，
  而这些事情前置代理做得更好也更容易运维。
- **per-connection 的 `Outbound` 分档。** 想给内网管理连接和玩家连接不同的压缩策略，
  起两个 `Server`（不同端口或不同 WS path）。这比在 API 上开一个「每连接覆盖」的口子
  简单得多，而后者会让 `Outbound` 从配置变成状态。
- **运行时可插拔 pipeline。** 见上。
- **`OnWritable` 边沿触发回调。** 轮询式 `Writable()` 覆盖了目标场景——游戏网关的推送
  是 tick 驱动的，每个 tick 问一次就够。**如果你的推送是纯事件驱动的**，
  `Writable()` 变 false 之后没有边沿通知你它何时恢复，这时要么改成 tick 轮询，
  要么等 `Options.OnWritable` 这个可选字段（不动 `Handler`）加进来。
- **公开的 `Frame` 引用计数。** 见 D20。
- **等待在途 `AsyncDo` 的 `Shutdown`。** 见 [Server 生命周期](#ctx-约束的是什么不约束什么)。

---

## 从旧 API 迁移

| 旧 | 新 |
| --- | --- |
| `Config` | `Options[S]` |
| `Config.CHB` / `ConnHandlerBuilder` | `Options.Handler` / `Handler[S]`（不再有 builder） |
| `ConnHandler.Handle(raw)` | `Handler.OnMessage(c, msg) error` |
| `ConnHandler.Close()` | `Handler.OnClose(c, reason)` |
| `ReadyHandler.OnReady()` | `Handler.OnOpen(c) (S, error)` |
| `Config.SB` / `SenderBuilder` / `SenderI` | `Options.Outbound`（纯数据，无接口） |
| `SenderConfig` | `Outbound`（`MaxClusterSize`→`MaxCluster`，`MaxBufferSize`→`MaxBuffer`，新增 `HighWater`、`Dict`） |
| `CompressLevel` 零值 = `CompressBetter` | 零值 = `CompressFastest`（**行为变更**，见 D14） |
| `DefaultSenderBuilder` | `DefaultOutbound()` |
| `Conn.SendNoEncrypt`（明文旁路） | `Conn.SendAlone`（**仍然加密**，见 D21） |
| `Conn.SendStatic(data, compressed)` | `srv.NewFrame(data)` + `Conn.SendFrame`（GC 管理，无需释放） |
| `Conn.UpdateCipher`（任意 goroutine） | `Conn.SetCipher`（**只能在事件循环线程上**；出站经队列 barrier 生效，见 D25） |
| `Conn.AsyncDo`（重入、并发语义未定义） | 保留并收紧：**拒绝重入**、回调返回后才启动、`OnClose` 推迟到 `resume` 之后 ⇒ 函数体属于串行域，**可以直接访问 `State`**（D18）。裸暂停另开 `Conn.Pause()`，不带此保证 |
| `Conn.Close()` | `Conn.Close(reason)` |
| `Conn.RemoteIp/RemotePort/Remote` | `Conn.Remote() netip.AddrPort` |
| `Config.Transport` + 4 个 WS 字段 | `Options.WebSocket *WebSocketOptions` |
| `Config.MaxMessageSize` | `Limits.MaxMessage` + `Limits.MaxPending`（拆分） |
| `Config.MaxConnections/IdleTimeout/TCPKeepAlive` | `Limits.MaxConns/Idle/KeepAlive` |
| `Config.Logger`（gnet 类型，printf 风格，进程级全局） | `Options.Log`（zap 形状，`*zap.Logger` 直接可用，每 server 一份） |
| `ws.RejectConnectionError(...)` | `gate.RejectUpgrade(status, reason)` |
| `Cipher.Encrypt/Decrypt` | `Cipher.Seal/Open`（带 `aad` = 帧头）+ `Overhead()`，见 D24 |
| `StartServer` / `StartEventLoop` / `StartWebSocketServer` | `Listen` / `Run` |
| `Server.Stop(ctx)` | `Server.Shutdown(ctx)` |
| —— | 新增：`Conn.Post()`、`Conn.Writable()`、`Conn.ID()`、`Conn.SendFunc()`、`Server.Stats()/LoopStats()`、`Socket`、`Options.Proxy`、`Outbound.Dict/StallTimeout/CloseLinger`、`Limits.MaxOutboundBytes/MaxHandshaking/HandshakeTimeout/MaxPause` |
