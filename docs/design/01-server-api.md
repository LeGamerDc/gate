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
- [背压](#背压)
- [阻塞任务：Pause 与 AsyncDo](#阻塞任务pause-与-asyncdo)
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
	"log/slog"
	"time"

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
		Outbound: gate.DefaultOutbound,
		Limits: gate.Limits{
			MaxMessage: 64 << 10,
			Idle:       5 * time.Minute,
		},
		Log: slog.Default(),
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

// ── 连接句柄 ─────────────────────────────────────────────────────
type Conn[S any] struct {
	State S // 业务状态；只允许在回调与 AsyncDo 内访问
}
func (c *Conn[S]) Send(b []byte) error        // 复制入队，允许压缩 / 合包 / 加密
func (c *Conn[S]) SendPlain(b []byte) error   // 复制入队，明文独立帧
func (c *Conn[S]) SendFrame(f *Frame) error   // 零拷贝，引用计数；广播用
func (c *Conn[S]) Writable() bool             // 出站积压是否低于高水位
func (c *Conn[S]) Pause() (resume func())     // 暂停入站投递
func (c *Conn[S]) AsyncDo(f func())           // Pause 的语法糖
func (c *Conn[S]) Close(reason error)         // reason 会原样传给 OnClose
func (c *Conn[S]) SetCipher(Cipher)           // 可从任意 goroutine 调用
func (c *Conn[S]) Handshake() *Handshake      // 非 WebSocket 返回 nil
func (c *Conn[S]) Remote() netip.AddrPort
func (c *Conn[S]) ID() uint64                 // 进程内唯一且不复用

// ── 预编码帧：一份数据发给 N 条连接 ──────────────────────────────
type Frame struct{ /* opaque */ }
func NewFrame(payload []byte, opts ...FrameOption) (*Frame, error)
func Compressed() FrameOption  // payload 已经是压缩后的字节
func Encrypted() FrameOption   // payload 已经是加密后的字节
func (f *Frame) Retain() *Frame
func (f *Frame) Release()

// ── 配置 ─────────────────────────────────────────────────────────
type Options[S any] struct {
	Addr      string
	Loops     int
	Handler   Handler[S]
	Outbound  Outbound
	Limits    Limits
	Socket    Socket
	WebSocket *WebSocketOptions // nil ⇒ 裸 TCP
	Log       Logger
}

type Outbound struct {
	CompressThreshold int
	CompressLevel     CompressLevel
	MaxCluster        int
	MaxBuffer         int
	HighWater         int
}
var DefaultOutbound = Outbound{
	CompressThreshold: 1 << 10,
	MaxCluster:        32 << 10,
	MaxBuffer:         2 << 20,
	HighWater:         512 << 10,
}

type Limits struct {
	MaxMessage int
	MaxPending int
	MaxConns   int
	Idle       time.Duration
	KeepAlive  time.Duration
}

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
	Seal(dst, plaintext []byte) []byte
	Open(dst, ciphertext []byte) ([]byte, error)
}

type Logger interface { // *slog.Logger 直接满足
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type CompressLevel uint8
const (
	CompressBetter CompressLevel = iota // 零值
	CompressFastest
	CompressBalanced
	CompressBest
)

type Stats struct{ /* 见「日志与统计」 */ }

// 不需要每连接状态时用它：gate.Options[gate.None]
type None = struct{}
```

数一下：**1 个必须实现的接口（3 个方法）**、2 个可选实现的接口（`Cipher`、`Logger`）、
1 个连接句柄、1 个帧对象、5 个配置结构体。没有 builder，没有工厂，没有可选扩展接口。

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
  传给 `c.Send` / `c.SendPlain` 是安全的——它们立即复制。
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

**线程约束**：`State` 只能在 `OnOpen` / `OnMessage` / `OnClose` 和 `AsyncDo` 的函数体内访问。
这四处对同一条连接是严格串行的。从别的 goroutine 碰它需要业务自己同步。

### `ID`

进程内唯一、**永不复用**的 64 位标识。业务的连接注册表应该用它做 key，而不是用
`*Conn` 指针或 `Remote()`：前者会随连接对象池化而复用，后者在 NAT 后面会重复。

---

## 发送

```go
c.Send(b)       // 复制 b；允许 gate 压缩、合包、加密
c.SendPlain(b)  // 复制 b；明文独立帧，不压缩不合包不加密
c.SendFrame(f)  // 不复制；发送一个预编码好的帧
```

三个接口都是**入队**语义，立刻返回，真正的 `writev` 由事件循环合批执行。
返回 `nil` 只表示「已入队」，不表示「已上线路」。

`Send` 和 `SendPlain` 返回之后调用方可以立即复用 `b`。

`SendPlain` 的用途是那些不该被 gate 二次加工的字节：已经在业务层加密过的、
或者对端会用固定 offset 解析的。

三个接口都可以从**任意 goroutine** 调用。

### 错误

| 返回 | 含义 | 业务通常怎么做 |
| --- | --- | --- |
| `nil` | 已入队 | — |
| `ErrConnClosed` | 连接已关闭 | 停止推送，清理会话 |
| `ErrSendQueueFull` | 积压达到 `Outbound.MaxBuffer` | 丢弃非关键推送，或提前用 `Writable()` 判断 |
| `ErrMessageTooLarge` | 超过协议上限 32MB | 业务 bug |
| `ErrCipherConflict` | 对一条配了 `Cipher` 的连接调用 `SendFrame` | 见 [Frame](#frame预编码帧与广播) |

---

## Frame：预编码帧与广播

广播的正确形状是**编码一次、分发 N 次**，而不是让 N 条连接各自压缩加密一遍。

```go
f, err := gate.NewFrame(payload)   // 编码一次：header + 可选压缩，引用计数 = 1
if err != nil {
	return err
}
for _, c := range room.Conns() {
	_ = c.SendFrame(f)             // 内部 Retain，flush 完成后 Release
}
f.Release()                        // 交出调用方这一份引用
```

`SendFrame` 不复制、不改写 `f`。引用计数归零时那块内存才回到池里——因此调用方
**不需要**知道 flush 何时发生。

### 为什么不是「data 必须永久不可变」

上一版的 `SendStatic(data, alreadyCompressed)` 契约是「`data` 必须永久保持不可变」，
理由是 gate 没有 flush 完成回调，调用方无从得知何时可以安全复用这块内存。

这是把一个**生命周期问题**转嫁给了用户的纪律。引用计数把它变回类型的职责：
`Retain` / `Release` 说了算，用完就回收，既没有「永久」这种不可能的约束，
也不会泄漏。这是 Netty `ByteBuf` 的做法。

### 与加密的关系

per-connection 独立密钥和「编码一次分发多次」在根本上不兼容。gate 不假装能同时做到：

- 对一条设置了 `Cipher` 的连接调用 `SendFrame` 返回 `ErrCipherConflict`——**显式失败，
  不静默降级成明文**。
- 房间级共享密钥的场景：业务自己加密 payload，然后
  `gate.NewFrame(sealed, gate.Encrypted())`，gate 只负责打标记位。

---

## 背压

两条水位线，语义完全不同：

```go
Outbound{
	HighWater: 512 << 10, // 软：越过之后 c.Writable() 返回 false
	MaxBuffer: 2 << 20,   // 硬：越过之后关闭连接（ErrBackpressure）
}
```

`Writable()` 让业务可以**主动降级**而不是被动断线：

```go
func (h *handler) pushWorldState(c *gate.Conn[*player], snap []byte) {
	if !c.Writable() {
		return // 这条连接跟不上了，丢掉这一帧状态同步，等它追上来
	}
	_ = c.Send(snap)
}
```

关键位置消息照发不误——它们会一直入队到 `MaxBuffer`，超过就说明这条连接确实已经
没救了，关掉比继续堆积诚实。

**硬上限触发时关闭连接，而不是丢消息。** 从一条有状态的帧流中间抽掉几条，只会让
对端状态机静默错乱，业务层什么都察觉不到；关闭连接则让对端立刻知道会话没了，
重连即可恢复。这条规则在整个 gate 里没有例外。

> 参考 Netty 的 `WriteBufferWaterMark` + `isWritable()`。Netty 还有一个
> `channelWritabilityChanged` 事件回调；gate 这一版只做轮询式的 `Writable()`，
> 因为它覆盖了绝大多数场景（推送前问一句）而不需要往 `Handler` 上加第四个方法。
> 如果将来出现了真的需要边沿触发的场景，再以 `Options.OnWritable` 这种**可选字段**
> 的形式加进来，不动 `Handler`。

---

## 阻塞任务：Pause 与 AsyncDo

`OnMessage` 跑在事件循环上，**不允许阻塞**。要做 RPC / DB 这类事情，
必须先把这条连接的入站投递暂停掉：

```go
func (h *handler) OnMessage(c *gate.Conn[*player], msg []byte) error {
	resume := c.Pause()          // 从这一刻起，这条连接不再投递新消息
	go func() {
		defer resume()           // 恢复投递
		profile := rpc.Load(...)
		_ = c.Send(encode(profile))
	}()
	return nil
}
```

`AsyncDo` 是它的语法糖，等价于上面这段：

```go
c.AsyncDo(func() { ... })
```

两者的区别只在于**谁提供执行体**。业务通常有自己的 worker pool（限流、复用 goroutine），
`Pause` 让它可以接管；`AsyncDo` 则是「随手起一个 goroutine」的便捷写法。

语义：

- 暂停期间数据不会丢，只是留在缓冲里不投递。
- 可重入：多次 `Pause` 需要同样多次 `resume` 才恢复。
- `resume` 幂等，重复调用无害。
- 只应在 `OnMessage` 内 `Pause`。从别的 goroutine 调用时「单连接内串行」的保证
  不成立——事件循环可能已经越过了暂停检查。
- 暂停期间对端仍在发数据。它的上限由 `Limits.MaxPending` 约束，超过就关闭连接
  （`ErrPendingOverflow`）。

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
| `Outbound` | 全关 | 零值不压缩、不合包、不限积压。要默认行为请显式写 `gate.DefaultOutbound` |
| `Limits` | 见下 | |
| `Socket` | 见下 | |
| `WebSocket` | `nil` | nil ⇒ 裸 TCP；非 nil ⇒ WebSocket |
| `Log` | 必填 | `slog.Default()` 即可 |

### `Limits`

| 字段 | 零值 | 说明 |
| --- | --- | --- |
| `MaxMessage` | 32MB（协议上限） | 单条入站消息上限。**值得按业务收紧**，它决定单条连接能让服务端缓冲多少数据 |
| `MaxPending` | `MaxMessage + 64KB` | `Pause` 期间允许积压的入站字节 |
| `MaxConns` | 不限 | 超过后新连接立即关闭（`ErrConnLimit`） |
| `Idle` | 不启用 | 超时未**交付过完整消息**的连接会被主动关闭 |
| `KeepAlive` | 60s | `SO_KEEPALIVE` 空闲探测时长；`gate.DisableKeepAlive` 关闭 |

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
| `RecvBuffer` | 系统默认 | `SO_RCVBUF` |
| `SendBuffer` | 系统默认 | `SO_SNDBUF` |
| `Nagle` | `false`（即 `TCP_NODELAY`） | 置 true 才打开 Nagle |

**10 万连接下内核 socket 缓冲才是内存大头。** Linux 默认 rmem/wmem 各百来 KB，
真吃满就是几十 GB。一个只跑小消息的网关应该显式把它压到几十 KB。上一版完全没有
透出这两个旋钮，却在应用层花大力气把每连接几 KB 的暂存空间池化掉——数量级差了两个。

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

`Run(ctx, opts)` = `Listen` + `Wait`，且 `ctx` 取消时自动 `Shutdown`。给 `main()` 用。

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
| `ErrBackpressure` | 出站积压越过 `Outbound.MaxBuffer` |
| `ErrPendingOverflow` | `Pause` 期间入站积压越过 `Limits.MaxPending` |
| `ErrProtocol` | 线路协议违规（包装了 codec / WebSocket 的具体错误） |
| `ErrHandlerPanic` | 业务回调 panic，已被恢复屏障接住 |
| `ErrConnLimit` | 越过 `Limits.MaxConns`（此时 `OnOpen` 都不会被调用） |
| `ErrServerClosed` | `Shutdown` |
| 业务自己的 error | `OnMessage` 返回的、或 `Close(reason)` 传入的 |

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
		log.Warn("protocol violation", "remote", c.Remote(), "err", reason)
	}
	unregister(c.State.id)
}
```

`Close(reason)` 允许业务自定义原因，`nil` 表示正常关闭：

```go
c.Close(errKickedByAdmin)
```

### panic 隔离

`OnOpen` / `OnMessage` / `OnClose` / `OnUpgrade` / `AsyncDo` 的函数体，每一处都有恢复屏障。
一条能让业务解码 panic 的畸形消息只会关闭它自己那条连接，`OnClose` 收到
`ErrHandlerPanic`，其余 99999 条连接不受影响。

`Cipher` 是唯一的例外，见下。

---

## Cipher

```go
type Cipher interface {
	Overhead() int
	Seal(dst, plaintext []byte) []byte
	Open(dst, ciphertext []byte) ([]byte, error)
}

c.SetCipher(myCipher)  // 可从任意 goroutine 调用；nil 表示关闭加密
```

`Overhead() == 0` 时（流式 / CTR 类密码），gate 会传 `dst = src[:0]` 走**原地**路径，
不产生任何拷贝——和上一版的 `Encrypt([]byte)` 成本相同。
`Overhead() > 0` 时（AEAD，如 AES-GCM / ChaCha20-Poly1305）gate 按需预留空间。

> **为什么要改掉 `Encrypt([]byte)` / `Decrypt([]byte)`。** 原地、不改长度、无返回值的
> 形状排除了所有 AEAD——而游戏网关要做防篡改迟早会撞上。更重要的是 `Decrypt`
> 没有错误通道：认证失败是**正常的协议错误**，不该只能靠 panic 表达。
> 现在 `Open` 返回 error，连接以 `ErrProtocol` 关闭，业务在 `OnClose` 里就能看到。

`Seal` / `Open` 在入站解密和出站加密的热路径上被逐帧调用，**不加恢复屏障**——
每帧一次 `defer recover()` 的代价不值得。因此 `Cipher` 的实现必须保证不 panic。
这是整个 API 里唯一一条「你必须遵守否则进程会死」的约定，写在这里而不是藏在注释里。

`SetCipher` 最典型的用法是异步握手完成后换密钥（也就是从 `AsyncDo` 的 goroutine 里调），
所以它必须是并发安全的。gate 保证：切换在**帧边界**生效，同一批消息里换了密钥，
后续帧立刻用新密钥解，不会出现「用旧密钥解新帧」。

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

## 日志与统计

### `Logger`

```go
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}
```

签名与 `*slog.Logger` 完全一致，所以 `Log: slog.Default()` 直接可用，
也可以传任何自己包装的实现。

只有三级、没有 `Debugf` / `Fatalf`、也没有 `printf` 风格：gate 自己的日志全部在
错误路径上，结构化参数（`"remote", c.Remote()`）比格式化字符串更适合被采集。

### `Stats`

```go
type Stats struct {
	ConnsOpen     int64  // 当前
	ConnsAccepted uint64 // 累计
	ConnsRejected uint64

	MessagesIn  uint64
	MessagesOut uint64
	BytesIn     uint64
	BytesOut    uint64 // 线路字节（压缩后）
	BytesOutRaw uint64 // 压缩前
	FramesOut   uint64 // 线路帧数

	ClosedIdle         uint64
	ClosedBackpressure uint64
	ClosedProtocol     uint64
	SendQueueFull      uint64
}
```

两个比值是这个库最该被观察的东西：

- `BytesOut / BytesOutRaw` —— 压缩率
- `MessagesOut / FramesOut` —— 合包聚合度（每个线路帧平均携带多少条业务消息）

计数器是每事件循环独立的，`Stats()` 读的时候求和。热路径上没有任何接口调用。

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
| 跨 goroutine `Send` 唤醒 | `eventfd` + MPSC 队列 | `EVFILT_USER` + MPSC 队列 |
| `Limits.Idle` 扫描 | `timerfd` 驱动，per-loop LRU 链表 | `EVFILT_TIMER` 驱动，同一套 LRU |
| 出站合批 | `writev` | `writev` |
| `Writable()` | 用户态积压记账 | 同左（与 poller 无关） |
| accept 分发 | `SO_REUSEPORT`，内核哈希到各 loop | Darwin 没有对应的内核级 accept 负载均衡（FreeBSD 是 `SO_REUSEPORT_LB`），退化为单 acceptor + 轮转投递 |

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

### D5. `SendStatic` 的永久不可变契约 → `Frame` 引用计数

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

**选择。** gate 自己定义 `Logger`（签名对齐 `*slog.Logger`，不发明新东西）和
`RejectUpgrade`。

**代价。** 多两个类型声明和一层内部适配。

### D8. `AsyncDo` → `Pause` 为原语

见 [阻塞任务](#阻塞任务pause-与-asyncdo)。名字讲清楚它真正做的事，同时让业务能接管执行体。

### D9. 拆开 `MaxMessageSize` 的两个职责

上一版这一个字段既是「单条消息上限」又是「阻塞期间入站积压上限」，文档要为此解释
两大段。两个职责就是两个字段：`Limits.MaxMessage` 和 `Limits.MaxPending`。

### D10. 透出 socket 缓冲配置

10 万连接下内核 socket 缓冲是内存大头，上一版完全不可配。见 [`Socket`](#socket)。

### D11. `Cipher` 改成 `Seal` / `Open`

见 [Cipher](#cipher)。为 AEAD 留出空间，同时给解密失败一个合法的错误通道。

### D12. `Transport` 枚举 + 并列 WS 字段 → `WebSocket *WebSocketOptions`

一个决策而不是两个，不存在互相矛盾的配置状态。零值（nil）落在更常见的裸 TCP 上。

---

## 参考与借鉴对照

| 来源 | 借鉴 | gate 里的落点 |
| --- | --- | --- |
| **Netty** | `ByteBuf` 引用计数 | `Frame.Retain` / `Release`，广播编码一次 |
| **Netty** | `WriteBufferWaterMark` + `isWritable()` | `Outbound.HighWater` + `Conn.Writable()`，业务可主动降级 |
| **Netty** | `ChannelOutboundBuffer` 是内部类 | `SenderI` 降级为内部实现（D4） |
| **Netty** | `@Sharable` 单 handler 实例 | `Handler` 服务所有连接（D1） |
| **Netty** | `inEventLoop()` 快路径 | 内部优化：已在事件循环上时直接入队并在事件末尾 flush，不做跨线程唤醒 |
| **evio** | 回调返回 `action` | `OnMessage() error`（D2），并让 error 顺带携带原因 |
| **evio** | 极小的 API 表面 | 全部公共 API 压到一屏 |
| **Seastar** | shard-per-core、shared-nothing | 连接不跨事件循环迁移；`Stats` 每 loop 独立计数 |
| **Go stdlib** | `*slog.Logger` 签名 | `Logger` 接口不发明新形状（D7） |
| **Go stdlib** | `net.Listen` 的命名与就绪语义 | `Listen` 返回即在监听 |

**明确不借鉴的：** Netty 的 `ChannelPipeline`（运行时可插拔的 handler 链）。
gate 的传输层只有 TCP 和 WS 两种、编解码只有一种，为这点灵活性付出每层一次虚调用
和每层一次缓冲传递不划算。gate 内部的分层是编译期确定的。

---

## 非目标

- **上层协议编解码。** 业务拿到 `[]byte`。
- **鉴权、路由、心跳、连接注册、房间广播语义。** gate 提供广播所需的**机制**
  （`Frame` + 引用计数），不提供**策略**。
- **TLS / wss 终结。** 前置 nginx / envoy / LB。理由：TLS 在网关进程内终结会把
  证书轮转、会话票据、握手 CPU 尖峰、以及一整套 OpenSSL 状态机塞进事件循环，
  而这些事情前置代理做得更好也更容易运维。
- **per-connection 的 `Outbound` 分档。** 想给内网管理连接和玩家连接不同的压缩策略，
  起两个 `Server`（不同端口或不同 WS path）。这比在 API 上开一个「每连接覆盖」的口子
  简单得多，而后者会让 `Outbound` 从配置变成状态。
- **运行时可插拔 pipeline。** 见上。
- **`OnWritable` 边沿触发回调。** 轮询式 `Writable()` 覆盖绝大多数场景；
  真需要时以 `Options` 可选字段加入，不动 `Handler`。

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
| `SenderConfig` | `Outbound`（`MaxClusterSize`→`MaxCluster`，`MaxBufferSize`→`MaxBuffer`，新增 `HighWater`） |
| `DefaultSenderBuilder` | `DefaultOutbound` |
| `Conn.SendNoEncrypt` | `Conn.SendPlain` |
| `Conn.SendStatic(data, compressed)` | `NewFrame(data, Compressed())` + `Conn.SendFrame` |
| `Conn.UpdateCipher` | `Conn.SetCipher` |
| `Conn.AsyncDo` | 保留；原语改为 `Conn.Pause()` |
| `Conn.Close()` | `Conn.Close(reason)` |
| `Conn.RemoteIp/RemotePort/Remote` | `Conn.Remote() netip.AddrPort` |
| `Config.Transport` + 4 个 WS 字段 | `Options.WebSocket *WebSocketOptions` |
| `Config.MaxMessageSize` | `Limits.MaxMessage` + `Limits.MaxPending`（拆分） |
| `Config.MaxConnections/IdleTimeout/TCPKeepAlive` | `Limits.MaxConns/Idle/KeepAlive` |
| `Config.Logger`（gnet 类型） | `Options.Log`（gate 自己的 `Logger`，`*slog.Logger` 直接可用） |
| `ws.RejectConnectionError(...)` | `gate.RejectUpgrade(status, reason)` |
| `Cipher.Encrypt/Decrypt` | `Cipher.Seal/Open` + `Overhead()` |
| `StartServer` / `StartEventLoop` / `StartWebSocketServer` | `Listen` / `Run` |
| `Server.Stop(ctx)` | `Server.Shutdown(ctx)` |
| —— | `Conn.Writable()`、`Conn.ID()`、`Server.Stats()`、`Socket`（新增） |
