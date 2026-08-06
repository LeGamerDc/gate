# 事件循环

本文定义 gate 自建的 reactor：poller 抽象、一轮迭代的结构、accept、读写路径、
跨 goroutine 投递、连接生命周期、超时扫描与内存布局。

它要同时跑在 **Linux（epoll）** 和 **macOS（kqueue）** 上，而两者的能力并不等价。
本文的组织原则是：**先定义与平台无关的模型，再列出两个平台各自怎么实现它**，
公共 API 里不允许出现任何只有一边成立的概念（见
[01 的平台无关性](01-server-api.md#平台无关性为什么-api-里没有-epoll-的影子)）。

---

## 目录

- [模型](#模型)
- [Poller 抽象](#poller-抽象)
- [一轮迭代](#一轮迭代)
- [accept](#accept)
- [读路径](#读路径)
- [写路径](#写路径)
- [跨 goroutine 投递](#跨-goroutine-投递)
- [连接生命周期](#连接生命周期)
- [超时与空闲回收](#超时与空闲回收)
- [Pause 的实现](#pause-的实现)
- [内存布局](#内存布局)
- [平台差异总表](#平台差异总表)
- [不变式清单](#不变式清单)
- [设计决策记录](#设计决策记录)

---

## 模型

**thread-per-core，shared-nothing，连接永不迁移。**

```
       ┌── Loop 0 ──┐  ┌── Loop 1 ──┐        ┌── Loop N-1 ─┐
       │ poller     │  │ poller     │        │ poller      │
       │ 连接集合    │  │ 连接集合    │  ...   │ 连接集合     │
       │ 读缓冲      │  │ 读缓冲      │        │ 读缓冲       │
       │ slab 池     │  │ slab 池     │        │ slab 池      │
       │ zstd 编码器 │  │ zstd 编码器 │        │ zstd 编码器  │
       │ LRU / 统计  │  │ LRU / 统计  │        │ LRU / 统计   │
       └────────────┘  └────────────┘        └─────────────┘
```

一个 loop = 一个 goroutine = 一个 poller = 一份内存池 = 一个 zstd 编码器。
连接在 accept 时归属某个 loop，直到关闭都不换。

**这个模型是其余一切的前提。** 它直接决定了：

- 所有连接状态（读残片、出站队列、cipher、LRU 位置）都是**单线程访问**的，
  不需要同步。唯一的例外是出站队列的入队口——那是给跨 goroutine `Send` 用的，
  用一把无争用的锁（见 [03](03-outbound.md#in-loop-快路径)）。
- 统计计数器每 loop 一份，不存在跨核往返。
- 内存池**几乎**每 loop 一份，分配与释放都在同一个线程上，不需要原子操作。
  唯一的例外是 `Send` / `SendFunc` 的复制目标——它们可从任意 goroutine 入队，
  必须走并发安全的全局分级池（见 [06 的所有权表](06-connection-state-machine.md#字节所有权表)）。
- **所有系统调用都发生在 loop 线程上。** 这一条把上一版为「陈旧连接对象碰到已被
  回收甚至已被新连接复用的 fd」写的那一整类防御，变成了结构性事实。

---

## Poller 抽象

窄到只有六个操作。窄是有意的——它必须窄到 epoll、kqueue 和（将来的）io_uring
都能实现，否则换底座又变成一次破坏性改动。

```go
type poller interface {
	// 注册一个 fd，tok 会随事件原样返回。
	add(fd int, tok token, want interest) error
	// 修改兴趣集合（Pause / 出站积压变化时用）。
	mod(fd int, tok token, want interest) error
	del(fd int) error

	// wait 阻塞到有事件或超时。timeout < 0 表示无限等待。
	wait(out []event, timeout time.Duration) (int, error)

	// notify 让阻塞中的 wait 立刻返回。可从任意 goroutine 调用，且是幂等的
	// ——连续多次 notify 只保证 wait 至少醒一次。
	notify() error

	close() error
}

type interest uint8
const (
	interestRead  interest = 1 << 0
	interestWrite interest = 1 << 1
)

type event struct {
	tok   token
	read  bool
	write bool
	// hup 表示对端关闭或出错。仍然要走一次 read 拿到确切原因
	// （EOF 还是具体的 errno），所以它只是一个提示。
	hup bool
}
```

`token` 是一个 `uint64`，塞进 epoll 的 `epoll_data.u64` / kqueue 的 `udata`，
所以事件回来时**不需要任何查表或哈希**，见[连接生命周期](#连接生命周期)。

### 两个平台的落点

| 操作 | epoll | kqueue |
| --- | --- | --- |
| `add` / `mod` / `del` | `epoll_ctl(ADD/MOD/DEL)` | `kevent` 提交 `EV_ADD` / `EV_ENABLE` / `EV_DISABLE` / `EV_DELETE` |
| 读写兴趣 | 一个 fd 一个 `epoll_event`，`EPOLLIN`/`EPOLLOUT` 是位 | **两个独立的 filter**（`EVFILT_READ` / `EVFILT_WRITE`），要分别增删 |
| `wait` | `epoll_wait(..., ms)` | `kevent(..., *timespec)` |
| `notify` | 写一个常驻的 `eventfd` | `EVFILT_USER` + `NOTE_TRIGGER` |
| 超时精度 | 毫秒 | 纳秒 |

kqueue 那一行是实现上最容易出错的地方：epoll 的 `mod` 是「设置成这个集合」，
kqueue 是「对每个 filter 分别开关」。抽象层必须记住当前的 `interest`，
把 `mod` 翻译成一组增量的 `EV_ENABLE` / `EV_DISABLE`，而不是每次都全量重设。

还有一条 kqueue 特有的坑：**同一个 fd 的读、写就绪是两个独立事件**（两个 filter
各报各的），`wait` 的实现必须按 `token` 把它们**聚合成一个 `event`**——上层承诺
「先写后读」的处理顺序（见[写路径](#写路径)），聚合是这个承诺的前提。
epoll 天然是合并的，一个 fd 一个 event。

---

## 一轮迭代

```
loop.run():
  for {
      timeout := 下一个截止时刻 - now        ← 五条 LRU 头 + EMFILE backoff 的最近者
      n := poller.wait(events, timeout)

      ── 阶段 1：分发事件 ────────────────────────────────
      for e in events[:n]:
          switch e.tok.kind:
          case listener:  accept 循环（见 accept）
          case notify:    排空 MPSC 收件箱 → 标脏 / 执行 Post
          case conn:
              c := 解析 tok 并校验 generation；失效则跳过
              if e.write: 续写出站链；写空则摘掉写兴趣
              if e.read:  读 → 解析 → 投递（handler 可能 Send）
                          → **立即 flush 这条连接**      ← cache 还热
      ── 阶段 2：flush 脏连接 ────────────────────────────
      for c in dirty 链: c.flush()               ← 幂等，阶段 1 flush 过的直接返回

      ── 阶段 3：超时扫描 ────────────────────────────────
      到点才做：idle / handshake / pause / stall / linger 五条 LRU 的头部

      ── 阶段 4：回收 ───────────────────────────────────
      for c in reap 链: 执行 Draining → Detached 的拆除序列（见关闭流程）
                        fd、缓冲、槽位立即回收；OnClose 视串行域是否空闲
  }
```

几个刻意的选择：

**读完立刻 flush 那条连接**，而不是攒到阶段 2 统一做。请求-响应是主流量，
此时连接的 `Conn`、出站队列、cipher 都还在 cache 里；推迟到阶段 2 要把它们再拉回来一次。
阶段 2 只负责跨 goroutine `Send` 标脏的那些连接——flush 幂等，重复调用直接返回。

**超时只有一个时间源**：`wait` 的 timeout 参数。不用 `timerfd` / `EVFILT_TIMER`，
少一个 fd、少一处平台差异、少一类「定时器 fd 泄漏」的故障模式。毫秒精度对秒级的
超时判定绰绰有余。

**关闭统一在阶段 4**。阶段 1~3 里任何地方决定关闭一条连接，都只是打标记 + 挂进
reap 链。这样本轮迭代中不会有任何代码碰到一个已经 `close()` 的 fd——上一版
`closing` / `stopped()` / `writable()` 那一整套跨线程可见性推理，在这里退化成
「本轮迭代内的一个布尔判断」。

---

## accept

两个平台的能力差异最大，也是唯一一处实现路径完全不同的地方。

### Linux：`SO_REUSEPORT`，每个 loop 自己 listen

```
loop_i: socket() → SO_REUSEPORT → bind(addr) → listen() → 注册到自己的 poller
```

内核按四元组哈希把新连接分到各个 listener 队列。收益：

- 没有单点 acceptor，没有跨 loop 投递；
- 新连接的数据结构从一开始就分配在**将来要处理它的那个核**上；
- 没有 accept 惊群。

两个必须知道的陷阱：

1. **listener 集合变化会重新哈希。** 运行期关掉一个 listener，内核会把已经排进它
   队列、但还没被 accept 的连接直接丢弃。所以 listener 只在 `Shutdown` 时统一关闭，
   运行期绝不动。
2. **哈希是按连接算的，不是按负载算的。** 长连接场景下，如果客户端来源分布不均，
   loop 之间的连接数会长期倾斜。`LoopStats()` 暴露每 loop 的 `ConnsOpen` 就是为了
   让这件事可观测。真的倾斜到需要处理时，可以上 `SO_ATTACH_REUSEPORT_CBPF`
   自定义分发——那是后续优化，不在 v1。

3. **`Addr = ":0"`（随机端口）不能让每个 listener 各自 bind 0**——那样每个会拿到
   **不同的**端口。第一个 listener bind 0 取回真实端口，其余 listener 绑同一个端口。

### macOS：单 listener + 轮转投递

Darwin **没有** Linux 那种内核级 accept 负载均衡（FreeBSD 的对应物是
`SO_REUSEPORT_LB`，Darwin 没有）。所以：

```
loop_0 持有唯一的 listener
  accept() → 选一个目标 loop（最少连接数）→ MPSC 投递 fd → 目标 loop 注册它
```

代价是每条新连接一次跨 loop 投递（一次 MPSC push + 也许一次 `notify`）。
连接建立本来就要走若干次系统调用，这一次投递无关紧要；而且 macOS 是开发与测试平台，
不是 10 万连接的生产目标。

**这个差异对业务完全不可见**，`Options` 里没有 `ReusePort` 字段——它在 macOS 上
没有 Linux 的语义，暴露出去只会让配置在一个平台上是谎言。

### 两个平台共有的部分

- **每轮迭代的 accept 有上限**（例如 64 个），避免连接风暴饿死已有连接的读写。
  没 accept 完不要紧，水平触发下一轮还会再报。
- **`MaxConns` 先原子占位再 accept。** check-then-add 在多 loop 下不是原子的，
  并发的 accept 会全部读到低于上限的计数、全部放行，一次连接风暴就能冲破上限。
  满额时也**必须** `accept` 之后立刻 `close`（明确拒绝），不能只是跳过——
  连接留在内核 backlog 里，水平触发会每轮都报 listener 可读，空转。
- **`EMFILE` 必须特殊处理。** fd 耗尽时 `accept` 返回 `EMFILE`，而连接仍然挂在
  内核的 accept 队列里，水平触发会**立刻再次报告可读**——一个满载 CPU 的死循环，
  而且日志会瞬间刷爆。处理方式：

  ```
  启动时预留一个 fd（打开 /dev/null）
  遇到 EMFILE:
      close(预留 fd) → accept() 拿到那条连接 → 立刻 close 它（明确拒绝）
      → 重新打开预留 fd
      → 摘掉 listener 的读兴趣，进入指数退避      ← loop 级 backoff 时刻，到点恢复
  ```

  `ENFILE` / `ENOBUFS` / `ENOMEM` 走同一条路径——它们都是「资源暂时耗尽、
  立刻重试必然再失败」的形态，只是耗尽的资源不同。

  这是 10 万连接下的真实故障模式，不是理论问题：`ulimit -n` 配小了、或者 fd 泄漏，
  都会走到这里。

---

## 读路径

入站解析是**增量状态机**，不是「攒够一整帧再解析」——固定 64KB 的 rbuf 装不下
一条 1MB 的帧，而反复把增长中的残片拷来拷去是 O(n²)。完整规格见
[06 的入站模型](06-connection-state-machine.md#入站的精确模型)：

```
每 loop 一块 64KB 读缓冲（rbuf）

小帧快路径（帧长 <= rbuf 容量）：
  ├─ 若该连接有残片（carry）：先拷进 rbuf 头部
  ├─ read(fd, rbuf[残片长度:])
  ├─ 在 rbuf 上**原地**解析出一条条完整帧 → 交给业务
  └─ 剩下不足一帧的尾巴 → 存回 carry（从分级池借）

大帧路径（帧长 > rbuf 容量）：
  ├─ 帧头校验通过后，一次性从分级池申请整帧目标缓冲（frameBuf）
  ├─ 后续 read 直接填 frameBuf 的剩余区——没有重复拷贝
  └─ 收齐 → 投递 → 归还
```

**稳态下 carry 是空的**，因此 10 万条连接的入站内存占用是 **0**——读缓冲是每 loop 一块，
不是每连接一块。这是这个量级下最重要的一条内存性质。

### 水平触发 + 读取预算

每次读事件最多读 `readBudget` 次（或直到 `EAGAIN`），然后**主动让出**，
剩下的留给下一轮迭代。

选水平触发而不是边缘触发，理由有两条，且都很硬：

1. **边缘触发要求读到 `EAGAIN` 为止**，否则事件就丢了。这意味着一条持续高速灌数据的
   连接可以在一轮迭代里独占 loop，把同一个 loop 上其余几万条连接全部饿死。
   水平触发下「这轮先读这么多，剩下的下轮再说」是安全的。
2. **`Pause()` 的语义需要它。** 暂停一条连接 = 摘掉读兴趣 = 一个字节都不再从内核读，
   数据留在内核缓冲里由 TCP 流控替我们做背压（见
   [01](01-server-api.md#内核缓冲占满时会发生什么)）。边缘触发下「暂时不读」会直接
   丢掉事件。

代价是每轮事件多一次 `read` 系统调用（那次返回 `EAGAIN` 的）——除非用读取预算提前
让出，那连这一次都省了。

### 残片与大帧缓冲的上界

- **carry 的上界是 rbuf 容量（64KB），不是 `MaxMessage`**——超过 rbuf 的帧走大帧
  路径直读 frameBuf，carry 里永远只有「不足一帧且装得进 rbuf」的尾巴。
- **frameBuf 的上界是 `Limits.MaxMessage`**：帧头校验（含长度）在申请之前完成，
  谎报长度换不来分配。
- `Pause` 时：本轮已经读进 `rbuf`、但来不及投递的字节要全部转进残片缓冲，
  由 `Limits.MaxPending` 封顶，超过即关闭（`ErrPendingOverflow`）。

  这解释了 `MaxPending` 到底是什么：**不是**「暂停期间对端能发多少」（那个由内核缓冲
  和 TCP 流控管），而是「暂停发生在一批数据中间时，已经从内核读出来的那一小块」。
  上界是「一次读批次 + 一条最大消息」，所以默认值 `MaxMessage + 64KB` 正好。

---

## 写路径

出站队列的结构、编码与背压在 [03](03-outbound.md) 定义，这里只讲与 poller 的接口。

```
flush 之后仍有未写完的字节:
    若当前没有写兴趣 → mod(fd, interest|Write)
写空:
    若当前有写兴趣   → mod(fd, interest&^Write)
```

要点：

- **只有出站积压非空的连接才在写兴趣集合里。** 这本身就是「哪些连接背压了」的现成
  索引，不需要额外数据结构，也不需要每次去问内核。
- 写兴趣的增删是**边沿动作**，不是每次 flush 都调一次 `mod`——积压是异常态，
  稳态下这两次系统调用一次都不会发生。
- 收到写事件后**先续写、再处理读**（阶段 1 里 write 在 read 之前）：优先把已经欠下的
  字节还掉，避免读进来的新数据又往一个已经满的队列上堆。

---

## 跨 goroutine 投递

一个 loop 一个 MPSC 收件箱，承载三类事情：

| 来源 | 投递什么 |
| --- | --- |
| 跨 goroutine 的 `Send` / `SendFrame` / `SendFunc` | 「这条连接脏了」 |
| `Conn.Post(f)` | 一个待执行的闭包 |
| macOS 的 accept | 一个待注册的新 fd |

```go
// 生产者（任意 goroutine）
inbox.push(item)                     // 无锁 MPSC，一次 CAS
if armed.CompareAndSwap(false, true) {
	poller.notify()                  // 只有 空→非空 这一次转变才做系统调用
}

// 消费者（loop 线程，收到 notify 事件时）
armed.Store(false)
for item := inbox.drain() { ... }    // 一次性摘走整条链
```

`armed` 这个标志是关键：**N 条消息发往同一个 loop 上的 M 条连接，总共只产生
一次 `notify` 系统调用。** 上一版是每条连接一次 `Wake`，每次都构造一个任务对象、
并且底座会先完整重跑一遍入站处理再执行回调。

排空之后必须**先清 `armed` 再 drain**——反过来会丢事件：drain 完到清 `armed` 之间
push 进来的项目，其生产者会看到 `armed == true` 而跳过 `notify`，于是这一项要等到
下一次有人 push 才被发现。

### `Post` 的两条排队规则

1. **连接已关闭 ⇒ 闭包不执行、不报错。** 执行前**同时**校验 generation 与主状态——
   gen 到 `Detached` 才递增，而 `Draining` 从 `Close` 线性化那一刻就可能开始，
   只查 gen 会留下一个「已承诺关闭但闭包仍执行」的窗口。异步回来发现
   会话没了是常态，不该让每个调用点都写一遍判断。
2. **连接处于暂停中 ⇒ 闭包排进该连接的待执行队列**，`resume` 之后按投递顺序执行。

第 2 条是「暂停 = 整个串行域暂停」的一部分：不这样做的话，`Post` 的闭包会和
`AsyncDo` 的函数体在业务状态上撞车——而后者已经被承诺是串行域的一员
（见 [01](01-server-api.md#这个保证是怎么来的)）。排队的闭包有数量上限，
超过即关闭连接（`ErrPendingOverflow`）。

---

## 连接生命周期

### 句柄：槽位 + generation

```go
type token uint64   // 位段划分：kind(2) | generation(30) | slot(32)
                    // kind ∈ { listener, notify, conn }——事件分发第一步就靠它

type slot struct {   // 64 位上 16 字节（指针 + uint32 + padding）
	conn *Conn
	gen  uint32
}
// loop.slots []slot ——按 loop 内自管的稠密下标索引（freelist 分配）
```

**槽位下标不是 fd。** fd 只在**进程**内稠密（内核总是取最小空闲值），不在
**每个 loop** 内稠密——连接被摊到 N 个 loop 之后，单个 loop 看到的 fd 序列满是洞，
按 fd 索引就要每个 loop 都开一个「全进程最大 fd 号」大小的数组，×N 份。
每个 loop 用自己的 freelist 分配稠密下标，事件回来仍是 O(1) 查表、零哈希、零 map。

每次槽位被复用就 `gen++`，事件里带回来的 `token` 和槽位当前的 `gen` 对不上就说明
这是一个陈旧事件，直接丢弃。

### 三个「唯一」不要混淆

| | 是什么 | 会不会复用 |
| --- | --- | --- |
| `fd` | 内核句柄 | **会**，而且很快 |
| `token`（槽位 + gen） | 内部事件标识 | 槽位会复用，`token` 因 gen 而不会 |
| **`Conn.ID()`** | 公开的连接标识 | **永不复用**：`loopID << 48 \| 每 loop 自增计数器` |

`ID()` 用每 loop 的计数器而不是全局原子量，避免一条被所有 loop 争抢的 cache line。

### `*Conn` 本身永不复用

这是 [01 的 D17](01-server-api.md#d17-conns-永不复用)，在这里再说一次实现侧的含义：
**槽位、读残片缓冲、出站 chunk 节点都池化，`Conn` 对象不池化。**

`Conn` 会逃逸到业务的注册表、定时任务、`resume` 闭包里，而 reactor 无法知道业务什么
时候放手。复用它意味着一个持有旧指针的 goroutine 调用 `Send` 会把数据发给另一个玩家。
每条连接一次堆分配，换掉一整类跨会话数据泄漏，非常划算。

### 为什么跨线程路径永远碰不到 fd

这是整个设计里最重要的一条安全性质：

- 跨 goroutine 的 `Send` 只做两件事：**入队**、**标脏**。两者都不碰 fd。
- `Post` 只往 MPSC 里塞闭包。不碰 fd。
- 所有 `read` / `writev` / `epoll_ctl` / `close` 都发生在 loop 线程上。
- `close(fd)` 只发生在阶段 4，此时本轮迭代的所有其它阶段都已经跑完。

因此「往一个已经被回收、并且可能已被新连接复用的 fd 上写数据」**在结构上不可能发生**，
不需要任何状态位去防守。上一版为此写的 `stateClosed` / `writable()` 判断仍然保留，
但职责降级成「连接不可写时不要白白编码一遍」。

### 关闭流程

与 [06 的主状态机](06-connection-state-machine.md#主状态)逐字对齐：
`Open → Draining →（排空 / linger 到期 / 写失败）→ Detached → Closed`。

```
任何阶段决定关闭:
    在出站锁的**同一个临界区**内: 若 closing 已置位 → 返回（已有关闭者）
                                  否则置 closing + 记录 reason
        ↑ 这两步不能拆开。拆成「CAS 一个原子量」+「稍后记 reason」时，抢到
          CAS 的那个可能被抢占，让 CAS 失败的那个先写进 reason，胜负颠倒；
          而 Send 检查的就是这把锁下的 closing，D15「Close 返回之后 Send 一定
          报错」也随之失守。见 06 的线性化点总表。
        停止入站投递；停止接受新消息入队
        若 WS 且 reason 可发 close 帧 → close 帧排进出站链尾（见 05）
        进入 Draining：尝试写一次，挂进 linger 链
        ← 之后只在「可写事件」或「CloseLinger 到期」时再动，不空转

Draining → Detached（排空 / 超时 / 写失败触发；一定发生，不等任何业务代码）:
    1. conn.core 置 nil           ← 公共方法从此返回 ErrConnClosed / no-op
    2. poller.del(fd) → close(fd)
    3. 丢弃 stage1 / stage2，归还池内存，退还预算
    4. 从五条 LRU 摘除；槽位 conn 置 nil；槽位 gen++

Detached → Closed（可能被推迟）:
    5. 若仍有在途 AsyncDo → 挂进 loop 的 pendingClose 链，等 resume
       （resume 的控制项直接携带 core 引用，不走 token——gen 已在第 4 步递增）
    6. OnClose(c, reason)          ← 恢复屏障包住；壳上的 ID/Remote/Handshake 仍可读
```

`Draining` 是 `Outbound.CloseLinger` 的落点：连接在 linger 链上停留，直到排空或
超时。它保证了「回一条拒绝消息再关闭」这个模式**至少被尝试过**——
但不保证送达，见 [01](01-server-api.md#尽力-flush到底承诺了什么)。

**`Detached` 与 `Closed` 分开是 `AsyncDo` 串行域保证的实现基础**（[01 的 D18](01-server-api.md#d18-把-asyncdo-的函数体拉进串行域)）：
`f` 正在另一个 goroutine 上跑时不能调 `OnClose`，否则两者会在业务状态上撞车。
但也**不能因此推迟拆 socket**——fd 和缓冲必须立刻回收，10 万连接的规模下这些资源
一刻都等不起。所以「拆资源」立刻做，「通知业务」排队等。

代价是 `Conn` 对象（含 `State`）活得比 socket 长。fd 不泄漏；业务泄漏一个 goroutine
才会跟着泄漏一个 `Conn`。`Shutdown` 不等待这些连接，见 [01](01-server-api.md#ctx-约束的是什么不约束什么)。

---

## 超时与空闲回收

**五条每 loop 的侵入式 LRU + 一个 loop 级 backoff 时刻**，全部 O(1) 更新、
O(k) 过期，不引入任何时间堆或时间轮（完整表见
[06 的截止时刻](06-connection-state-machine.md#截止时刻)）。

### 空闲回收用 LRU 链表

```
活跃（交付了一条完整消息）→ 把连接移到链表尾部    O(1)
扫描 → 从链表头开始，遇到第一个未超时的就停       O(k)，k = 真正超时的条数
```

这比「遍历全部连接」好一个数量级：10 万连接、1 秒一次扫描，前者是每秒 10 万次原子读，
后者通常是 0 次比较。

之所以能用这么简单的结构，是因为 **shared-nothing 模型让「活跃时间」在单个 loop 内
天然单调递增**——同一个 loop 上后处理的连接活跃时间一定不早于先处理的。
跨 loop 就不成立了，但我们从不跨 loop。

活跃的判据是**「交付了一条完整消息」**，不是「收到了字节」。两个后果：
只发半个帧吊着连接的 slowloris 会被回收；反过来，服务端单向下推不会让连接显得活跃。

### 另外四条

| 超时 | 结构 | 说明 |
| --- | --- | --- |
| `HandshakeTimeout` | 单独一条 LRU（`Proxy` / `Handshaking` 的连接） | 握手期的连接一条完整消息都没交付过，**空闲 LRU 管不到它**，必须单列 |
| `MaxPause` | 单独一条 LRU（被暂停的连接） | 暂停期间刻意不计入空闲时长，同样脱离了空闲 LRU 的管辖 |
| `StallTimeout` | 单独一条 LRU，按 `lastProgress` 排序 | 初稿写「复用写兴趣集合」——**错了**：那个集合没有时间序，扫描无法在第一个未超时处停下。`lastProgress`（最后写出 ≥1 字节的时刻）满足单调性，可以成链 |
| `CloseLinger` | 单独一条 LRU（`Draining` 的连接） | 初稿把它挂在 reap 链上每轮重试写——**错了**：别的连接持续活跃时，每轮都对一个满缓冲的 fd 撞一次 `EAGAIN`，是 syscall 风暴。linger 链上的连接**只在两种时机动**：收到可写事件，或 `CloseLinger` 到期 |

前三者都是 [01 的 D23](01-server-api.md#d23-补齐三个无人管辖的时间窗口) 说的
「无人管辖的时间窗口」。共同点是：**连接存在，但它不满足任何现有回收机制的判据。**

另有一个 loop 级的 **EMFILE backoff 时刻**（见 [accept](#accept)）。
每轮迭代取六个时间源（五条链表头 + backoff）里最近的截止时刻，作为 `wait` 的 timeout。

---

## Pause 的实现

```
Pause():
    depth++                              ← 可重入（AsyncDo 只允许一层，见下）
    if depth == 1:
        mod(fd, interest &^ Read)        ← 从这一刻起不再有读事件
        从空闲 LRU 摘除，挂进 pause LRU
        记录暂停开始时刻

resume():                                ← 可从任意 goroutine 调用
    once 保证幂等 → 投递一个「resume 控制项」到 loop 收件箱
    （控制项直接携带 core 引用，不走 token——连接可能已 Detached、槽位已复用）

loop 处理 resume 控制项（串行域内）:
    if --depth == 0:
        从 pause LRU 摘除
        执行暂停期间排队的 Post 闭包        ← 串行域按序恢复
        若连接已在 pendingClose 上 → 走 Detached → Closed，到此为止
        mod(fd, interest | Read)
        touch 后挂回空闲 LRU
```

`depth`、LRU、poller 注册都**只在 loop 线程上改**——初稿允许 `resume` 直接
`mod(fd)`，与 R2「所有 syscall 在 loop 上」直接冲突，第二轮评审纠出。
`Pause` 本身只能在 **loop 线程上**调用（回调与 `Post` 的函数体，**不含 `AsyncDo`
的函数体**——它跑在别的 goroutine 上），所以它可以直接改。

`AsyncDo` 在这之上多加两条：**同一连接同时只允许一个在途任务**（第二次调用返回
`ErrAsyncBusy`），且 **goroutine 在当次回调返回之后才启动**。这两条加上「`resume`
之前不执行 `OnClose` / `Post`」，构成了 `f` 属于串行域的完整论证，见
[01](01-server-api.md#这个保证是怎么来的)。

三条必须写进文档的性质：

1. **生效点是「不再发起新的 read 系统调用」**，不是「立刻停止一切」。已经读进
   `rbuf` 的字节该投递的还要投递不完的转进残片；`Pause` 不能撤销已经进入用户态的数据。
2. **暂停期间观察不到对端断线。** 摘掉读兴趣之后 FIN / RST 也不会报了
   （kqueue 上尤其明显，`EVFILT_READ` 被禁用就什么都收不到）。所以一条在
   `AsyncDo` 期间断掉的连接，要到 `resume` 之后才会被发现。`MaxPause` 兜住这一条。
3. **每次 `Pause` / `resume` 各一次 `epoll_ctl` / `kevent`**，量级是微秒。
   它是给「偶尔一次的阻塞调用」用的，不适合每条消息都走一遍。

---

## 内存布局

### 每 loop（不随连接数增长）

| 项 | 量级 |
| --- | --- |
| 读缓冲 `rbuf` | 64KB |
| 事件数组 | 几 KB |
| iovec 暂存 | 几 KB（帧头不在这里——它内联在 chunk 里，见 [03](03-outbound.md#stage-2待发送链)） |
| zstd 编码器 | 0.27MB（Fastest）~ 4.27MB（Better），见 [01](01-server-api.md#压缩的内存) |
| slab：`chunk` 节点、残片缓冲、`[]stage1Item` 队列 | 随在途量，不随连接数 |
| `slots []slot` | 16 字节 × 槽容量（loop 内稠密 freelist 下标，不按 fd） |

### 每连接

| 项 | 量级 | 备注 |
| --- | --- | --- |
| `Conn` + 内联的 `State` | 与 `S` 有关 | **不池化**，每条连接一次分配 |
| 读残片 `carry` | 稳态 **0** | 上界 = rbuf 容量（64KB）；大帧不进 carry |
| 大帧目标缓冲 `frameBuf` | 稳态 **0** | 仅大帧在途时占用，上界 `MaxMessage` |
| 出站队列 | 稳态 **0** | 按在途量付费 |
| 内核 socket 缓冲 | **几十 KB ~ 几百 KB** | 由 `Socket.RecvBuffer` / `SendBuffer` 决定 |
| WebSocket 握手信息 | 见 [05](05-websocket.md) | 握手完成后立刻释放握手期的临时状态 |

**内核 socket 缓冲仍然是这张表里最大的一项**，比所有用户态项加起来大一到两个数量级。
10 万连接的内存预算，第一件要做的事是把它按业务收紧。

### `Conn` 的字段布局

热字段（`fd`、`state`、`interest`、残片指针、出站链头、`lastActive`、LRU 节点）
挤进前一到两条 cache line；冷字段（握手信息、`Handshake` 指针、统计）放后面。
一轮迭代里对每条活跃连接的访问模式是「碰热字段，几乎不碰冷字段」，
布局对了就是每连接每轮少一次 cache miss。

---

## 平台差异总表

| | Linux（epoll） | macOS（kqueue） |
| --- | --- | --- |
| 就绪通知 | `epoll_wait`，**水平触发** | `kevent`，默认水平触发 |
| 读写兴趣 | 一个 event 的两个位 | 两个独立 filter，分别增删 |
| 事件合并 | 读写天然在同一个 event 里 | 同一 fd 返回两个事件，`wait` 内按 token 聚合，保证先写后读 |
| 唤醒 | `eventfd` | `EVFILT_USER` + `NOTE_TRIGGER` |
| 定时 | `epoll_wait` 的 ms timeout | `kevent` 的 `timespec` |
| accept 分发 | `SO_REUSEPORT`，每 loop 一个 listener | 单 listener + 轮转投递 |
| 批量写 | `writev` | `writev` |
| `Pause` | `EPOLL_CTL_MOD` 去掉 `EPOLLIN` | `EV_DISABLE` on `EVFILT_READ` |
| keep-alive | `TCP_KEEPIDLE/INTVL/CNT` | `TCP_KEEPALIVE`（只有 idle）+ `TCP_KEEPINTVL/CNT` |
| `SO_RCVBUF` | 内核把设定值翻倍记账；显式设置会关掉自动调优 | 语义接近，无翻倍 |

**只有 accept 那一行是行为上的实质差异**，其余都是同一语义的不同写法。
而 accept 的差异对业务不可见——`Options` 里没有对应的开关。

---

## 不变式清单

| # | 不变式 |
| --- | --- |
| R1 | 连接归属某个 loop 之后永不迁移 |
| R2 | 所有系统调用（read / writev / poller ctl / close）都在该连接所属的 loop 线程上 |
| R3 | 跨 goroutine 路径只做「入队 / 标脏 / 塞闭包」，**永不触碰 fd** |
| R4 | `close(fd)` 只发生在一轮迭代的回收阶段，此时其余阶段已全部跑完 |
| R5 | 事件里的 `token` 必须校验 generation；`Post` 闭包执行前**同时**校验 generation 与主状态 |
| R6 | `Conn.ID()` 进程内唯一且永不复用；`*Conn` 对象也永不复用 |
| R7 | 每 loop 至多一次 `notify` 在途（`armed` 标志），先清标志再 drain |
| R8 | 读缓冲每 loop 一块；连接级只保留「不足一帧」的残片，稳态为空 |
| R9 | 只有出站积压非空的连接在写兴趣集合里 |
| R10 | 只有一个时间源：`wait` 的 timeout。不使用 `timerfd` / `EVFILT_TIMER` |
| R11 | 每轮 accept 有上限；`EMFILE` / `ENFILE` / `ENOBUFS` / `ENOMEM` 走预留 fd 路径 + 指数退避 |
| R12 | 回收分两步：拆 socket（`Draining → Detached`）立刻做，`OnClose` 可以等 `resume`。二者之间连接资源已全部归还 |
| R13 | 暂停期间 `Post` 的闭包排队不执行；`resume` 时按投递顺序放出 |
| R14 | kqueue 下同一 fd 的读写事件按 token 聚合成一个 event，处理顺序先写后读 |
| R15 | `Draining` 的连接只在可写事件或 `CloseLinger` 到期时动，绝不逐轮重试 |
| R16 | `resume` 经控制项回 loop 执行；`depth` / LRU / poller 注册只在 loop 线程上改 |

---

## 设计决策记录

### R-D1. 自建 reactor 而不是继续用 gnet

**动机不是性能，是抽象形状。** 上一版相当一部分复杂度是在补偿底座：

| 底座提供的形状 | 上一版为此付出的 |
| --- | --- |
| 无条件把 socket 读干进每连接的弹性缓冲 | 阻塞期入站积压的封顶启发式——一个可远程触发的 OOM 全靠应用层兜 |
| `Wake(cb)` = 一个 poller 任务，且会先重跑一遍入站处理 | 整套唤醒不变式（`triggered` / `wakeGen` / `disarm`），以及每次 flush 白跑一趟解帧 |
| 异步 `Close` | `closing` / `stopped()` / `writable()` 三个状态谓词，外加「fd 可能已被复用」的推理 |
| 全局 logger | 「同进程只有第一个 server 的 Logger 生效」这条对用户可见的怪癖 |
| 无 recover | 七处 `safeCall` 屏障 |
| 只能遍历全部连接 | 10 万连接上每轮全表扫 |

自建之后，前四行整行消失，最后一行换成 O(1) 数据结构。**这些不是性能优化，
是把「靠纪律维持的不变式」换成「结构上不可能违反」。**

代价是要自己写 epoll / kqueue 两套实现、accept 两条路径、以及全部的缓冲管理。
这是一次性成本，而上面那些是持续成本。

### R-D2. 水平触发 + 读取预算，而不是边缘触发

见[读路径](#水平触发--读取预算)。两条理由：**公平性**（ET 下一条高速连接能饿死同 loop
的其余几万条）和 **`Pause` 的可实现性**（ET 下「暂时不读」会丢事件）。

代价是每轮多一次返回 `EAGAIN` 的 `read`——用读取预算提前让出的话连这次都省了。

### R-D3. flush 是迭代里的一个阶段，读完立刻 flush

见[一轮迭代](#一轮迭代)与 [03 的 O-D2](03-outbound.md#o-d2-flush-是事件循环的一个阶段不是一个任务)。
请求-响应场景下回包的额外成本从「一个任务对象 + 一次唤醒 + 一趟多余的入站解析」
降到 0，延迟从「下一轮迭代」变成「本轮末尾」。

### R-D4. 槽位 + generation 的 token，不用 map

fd 稠密 ⇒ 直接当数组下标。事件回来时零查表、零哈希、零锁。generation 让陈旧事件
自动失效。

**但公开的 `Conn.ID()` 与它无关**：那个必须永不复用，用每 loop 的自增计数器加
loop 前缀生成。两者混用就会出现「注册表 key 撞车」。

### R-D5. 只用一个时间源

`wait` 的 timeout 参数就是全部。不用 `timerfd` / `EVFILT_TIMER`：少一个 fd、
少一处平台差异、少一类故障模式。五条独立的侵入式 LRU + 一个 backoff 时刻，
取最近的截止时刻。

### R-D6. `EMFILE` 走预留 fd 路径

fd 耗尽时如果只是「记一条日志然后继续」，水平触发会立刻再次报告 listener 可读，
变成一个满载 CPU 的死循环加刷屏日志。预留一个 fd 用来「腾出位置 → accept → 立刻
关闭 → 归还」，同时临时摘掉 listener 的读兴趣。

这是 10 万连接下的真实故障模式（`ulimit -n` 配小、fd 泄漏），不是理论问题。

### R-D7. macOS 用单 acceptor，且这个差异不出现在 API 上

Darwin 没有内核级 accept 负载均衡。与其在 `Options` 上开一个在一个平台上是谎言的
`ReusePort` 开关，不如让 gate 各平台各自选最优路径，业务完全不感知。

macOS 的定位是开发与测试平台，单 acceptor 的额外成本（每条新连接一次跨 loop 投递）
在这个定位下无关紧要。

### R-D8. 第二轮评审驱动的四个结构修正

| 初稿 | 修正 | 为什么初稿是错的 |
| --- | --- | --- |
| 槽位按 fd 直接索引 | 每 loop 稠密 freelist 下标 | fd 只在进程内稠密，不在单个 loop 内稠密 |
| `StallTimeout` 复用写兴趣集合 | 按 `lastProgress` 的独立 LRU | 写兴趣集合没有时间序，扫描停不下来 |
| `CloseLinger` 在 reap 链上逐轮重试写 | 事件驱动的 linger 链 | 对满缓冲 fd 逐轮撞 `EAGAIN` 是 syscall 风暴 |
| `resume` 直接 `mod(fd)` | once + 控制项投递回 loop | 违反 R2（所有 syscall 在 loop 线程上） |

四条的共性：**初稿都想「复用现成结构省一条链」，而复用的结构缺少所需的性质**
（时间序、线程归属、密度）。这类省结构的诱惑要用不变式表来挡。
