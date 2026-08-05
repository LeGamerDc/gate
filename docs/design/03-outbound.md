# 出站流水线

本文定义从 `Conn.Send` 到 `writev` 之间发生的一切：队列形状、合批时机、分组算法、
编码、部分写、背压与失败处理。

这一层是 gate 最主要的价值点所在——**业务只做入队，真正的写收敛到事件循环上批量执行**。
它与 [04-reactor](04-reactor.md) 的耦合最深（合批时机、跨线程投递、可写事件都由 reactor
提供），与 [05-websocket](05-websocket.md) 的耦合在于最后一跳的封帧。三份文档一起读。

---

## 目录

- [数据流全景](#数据流全景)
- [两级队列](#两级队列)
- [合批：什么时候 flush](#合批什么时候-flush)
- [in-loop 快路径](#in-loop-快路径)
- [分组算法](#分组算法)
- [编码](#编码)
- [写与部分写](#写与部分写)
- [背压](#背压)
- [失败处理](#失败处理)
- [与 WebSocket 的接口](#与-websocket-的接口)
- [内存与零分配清单](#内存与零分配清单)
- [不变式清单](#不变式清单)
- [设计决策记录](#设计决策记录)

---

## 数据流全景

```
    业务 goroutine                  事件循环 goroutine
  ───────────────────           ──────────────────────────────
   Send / SendAlone
   SendFrame / SendFunc
          │
          │ 入队（复制或移交所有权）
          ▼
   ┌─────────────────┐
   │  待编码队列      │  []sendMsg，业务消息，尚未编码
   │  (stage 1)      │
   └────────┬────────┘
            │            flush：分组 → 压缩 → 加密 → 写帧头
            │            ────────────────────────────────────▶
            ▼
                              ┌──────────────────┐
                              │   待发送链        │  已编码的线路字节
                              │   (stage 2)      │  FIFO，可能部分已写出
                              └────────┬─────────┘
                                       │  writev（最多 IOV_MAX 个 iovec）
                                       ▼
                                    socket
                              写不完 → 记住偏移，注册可写事件
```

两级的分工：

- **stage 1** 是「还没决定怎么编码」的业务消息。合包与压缩的决策要看**一整批**才能做，
  所以必须先攒着。
- **stage 2** 是「已经是线路字节」的编码结果。它存在的唯一理由是**部分写**：
  `writev` 写不完时剩下的字节必须原样留着，不能重编码（重编码会得到不同的字节，
  帧流就错位了）。

`Outbound.MaxBuffer` 与 `Writable()` 计的都是 **stage1 + stage2 的总字节数**——
一个数，一处记账。上一版这里是两处（sender 自己的队列 + 底层连接的出站缓冲），
两处各自判断、各自的失败行为，是一个持续的混淆源。

---

## 两级队列

### stage 1：待编码队列

```go
type sendMsg struct {
	data                    []byte
	maskPermit, maskAlready byte
	owned                   bool  // data 是否来自 gate 的池，需要归还
}
```

字段顺序不是随手排的：切片放最前，三个单字节标记挤在尾部的对齐 padding 里。
反过来会因为切片要 8 字节对齐而在中间垫 6 字节，结构体从 32 涨到 40——队列是一整段
`[]sendMsg`，每条消息多 8 字节意味着同一次 flush 要多碰 25% 的 cache line。

| 来源 | `data` 从哪来 | `owned` |
| --- | --- | --- |
| `Send` / `SendAlone` | 从分级池借一块，复制进去 | true |
| `SendFunc` | 就是交给 `fill` 的那块，`fill` 返回即入队 | true |
| `SendFrame` | 指向 `Frame` 的内部字节 | false，改为持有一份 `*Frame` 引用 |

`maskPermit` 记「允许 gate 做哪些加工」：`Send` 是 `z|c|e`，`SendAlone` 是 `e`
（**只关掉压缩与合包，不关加密**），`SendFrame` 是 `0`。
`maskAlready` 记「这些字节已经是什么状态」——`Frame` 若已压缩则带 `z`，
带 `PreEncrypted()` 则带 `e`。

一条消息能进 compound 的条件是 **`maskPermit` 允许合包 且 `maskAlready` 为空**：
compound 的子帧按协议约定不得携带 `z`/`c`/`e`（见 [02](02-wire-protocol.md#compound)），
一条「已经压缩好」的消息进了 compound，它的 `z` 标记就再没有地方表达。

### stage 2：待发送链

```go
type chunk struct {
	buf  []byte  // 线路字节；来自分级池，或 Frame 的内部字节
	off  int     // 已经写出去多少（部分写）
	ref  *Frame  // 非 nil 表示 buf 属于这个 Frame，持有它直到写完
	next *chunk
}
```

严格 FIFO。新编码的结果永远追加在尾部，**绝不插队**——否则部分写留下的半个帧后面
会跟上别的帧，对端看到的就是错位的流。

### `Frame` 的生命周期

`Frame` 不可变，由 **GC** 管理，出站路径上没有引用计数：

```
srv.NewFrame(payload)  → 编码一次（帧头 + 按 server 配置压缩）
SendFrame              → chunk.ref = f，队列自然持有引用
该 chunk 写完           → chunk.ref = nil
最后一个引用消失         → GC 回收
```

`SendFrame` **失败时不持有引用**（`ErrConnClosed` / `ErrSendQueueFull` /
`ErrCipherConflict` 都在挂上 `chunk.ref` 之前返回）。实现上要做成结构性的：
先做全部检查，最后一步才构造 chunk。

这条规则对 GC 版本只是「不多占一份引用」，代价很小；但如果将来内部改成引用计数，
它就是配平的关键，所以现在就写进不变式表（O9）。

---

## 合批：什么时候 flush

只有三个触发点：

| 触发 | 场景 | 由谁 |
| --- | --- | --- |
| **入站事件收尾** | 处理完一个连接的入站数据（handler 可能 Send 了回包） | reactor，见 [04](04-reactor.md#一轮迭代) |
| **dirty 链** | 跨 goroutine 的 `Send` 把连接标脏 | reactor 的 flush 阶段 |
| **可写事件** | 之前部分写，socket 现在能写了 | reactor |

flush 是**事件循环里的一个阶段**，不是一个回调、不是一个任务对象。这是自建 reactor
相对上一版最大的结构性收益：上一版每次 `Send` 都要 arm 一次 `Wake`，而那意味着一个
poller task 对象、一次可能的 eventfd 写、以及**底座会先完整重跑一遍 OnTraffic 再执行
回调**——WebSocket 模式下那是一整趟 `readBufferBytes` + `decodeMessages` + 缓冲收缩。

flush 是**幂等**的：队列空就直接返回。所以「入站事件收尾已经 flush 过、dirty 链里
又出现一次」不需要任何去重。

---

## in-loop 快路径

最常见的用法是在 `OnMessage` 里回包。这条路径上，flush 马上就要在本轮事件收尾时发生，
**不需要任何跨线程唤醒**。

```go
func (o *outbound) send(m sendMsg) error {
	o.mu.Lock()
	// ...配额检查、入队...
	// arm 唤醒：只有当没人负责 flush 时才需要
	wake := !o.armed && !o.inLoop && o.conn.writable()
	if wake { o.armed = true }
	o.mu.Unlock()

	if wake { o.conn.loop.postDirty(o.conn) }  // 跨线程：MPSC + 也许一次 notify
	return nil
}
```

`inLoop` 由事件循环在处理该连接的入站数据期间置上，收尾时清掉：

```go
// 事件循环，处理完这个连接的入站数据之后：
o.mu.Lock()
o.inLoop = false
q := o.take()       // 取走 stage 1
o.mu.Unlock()
o.encode(q)         // → stage 2 → writev
```

**正确性论证。** 关键是「读 `inLoop`」和「入队」在同一个临界区里，而事件循环清
`inLoop` 和取队列也在同一个临界区里。按 `mu` 的获取顺序分两种情况：

- **send 先拿到锁**：消息已经在队列里，随后事件循环的 `take()` 一定看得见它。
- **事件循环先拿到锁**：那它已经把 `inLoop` 清成 false，于是 send 读到 false，
  走正常的 arm 路径。

两种情况都有人负责，不存在「既没 arm 也没人 flush」的窗口。这与
[05](05-websocket.md) 里「握手完成前不可能有消息入队」是同一类结构性论证。

> **为什么不做成完全无锁的 in-loop 路径。** 理想形态是「在事件循环线程上 `Send`
> 直接 append 到一个无同步的本地队列」。做不到，因为 Go 里没有便宜的方式让 `Send`
> **可靠地**判断自己跑在哪个 goroutine 上：`inLoop` 这个标记会被外部 goroutine 读到
> true（那是对的，它只用来抑制唤醒），但如果拿它来选择「往哪个队列写」，就会出现
> 外部 goroutine 往本该无同步的本地队列里写——一个静默的数据竞争。
>
> 另一条路是加一个 `Reply()`，文档规定只能在回调内调用。它能省掉每条消息一次
> 无争用的 CAS 对（约 10~20ns），代价是一个用错了只会静默数据竞争的软契约
> （`-race` 能抓到，但生产上不会开）。**v1 不做**：被省掉的那部分（poller task +
> eventfd + 多跑一趟入站解析）比锁本身大一个数量级，先把大的拿掉，锁留到 profile
> 说话再说。

---

## 分组算法

flush 拿到一整批消息之后，先分组再编码：

```
i = 0
while i < len(buf):
    if buf[i] 不可合包:
        j = i+1; while j < len && buf[j] 不可合包: j++
        编码 buf[i:j] 为独立帧          ← 连续的不可合包消息一次 writev 打包
    else:
        j, size = cluster(buf, i)       ← 从 i 起向后收集可合包的
        if j-i == 1: 编码 buf[i:j] 为独立帧   ← 单条不值得套一层 compound
        else:        编码 buf[i:j] 为 compound，是否压缩看 size
    i = j
```

`cluster` 的累加口径是 `Σ frameSize(len(data))`，也就是**子帧头也算进去**的那个数。
它同时是三件事：

1. 与 `MaxCluster` 比较的口径；
2. 与 `CompressThreshold` 比较的口径；
3. **最终写进外层帧头长度字段的那个值**。

三者必须是同一个数。只按 payload 长度累加会漏掉每条 2~4 字节的子头：极端情况下
（大量空消息）body 全是头而 `size` 恒为 0，一组可以无限长下去，最终帧远超
`MaxCluster` 甚至越过协议上限被对端拒收。

`size` **只统计真正进组的消息**。累加写在 break 判断之前是个经典错误：
`2×100 字节 + 一条 5000 字节的不可合包消息`，一个实际只有 200 字节的组会拿着 5200
去和 `CompressThreshold` 比，于是白压一遍。

`MaxCluster` 在构造时就夹进协议上限（`min(MaxCluster, maxMessageSize)`）：
配到 32MB 附近时，两条 16MB 的消息加上子头正好是 32MB+8，帧一发出去对端立刻断链。
构造时夹一次比热路径上反复判断便宜，也让「compound body 不会超限」成为结构性事实。

---

## 编码

两条路径，都遵循 [02](02-wire-protocol.md#帧体三个变换与它们的顺序) 定义的顺序。

### 独立帧

```
for 每条消息:
    data, flag := msg.data, msg.maskAlready
    if 允许压缩 && CompressThreshold > 0 && len(data) > CompressThreshold:
        out := encoder.EncodeAll(data, 池化 dst)
        if len(out) < len(data):        // 压完更大就用原文
            data, flag = out, flag|z
    if cipher != nil && 允许加密:
        data = cipher.Seal(池化 dst, data)   // Overhead()==0 时 dst = data[:0]，原地
        flag |= e
    header := encodeHeader(len(data)); header[0] |= flag
    → 两个 iovec：{header, data}
```

一次 `writev` 打包多条消息，最多 `IOV_MAX`（Linux / macOS 都是 1024）个 iovec，
也就是最多 512 条消息。实现按 chunk 分批，chunk 大小是可调常量。

`header` 来自**每 loop 一块**的暂存数组，而不是循环体里的局部 `[4]byte`——后者每条
消息都要单独逃逸一次。`writev` 会读走这些切片，所以每个 header 都必须一直有效到
写调用返回，按 chunk 划分正好满足。

### compound

```
body := 4 字节占位 + Σ (子帧头 + 子 payload)     ← 先占住外层头的位置
if 该压缩:
    out := encoder.EncodeAll(body[4:], 池化 dst)
    if len(out) < len(body): body, flag = out, flag|z
if cipher != nil:
    body = cipher.Seal(...); flag |= e
写外层帧头（长度 = 最终字节数），flag |= c
→ 一个 iovec
```

先占 4 字节再回填，是为了压不压缩都能**就地**写头，省掉一次搬运。
最终长度 < 4096 时头只要 2 字节，`data = data[2:]` 即可，不必移动 body。

`Overhead() > 0` 的 AEAD 会改变长度，所以**必须先 seal 再写头**，
见 [02](02-wire-protocol.md#与-aead-的关系)。

### 兜底校验

写头之前再判一次 `len(data) <= maxMessageSize`。走到这里说明上游不变式破了
（业务侧的超长消息在 `Send` 就被挡了，压缩只会更小，加密只增加 `Overhead`），
但**宁可关连接也不能写一个长度会溢出帧头的帧**——那会把长度和 `m`/`z`/`c`/`e`
标记同时改错，收发两端都察觉不到。

---

## 写与部分写

```
flush:
    把 stage 1 编码成若干 chunk，追加到 stage 2 尾部
    while stage 2 非空:
        凑最多 IOV_MAX 个 iovec（从链头开始，第一个 chunk 从 off 起）
        n, err := writev(fd, iovec)
        if err == EAGAIN:  注册可写事件；返回
        if err != nil:     写失败路径（见下）
        按 n 推进链头的 off，写满的 chunk 出链并归还内存 / 放开 Frame 引用
        if n < 本次请求的总字节: 注册可写事件；返回   ← 内核缓冲满了
    stage 2 空 → 摘掉可写事件注册
```

要点：

- **`writev` 永远不阻塞**，socket 是非阻塞的。写不完就记住位置、注册可写事件，
  剩下的交给下一次事件。
- **只有 stage 2 非空的连接才在可写事件集合里。** 这本身就是「哪些连接背压了」的
  现成索引，不需要额外的数据结构，也不需要每次去问内核。
- **部分写的字节绝不重编码。** chunk 里存的是最终线路字节，`off` 是唯一的进度。
- 归还内存的时机是**该 chunk 完全写出去之后**，不是 `writev` 返回之后——一次
  `writev` 可能只写了链头 chunk 的一半。

---

## 背压

两条线，语义完全不同：

```
Send 侧（准入控制）
  stage1 + stage2 + 这条消息的 frameSize > MaxBuffer
    → 拒绝入队，返回 ErrSendQueueFull       ← 消息没进队列，帧流没有洞
    → gate 不关连接

写侧（卡死检测）
  stage 2 非空，且距上一次「成功写出至少一个字节」超过 StallTimeout
    → 关闭连接，OnClose(ErrBackpressure)
```

以及一条软线：`stage1 + stage2 > HighWater` 时 `Writable()` 返回 false，让业务主动降级。

### 为什么准入控制不关连接

核心不变式是：**gate 从不静默丢消息——要么送出，要么明确告诉你没送出。**

准入控制满足这条：消息**从未进入队列**，帧流上没有洞，业务拿到 `ErrSendQueueFull`
后自己决定——非关键推送就丢掉，关键消息就 `c.Close(...)`。这比上一版好：上一版是在
flush 时发现缓冲超限，然后关闭连接并**丢弃队列里已经接受过的、数量未知的消息**，
业务只能从 `OnClose` 推断出「有东西没发出去」，但不知道是哪些。

### 为什么需要 `StallTimeout`

只有尺寸上限的话，一个「窗口卡死但 TCP 连接还活着」的对端会让连接一直占着
`MaxBuffer` 字节。而 `Limits.Idle` 救不了它——空闲判据是「交付过完整消息」，
一个卡死的对端确实不再发消息，但它也可能只是在慢慢读。

判据是**「有没有写出去哪怕一个字节」而不是「积压有没有超限」**：一条长期贴着
上限但在稳定排空的连接是健康的（业务推得比它读得快，`ErrSendQueueFull` 已经在
提醒了）；一条一个字节都写不出去的连接是死的。

检查搭在 reactor 已有的定时扫描上，不引入新的时间源，见 [04](04-reactor.md#超时与空闲回收)。

---

## 失败处理

| 情况 | 处置 | 已入队消息 |
| --- | --- | --- |
| `writev` 返回 `EAGAIN` | 注册可写事件，等下一次 | 保留 |
| `writev` 返回真正的错误 | **关闭连接** | **丢弃** |
| `StallTimeout` 到期 | 关闭连接，`ErrBackpressure` | 丢弃 |
| 编码期发现越限（兜底） | 关闭连接 | 丢弃 |
| 业务 `Close(reason)` | 尽力 flush 之后关闭 | 尽力送出 |

**写失败之后必须立刻停止投递。** 一次失败的 `writev` 可能已经把一帧的一部分送上了
线路，帧流从此有个洞；继续把后面的 chunk 灌进去，只会让对端在一个错位的流上继续解析。
这与「不静默丢消息」是同一条原则的两面。

实现上要注意顺序：**先把 outbound 自己置成关闭，再去关连接。** 只关连接是不够的——
关闭在 reactor 里是延迟到本轮迭代末尾执行的，此刻连接还是 open 状态，并发的 `Send`
可以立刻重新入队并把连接标脏，那次 flush 如果排在关闭之前，就会往这条已经有洞的
帧流里继续写。

---

## 与 WebSocket 的接口

WS 模式下每次 `writev` 的一批字节要包进一个 WebSocket 二进制帧。接口是一个只有两个
方法的内部抽象：

```go
type framer interface {
	// frame 在 vec 前面插入传输层需要的帧头，返回新的 iovec。
	// 裸 TCP 是恒等变换；WebSocket 插一个 2~10 字节的帧头。
	frame(scratch *[]byte, vec [][]byte, total int) [][]byte
}
```

**一次 `writev` 的一整批 gate 帧装进一个 WS 二进制消息**，而不是每条 gate 帧一个
WS 帧。gate 的帧本身是自定界的，所以外层用一个 WS 帧就够；这样 WS 帧头从「每条消息
2~10 字节」降到「每批 2~10 字节」。

分批（`IOV_MAX`）导致一次 flush 拆成多次 `writev` 时，每一批各自成为一个 WS 二进制
消息——这是合法的，对端把它们的 payload 首尾相接就还原成同一个 gate 字节流。

WS 帧头的暂存也是**每 loop 一份**，不挂在连接上：它只在一次写调用期间有用，
而所有出站写都发生在连接自己的事件循环上且中途不让出，所以池里同时在外的对象数
等于事件循环数，不随连接数增长。挂成连接级字段的话，十万连接光这一块就要白占
一百多 MB。

细节见 [05-websocket](05-websocket.md#出站封帧)。

---

## 内存与零分配清单

稳态下（`-benchmem` 可验证）出站路径 **0 allocs/op**：

| 内存 | 来源 | 归还时机 |
| --- | --- | --- |
| `Send` 的复制目标 | 分级池 | 该消息编码完成后 |
| 压缩输出 | 分级池 | 该帧写出后 |
| AEAD 的 `dst` | 分级池 | 同上 |
| compound body | 分级池 | 该帧写出后 |
| iovec 数组、header 暂存 | **每 loop 一份**的暂存对象 | 一次写调用结束 |
| chunk 节点 | 每 loop 的 slab | 该 chunk 写完 |
| stage 1 队列 `[]sendMsg` | 每 loop 的池 | flush 完成 |
| zstd 编码器 | **每 loop 一个**，concurrency=1 | 常驻 |

跨调用的暂存空间**一律按事件循环池化，不挂成连接级字段**——这是 10 万连接下的
硬要求，理由见 [01 的内存预算](01-server-api.md#按事件循环增长的内存不随连接数增长)。

`Frame` 是唯一跨连接、跨 loop 共享的对象，但它**由 GC 管理**（见
[01 的 D20](01-server-api.md#d20-frame-由-gc-管理不公开引用计数)）：chunk 持有一个
`*Frame` 字段，写完置 nil，最后一个引用消失时由 GC 回收。因此出站路径上
**没有任何原子引用计数**；其余池化内存都不跨 loop，普通整数即可。

---

## 不变式清单

实现和测试都以这张表为准：

| # | 不变式 |
| --- | --- |
| O1 | gate 从不静默丢消息：要么送出，要么 `Send` 明确返回错误 |
| O2 | stage 2 是严格 FIFO，任何新数据只追加在尾部 |
| O3 | 部分写的字节永不重编码；`off` 是唯一进度 |
| O4 | 所有出站写都发生在连接所属的事件循环上；跨线程路径只入队与标脏，**永不触碰 fd** |
| O5 | `MaxBuffer` / `HighWater` 计的是 stage1 + stage2 的总字节，一处记账 |
| O6 | 一切计量用 `frameSize`（payload + header），不用 `len(payload)` |
| O7 | compound body 的字节数、`MaxCluster` 的比较口径、外层帧头的长度值是同一个数 |
| O8 | 写失败之后立刻停止投递；先关 outbound，再关连接 |
| O9 | `SendFrame` 失败时不 retain；调用方不需要为失败路径配平 |
| O10 | flush 幂等：队列空即返回 |
| O11 | 连接不可写时不可能有消息入队（见 [05](05-websocket.md#握手与可写时机)） |

---

## 设计决策记录

### O-D1. 两级队列，一处记账

**问题。** 上一版有两处独立的积压计量：sender 自己的待发送队列（超限时 `Send` 返回
错误）和底层连接的出站缓冲（超限时关闭连接）。两处口径不同、失败行为不同，
而业务看到的是同一件事「发不出去了」。

**选择。** stage 1（待编码）+ stage 2（待发送）两级队列，`MaxBuffer` / `HighWater` /
`Writable()` 全部计总和。

**代价。** 需要自己维护 stage 2，而不是把部分写甩给底座。这正是自建 reactor 换来的
能力——也是必须自己做的事。

### O-D2. flush 是事件循环的一个阶段，不是一个任务

**问题。** 上一版每次 `Send` 要 arm 一次 `Wake`，代价是一个 poller task 对象、
一次可能的 eventfd 写，以及底座**先完整重跑一遍入站处理再执行回调**——
WS 模式下那是一整趟解帧 + 缓冲收缩。

**选择。** flush 是循环里的第 N 步。三个触发点（入站收尾、dirty 链、可写事件）
都只是「什么时候走到这一步」。

**收益。** 请求-响应这个主流量场景下，回包的额外成本从「一个任务 + 一次唤醒 +
一趟多余的入站解析」降到 0，而且延迟从「下一轮迭代」变成「本轮末尾」。

### O-D3. in-loop 抑制唤醒，但不做无锁本地队列

见 [in-loop 快路径](#in-loop-快路径)。**先拿掉数量级大的那部分（唤醒），
把锁留给 profile。** 无锁本地队列需要可靠判断「我在不在事件循环线程上」，
Go 里做不到；退而求其次的 `Reply()` 软契约用错了是静默数据竞争，v1 不引入。

### O-D4. 背压拆成「准入控制」与「卡死检测」

**问题。** 上一版只有尺寸上限，且触发时**关闭连接并丢弃队列里数量未知的消息**。
业务只知道「有东西没发出去」，不知道是哪些。

**选择。**
- 尺寸上限做成**准入控制**：`Send` 拒绝，消息从未入队，帧流没有洞，业务知道是哪条。
- 另加**卡死检测** `StallTimeout`：判据是「有没有写出去哪怕一个字节」，
  而不是「积压有没有超限」。一条贴着上限但在稳定排空的连接是健康的。

**代价。** 多一个配置项，多一处定时检查（搭在已有的扫描上）。

**这修正了 01 里的描述**：`MaxBuffer` 不再是「越过即关闭连接」，而是准入线；
`ErrBackpressure` 这个关闭原因改由 `StallTimeout` 触发。

### O-D5. 一次 `writev` 的一批 gate 帧装进一个 WS 帧

WS 帧头从「每条消息」摊到「每批」。gate 帧自定界，所以外层一个 WS 帧就够。
上一版已经这样做了，这里把它写成规格。

### O-D6. 跨线程路径永不触碰 fd

上一版为「陈旧连接对象 → 已被回收甚至已被新连接复用的 fd」这一类缺陷写了相当多的
状态判断（`stateClosed`、`writable()`、`closing`）。

自建 reactor 之后这件事变成结构性的：**跨 goroutine 的 `Send` 只做两件事——入队、
标脏——两者都不碰 fd**。所有 syscall 都发生在事件循环线程上，而 fd 的关闭也发生在
那里（延迟到迭代末尾），两者天然互斥。

`writable()` 这道闸仍然保留，但它的职责从「防止写到别人的 fd」降级成
「不可写时不要白白编码」。
