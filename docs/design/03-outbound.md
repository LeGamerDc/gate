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
   │  待编码队列      │  []stage1Item：消息 / Frame / cipher barrier
   │  (stage 1)      │
   └────────┬────────┘
            │            flush：分组 → 压缩 → 加密 → 帧头（含 WS）落成 chunk
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

`Outbound.MaxBuffer` 与 `Writable()` 比较的都是**同一个数**：`reservedWire`——
入队时按保守上界扣、编码与写出时分两次退还的在途线路字节记账，
口径见 [06 的预算模型](06-connection-state-machine.md#预算模型)。
仍然是一处记账；上一版是两处（sender 自己的队列 + 底层连接的出站缓冲），
两处各自判断、各自的失败行为，是一个持续的混淆源。

---

## 两级队列

### stage 1：待编码队列

stage 1 不是单一的消息数组，而是一个 **tagged union**——`SetCipher` 要在队列里
留下一个可见的边界（见 [SetCipher barrier](#setcipher队列里的-barrier)）：

```go
type stage1Item struct {
	kind itemKind // itemMessage | itemFrame | itemCipherBarrier | itemRaw
	// itemMessage
	data                    []byte
	maskPermit, maskAlready byte
	// itemFrame
	frame *Frame
	// itemCipherBarrier
	cipher Cipher
}
```

`itemRaw` 是已经是最终线路字节的原始段（WS 的 101 响应、HTTP 拒绝、WS 控制帧）：
它绕过 gate 编码与 WS 封帧，但**仍走同一条 FIFO**——close 帧因此在结构上不可能
插进半个数据帧中间，也不可能被后来的数据帧越过（W13）。它复用 `data` 字段。

| 来源 | 载荷 |
| --- | --- |
| `Send` / `SendAlone` | `itemMessage`，`data` 从**全局并发安全分级池**借一块复制进去 |
| `SendFunc` | `itemMessage`，`data` 就是交给 `fill` 的那块（commit 时入队） |
| `SendFrame` | `itemFrame`，持有一份 `*Frame` 引用 |
| `SetCipher` | `itemCipherBarrier` |

`maskPermit` 记「允许 gate 做哪些加工」：`Send` 是 `z|c|e`，`SendAlone` 是 `e`
（**只关掉压缩与合包，不关加密**）。
`maskAlready` 记「这些字节已经是什么状态」——`Frame` 若在 `NewFrame` 时已压缩则带 `z`
（`PreEncrypted` **不**带 `e`：它声明的是业务自理的密文，不复用 gate 的 `e` 位，
见 [01 的 Frame](01-server-api.md#frame预编码帧与广播)）。

一条消息能进 compound 的条件是 **`maskPermit` 允许合包 且 `maskAlready` 为空**：
compound 的子帧按协议约定不得携带 `z`/`c`/`e`（见 [02](02-wire-protocol.md#compound)），
一条「已经压缩好」的消息进了 compound，它的 `z` 标记就再没有地方表达。
**一个 compound 分组也绝不跨 cipher barrier**——一个合并帧只能用一个密钥。

### stage 2：待发送链

```go
type chunkKind uint8
const (
	chunkInline   chunkKind = iota // 帧头（gate 2~4 字节 / WS 2~10 字节），内联存储
	chunkPooled                    // payload，来自分级池
	chunkFrame                     // payload，属于某个 *Frame
)

type chunk struct {
	kind  chunkKind
	off   int32    // 本 chunk 已写出的字节数，唯一的进度表示
	n     int32    // 有效字节数
	hdr   [10]byte // kind == chunkInline
	buf   []byte   // kind == chunkPooled | chunkFrame
	frame *Frame   // kind == chunkFrame：持有引用防 GC
	next  *chunk
}
```

三条不变式（与 [06](06-connection-state-machine.md#出站的精确模型) 一致）：

- **stage 2 里的每一个字节都是最终线路字节。** 编码在入链之前完成——包括 WS 帧头——
  入链之后不再变换。
- **`off` 是唯一的进度表示。** 部分写只推进 `off`，永不重新生成任何字节。
- **严格 FIFO，只在尾部追加**——否则部分写留下的半个帧后面会跟上别的帧，
  对端看到的就是错位的流。

**帧头内联在 chunk 节点里，不放每-loop 暂存区。** 帧头如果放暂存区，一次部分写之后
那块暂存已经被别人复用，续写只能重新生成——而重新生成的 WS 帧头会让线路直接错位。
内联意味着帧头和 payload 一样是持久的线路字节。

### `Frame` 的生命周期

`Frame` 不可变，由 **GC** 管理，出站路径上没有引用计数：

```
srv.NewFrame(payload)  → 编码一次（帧头 + 按 server 配置压缩）
SendFrame              → chunk.frame = f，队列自然持有引用
该 chunk 写完           → chunk.frame = nil
最后一个引用消失         → GC 回收
```

`SendFrame` **失败时不持有引用**（`ErrConnClosed` / `ErrSendQueueFull` /
`ErrCipherConflict` 都在挂上 `chunk.frame` 之前返回）。实现上要做成结构性的：
先做全部检查，最后一步才构造 chunk。

这条规则对 GC 版本只是「不多占一份引用」，代价很小；但如果将来内部改成引用计数，
它就是配平的关键，所以现在就写进不变式表（O9）。

### SetCipher：队列里的 barrier

「`SetCipher` 在事件循环上调用」**不足以**给密钥切换一个确定的位置——加密发生在
flush 时，不是入队时，已入队未编码的消息会被新密钥追上（第二轮评审的 P0）。
切换点必须表达在**队列里**：

- `SetCipher(k)` 往 stage 1 尾部追加一个 `itemCipherBarrier`。
- 编码器按顺序走：遇到 barrier 就切换当前使用的 cipher，**并结束当前的 compound
  分组**——一个合并帧不能跨密钥边界。
- 于是 `Send(A); SetCipher(k); Send(B)` 的语义是精确的：A 用旧密钥，B 用新密钥。

入站方向没有队列问题：cipher 是只在串行域内读写的普通字段，切换对**下一个被解析
的帧**生效。完整语义见 [06 的 Cipher epoch](06-connection-state-machine.md#cipher-epoch)。

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
func (o *outbound) send(it stage1Item) error {
	o.mu.Lock()
	// ...配额检查、入队...
	// arm 唤醒：只有当没人负责 flush 时才需要
	wake := !o.armed && !o.inLoop && o.conn.writable()
	if wake {
		o.armed = true
		o.conn.loop.postDirty(o.conn) // 与 armed 同临界区，见下面的补充规则
	}
	o.mu.Unlock()

	if wake { o.conn.loop.maybeNotify() } // eventfd / EVFILT_USER 唤醒可以留在锁外
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

两条补充规则（第二轮评审补上的，缺了会破坏侵入式 dirty 链）：

- **dirty 节点的发布与 `armed` 的置位必须在同一个临界区内完成**（或用 CAS 抢占
  节点的「在链中」标志）。锁内置 `armed`、锁外 `postDirty` 的写法有一个窗口：
  事件循环已经 `take()` 并清了 `armed`，另一个 `Send` 于是再 post 一次——
  同一个连接被挂进 dirty 链两次，而 dirty 节点是内嵌在连接里的侵入式节点，
  重复挂链直接破坏链表结构。
- **任何把 `inLoop` 置为 `true` 的路径都必须在批边界用 `defer` 清掉**，
  否则一次 panic 会让这条连接的唤醒被永久抑制。

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
        flag |= e
        header := encodeHeader(len(data)+Overhead()); header |= flag  // 先算长度拼头
        data = cipher.Seal(池化 dst, data, header)  // 帧头做 AAD；Overhead()==0 时原地
    else:
        header := encodeHeader(len(data)); header |= flag
    → 两个 chunk：inline(header) ++ pooled(data)
```

一次 `writev` 打包多条消息，最多 `IOV_MAX`（Linux / macOS 都是 1024）个 iovec。
iovec 数组是每 loop 暂存，只活一次写调用。

`header` 写进该帧的 **inline chunk**（`chunk.hdr`），不来自每-loop 暂存——
帧头是持久线路字节，必须活到「这个 chunk 完全写出去」，而暂存区在下一次编码就会
被复用。部分写之后续写的是同一份字节，
见 [06 的所有权表](06-connection-state-machine.md#字节所有权表)。

### compound

```
body := Σ (子帧头 + 子 payload)      ← 池化缓冲；子头标记位全 0
flag := c
if 该压缩:
    out := encoder.EncodeAll(body, 池化 dst)
    if len(out) < len(body): body, flag = out, flag|z
if cipher != nil:
    flag |= e
    外层头 := encodeHeader(len(body)+Overhead()) | flag   ← 写进 inline chunk
    body = cipher.Seal(池化 dst, body, 外层头)            ← 帧头做 AAD
else:
    外层头 := encodeHeader(len(body)) | flag
→ 两个 chunk：inline(外层头) ++ pooled(body)
```

外层帧头在 inline chunk 里，body 缓冲**不需要**预留头部占位——初稿「占 4 字节
再回填」的技巧随之作废。`Overhead() > 0` 时**必须先算长度、拼出帧头、再 seal**
（帧头是 AAD），见 [02](02-wire-protocol.md#与-aead-的关系)。

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
        按实际写出的字节数退还预算；更新 lastProgress（stall LRU 移尾）
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
  reservedWire + charge(这条消息) > MaxBuffer
    → 拒绝入队，返回 ErrSendQueueFull       ← 消息没进队列，帧流没有洞
    → gate 不关连接

写侧（卡死检测）
  stage 2 非空，且距上一次「成功写出至少一个字节」超过 StallTimeout
    → 关闭连接，OnClose(ErrBackpressure)
```

以及一条软线：`reservedWire > HighWater` 时 `Writable()` 返回 false，让业务主动降级。
`charge` 与两段退还的完整口径见 [06 的预算模型](06-connection-state-machine.md#预算模型)。

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

**这个判据只抓「零进度」，有一个已知盲区**：一个每 29 秒读走 1 字节的对端可以
永久重置计时器，同时占着 `MaxBuffer`。完整的慢客户端保护还需要「最老消息年龄」
或「持续超过 `HighWater` 的时长」这类判据——v1 不加第三个旋钮，把盲区写在这里，
并靠 `Limits.MaxOutboundBytes` 兜住它的总代价：单条连接至多占 `MaxBuffer`，
全体加起来不越过全局预算。

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

**WS 封帧发生在编码时，不是写时。** 这是第二轮评审后的关键修正——「写时插头 +
部分写 + 暂存复用」三者组合会重发一个不同的 WS 帧头，线路直接错位：

```
flush:
    seg := encode(stage1)              // 一段 chunk 链，若干条完整 gate 帧
    if WebSocket:
        h := WS 帧头 chunk（长度 = seg 的总字节数）
        seg = h ++ seg                  // 帧头也是一个持久 chunk
    stage2.append(seg)
```

于是 stage 2 是一条**纯字节链**，`writev` 完全不需要知道 WebSocket 的存在，
部分写只是推进 `off`。三个连带结论：

- **一次编码的一整批 gate 帧装进一个 WS 二进制帧**：WS 帧头从「每条消息 2~10 字节」
  摊到「每批 2~10 字节」。
- **每个 WS 二进制帧一定结束在 gate 帧边界上**——它包的就是整数条 gate 帧。
  这让 [05 的「不做分片重组」论证](05-websocket.md#入站帧层)只依赖 fragment 等价。
- `IOV_MAX` 的分批发生在 `writev` 层，**不影响 WS 分帧**：一个 WS 帧跨多次
  `writev` 发出去完全正常。

细节见 [05-websocket](05-websocket.md#出站封帧)。

---

## 内存与零分配清单

稳态下（`-benchmem` 可验证）出站路径 **0 allocs/op**：

| 内存 | 来源 | 归还时机 |
| --- | --- | --- |
| `Send` 的复制目标 | **全局并发安全**分级池 | **该 chunk 完全写出**后——不压缩不加密不合包时它就是线路字节 |
| 压缩输出 | 每 loop 分级池 | 该 chunk 写完出链 |
| AEAD 的 `dst` | 每 loop 分级池 | 同上 |
| compound body | 每 loop 分级池 | 同上 |
| 帧头（gate / WS） | **chunk 节点内联** | 随 chunk 出链 |
| iovec 数组 | **每 loop 一份**的暂存对象 | 一次写调用结束 |
| chunk 节点 | 每 loop 的 slab | 该 chunk 写完 |
| stage 1 队列 `[]stage1Item` | 每 loop 的池 | flush 完成 |
| zstd 编码器 | **每 loop 一个**，concurrency=1 | 常驻 |

跨调用的暂存空间**一律按事件循环池化，不挂成连接级字段**——这是 10 万连接下的
硬要求，理由见 [01 的内存预算](01-server-api.md#按事件循环增长的内存不随连接数增长)。
**唯一的例外**是 `Send` / `SendFunc` 的复制目标：入队可能发生在任意 goroutine 上，
这块内存必须来自**并发安全的全局分级池**（在别的 goroutine 上取、在 loop 上放），
见 [06 的所有权表](06-connection-state-machine.md#字节所有权表)。

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
| O5 | `MaxBuffer` / `HighWater` / `Writable()` 比较的都是 `reservedWire`（保守上界、两段退还），一处记账 |
| O6 | 一切计量用 `frameSize`（payload + header），不用 `len(payload)` |
| O7 | compound body 的字节数、`MaxCluster` 的比较口径、外层帧头的长度值是同一个数 |
| O8 | 写失败之后立刻停止投递；先关 outbound，再关连接 |
| O9 | `SendFrame` 失败时不 retain；调用方不需要为失败路径配平 |
| O10 | flush 幂等：队列空即返回 |
| O11 | 连接不可写时不可能有消息入队（见 [05](05-websocket.md#握手与可写时机)） |
| O12 | stage 2 的每个字节都是最终线路字节：帧头内联在 chunk，WS 封帧在编码时 |
| O13 | 一个 compound 分组绝不跨 cipher barrier |
| O14 | dirty 节点的发布与 `armed` 置位在同一临界区；`inLoop` 由 `defer` 清 |

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

### O-D5. 一批 gate 帧装进一个 WS 帧，且封帧发生在编码时

WS 帧头从「每条消息」摊到「每批」。gate 帧自定界，所以外层一个 WS 帧就够。
上一版已经这样做了；这一版把**时机**修正为编码时——初稿放在 `writev` 时、
帧头存每-loop 暂存，与部分写组合会重发一个不同的 WS 帧头（第二轮评审的 P0），
见[与 WebSocket 的接口](#与-websocket-的接口)。

### O-D6. 跨线程路径永不触碰 fd

上一版为「陈旧连接对象 → 已被回收甚至已被新连接复用的 fd」这一类缺陷写了相当多的
状态判断（`stateClosed`、`writable()`、`closing`）。

自建 reactor 之后这件事变成结构性的：**跨 goroutine 的 `Send` 只做两件事——入队、
标脏——两者都不碰 fd**。所有 syscall 都发生在事件循环线程上，而 fd 的关闭也发生在
那里（延迟到迭代末尾），两者天然互斥。

`writable()` 这道闸仍然保留，但它的职责从「防止写到别人的 fd」降级成
「不可写时不要白白编码」。
