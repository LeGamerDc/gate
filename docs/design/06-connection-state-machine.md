# 单连接状态机与字节所有权

这份文档是 01–05 的**唯一事实来源**。凡与它冲突的表述，以它为准（勘误见[最后一节](#对-0105-的勘误)）。

## 为什么需要它

01–05 各自证明了一小段正确性：03 证明了「in-loop 抑制唤醒不会漏 flush」，
04 证明了「跨线程路径碰不到 fd」，05 证明了「不做分片重组与重组等价」。
每一段单独看都成立，**组合起来却不成立**——因为它们隐含地假设了对方的状态，
而没人把这些状态写在一起。

第二轮独立评审把这件事挑明了。几个最典型的组合失效：

| 现象 | 根因 |
| --- | --- |
| 部分写之后重新生成 WS 帧头，线路直接错位 | 「哪些字节是最终线路字节」没有被建模；03 说帧头在每-loop 暂存区，05 说在持久 chunk 上 |
| `Send(A); SetCipher(k); Send(B)` 让 A 也用新密钥 | 加密发生在 flush，而 `SetCipher` 只约束了「在哪个线程调用」，没有在队列里创造边界 |
| `SendFunc` 的 `fill` 期间 `Close` 返回，消息晚提交 | 「预留预算」和「线性化」被当成了同一个点 |
| 1MB 的帧收到 64KB 之后再也读不进来 | 读缓冲的所有权模型只考虑了「小帧原地解析」 |
| `resume` 从别的 goroutine 直接改 poller 注册 | 01 说 `resume` 可从任意 goroutine 调用，04 说所有 syscall 在 loop 上——两条都写了，没人对账 |

所以本文只做两件事：**一张状态机**和**一张字节所有权表**。其余的规则都从这两张表推导。

---

## 目录

- [状态机](#状态机)
- [字节所有权表](#字节所有权表)
- [出站的精确模型](#出站的精确模型)
- [入站的精确模型](#入站的精确模型)
- [预算模型](#预算模型)
- [Cipher epoch](#cipher-epoch)
- [截止时刻](#截止时刻)
- [线性化点总表](#线性化点总表)
- [公平性预算](#公平性预算)
- [Conn 的壳与核](#conn-的壳与核)
- [对 01–05 的勘误](#对-0105-的勘误)
- [故障注入要求](#故障注入要求)

---

## 状态机

一个连接有**一个主状态**和**三个正交子状态**。把它们拍平成一个状态机会组合爆炸，
所以显式分开，并用一张表规定哪些操作在哪些状态下合法。

### 主状态

```
                    ┌──────────┐
   accept ─────────▶│ Accepted │
                    └────┬─────┘
                         │ 注册到 poller
          ┌──────────────┼──────────────┐
          │ Proxy != Off │              │ 裸 TCP 且 Proxy == Off
          ▼              │              │
    ┌──────────┐         │              │
    │  Proxy   │─────────┤              │
    └──────────┘         │ WS           │
                         ▼              │
                  ┌──────────────┐      │
                  │ Handshaking  │      │
                  └──────┬───────┘      │
                         │ 101 已完整排入出站链  │
                         └──────┬───────┘
                                ▼
                          ┌──────────┐
                          │   Open   │◀─── 唯一会触发业务回调的状态
                          └────┬─────┘
                               │ 任何关闭原因
                               ▼
                         ┌───────────┐
                         │ Draining  │  尽力排空出站，最长 CloseLinger
                         └─────┬─────┘
                               │ 排空 / 超时 / 写失败
                               ▼
                         ┌───────────┐
                         │ Detached  │  fd 已关，loop 资源已归还
                         └─────┬─────┘   OnClose 尚未调用
                               │ 串行域空闲（无在途 AsyncDo）
                               ▼
                          ┌──────────┐
                          │  Closed  │  OnClose 已调用，只剩壳
                          └──────────┘
```

| 状态 | fd | 在 poller 里 | 业务可见 | 出站队列 |
| --- | --- | --- | --- | --- |
| `Accepted` | 有 | 否 | 否 | 空 |
| `Proxy` | 有 | 读 | 否 | 空 |
| `Handshaking` | 有 | 读（+写，若 101 未写完） | 否 | 只有 101 响应 |
| `Open` | 有 | 读（+写，若有积压） | **是** | 可用 |
| `Draining` | 有 | **只有写** | 否 | 只出不进 |
| `Detached` | **已关** | 否 | 否 | 已丢弃 |
| `Closed` | — | — | 否（`OnClose` 已返回） | — |

**`Open` 是唯一会触发 `OnMessage` / `Post` / `AsyncDo` 的状态。**
`OnOpen` 在进入 `Open` 的那一刻调用，`OnClose` 在 `Detached → Closed` 时调用。

`Draining → Detached` 立刻拆掉 fd 和全部 loop 资源，**不等任何业务代码**；
`Detached → Closed` 才等串行域。这个拆分是 `AsyncDo` 串行域保证的实现基础，
也保证了 10 万连接下 fd 与内存一刻都不会被业务代码拖住。

### 正交子状态

只在 `Open` 下有意义：

| 子状态 | 取值 | 影响 |
| --- | --- | --- |
| 读闸 | `Reading` / `Paused(depth≥1)` | `Paused` 时不注册读兴趣、不投递消息、不执行 `Post` |
| 写态 | `WriteIdle` / `WriteBlocked` | `WriteBlocked` 时注册写兴趣，并进入 stall 计时 |
| 异步 | `AsyncIdle` / `AsyncBusy` | `AsyncBusy` 时拒绝新的 `AsyncDo`（`ErrAsyncBusy`） |

进入 `Draining` 时：读闸的 `Paused` 状态保留（因为它决定 `OnClose` 什么时候能调），
写态继续工作直到排空或超时，异步状态保留。

### 操作合法性表

行是操作，列是主状态。`✓` 正常执行，`✗` 返回 `ErrConnClosed`，`—` 不可能发生。

| 操作 | Accepted | Proxy | Handshaking | Open | Draining | Detached | Closed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Send` / `SendAlone` / `SendFrame` | — | — | —¹ | ✓ | ✗ | ✗ | ✗ |
| `SendFunc` | — | — | —¹ | ✓ | ✗ | ✗ | ✗ |
| `Post` | — | — | —¹ | ✓（暂停时排队） | 丢弃 | 丢弃 | 丢弃 |
| `AsyncDo` | — | — | — | ✓ / `ErrAsyncBusy` | ✗ | ✗ | ✗ |
| `Pause` | — | — | — | ✓ | ✗ | ✗ | ✗ |
| `resume` | — | — | — | ✓ | ✓² | ✓² | no-op |
| `Close` | — | ✓ | ✓ | ✓ | no-op | no-op | no-op |
| `SetCipher` | — | — | — | ✓ | ✗ | ✗ | ✗ |
| `Writable` | — | — | — | ✓ | false | false | false |

¹ 业务在 `OnOpen` 之前拿不到 `*Conn`，所以这些调用**在结构上不可能发生**。
这条不变式消掉了「不可写时消息怎么办」的一整块机制，见 [05 的 W-D3](05-websocket.md#w-d3-onopen-在-101-之后因此不可写时不可能有消息入队)。

² `resume` 在 `Draining` / `Detached` 下仍然有效——它可能正是解锁 `Detached → Closed`
的那一步。

### 迁移触发表

| 起点 | 事件 | 终点 | 动作 |
| --- | --- | --- | --- |
| `Accepted` | 注册成功 | `Proxy` / `Handshaking` / `Open` | 按配置分支；设握手截止时刻 |
| `Proxy` | PROXY 头解析完成 | `Handshaking` / `Open` | 更新 `Remote()`；**剩余字节留在 carry** |
| `Proxy` | 头非法 / `ProxyRequired` 但没有 | `Draining` | 不发任何响应 |
| `Handshaking` | `OnUpgrade` 通过，101 已完整入出站链 | `Open` | `OnOpen`；握手截止时刻清除 |
| `Handshaking` | `OnUpgrade` 拒绝 | `Draining` | 出站链里放 HTTP 错误响应 |
| `Open` | `OnMessage` 返回 error / `Close(r)` / 空闲 / stall / 协议错误 / panic / `Shutdown` | `Draining` | 记录 `reason`；停止入站投递；停止接受新消息入队 |
| `Draining` | 出站排空 | `Detached` | 见下 |
| `Draining` | `CloseLinger` 到期 / 写失败 | `Detached` | 丢弃剩余出站 |
| `Detached` | 无在途 `AsyncDo` | `Closed` | `OnClose(c, reason)`；core 置 nil |

`Draining → Detached` 的动作序列（**顺序是规格**）：

```
1. 若 WS 且 reason 属于「可以发 close 帧」的一类 → 已在进入 Draining 时排进出站链
2. poller.del(fd) → close(fd)
3. 丢弃 stage1 / stage2，归还全部池内存，退还预算
4. 从五条 LRU 上摘除；槽位 conn 置 nil；槽位 gen++
5. 若 AsyncBusy → 挂进 loop 的 pendingClose 链，等 resume
   否则 → 直接进入 Detached → Closed
```

---

## 字节所有权表

每一块内存：从哪来、活多久、谁负责归还、会不会跨 loop。
**这张表是判断「有没有 use-after-free / 泄漏 / 跨线程访问」的唯一依据。**

### 入站

| 缓冲 | 来源 | 生命周期 | 归还点 | 跨 loop |
| --- | --- | --- | --- | --- |
| `rbuf`（每 loop 64KB） | loop 常驻 | 永久 | 不归还 | 否 |
| `carry`（入站残片） | **全局并发安全分级池** | 跨事件，直到拼齐一帧 | 拼齐或连接销毁 | 否（只在 loop 上取放） |
| `frameBuf`（大帧目标缓冲） | 全局分级池 | 从「知道长度」到「投递完成」 | 投递后 | 否 |
| 解密输出 | 原地（`Overhead()==0`）或分级池 | `deliver` 期间 | `deliver` 返回 | 否 |
| 解压输出 | 分级池 | `deliver` 期间 | `deliver` 返回 | 否 |
| 交给 `OnMessage` 的 `msg` | 上述之一的**切片**（三索引封 cap） | **仅本次调用** | 不归还（借用） | 否 |

`msg` 是借用，这条约束一路从 codec 传导到公共 API：`OnMessage` 返回后不得再持有它。

### 出站

| 缓冲 | 来源 | 生命周期 | 归还点 | 跨 loop |
| --- | --- | --- | --- | --- |
| `Send` 的复制目标 | **全局并发安全分级池**（可能在任意 goroutine 上取） | 入队 → **该字节真正写完** | 见下 | **是**（在别的 goroutine 上取，在 loop 上放） |
| `SendFunc` 的 reservation | 同上 | reserve → commit 后同上 / abort 时立即 | 同上或 abort | 是 |
| `Frame` 的字节 | GC | 直到最后一个引用消失 | GC | **是**（多连接共享） |
| compound body | 分级池（loop 上取） | 编码 → 写完 | chunk 出链 | 否 |
| 压缩输出 | 分级池 | 编码 → 写完 | chunk 出链 | 否 |
| AEAD 输出 | 原地或分级池 | 编码 → 写完 | chunk 出链 | 否 |
| gate 帧头 | **chunk 节点内联** | 与 chunk 同寿 | 随 chunk | 否 |
| WS 帧头 | **chunk 节点内联** | 与 chunk 同寿 | 随 chunk | 否 |
| chunk 节点 | 每 loop slab | 入链 → 写完出链 | 出链 | 否 |
| iovec 数组 | 每 loop 暂存 | 一次 `writev` | 调用返回 | 否 |

三条要点：

**① `Send` 的复制目标可能直接成为线路字节。** 一条不压缩、不加密、不合包的消息，
它的 payload 从入队到写出线路是同一块内存。所以它的归还点是**「该 chunk 完全写出去」**，
不是「编码完成」。03 里写的「编码完成后归还」是错的。

**② 帧头必须内联在 chunk 节点里。** 帧头如果放在每-loop 暂存区，一次部分写之后
那块暂存已经被别人用了，续写时只能重新生成——而重新生成的 WS 帧头会让线路错位。
内联意味着帧头和 payload 一样是持久的线路字节。

**③ 只有两类内存跨 loop：`Send` 的复制目标和 `Frame`。**
前者必须走**并发安全的全局分级池**（任意 goroutine 可能调 `Send`），
后者由 GC 管。其余全部是每 loop 的 slab，取放都在同一个线程上，不需要任何原子操作。

> 04 里「所有池每 loop 一份、无需同步」的说法只对后者成立。`Send` 的复制路径必须是
> 并发安全的池——这是「`Send` 可从任意 goroutine 调用」的直接后果。

---

## 出站的精确模型

### chunk：线路字节的唯一载体

```go
type chunkKind uint8
const (
	chunkInline   chunkKind = iota // 帧头（gate 2~4 字节 / WS 2~10 字节）
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

不变式：

- **stage 2 里的每一个字节都是最终线路字节。** 编码在入链之前完成，入链之后不再变换。
- **`off` 是唯一的进度表示。** 部分写只推进 `off`，永不重新生成任何字节。
- **严格 FIFO，只在尾部追加。**

### WS 封帧发生在编码时，不是写时

```
flush:
    seg := encode(stage1)              // 一段 chunk 链，若干条完整 gate 帧
    if WebSocket:
        h := WS 帧头 chunk（长度 = seg 的总字节数）
        seg = h ++ seg                  // 帧头也是一个持久 chunk
    stage2.append(seg)
```

这样 stage 2 是一条**纯字节链**，`writev` 完全不需要知道 WebSocket 的存在，
部分写也只是推进 `off`。

两个连带结论：

- **每个 WS 二进制帧一定结束在 gate 帧边界上。** 因为它包的就是整数条 gate 帧。
  这比「允许一条 gate 帧跨两个 WS message」对第三方客户端友好得多，
  也让 05 的「不做分片重组」论证只依赖 fragment 等价，不需要更强的流语义假设。
- `IOV_MAX` 的分批发生在 `writev` 层，**不再影响 WS 分帧**。一个 WS 帧可以跨多次
  `writev` 发出去，这完全正常。

### 写循环

```
write():
    for stage2 非空:
        vec := 从链头收集最多 IOV_MAX 个 iovec（首个从 off 起）
        n, err := writev(fd, vec)
        switch:
        case err == EAGAIN:            注册写兴趣；进入 WriteBlocked；返回
        case err != nil:               写失败路径（丢弃队列 → Draining）
        }
        按 n 推进：链头 chunk 的 off += ...，写满的出链并归还（见所有权表）
        退还预算（按实际写出的字节数）
        更新 lastProgress（stall LRU 移到尾部）
        if n < 本次请求总字节: 注册写兴趣；进入 WriteBlocked；返回
    摘掉写兴趣；进入 WriteIdle
```

---

## 入站的精确模型

固定 64KB 的读缓冲装不下一条 1MB 的帧，所以解析必须是**增量状态机**，
而不是「攒够一整帧再解析」。

```
每连接的入站状态：
    phase   ∈ { needHeader, needBody }
    hdrBuf  [4]byte, hdrLen        // needHeader 时累积
    body    []byte                 // needBody 且大帧时的目标缓冲（分级池）
    bodyGot int
```

```
onReadable():
    if body != nil:                       ← 大帧：直接读进目标缓冲的剩余区
        n := read(fd, body[bodyGot:])
        bodyGot += n
        if bodyGot == len(body): deliver(body) 并归还
        return

    n := read(fd, rbuf[carryLen:])        ← 小帧快路径：拼上残片后原地解析
    data := rbuf[:carryLen+n]
    loop:
        解析帧头 → 不足 4 字节则把 data 存回 carry，返回
        校验（见 02 的校验规格）
        if 整帧都在 data 里:  原地 deliver，data 前移
        else if 帧长 <= rbuf 容量:  把 data 存回 carry，返回     ← 下一轮就够了
        else:                                                  ← 大帧
            body = 分级池取 size 字节
            copy(body, data 里已有的 payload 部分)
            bodyGot = 已有长度；data 清空；返回
```

要点：

- **小帧（≤ rbuf 容量）走共享 rbuf**，稳态下每连接入站内存为 0。
- **大帧一次性申请到位**，后续 `read` 直接填剩余区，没有任何重复拷贝，
  也不存在 O(n²)。
- `carry` 的上界因此是 **rbuf 容量**（64KB），而不是 `MaxMessage`。
  `Limits.MaxPending` 约束的是「暂停时已读出但未投递」的量，两者不是一回事。

### WebSocket 的入站是流式的

同理，WS 解帧**不等一个完整的 WS 帧**——一个 WS 帧可以装下一整批 gate 帧，
远大于单条 `MaxMessage`。

```
每连接的 WS 状态（全部是标量，没有缓冲）：
    remaining   int64    // 当前 WS 帧还剩多少 payload
    mask        [4]byte
    maskOff     uint8    // 跨读事件的掩码偏移
    fragmented  bool     // 是否处在一条分片消息中间
    hdrBuf      [14]byte, hdrLen
```

WS 级残片**最多 14 字节**（一个最长的帧头），不是一个完整帧。
payload 一到就地去掩码并喂给 gate 帧循环。

由此：

- `Limits.MaxMessage` **只约束 gate 帧的 payload**，不约束 WS 帧长度。
- WS 帧长度只需要一个宽松的上界（防止 64 位长度字段被滥用），
  真正的内存约束由 gate 帧层和 `MaxPending` 提供。

---

## 预算模型

三层，各自解决不同的问题：

| 层 | 名字 | 计量 | 目的 |
| --- | --- | --- | --- |
| 连接 | `reservedWire` | **保守上界**的线路字节 | 准入控制（`ErrSendQueueFull`）、`Writable()` |
| loop | 配额 | 该 loop 上所有连接的 `reservedWire` 之和 | 避免全局计数成为争抢点 |
| server | `MaxOutboundBytes` | 各 loop 配额之和（**近似**） | 挡住分布式慢客户端 |

### 保守上界

入队时不知道压缩能省多少，所以按**最坏情况**扣：

```
charge(payload) = frameSize(len(payload) + cipher.Overhead())
```

编码完成后知道了真实线路长度，**退还差额**；写出去之后，按实际写出的字节数继续退还。
于是 `reservedWire` 始终 ≥ 真实占用，准入控制不会因为「压缩后其实放得下」而失效，
也不会因为「AEAD 撑大了」而超限。

`SendFrame` 按**该帧的线路长度**计入**每一条**连接：广播的 backpressure 关心的是
「这条连接要传多少字节」，不是「进程里存了几份」。内存那一份由 GC 管，不进预算。

### 全局预算是近似的

精确的全局计数需要一条被所有发送核心争抢的 cache line，这与 shared-nothing 直接冲突。
所以采用**配额租约**：每个 loop 从 server 预算里租一段，用完再申请、富余时归还。

**这是一个刻意的近似**：瞬时总量可能超出 `MaxOutboundBytes` 一个租约粒度。
文档必须这么写，而不是一边承诺精确上限、一边宣称热路径无同步。

### 编码期的瞬时峰值

编码一条消息时，原文、压缩输出、AEAD 输出可能同时存活。峰值约为
`2 × 单条最大消息`，且**只在一条连接的编码期间**存在（编码在 loop 上串行）。
所以峰值是 `Loops × 2 × MaxMessage`，与连接数无关，不计入连接预算，
但要写进内存预算表。

---

## Cipher epoch

### 出站：队列里的 barrier

stage 1 是一个 tagged union，而不是单一的消息数组：

```go
type stage1Item struct {
	kind   itemKind // itemMessage | itemFrame | itemCipherBarrier
	// itemMessage
	data                    []byte
	maskPermit, maskAlready byte
	// itemFrame
	frame *Frame
	// itemCipherBarrier
	cipher Cipher
}
```

`SetCipher(k)` 往 stage 1 尾部追加一个 barrier。编码器按顺序走：遇到 barrier 就切换
当前使用的 cipher，**并结束当前的 compound 分组**（一个合并帧不能跨密钥边界）。

于是 `Send(A); SetCipher(k); Send(B)` 的语义是精确的：A 用旧密钥，B 用新密钥。
「`SetCipher` 在事件循环上调用」这一条**并不足以**创造这个边界——加密发生在 flush，
不是入队时。

### 入站：串行域内的即时切换

入站的 cipher 是一个普通字段，只在串行域内被读写（解密发生在 loop 上，
`SetCipher` 也必须在串行域内调用）。切换在**下一个被解析的帧**生效。

因此 `SetCipher` 的完整要求是：**只能在串行域内调用**（回调、`Post`、`AsyncDo` 的函数体）。
它同时做两件事：立刻换入站 cipher，往出站队列追加一个 barrier。

### AAD：完整帧头进入认证范围

```go
type Cipher interface {
	Overhead() int
	Seal(dst, plaintext, aad []byte) []byte
	Open(dst, ciphertext, aad []byte) ([]byte, error)
}
```

`aad` = **该帧最终的 2 或 4 字节帧头**（含 `m`/`z`/`c`/`e` 与长度）。

没有循环依赖：`Overhead()` 是常量、`Seal` 的输出长度精确，所以编码时可以先算出最终
线路长度、拼出帧头、再把帧头作为 AAD 交给 `Seal`。

**为什么必须做。** 02 里那份翻位分析漏了 `1→0` 方向，而那两条都是**静默**的：

| 翻转 | 后果 | 静默？ |
| --- | --- | --- |
| `z` 0→1 | 解压明文失败 → 关连接 | 否 |
| **`z` 1→0** | **不解压，把压缩字节当业务明文投递** | **是** |
| `c` 0→1 | 切分失败 → 关连接 | 否 |
| **`c` 1→0** | **不切分，把整个 compound body 当一条消息投递** | **是** |
| `e` 1→0 | 不解密，把密文当明文投递 | 是 |
| 长度 | 帧边界错位 → 关连接 | 否 |

服务端因为 `allow = e` 恰好躲过了 `z`/`c` 那两条（它们根本不允许为 1），
但**下行方向、以及任何第三方客户端实现都中招**。AAD 把这一整类关掉。

### 两条补充规则

- **`e = 1` 但连接没有配 Cipher ⇒ 协议违规**（`ErrCipherUnavailable`）。
  02 的校验规格只写了反方向，漏了这一条，会走到一个 `Open` 为 nil 的路径上。
- **收发必须使用各自独立的 nonce 序列**，实现内部保持两个计数器。
  两端的收发交错顺序天然不同，共用计数器会直接解密失败；
  若两个方向还共用 key，撞上同一 nonce 就是 AEAD 的灾难性失效。

---

## 截止时刻

**五条每 loop 的侵入式 LRU + 一个 loop 级 backoff**，全部 O(1) 更新、O(k) 过期。
不需要时间堆，也不需要 `timerfd` / `EVFILT_TIMER`——每轮取五者最近的那个当作
`wait` 的 timeout。

| 类别 | 链表按什么排序 | 何时入链 | 何时移到尾部 |
| --- | --- | --- | --- |
| idle | 最后交付完整消息的时刻 | `Open` 且 `Reading` | 每次交付消息 |
| handshake | 进入 `Proxy` / `Handshaking` 的时刻 | 进入时 | 不移动 |
| pause | 进入 `Paused` 的时刻 | `depth` 0→1 | 不移动 |
| **stall** | **`lastProgress`（最后写出 ≥1 字节的时刻）** | 进入 `WriteBlocked` | 每次写出字节 |
| **linger** | 进入 `Draining` 的时刻 | 进入时 | 不移动 |
| EMFILE backoff | —（loop 级单个时刻） | 遇到 `EMFILE` | — |

每条链表的排序键都是**单调递增**的（因为同一个 loop 上后发生的事件时刻一定不早于
先发生的），所以从头扫到第一个未超时的就能停。

`stall` 之所以能用 LRU 而不是最小堆，正是因为 `lastProgress` 满足这个单调性——
04 里说它「复用写兴趣集合」是错的，写兴趣集合没有时间序。

### `Draining` 期间不要空转

进入 `Draining` 时尝试写一次。**之后只在两种时机再动**：收到可写事件，
或 `CloseLinger` 到期。**不要每轮迭代都重试**——那会在别的连接持续活跃时反复撞
`EAGAIN`，形成 syscall 风暴。

---

## 线性化点总表

| 操作 | 线性化点 | 谁能观察到 |
| --- | --- | --- |
| `Send` / `SendAlone` / `SendFrame` | 取得 outbound 锁、扣预算、接进 stage 1 | 后续任何 `take()` |
| **`SendFunc`** | **commit 阶段**取得锁并接进 stage 1 | 同上 |
| `SetCipher` | 同上（追加 barrier）；入站字段在串行域内即时生效 | 编码器 |
| `Close` | 取得 outbound 锁并置 `closing` | 后续 `Send` 一律 `ErrConnClosed` |
| `Post` | 推进 loop 的 MPSC | loop 的 drain |
| `Pause` | 串行域内递增 `depth` | 投递循环 |
| `resume` | `once` 生效并推进控制项 | loop |

唯一需要的不变式：**先线性化的 `Send`，一定会被后续的排空看到。**
把「扣预算 + 入队」和「置 `closing`」放进同一把锁的临界区，这个窗口在结构上不存在。

### `SendFunc` 的两阶段协议

```
1. 【reserve】加锁：检查状态与预算，扣 charge(n)，拿一块 n 字节的池内存
             ——**这不是线性化点**，消息还没入队
   解锁
2. 【fill】  在锁外调用业务的 fill(b)              ← 绝不持锁跑业务代码
3. 【commit】重新加锁：
             若 closing → 归还内存、退还预算、返回 ErrConnClosed
             否则 → 接进 stage 1，**此刻才线性化**，按实际长度退还差额
```

第 2 步必须在锁外：持锁跑业务回调会把所有 `Send` / `Close` 挡在锁上，
而且 `fill` 内部若重入 `Send` 就是死锁。

`fill` 的返回值处理：

| `fill` 返回 | 处理 |
| --- | --- |
| `(k, nil)`，`0 <= k <= n` | 提交前 `k` 字节 |
| `k < 0` 或 `k > n` | 视为业务 bug：归还内存、退还预算、返回 `ErrInvalidLength` |
| `(_, err)` | 归还内存、退还预算、返回该 error（消息未入队） |
| panic | 恢复屏障接住：归还内存、退还预算，然后按 `OnMessage` panic 处理 |
| 永不返回 | gate 不介入。它占着一块预留内存，等同于业务泄漏——`MaxPause` 管不到这里，靠业务自己的超时 |

`Close` **不等待** `fill`。

### `Post` 必须同时检查 `closing`

只检查槽位 generation 是不够的：`Close` 已经返回、连接已经 `Draining`，
但槽位的 gen 要到 `Detached` 才递增。这中间 `Post` 的闭包会通过 generation 校验
并修改业务状态，违反「已关闭连接上的 `Post` 不执行」这条承诺。

**执行前同时检查 generation 和主状态。**

### dirty 链的发布必须在临界区内

03 的 in-loop 论证只证明了「队列与 `inLoop` 在同一把锁下不会漏看」，
没有证明 `armed` / `postDirty` 的生命周期。生产者在锁内置 `armed = true`、
在锁外 `postDirty`，中间事件循环可能已经 `take()` 并清了 `armed`，
于是另一个 `Send` 也会 post 一次——同一个连接被挂进 dirty 链两次，
而 dirty 节点是内嵌在 `Conn` 里的侵入式节点，重复挂链会破坏链表结构。

**规则：dirty 节点的发布与 `armed` 的置位在同一个临界区内完成**（用 CAS 抢占
节点的「在链中」标志），或者给每次投递带一个单调 ticket 让重复投递可被识别并丢弃。

以及：任何把 `inLoop` 置为 `true` 的路径都必须在批边界用 `defer` 清掉，
否则一次 panic 会让这条连接的唤醒被永久抑制。

---

## 公平性预算

一轮迭代里每一样能无限增长的东西都要有预算。没有预算的项都能让 timeout 和其他连接
长时间得不到处理。

| 项 | 预算 | 超出后 |
| --- | --- | --- |
| accept | 64 个/轮 | 留到下一轮（水平触发会再报） |
| 单连接读 syscall | 4 次或 256KB | 留到下一轮 |
| **单连接投递消息数** | 1024 条 | 重新标脏，下一轮继续 |
| **单连接写字节 / syscall** | 1MB 或 8 次 `writev` | 保持写兴趣，下一轮继续 |
| **inbox 项数** | 4096 | 留在 MPSC 里，下一轮继续 |
| **dirty 连接数** | 全部处理，但每条各自受写预算约束 | — |
| **`Post` 闭包数** | 1024 | 留到下一轮 |

有任何一项因预算被截断时，**本轮的 `wait` timeout 取 0**，
让下一轮立刻继续，而不是等到下一个事件或截止时刻。

64KB 的读缓冲里可以放 32768 条空消息，一次可写事件可以排空 1MB，
一个生产者可以一次投递十万个 dirty 连接——这些都不是理论数字。

---

## `Conn` 的壳与核

`*Conn` 永不复用（[01 的 D17](01-server-api.md#d17-conns-永不复用)），
但如果它直接持有 loop / server 的指针，一个被业务注册表泄漏的旧 `Conn`
会把整个 loop、`Handler`、slab、出站队列甚至整个 `Server` 一起留在 GC 根上——
泄漏的就不是一个小句柄，而是几百 MB。

```go
type Conn[S any] struct {
	State S
	id    uint64
	core  atomic.Pointer[connCore]   // Detached 时置 nil
}
```

- 所有方法先 `core.Load()`，为 nil 就返回 `ErrConnClosed` 或 no-op。
- `Draining → Detached` 时**先**把 `core` 换成 nil，**再**归还 loop 资源。
- 于是一个泄漏的 `Conn` 只钉住它自己的壳和 `State`。

`State` 是否清零由业务决定：gate 不动它（业务可能在 `OnClose` 之后从自己的清理协程里
读快照），但注册表必须在 `OnClose` 里删干净，否则泄漏一个 `Conn` 就泄漏一个 `State`。

---

## 对 01–05 的勘误

以本文为准。下面每一条都是前面文档里**写错或写漏**的地方。

### 出站

| 文档 | 原表述 | 更正 |
| --- | --- | --- |
| 03 | `chunk` 只有 `buf` / `off`，帧头在每-loop 暂存区 | 帧头**内联在 chunk 里**；stage 2 的每个字节都是持久线路字节 |
| 03 / 05 | WS 帧头在 `writev` 时由 `framer` 临时插入 | **WS 封帧在编码时完成**，帧头也是一个 chunk |
| 03 | `Send` 的复制目标「编码完成后归还」 | 归还点是**该 chunk 完全写出去** |
| 03 | stage 1 是 `[]sendMsg` | 是 tagged union，含 `itemCipherBarrier` |
| 03 | 「一处记账」未定义口径 | 见[预算模型](#预算模型)：入队按保守上界，编码与写出时分两次退还 |
| 03 | in-loop 论证 | 补 dirty 发布必须在临界区内、`inLoop` 必须 `defer` 清 |
| 01 | `MaxOutboundBytes` 默认不限 | 必须有有限默认值；且它是**近似**的（配额租约） |

### 入站

| 文档 | 原表述 | 更正 |
| --- | --- | --- |
| 04 | 残片拷回 64KB `rbuf` 头部再继续读 | 大帧一次性申请目标缓冲直读；`carry` 上界是 rbuf 容量而非 `MaxMessage` |
| 05 | 等一个完整 WS 帧再 emit | **流式**：WS 级残片最多 14 字节 |
| 05 | WS payload 上限 = `MaxMessage` | `MaxMessage` 只约束 gate payload；WS 帧长另设宽松上界 |

### 加密

| 文档 | 原表述 | 更正 |
| --- | --- | --- |
| 01 | `Seal(dst, plaintext)` / `Open(dst, ciphertext)` | 加 `aad` 参数，AAD = 完整帧头 |
| 02 | 翻位分析「只有 `e` 1→0 是静默的」 | **错**：`z` 1→0 与 `c` 1→0 同样静默，见[上文](#aad完整帧头进入认证范围) |
| 02 | 「长度依赖 Seal，存在循环依赖，所以做不了 AAD」 | **错**：`Overhead()` 是常量、`Seal` 长度精确，可以先算长度再拼头 |
| 02 | 校验规格只检查「有 Cipher 但 `e = 0`」 | 补对称检查：`e = 1` 但没有 Cipher ⇒ `ErrCipherUnavailable` |
| 01 | `SetCipher` 在事件循环上调用即可保证顺序 | **不够**：加密在 flush 时发生，必须用队列里的 barrier |
| 01 | Cipher 「唯一没有恢复屏障的回调」 | 已改：屏障在批边界。相关旧段落要删干净 |

### 生命周期

| 文档 | 原表述 | 更正 |
| --- | --- | --- |
| 01 | `resume` 可从任意 goroutine 调用（且 04 直接 `mod(fd)`） | `resume` 用 `once` 保证幂等后**投递控制项**；`depth` / LRU / poller 只在 loop 上改 |
| 04 | 槽位按 fd 直接索引，`slot` 8 字节 | fd 只在**进程**内稠密，不在**每个 loop** 内稠密。用每 loop 的稠密 freelist；`slot{ptr,uint32}` 在 64 位上是 16 字节 |
| 04 | `token` 未给 `kind` 留位 | `token` 必须显式划分 `kind` / `slot` / `generation` 位段 |
| 04 | `Post` 执行前只校验 generation | 同时校验主状态：gen 到 `Detached` 才变，中间有窗口 |
| 04 | 回收阶段每轮重试 write | `Draining` 只在可写事件或 `CloseLinger` 到期时动 |
| 04 | 「所有池每 loop 一份、无需同步」 | `Send` 的复制目标必须走**并发安全的全局池** |
| 04 | 事件的 read / write 已合并 | kqueue 会为同一 fd 返回两个独立事件，必须按 token 聚合，并保证「先写后读」 |
| 04 | `Addr=":0"` + `SO_REUSEPORT` | 多个 listener 各自 bind 0 会拿到**不同端口**。第一个 listener 定端口，其余绑同一个 |
| 04 | `MaxConns` 满额时 | 仍需 `accept` 之后立即 `close`，否则 backlog 清不掉、水平触发空转 |
| 04 | `EMFILE` 下一轮恢复 listener | 要有**退避**，并同样处理 `ENFILE` / `ENOBUFS` / `ENOMEM` |
| 01 | PROXY protocol 只写在 API 层 | 状态机里补 `Proxy` 状态：`PROXY → (WS 握手) → Open`，与握手共用截止时刻 |

### 语义与措辞

| 文档 | 原表述 | 更正 |
| --- | --- | --- |
| 01 | 背压段仍写「硬线关连接」 | `MaxBuffer` 是准入线；关连接由 `StallTimeout` 触发 |
| 01 | 「回一条拒绝消息再关闭」**可靠** | 只保证排在关闭之前被尝试，不保证送达 |
| 01 | 协议错误直接关 TCP | 与 05 统一：WS 下发 1002 再关 |
| 01 | `StallTimeout` 作为慢客户端保护 | 它只抓「零进度」。每 29 秒写 1 字节就能永久重置——需要另加「最老消息年龄」或「持续 HighWater 时长」才算完整保护 |
| 05 | 握手大小检查 | 找到 `\r\n\r\n` 之后**仍要**检查 `requestEnd <= MaxHandshakeBytes`，否则一次大 read 就能绕过 |
| 05 | 「101 写进 socket」 | 非阻塞 socket 可能部分写。改成「101 已完整排在所有 WS 数据之前且不可被越过」 |
| 05 | close 帧按 reason 映射即可 | 需要独立的 WS 关闭状态机（`CLOSE_SENT` / `CLOSE_RECEIVED`）：握手期不能发 close 帧；收到 peer close 后要停止投递后续数据帧；半个 WS 帧写到一半时不能插入 close |
| 05 | 帧校验清单 | 补：扩展长度的规范编码（125 不得用 126 编码、65535 不得用 127）、64 位长度最高位必须为 0、close payload 长度不得为 1、close code 与 reason 的 UTF-8 校验 |
| 02 | 扩展空间「4096 个取值」 | 长度码是 4096 个，但连同 `z`/`c`/`e` 的 8 种组合，线路位型是 32768 个。且这些位不再表达长度，必须显式定义成「扩展头逃逸码 + 版本 + 类型 + 新长度」 |
| 01 | `PreEncrypted` 与双向 Cipher | 广播帧要求连接没有 Cipher，但客户端要解 `e=1` 的帧又必须配 Cipher——收发 Cipher 需要拆开配置，或把 `PreEncrypted` 定义成「业务完全自理，不复用 gate 的 `e` 标记」 |

---

## 故障注入要求

这份状态机的正确性**不能靠阅读证明**，必须靠故障注入覆盖。最低要求：

**① 部分写的全组合。** 一个可编程的 `writev`，能在**任意字节位置**返回部分成功：

- 半个 gate 帧头 / 半个 WS 帧头 / payload 中间 / 恰好一个 chunk 边界
- 连续多次部分写
- 部分写之后紧接 `EAGAIN`、紧接真正的错误、紧接 `Close`

断言：对端收到的字节流与「一次性全部写出」逐字节相同。

**② 部分读的全组合。** 同上，`read` 在任意位置截断：

- 半个帧头 / 半个 WS 帧头 / 大帧读到一半 / 掩码跨读事件

**③ 状态机的每条边。** 上面迁移触发表里的每一行至少一个用例，
特别是这几条难到达的：

- `Draining` 期间收到可写事件 → 排空 → `Detached`
- `Detached` 时仍有在途 `AsyncDo` → `resume` 后才 `OnClose`
- `Handshaking` 期间 `Close`
- `fill` panic / 返回越界长度 / 期间 `Close`
- `SetCipher` barrier 恰好落在一个 compound 分组中间

**④ 预算守恒。** 每个用例结束时断言：`reservedWire == 0`、
池的借出与归还计数相等、chunk slab 全部归还、loop 配额回到初值。

**⑤ 线性化。** 并发 `Send` / `Close` / `Post` / `SetCipher` 的随机交错，
断言：返回 `nil` 的 `Send` 要么上了线路、要么在关闭时被明确丢弃并退还了预算；
绝不出现「返回 nil 但内存泄漏」或「返回 error 但字节上了线路」。
