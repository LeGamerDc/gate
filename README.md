# gate

面向**海量长连接**（目标 10 万级并发）的 TCP / WebSocket 网关库。自建线路协议
与 epoll / kqueue 事件循环，不依赖任何网络框架。

它不定义业务协议，只把「连接管理、拆包、可选加密、发送侧合包与压缩」这几件
所有网关都要重做一遍的事收敛成一个可复用组件。

```go
type handler struct{}

func (handler) OnOpen(c *gate.Conn[*player]) (*player, error) { return &player{}, nil }
func (handler) OnMessage(c *gate.Conn[*player], msg []byte) error { return c.Send(msg) }
func (handler) OnClose(c *gate.Conn[*player], reason error) {}

err := gate.Run(ctx, gate.Options[*player]{
    Addr:     ":8081",
    Handler:  handler{},
    Outbound: gate.DefaultOutbound(),
})
```

业务只实现一个接口（三个方法）。`WebSocket` 字段非 nil 就是 WebSocket 模式，
业务代码一行都不用改。

## 它做什么

- **发送侧合批**：业务只做「入队」，真正的 `writev` 收敛到事件循环上批量执行，
  一次唤醒摊薄 N 条消息的系统调用开销。
- **先拼包再整体压缩**：100 条相似小消息拼成 30KB 再压一次，压缩比远高于压
  100 次（zstd 跨消息复用上下文）。可挂协议样本训出来的字典。
- **可选逐连接加密**：`Cipher` 接口，帧头进 AEAD 的认证范围。
- **预编码帧广播**：`NewFrame` 编码一次，`SendFrame` 分发给 N 条连接。
- **背压做成两条线**：准入控制（`Send` 报错，消息从未入队）与卡死检测
  （长时间一个字节都写不出去才关连接）。

稳态下入站与出站热路径都是 **0 allocs/op**（有硬断言守着，回归即失败）。

## 语义承诺

- **绝不静默丢消息**：要么送出，要么 `Send` 明确返回错误。
- **两种传输一份语义**：WebSocket 只是 gate 帧循环前面多一层字节变换，
  两者共用同一份投递代码——一致性是结构性的，不是靠测试对齐的。
- **业务 panic 不打死进程**：每个回调边界都有恢复屏障，爆炸半径收敛到单条连接。
- **`Close` 返回之后 `Send` 一定报错**。

## 状态

实现完整，测试包含双侧 fuzz（5 个目标）、状态机故障注入、真实 socket 的端到端
矩阵（含第三方 WebSocket 客户端互通）、`-race` 并发线性化与流失压力。
经过两轮独立评审。

平台：Linux（epoll，生产目标）与 macOS（kqueue，开发与测试）。

## 文档

[docs/design/](docs/design/) 是完整的设计文档，也是实现的规格来源：

| | |
| --- | --- |
| [01 服务端接口](docs/design/01-server-api.md) | 用户看到什么、怎么用、为什么这样设计 |
| [02 线路协议](docs/design/02-wire-protocol.md) | 帧头、标记位、compound、策略掩码、攻击面 |
| [03 出站流水线](docs/design/03-outbound.md) | 两级队列、合批时机、编码、部分写、背压 |
| [04 事件循环](docs/design/04-reactor.md) | epoll / kqueue、accept、读写路径、生命周期、超时 |
| [05 WebSocket](docs/design/05-websocket.md) | 握手、流式帧层、与 TCP 的语义对齐 |
| [06 连接状态机](docs/design/06-connection-state-machine.md) | **字节所有权与状态机，冲突时以它为准** |
| [07 测试分层](docs/design/07-testing.md) | 每条不变式都能在测试名里 grep 到 |

三张不变式表（O1–O14 出站、R1–R16 reactor、W1–W14 WebSocket）是实现与测试的
共同判据。
