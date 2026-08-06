// Package gate 是一个面向海量长连接（目标 10 万级并发）的 TCP / WebSocket
// 网关库。它不定义业务协议，只把「连接管理、拆包、可选加密、发送侧合包与
// 压缩」这几件所有网关都要重做一遍的事收敛成一个可复用组件。
//
// # 五分钟上手
//
//	type player struct{ id int64 }
//
//	type handler struct{}
//
//	func (handler) OnOpen(c *gate.Conn[*player]) (*player, error) {
//		return &player{id: int64(c.ID())}, nil
//	}
//	func (handler) OnMessage(c *gate.Conn[*player], msg []byte) error {
//		return c.Send(msg) // 回声
//	}
//	func (handler) OnClose(c *gate.Conn[*player], reason error) {}
//
//	func main() {
//		err := gate.Run(context.Background(), gate.Options[*player]{
//			Addr:     ":8081",
//			Handler:  handler{},
//			Outbound: gate.DefaultOutbound(),
//			Log:      zap.L(),
//		})
//		...
//	}
//
// # 线程模型
//
// 事件循环是 thread-per-core、shared-nothing 的，连接归属某个循环之后永不
// 迁移。由此得到两条对使用者可见的规则：
//
//   - Send / SendAlone / SendFrame / SendFunc / Close / Post / resume 可以从
//     任意 goroutine 调用；
//   - Conn.State 只能在**串行域**内访问——OnOpen / OnMessage / OnClose /
//     Post / AsyncDo 的函数体。不在串行域里就用 Post 排进去。
//
// SetCipher / Pause / AsyncDo 的约束**比 State 窄一格**：只能在事件循环线程上
// 调用——回调与 Post 的函数体，不含 AsyncDo 的函数体。它们要改的是 gate 自己的
// 连接状态（cipher、读闸、LRU 归属、poller 注册），那些是循环私有的；而 AsyncDo
// 的函数体虽然属于**业务状态**意义上的串行域，却跑在另一个 goroutine 上。
// 在 AsyncDo 里需要它们时用 Post 排回去。
//
// # 两种传输，一份语义
//
// TCP 与 WebSocket 对业务呈现完全相同的语义：WebSocket 只是 gate 帧循环
// 前面多了一层字节变换，两者共用同一份投递代码。唯一对业务可见的差异是
// Conn.Handshake()——WebSocket 下返回升级请求快照，TCP 下返回 nil。
//
// # 关闭语义
//
// gate 从不静默丢消息：要么送出，要么 Send 明确返回错误。出站积压达到准入
// 上限时 Send 返回 ErrSendQueueFull（消息从未入队，帧流没有洞），连接照常
// 活着；写失败或帧流已经错位时才关闭连接。OnClose 的 reason 用 errors.Is
// 判断，覆盖对端关闭、空闲回收、背压、协议违规、业务 panic 等。
//
// Close 返回之后的 Send 一定报错；与 Close 并发的 Send 返回什么则不保证
// （要确定就得让 Send 阻塞等事件循环确认，而「不阻塞、只入队」是这套设计
// 的前提）。
//
// # 设计文档
//
// docs/design/ 下有完整的设计文档（协议、出站流水线、reactor、WebSocket、
// 连接状态机与字节所有权、测试分层）。实现中的每条不变式都能在那里找到
// 出处，也能在测试名里 grep 到。
package gate
