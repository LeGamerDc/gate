package gate

import (
	"sync"
	"testing"

	"github.com/panjf2000/gnet/v2"
)

/*
入站缓冲的总量上限。

AsyncDo 挂起期间 gate 不消费任何入站数据，而 gnet 会继续把新到的字节追加进连接
的弹性入站缓冲。此前 TCP 侧对这个总量没有任何上限：一个在慢速 AsyncDo 期间持续
灌数据的客户端，可以把单条连接的内存一路撑上去。默认又不开 IdleTimeout，所以
连接也不会被回收。

WebSocket 侧一直有 MaxWebSocketBufferedBytes 封顶，TCP 侧没有——这既是一个可远
程触发的内存 DoS，也是两种传输语义不一致的地方。
*/

// tcpFakeConn 实现 Conn.onTraffic 会用到的那几个 gnet.Conn 方法。
type tcpFakeConn struct {
	gnet.Conn

	buffered  int
	data      []byte
	discarded int
	closes    int
}

func (c *tcpFakeConn) InboundBuffered() int { return c.buffered }

func (c *tcpFakeConn) Peek(int) ([]byte, error) { return c.data, nil }

func (c *tcpFakeConn) Discard(n int) (int, error) {
	c.discarded += n
	if n > len(c.data) {
		n = len(c.data)
	}
	c.data = c.data[n:]
	return n, nil
}

func (c *tcpFakeConn) Close() error {
	c.closes++
	return nil
}

func newInboundTestConn(tb testing.TB, maxMessage int) (*Conn, *tcpFakeConn) {
	tb.Helper()

	g := &tcpFakeConn{}
	conn := &Conn{conn: g, codec: serverCodec(maxMessage)}
	conn.init()
	conn.markOpen()
	conn.handler = &funcHandler{fn: func([]byte) {}}
	return conn, g
}

// AsyncDo 挂起期间，入站缓冲涨过上限必须关连接。
func TestInboundBufferIsBoundedWhileBlocked(t *testing.T) {
	const maxMessage = 64 * 1024

	conn, g := newInboundTestConn(t, maxMessage)
	// 模拟一个还没结束的 AsyncDo。
	conn.blocking.Add(1)

	g.buffered = maxMessage + inboundBufferSlack + 1
	if action := conn.onTraffic(); action != gnet.Close {
		t.Fatalf("action = %v, want Close; a blocked connection must not buffer without bound", action)
	}
}

// 上限之内不能误伤：AsyncDo 期间攒下一批数据是完全正常的。
func TestInboundBufferWithinLimitSurvivesWhileBlocked(t *testing.T) {
	const maxMessage = 64 * 1024

	conn, g := newInboundTestConn(t, maxMessage)
	conn.blocking.Add(1)

	g.buffered = maxMessage + inboundBufferSlack
	if action := conn.onTraffic(); action != gnet.None {
		t.Fatalf("action = %v, want None; buffering up to the limit is legitimate", action)
	}
}

// 没有阻塞时不该有这个检查：稳态下 gate 每轮事件都会把完整帧全部消费掉，
// 缓冲区里剩下的只可能是半个帧，本来就被 MaxMessageSize 封着。
//
// 这条用例守的是"别把上限检查加到热路径的每一轮上"：一个未阻塞、缓冲区很大的
// 连接（比如一次读事件恰好带回来很多数据）不能被误杀。
func TestInboundBufferLimitDoesNotApplyWhenDraining(t *testing.T) {
	const maxMessage = 64 * 1024

	conn, g := newInboundTestConn(t, maxMessage)
	g.buffered = maxMessage + inboundBufferSlack + 1
	// 缓冲里就是一条完整的合法消息，gate 会把它消费掉。
	g.data = gateFrame([]byte("hello"))

	if action := conn.onTraffic(); action != gnet.None {
		t.Fatalf("action = %v, want None; a connection that is actively draining must not be closed", action)
	}
	if g.discarded == 0 {
		t.Fatal("the message was not consumed")
	}
}

// 关闭之后，尚未发出的消息不能继续钉在队列里。
//
// Send 每条消息都分配并复制一份完整 payload，而 writeLoop 一退出就没人排空
// sendCh 了。业务留着这个已关闭的 Client（读 Wait() 的错误、做重连决策都很常见）
// 就等于留着整个队列的内存。
func TestClientDropsQueuedPayloadsAfterClose(t *testing.T) {
	// 服务端不读，让消息全部堆在客户端队列里。
	client, serverConn := dialAuditClient(t, &ClientConfig{
		Handler:       newMatrixHandler(),
		SendQueueSize: 64,
	})
	_ = serverConn

	// 先把队列真的堆起来，并断言它确实非空——否则本机 socket 缓冲很可能
	// 已经把这些字节全吸走了，队列本来就是空的，这条用例会无条件通过。
	for i := 0; i < 64; i++ {
		if err := client.Send(make([]byte, 4096)); err != nil {
			break
		}
	}
	if n := len(client.sendCh); n == 0 {
		t.Skip("send queue never backed up on this machine; nothing to observe")
	}

	_ = client.Close()
	_ = waitClientStop(t, client)

	if n := len(client.sendCh); n != 0 {
		t.Fatalf("%d message(s) are still pinned in the send queue after close", n)
	}
}

// 关闭与 Send 抢跑时，也不能有消息滞留在队列里。
//
// send() 的 select 在 closed 和"队列有空位"同时就绪时会随机挑，所以一条消息
// 完全可能在 writeLoop 已经排空退出之后才被塞进去。跑多轮把这个交错撞出来。
// 关闭与 Send 抢跑之后，队列里同样不能有残留。
//
// 坦白一点：这是一条**不变式压力测试**，不是那个具体交错的回归测试。
// 理论上的窗口是 send() 的 select 在 closed 和 sendCh 同时就绪时随机挑了后者，
// 而且恰好落在 writeLoop 的排空扫到"队列空了"之后。我把队列做到很小、发送方开
// 到很多、服务端一个字节都不读（发送方会全部堵在 select 上），仍然撞不出来：
// 把修复注释掉之后跑 8 轮依然全绿。原因是排空是个循环，parked 的发送方一旦把值
// 交出来就会被同一轮循环取走，真正的残留窗口只剩最后那次 default 判断的瞬间。
//
// 所以它证明的是"这条路径上没有可观测的滞留"，不是"这个修复必要"。修复本身留着：
// 代价是一次 isClosed()，而滞留的上界是 SendQueueSize 条消息（默认 1024）。
func TestClientDropsPayloadsQueuedWhileClosing(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		client, _ := dialAuditClient(t, &ClientConfig{
			Handler:       newMatrixHandler(),
			SendQueueSize: 2,
		})

		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 40; i++ {
					// 关闭之后返回 ErrClientClosed 是正常结果。
					_ = client.Send(make([]byte, 8192))
				}
			}()
		}
		close(start)
		_ = client.Close()
		wg.Wait()
		_ = waitClientStop(t, client)

		if n := len(client.sendCh); n != 0 {
			t.Fatalf("iter %d: %d message(s) left pinned in the send queue after close", iter, n)
		}
	}
}
