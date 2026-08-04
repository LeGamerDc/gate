package gate

import (
	"errors"
	"sync"
	"testing"
)

/*
出站写失败的处理。

gate 在"出站缓冲打满"时的选择是**关连接**，理由写在 closeBlocked 上方：
从一条有状态的帧流中间把消息丢掉，只会让对端的状态机静默错乱，业务层什么都
察觉不到。

一次真正失败的写造成的后果完全一样——这一帧没能上线路，帧流从此有个洞。
所以它必须走同样的处理，而不是记一条日志然后接着往下写。
*/

var errWriteBroken = errors.New("simulated outbound write failure")

// failingWriter 从第 failAfter 次调用开始失败（1 表示第一次就失败）。
type failingWriter struct {
	mu        sync.Mutex
	failAfter int
	calls     int
}

func (w *failingWriter) record() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.failAfter > 0 && w.calls >= w.failAfter {
		return errWriteBroken
	}
	return nil
}

func (w *failingWriter) write([]byte) error    { return w.record() }
func (w *failingWriter) writev([][]byte) error { return w.record() }

func (w *failingWriter) callCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

// newFailingSender 造一个出站会失败的 sender。
func newFailingSender(tb testing.TB, cfg *SenderConfig, failAfter int) (*sender, *failingWriter, *fakeGnetConn) {
	tb.Helper()

	g := &fakeGnetConn{}
	w := &failingWriter{failAfter: failAfter}
	conn := &Conn{conn: g, outbound: w}
	conn.init()
	conn.markOpen()

	s, ok := NewSenderBuilder(cfg).Build(conn).(*sender)
	if !ok {
		tb.Fatal("senderBuilder.Build did not return *sender")
	}
	conn.sender = s
	return s, w, g
}

// 独立帧路径：一次 writev 失败之后不能再发后续的 chunk，并且必须关连接。
func TestSenderStopsAndClosesAfterSeparateWriteFails(t *testing.T) {
	// 不合并，100 条消息按 separateChunk=32 会分成 4 次 writev。
	s, w, g := newFailingSender(t, &SenderConfig{}, 1)

	for i := 0; i < 100; i++ {
		if err := s.Send([]byte("x")); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	g.fire(nil)

	if n := w.callCount(); n != 1 {
		t.Fatalf("outbound was called %d time(s) after the first write failed; "+
			"a broken stream must not keep receiving frames", n)
	}
	if g.closeCount() == 0 {
		t.Fatal("the connection was left open after an outbound write failed")
	}
}

// 合并帧路径：同样必须停手并关连接。
func TestSenderStopsAndClosesAfterCompoundWriteFails(t *testing.T) {
	// maxCluster 很小，保证 100 条消息被分成多个 compound 组。
	s, w, g := newFailingSender(t, &SenderConfig{MaxClusterSize: 16}, 1)

	for i := 0; i < 100; i++ {
		if err := s.Send([]byte("hello")); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	g.fire(nil)

	if n := w.callCount(); n != 1 {
		t.Fatalf("outbound was called %d time(s) after the first compound write failed", n)
	}
	if g.closeCount() == 0 {
		t.Fatal("the connection was left open after a compound write failed")
	}
}

// 连接已经关闭导致的写失败是一个正常竞态（业务可以从任意 goroutine 发送）：
// 同样要停手，但不该再关一次连接、也不该按 error 级别刷屏。
//
// 这里直接调 writeFailed 而不是"先 markClosed 再 flush"：后者根本走不到这段代码
// ——take() 看到连接已关闭时会直接返回 flush=false，writeOutbound 一次都不会被调用，
// 于是断言 closeCount()==0 恒成立，无论 writeFailed 怎么写都通过。
func TestSenderWriteFailureOnClosedConnIsQuiet(t *testing.T) {
	g := &fakeGnetConn{}
	conn := &Conn{conn: g, outbound: &captureWriter{}}
	conn.init()
	conn.markOpen()

	s, ok := NewSenderBuilder(&SenderConfig{}).Build(conn).(*sender)
	if !ok {
		t.Fatal("senderBuilder.Build did not return *sender")
	}
	conn.sender = s

	s.writeFailed(ErrConnClosed)

	if g.closeCount() != 0 {
		t.Fatalf("Close() was called %d time(s) for an already-closed connection", g.closeCount())
	}
	// 而且不能顺手把 sender 也毒掉：这只是一次正常的关闭竞态。
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		t.Fatal("a benign ErrConnClosed must not poison the sender")
	}
}

// 与上一条对照：真正的写错误必须同时关连接、毒掉 sender。
func TestSenderRealWriteFailurePoisonsAndCloses(t *testing.T) {
	g := &fakeGnetConn{}
	conn := &Conn{conn: g, outbound: &captureWriter{}}
	conn.init()
	conn.markOpen()

	s, ok := NewSenderBuilder(&SenderConfig{}).Build(conn).(*sender)
	if !ok {
		t.Fatal("senderBuilder.Build did not return *sender")
	}
	conn.sender = s

	s.writeFailed(errWriteBroken)

	if g.closeCount() == 0 {
		t.Fatal("a real write failure must close the connection")
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		t.Fatal("a real write failure must stop the sender from accepting more messages")
	}
}

// 写失败之后，sender 必须彻底停下来——包括**后续新入队的消息**。
//
// 只关连接是不够的：Conn.Close() 是异步的，此刻 state 还是 open，而 writable()
// 刻意不看 closing。与此同时 callback 一进来就在 take() 里复位了 triggered，
// 于是并发的 Send 可以立刻重新入队并再排一次 Wake；那次 Wake 只要排在关闭 task
// 前面，就会往一条已经有洞的帧流里继续写。
func TestSenderStaysPoisonedAfterWriteFailure(t *testing.T) {
	s, w, g := newFailingSender(t, &SenderConfig{}, 1)

	if err := s.Send([]byte("first")); err != nil {
		t.Fatalf("send: %v", err)
	}
	g.fire(nil)
	if n := w.callCount(); n != 1 {
		t.Fatalf("outbound was called %d time(s), want 1", n)
	}

	// 写失败之后再发：必须被拒，而不是重新排一次唤醒。
	if err := s.Send([]byte("second")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("send after a write failure returned %v, want ErrConnClosed", err)
	}
	// 就算真有一次迟到的唤醒跑进来，也不能再写。
	g.fire(nil)
	if n := w.callCount(); n != 1 {
		t.Fatalf("outbound was called %d time(s) after the stream was already broken", n)
	}
}
