package gate

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/panjf2000/gnet/v2"
)

// fakeGnetConn 只实现 sender 真正会碰到的那几个 gnet.Conn 方法，其余的留给内嵌
// 的 nil 接口：一旦 sender 越界调用了别的方法就会直接 panic，正好当成护栏。
type fakeGnetConn struct {
	gnet.Conn

	mu       sync.Mutex
	buffered int
	wakes    int
	closes   int
	wakeErr  error
	cb       gnet.AsyncCallback

	// wakeErrRunsCallback 复现 gnet v2.9.7 poller.Trigger 的真实语义：task 先入队，
	// 之后才写 eventfd 唤醒 poller，所以"Wake 返回错误"和"callback 照样跑了"
	// 完全可以同时发生。
	wakeErrRunsCallback bool
	// beforeWakeReturns 在 callback 跑完、Wake 返回错误之前调用，用来把一次更新的
	// arm 插进这条时间线里。
	beforeWakeReturns func()
}

func (f *fakeGnetConn) OutboundBuffered() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buffered
}

func (f *fakeGnetConn) Wake(cb gnet.AsyncCallback) error {
	f.mu.Lock()
	f.wakes++
	err := f.wakeErr
	runCb, after := false, f.beforeWakeReturns
	if err == nil {
		f.cb = cb
	} else {
		runCb = f.wakeErrRunsCallback
	}
	f.mu.Unlock()

	// 默认语义：Wake 失败时 callback 不跑。wakeErrRunsCallback 打开的是另一半
	// 现实——task 已经入队，事件循环照样会跑它，只是唤醒信号没送达。
	// 两种情况 sender 都必须扛得住（见 sender.go 顶部的唤醒不变式 3）。
	if runCb {
		_ = cb(f, nil)
		if after != nil {
			after()
		}
	}
	return err
}

func (f *fakeGnetConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeGnetConn) setBuffered(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buffered = n
}

func (f *fakeGnetConn) setWakeErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wakeErr = err
}

// setWakeErrRunsCallback 让失败的 Wake 依旧同步跑一次 callback，并在返回错误
// 之前调用 after（可为 nil）。
func (f *fakeGnetConn) setWakeErrRunsCallback(err error, after func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wakeErr, f.wakeErrRunsCallback, f.beforeWakeReturns = err, true, after
}

func (f *fakeGnetConn) wakeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wakes
}

func (f *fakeGnetConn) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

// fire 模拟事件循环执行挂起的 Wake 回调。
func (f *fakeGnetConn) fire(err error) {
	f.mu.Lock()
	cb := f.cb
	f.cb = nil
	f.mu.Unlock()
	if cb != nil {
		_ = cb(f, err)
	}
}

// captureWriter 顶替 Conn.outbound，把所有出站字节拼成一条线路流供测试解析。
type captureWriter struct {
	mu     sync.Mutex
	stream bytes.Buffer
	calls  int
}

func (w *captureWriter) write(b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	w.stream.Write(b)
	return nil
}

func (w *captureWriter) writev(bs [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	for _, b := range bs {
		w.stream.Write(b)
	}
	return nil
}

func (w *captureWriter) wire() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.stream.Bytes()...)
}

func (w *captureWriter) callCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func newTestSender(tb testing.TB, cfg *SenderConfig) (*sender, *captureWriter, *fakeGnetConn) {
	tb.Helper()

	g := &fakeGnetConn{}
	w := &captureWriter{}
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

// parseWire 把 sender 写出的字节流切成一组帧。用最宽松的 clientCodec 策略，
// 标记是否合理交给具体用例断言。
func parseWire(tb testing.TB, wire []byte) []frame {
	tb.Helper()

	c := clientCodec(maxMessageSize, maxMessageSize)
	var out []frame
	for len(wire) > 0 {
		f, n, ok, err := c.parse(wire)
		if err != nil {
			tb.Fatalf("parse wire: %v", err)
		}
		if !ok {
			tb.Fatalf("truncated frame on the wire, %d byte(s) left", len(wire))
		}
		out = append(out, f)
		wire = wire[n:]
	}
	return out
}

// frameMessages 把一个帧还原成它承载的业务消息（解密 → 解压 → 展开 compound）。
// 注意解密是就地做的，同一个帧不要解两次。
func frameMessages(tb testing.TB, f frame, cipher Cipher) [][]byte {
	tb.Helper()

	dec, err := newZstdDecoder(maxMessageSize)
	if err != nil {
		tb.Fatal(err)
	}
	defer dec.Close()

	var msgs [][]byte
	if err = clientCodec(maxMessageSize, maxMessageSize).deliver(f, cipher, dec,
		func(msg []byte, _ bool) error {
			msgs = append(msgs, append([]byte(nil), msg...))
			return nil
		}); err != nil {
		tb.Fatalf("deliver frame: %v", err)
	}
	return msgs
}

func compoundMsg(data []byte) sendMsg {
	return sendMsg{maskPermit: maskZ | maskC | maskE, data: data}
}

func plainMsg(data []byte) sendMsg {
	return sendMsg{data: data}
}

func requireMessages(tb testing.TB, got [][]byte, want ...[]byte) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("got %d message(s), want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			tb.Fatalf("message %d mismatch: got %d byte(s), want %d", i, len(got[i]), len(want[i]))
		}
	}
}

// #7 组大小必须只统计真正进组的消息。此前累加写在 break 之前，退出循环时把那条
// 被排除的消息也算进去了。
func TestSenderClusterCountsOnlyGroupedMessages(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 1024,
		MaxClusterSize:    32 * 1024,
	})

	buf := []sendMsg{
		compoundMsg(make([]byte, 100)),
		compoundMsg(make([]byte, 100)),
		plainMsg(make([]byte, 5000)), // 不可合并，必须被排除在组外
	}

	j, size := s.cluster(buf, 0)
	if j != 2 {
		t.Fatalf("cluster end = %d, want 2", j)
	}
	// 2×(100 payload + 2 字节子 header)。5000 那条被排除在组外，一个字节都不算。
	if size != 204 {
		t.Fatalf("cluster size = %d, want 204 (the excluded 5000-byte message must not be counted)", size)
	}
}

// 超出 MaxClusterSize 的那条同样不能被算进组里。
func TestSenderClusterStopsAtMaxClusterSize(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 1024,
		MaxClusterSize:    250,
	})

	buf := []sendMsg{
		compoundMsg(make([]byte, 100)),
		compoundMsg(make([]byte, 100)),
		compoundMsg(make([]byte, 100)),
	}

	j, size := s.cluster(buf, 0)
	if j != 2 {
		t.Fatalf("cluster end = %d, want 2", j)
	}
	// 第三条会把组顶到 306 > 250，必须留在组外。
	if size != 204 {
		t.Fatalf("cluster size = %d, want 204", size)
	}
}

// #7 端到端：一个真实只有 200 字节的组不应该拿 5200 去撞 1024 的压缩阈值。
func TestSenderSmallClusterIsNotCompressed(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 1024,
		MaxClusterSize:    32 * 1024,
	})

	var (
		small1 = bytes.Repeat([]byte("a"), 100)
		small2 = bytes.Repeat([]byte("b"), 100)
		big    = bytes.Repeat([]byte("c"), 5000)
	)
	s.pushTcp([]sendMsg{compoundMsg(small1), compoundMsg(small2), plainMsg(big)})

	frames := parseWire(t, w.wire())
	if len(frames) != 2 {
		t.Fatalf("got %d frame(s), want 2", len(frames))
	}
	if !frames[0].c {
		t.Fatal("the two compoundable messages should have been compounded")
	}
	if frames[0].z {
		t.Fatal("a 200-byte cluster must not be compressed against a 1024-byte threshold")
	}
	requireMessages(t, frameMessages(t, frames[0], nil), small1, small2)

	if frames[1].c || frames[1].z || frames[1].e {
		t.Fatal("the non-compoundable message must go out as a plain standalone frame")
	}
	requireMessages(t, frameMessages(t, frames[1], nil), big)
}

// 组够大时压缩仍然要发生，并且 maskZ 必须置上。
func TestSenderLargeClusterIsCompressed(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 1024,
		MaxClusterSize:    32 * 1024,
	})

	a := bytes.Repeat([]byte("a"), 800)
	b := bytes.Repeat([]byte("b"), 800)
	s.pushTcp([]sendMsg{compoundMsg(a), compoundMsg(b)})

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1", len(frames))
	}
	if !frames[0].c || !frames[0].z {
		t.Fatalf("want a compressed compound frame, got c=%v z=%v", frames[0].c, frames[0].z)
	}
	requireMessages(t, frameMessages(t, frames[0], nil), a, b)
}

// #6 关掉压缩不应该顺带关掉合并，而且没压过的 compound 帧绝不能置 maskZ。
func TestSenderCompoundsWithCompressionDisabled(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 0, // 压缩关闭
		MaxClusterSize:    32 * 1024,
	})

	a := bytes.Repeat([]byte("a"), 4000)
	b := bytes.Repeat([]byte("b"), 4000)
	s.pushTcp([]sendMsg{compoundMsg(a), compoundMsg(b)})

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1: compounding must not depend on CompressThreshold", len(frames))
	}
	if !frames[0].c {
		t.Fatal("MaxClusterSize > 0 alone must enable compounding")
	}
	if frames[0].z {
		t.Fatal("an uncompressed compound frame must not set maskZ")
	}
	requireMessages(t, frameMessages(t, frames[0], nil), a, b)
}

// 合并帧加密后 maskE 必须置上，且解密后能原样展开。
func TestSenderCompoundEncrypts(t *testing.T) {
	cipher := xorCipher{key: 0x5a}
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 0,
		MaxClusterSize:    32 * 1024,
	})
	s.conn.UpdateCipher(cipher)

	a := bytes.Repeat([]byte("a"), 64)
	b := bytes.Repeat([]byte("b"), 64)
	s.pushTcp([]sendMsg{compoundMsg(a), compoundMsg(b)})

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1", len(frames))
	}
	if !frames[0].c || !frames[0].e || frames[0].z {
		t.Fatalf("want an encrypted uncompressed compound frame, got c=%v z=%v e=%v",
			frames[0].c, frames[0].z, frames[0].e)
	}
	requireMessages(t, frameMessages(t, frames[0], cipher), a, b)
}

// MaxClusterSize <= 0 才是关闭合并的开关。
func TestSenderDoesNotCompoundWhenClusterDisabled(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 1024,
		MaxClusterSize:    0,
	})

	a := bytes.Repeat([]byte("a"), 100)
	b := bytes.Repeat([]byte("b"), 100)
	s.pushTcp([]sendMsg{compoundMsg(a), compoundMsg(b)})

	frames := parseWire(t, w.wire())
	if len(frames) != 2 {
		t.Fatalf("got %d frame(s), want 2 standalone frames", len(frames))
	}
	for i, f := range frames {
		if f.c {
			t.Fatalf("frame %d must not be a compound frame", i)
		}
	}
	if w.callCount() != 1 {
		t.Fatalf("both frames should leave in a single writev, got %d call(s)", w.callCount())
	}
}

// 单条可合并消息不该被包成 compound。
func TestSenderSingleCompoundableMessageStaysStandalone(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{
		CompressThreshold: 0,
		MaxClusterSize:    32 * 1024,
	})

	a := bytes.Repeat([]byte("a"), 100)
	s.pushTcp([]sendMsg{compoundMsg(a)})

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1", len(frames))
	}
	if frames[0].c || frames[0].z {
		t.Fatalf("a single message must go out standalone, got c=%v z=%v", frames[0].c, frames[0].z)
	}
	requireMessages(t, frameMessages(t, frames[0], nil), a)
}

// 一批 32 条以上的消息会被 pushSeparate 切成多次 writev；共用的 header 暂存区
// 不能让后一条消息踩掉前一条的 header。
func TestSenderSeparateChunkingKeepsHeadersIntact(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{})

	const n = separateChunk*2 + 5
	buf := make([]sendMsg, 0, n)
	want := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		msg := bytes.Repeat([]byte{byte('a' + i%26)}, i+1)
		want = append(want, msg)
		buf = append(buf, plainMsg(msg))
	}
	s.pushSeparate(buf...)

	frames := parseWire(t, w.wire())
	if len(frames) != n {
		t.Fatalf("got %d frame(s), want %d", len(frames), n)
	}
	for i, f := range frames {
		if !bytes.Equal(f.payload, want[i]) {
			t.Fatalf("frame %d payload mismatch", i)
		}
	}
	if w.callCount() != 3 {
		t.Fatalf("got %d writev call(s), want 3", w.callCount())
	}
}

// #2 连接关闭之后 Send 必须失败，业务层才能拿到断线信号。
func TestSenderSendAfterConnCloseFails(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{})

	if err := s.Send([]byte("ok")); err != nil {
		t.Fatalf("send on an open connection: %v", err)
	}

	s.conn.markClosed()
	for name, send := range map[string]func() error{
		"Send":           func() error { return s.Send([]byte("late")) },
		"SendNoEncrypt":  func() error { return s.SendNoEncrypt([]byte("late")) },
		"SendStatic":     func() error { return s.SendStatic([]byte("late"), false) },
		"SendStaticComp": func() error { return s.SendStatic([]byte("late"), true) },
	} {
		if err := send(); !errors.Is(err, ErrConnClosed) {
			t.Fatalf("%s after close = %v, want ErrConnClosed", name, err)
		}
	}
}

// #3 Close 之后 Send 也必须失败，即使连接本身还没被标记关闭。
func TestSenderSendAfterSenderCloseFails(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{})

	if err := s.Send([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if err := s.Send([]byte("late")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("Send after sender.Close = %v, want ErrConnClosed", err)
	}
}

// #3 Close 排空队列、可重入，并且事后到达的 Wake 回调不会写出任何东西。
func TestSenderCloseDrainsQueueAndIsIdempotent(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})

	for i := 0; i < 8; i++ {
		if err := s.Send(bytes.Repeat([]byte("x"), 128)); err != nil {
			t.Fatal(err)
		}
	}

	s.Close()
	s.Close() // 幂等：gate 可能在 OnClose 里重复调用
	s.Close()

	s.mu.Lock()
	q, queued, closed := s.queue, s.queued, s.closed
	s.mu.Unlock()
	if q != nil {
		t.Fatalf("queue not released, %d message(s) left", len(*q))
	}
	if queued != 0 {
		t.Fatalf("queued bytes = %d, want 0", queued)
	}
	if !closed {
		t.Fatal("sender not marked closed")
	}

	// 关闭之后事件循环仍然可能跑到那次挂起的 Wake 回调。
	g.fire(nil)
	if len(w.wire()) != 0 {
		t.Fatal("nothing may be written after Close")
	}
}

// #2 待发送队列必须有上界：一条已经死掉但还没被回收的连接不能把内存撑爆。
func TestSenderQueueIsBounded(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{MaxBufferSize: 1024})

	// 254 payload + 2 字节 header = 256，四条正好把 1024 的额度用满。
	payload := make([]byte, 254)
	for i := 0; i < 4; i++ {
		if err := s.Send(payload); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := s.Send(payload); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("send past MaxBufferSize = %v, want ErrSendQueueFull", err)
	}

	// flush 之后额度必须回来
	g.fire(nil)
	if err := s.Send(payload); err != nil {
		t.Fatalf("send after flush: %v", err)
	}
}

// #4 只允许有一次未回调的 Wake，且 Wake 不在锁内执行。
func TestSenderIssuesSingleOutstandingWake(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{})

	for i := 0; i < 16; i++ {
		if err := s.Send([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if got := g.wakeCount(); got != 1 {
		t.Fatalf("wake count = %d, want 1", got)
	}

	g.fire(nil)
	if err := s.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := g.wakeCount(); got != 2 {
		t.Fatalf("wake count after flush = %d, want 2", got)
	}
}

// #1 回调带错误返回时 triggered 也必须复位，否则 sender 永久熄火。
func TestSenderCallbackErrorDoesNotWedgeSender(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})

	if err := s.Send([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if got := g.wakeCount(); got != 1 {
		t.Fatalf("wake count = %d, want 1", got)
	}

	// 老实现只在 e == nil 分支里复位 triggered
	g.fire(errors.New("boom"))

	s.mu.Lock()
	triggered := s.triggered
	queue := s.queue
	s.mu.Unlock()
	if triggered {
		t.Fatal("triggered must be reset even when the wake callback reports an error")
	}
	if queue != nil {
		t.Fatal("the queue must be drained even when the wake callback reports an error")
	}

	if err := s.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if got := g.wakeCount(); got != 2 {
		t.Fatalf("wake count after a failed callback = %d, want 2", got)
	}

	g.fire(nil)
	frames := parseWire(t, w.wire())
	if len(frames) != 1 || !bytes.Equal(frames[0].payload, []byte("second")) {
		t.Fatalf("the message queued after the failed callback was not flushed: %v", frames)
	}
}

// #1/#4 Wake 本身失败时 gnet 可能不会调用 callback，triggered 只能由 send 复位，
// 而且失败的那一次绝不能让已经入队的消息丢掉。
//
// 失败同时还会关连接（见 TestSenderWakeFailureClosesConnectionInsteadOfStranding）；
// fakeGnetConn.Close 只计数、不改连接状态，所以这里仍然能继续观察队列的去向。
func TestSenderWakeFailureDoesNotWedgeSender(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})

	g.setWakeErr(errors.New("poller closed"))
	if err := s.Send([]byte("first")); err != nil {
		t.Fatalf("send should still accept the message: %v", err)
	}

	s.mu.Lock()
	triggered := s.triggered
	s.mu.Unlock()
	if triggered {
		t.Fatal("a failed Wake must reset triggered, gnet never runs the callback in that case")
	}

	g.setWakeErr(nil)
	if err := s.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if got := g.wakeCount(); got != 2 {
		t.Fatalf("wake count = %d, want 2", got)
	}

	g.fire(nil)
	frames := parseWire(t, w.wire())
	if len(frames) != 2 {
		t.Fatalf("got %d frame(s), want both messages flushed", len(frames))
	}
	if !bytes.Equal(frames[0].payload, []byte("first")) || !bytes.Equal(frames[1].payload, []byte("second")) {
		t.Fatal("messages flushed out of order")
	}
}

// 连接还在握手（WebSocket）时消息应该留在队列里，而不是撞上 writable 闸口被丢掉。
func TestSenderKeepsQueueWhileHandshaking(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})
	s.conn.state.Store(uint32(stateHandshaking))

	if err := s.Send([]byte("early")); err != nil {
		t.Fatal(err)
	}
	g.fire(nil)
	if len(w.wire()) != 0 {
		t.Fatal("nothing may be written before the connection is open")
	}

	s.mu.Lock()
	queue, triggered := s.queue, s.triggered
	s.mu.Unlock()
	if queue == nil || len(*queue) != 1 {
		t.Fatal("the message must stay queued while the connection is handshaking")
	}
	if triggered {
		t.Fatal("triggered must always be reset so a later send can re-arm the wake")
	}

	s.conn.markOpen()
	if err := s.Send([]byte("ready")); err != nil {
		t.Fatal(err)
	}
	g.fire(nil)

	frames := parseWire(t, w.wire())
	if len(frames) != 2 {
		t.Fatalf("got %d frame(s), want 2", len(frames))
	}
	if !bytes.Equal(frames[0].payload, []byte("early")) {
		t.Fatal("the handshake-time message must be flushed first")
	}
}

// #8 出站缓冲打满时关闭连接，而不是从帧流中间悄悄抽掉消息。
func TestSenderBackpressureClosesConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *SenderConfig
	}{
		{"separate", &SenderConfig{MaxBufferSize: 1024, MaxClusterSize: 0}},
		{"compound", &SenderConfig{MaxBufferSize: 1024, MaxClusterSize: 32 * 1024}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, w, g := newTestSender(t, tc.cfg)
			g.setBuffered(4096) // 已经超过 MaxBufferSize

			s.pushTcp([]sendMsg{compoundMsg([]byte("a")), compoundMsg([]byte("b"))})

			if g.closeCount() == 0 {
				t.Fatal("backpressure must close the connection instead of dropping messages")
			}
			if len(w.wire()) != 0 {
				t.Fatal("no partial data should be written once the buffer is over the limit")
			}
		})
	}
}

// 单个 chunk 内部预算耗尽同样要关连接。
func TestSenderBackpressureMidChunkClosesConnection(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{MaxBufferSize: 512, MaxClusterSize: 0})

	buf := []sendMsg{
		plainMsg(bytes.Repeat([]byte("a"), 400)),
		plainMsg(bytes.Repeat([]byte("b"), 400)),
		plainMsg(bytes.Repeat([]byte("c"), 400)),
	}
	s.pushSeparate(buf...)

	if g.closeCount() == 0 {
		t.Fatal("running out of outbound budget mid-chunk must close the connection")
	}
	// 已经攒进 vb 的部分照常写出去；关键是不能静默丢掉剩下的。
	frames := parseWire(t, w.wire())
	if len(frames) == len(buf) {
		t.Fatal("expected the batch to be cut short by the budget")
	}
}

// SendStatic 的数据不拷贝也不改写。
func TestSenderStaticMessageIsNeitherCopiedNorMutated(t *testing.T) {
	cipher := xorCipher{key: 0x11}
	s, w, g := newTestSender(t, &SenderConfig{CompressThreshold: 1, MaxClusterSize: 32 * 1024})
	s.conn.UpdateCipher(cipher)

	static := bytes.Repeat([]byte("s"), 4096)
	want := append([]byte(nil), static...)
	if err := s.SendStatic(static, false); err != nil {
		t.Fatal(err)
	}
	g.fire(nil)

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1", len(frames))
	}
	if frames[0].z || frames[0].c || frames[0].e {
		t.Fatalf("static frames must carry no flags, got z=%v c=%v e=%v",
			frames[0].z, frames[0].c, frames[0].e)
	}
	if !bytes.Equal(frames[0].payload, want) {
		t.Fatal("static payload was altered on the wire")
	}
	if !bytes.Equal(static, want) {
		t.Fatal("static payload was mutated in place")
	}
}

// 超过协议上限的消息在入队前就要被拒。
func TestSenderRejectsOversizedMessage(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{})

	if err := s.SendStatic(make([]byte, maxMessageSize+1), false); !errors.Is(err, ErrMaxMessageSize) {
		t.Fatalf("oversized send = %v, want ErrMaxMessageSize", err)
	}
	if g.wakeCount() != 0 {
		t.Fatal("a rejected message must not schedule a wake")
	}
}

// send 从多个 goroutine 并发调用时不能丢消息、也不能漏掉唤醒。
func TestSenderConcurrentSendsAreAllFlushed(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})

	const producers = 8
	const perProducer = 32

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		done = make(chan struct{})
	)
	// 一个"事件循环"不停地消费挂起的 Wake。
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				g.fire(nil)
				return
			default:
				g.fire(nil)
			}
		}
	}()

	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := s.Send([]byte{byte(p), byte(i)}); err != nil {
					t.Errorf("send: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(stop)
	<-done
	g.fire(nil)

	frames := parseWire(t, w.wire())
	if len(frames) != producers*perProducer {
		t.Fatalf("got %d frame(s), want %d", len(frames), producers*perProducer)
	}
}

// frameOverhead 必须和 encodeHeader 写出的字节数完全一致，否则所有基于
// frameSize 的计量（队列封顶、出站预算、compound 分组）都会系统性地偏。
func TestFrameOverheadMatchesEncodeHeader(t *testing.T) {
	var b [4]byte
	for _, size := range []int{0, 1, moreHeaderSize - 1, moreHeaderSize, 1 << 20, maxMessageSize} {
		if got, want := frameOverhead(size), encodeHeader(b[:], size); got != want {
			t.Fatalf("frameOverhead(%d) = %d, but encodeHeader wrote %d byte(s)", size, got, want)
		}
	}
	// header 的长度字段只有 12+16 位；协议上限必须落在里面，否则长度会溢进
	// 首字节的 m/z/c/e 标记。
	if maxMessageSize >= maxHeaderLen {
		t.Fatalf("maxMessageSize %d does not fit the %d-bit header length field", maxMessageSize, 28)
	}
}

// #F1 空消息也要消耗队列配额。按 len(data) 记账时它们是完全免费的：
// Send([]byte{}) 循环可以在 MaxBufferSize 已设的情况下把队列涨到 OOM，
// 因为 queued 永远是 0，ErrSendQueueFull 永远不会触发。
func TestSenderZeroLengthSendsConsumeQueueBudget(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 64})

	// 一条空消息在线路上仍然要占 2 字节 header，32 条正好填满 64 字节额度。
	for i := 0; i < 32; i++ {
		if err := s.Send([]byte{}); err != nil {
			t.Fatalf("zero-length send %d: %v", i, err)
		}
	}
	if err := s.Send([]byte{}); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("zero-length send past MaxBufferSize = %v, want ErrSendQueueFull", err)
	}

	s.mu.Lock()
	queued, n := s.queued, len(*s.queue)
	s.mu.Unlock()
	if queued != 64 || n != 32 {
		t.Fatalf("queued = %d byte(s) over %d message(s), want 64 over 32", queued, n)
	}
}

// #F1 cluster 报出来的组大小必须等于真正编码出来的 compound body，
// 包括每条子消息 2/4 字节的子 header——这个数既要和 MaxClusterSize、
// CompressThreshold 比较，又要写进外层 header 的长度字段。
func TestSenderClusterSizeMatchesEncodedCompoundBody(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{MaxClusterSize: 32 * 1024})

	msgs := [][]byte{
		bytes.Repeat([]byte("a"), 1),
		{}, // 空消息：payload 0 字节，子 header 照样 2 字节
		bytes.Repeat([]byte("b"), moreHeaderSize), // 跨过 4KB 边界，子 header 4 字节
		bytes.Repeat([]byte("c"), 100),
	}
	buf := make([]sendMsg, 0, len(msgs))
	for _, m := range msgs {
		buf = append(buf, compoundMsg(m))
	}

	j, size := s.cluster(buf, 0)
	if j != len(buf) {
		t.Fatalf("cluster end = %d, want %d", j, len(buf))
	}
	if want := 3 + 2 + (moreHeaderSize + 4) + 102; size != want {
		t.Fatalf("cluster size = %d, want %d", size, want)
	}

	s.pushCompound(buf, false)
	frames := parseWire(t, w.wire())
	if len(frames) != 1 || !frames[0].c {
		t.Fatalf("got %d frame(s), want 1 compound frame", len(frames))
	}
	if len(frames[0].payload) != size {
		t.Fatalf("cluster accounted %d byte(s) but the encoded compound body is %d: "+
			"the per-message sub-headers are missing from the accounting",
			size, len(frames[0].payload))
	}
	requireMessages(t, frameMessages(t, frames[0], nil), msgs...)
}

// #F1 一组空消息同样要受 MaxClusterSize 约束。按 payload 长度累加时它们的
// next 恒等于 size，判断永远成立，于是一个组可以无限长下去，而每条空消息
// 在 body 里仍然贡献 2 字节子 header——帧远远超过 MaxClusterSize。
func TestSenderClusterBoundsZeroLengthMessages(t *testing.T) {
	const cluster = 64
	s, w, _ := newTestSender(t, &SenderConfig{MaxClusterSize: cluster})

	buf := make([]sendMsg, 128)
	for i := range buf {
		buf[i] = compoundMsg([]byte{})
	}

	j, size := s.cluster(buf, 0)
	if j != cluster/2 || size != cluster {
		t.Fatalf("cluster(end=%d, size=%d), want end=%d size=%d", j, size, cluster/2, cluster)
	}

	s.pushTcp(buf)
	frames := parseWire(t, w.wire())
	if len(frames) != len(buf)/(cluster/2) {
		t.Fatalf("got %d frame(s), want %d", len(frames), len(buf)/(cluster/2))
	}
	for i, f := range frames {
		if len(f.payload) > cluster {
			t.Fatalf("compound frame %d carries a %d-byte body, over the %d-byte MaxClusterSize",
				i, len(f.payload), cluster)
		}
	}
}

// #F2 MaxClusterSize 必须被夹进协议上限：两条 16MB 的消息 payload 加起来正好是
// 32MB，再算上子 header 就是 32MB+8，帧一发出去对端 codec 立刻按
// ErrMaxMessageSize 断链；长度真到 1<<28 更会溢进 header 的标记位。
func TestSenderCompoundNeverExceedsProtocolLimit(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{MaxClusterSize: 4 * maxMessageSize})

	if s.maxCluster != maxMessageSize {
		t.Fatalf("effective MaxClusterSize = %d, want it clamped to %d", s.maxCluster, maxMessageSize)
	}

	buf := []sendMsg{
		compoundMsg(make([]byte, maxMessageSize/2)),
		compoundMsg(make([]byte, maxMessageSize/2)),
	}
	if j, size := s.cluster(buf, 0); j != 1 {
		t.Fatalf("cluster grouped %d message(s) into a %d-byte body, over the %d-byte protocol limit",
			j, size, maxMessageSize)
	}

	// 兜底路径：即使有人绕过 cluster 直接喂一组超限的消息，也必须退回独立帧
	// 把它们发出去，而不是把一个对端必然拒收的帧丢上线路、也不是干脆丢掉。
	s.pushCompound(buf, false)
	frames := parseWire(t, w.wire())
	if len(frames) != len(buf) {
		t.Fatalf("got %d frame(s), want %d standalone frames", len(frames), len(buf))
	}
	for i, f := range frames {
		if f.c {
			t.Fatalf("frame %d: an oversized group must not be emitted as a compound frame", i)
		}
		if len(f.payload) != maxMessageSize/2 {
			t.Fatalf("frame %d carries %d byte(s), want %d", i, len(f.payload), maxMessageSize/2)
		}
	}
}

// #F2 压缩把 payload 压大时应该退回原文：既省掉对端一次无谓的解压，也保证一条
// 贴着上限的消息不会因为 zstd 的膨胀而越过协议上限。
func TestSenderKeepsUncompressedPayloadWhenCompressionGrowsIt(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{CompressThreshold: 8})

	// 随机噪声压不动，zstd 只会在原文外面再套一层 frame header。
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i*7919 + i*i)
	}
	if len(enc.EncodeAll(payload, nil)) <= len(payload) {
		t.Skip("payload turned out to be compressible, nothing to assert")
	}

	s.pushSeparate(sendMsg{maskPermit: maskZ, data: payload})
	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("got %d frame(s), want 1", len(frames))
	}
	if frames[0].z {
		t.Fatal("compression grew the payload, the frame must carry the original bytes instead")
	}
	requireMessages(t, frameMessages(t, frames[0], nil), payload)
}

// #F3 预算判断的是"这条消息放不放得下"，而不是"额度有没有用完"。后者会让每个
// chunk 的最后一条消息合法地把出站缓冲顶过 MaxBufferSize，而且因为它是最后一条，
// blocked 路径永远不触发，连接也就不会被关掉。
func TestSenderSeparateBudgetCoversTheLastMessage(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{MaxBufferSize: 1024})
	g.setBuffered(900) // 剩余额度 124

	// 这条消息上线要占 200+2 = 202 字节，放不下。
	s.pushSeparate(plainMsg(bytes.Repeat([]byte("a"), 200)))

	if n := len(w.wire()); n != 0 {
		t.Fatalf("wrote %d byte(s) with only 124 byte(s) of budget left", n)
	}
	if g.closeCount() == 0 {
		t.Fatal("a message that does not fit the outbound budget must take the blocked path and close the connection")
	}
}

// #F3 compound 侧同理：此前调用方只看"当前缓冲有没有超"，从来不看这一组加上去
// 会不会超。
func TestSenderCompoundBudgetCoversTheLastGroup(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{MaxBufferSize: 1024, MaxClusterSize: 32 * 1024})
	g.setBuffered(900) // 剩余额度 124

	// 组的 body 是 2×(100+2) = 204，加上外层 header 一共 206 字节，放不下。
	s.pushTcp([]sendMsg{
		compoundMsg(bytes.Repeat([]byte("a"), 100)),
		compoundMsg(bytes.Repeat([]byte("b"), 100)),
	})

	if n := len(w.wire()); n != 0 {
		t.Fatalf("wrote %d byte(s) with only 124 byte(s) of budget left", n)
	}
	if g.closeCount() == 0 {
		t.Fatal("a compound group that does not fit the outbound budget must close the connection")
	}
}

// #F4 Wake 失败的回滚必须只动"自己那一代"。gnet 的 Trigger 先入队 task 再写
// eventfd，所以 callback 完全可能在 Wake 返回错误之前就跑完，而此时一次更新的
// arm 可能已经生效——无条件复位会把它抹掉，于是同一时刻出现多次唤醒在路上。
func TestSenderFailedWakeDoesNotClobberNewerArm(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})

	var once sync.Once
	g.setWakeErrRunsCallback(errors.New("eventfd write failed"), func() {
		// callback 已经跑完（第一代结束），这里模拟紧接着又来了一条消息：
		// 它 arm 第二代并发出自己的 Wake，然后第一代的 Wake 才返回错误。
		once.Do(func() {
			g.setWakeErr(nil)
			if err := s.Send([]byte("second")); err != nil {
				t.Errorf("send inside the wake window: %v", err)
			}
		})
	})

	if err := s.Send([]byte("first")); err != nil {
		t.Fatal(err)
	}

	s.mu.Lock()
	triggered := s.triggered
	s.mu.Unlock()
	if !triggered {
		t.Fatal("a stale wake failure must not disarm the newer generation's wake")
	}

	if err := s.Send([]byte("third")); err != nil {
		t.Fatal(err)
	}
	if got := g.wakeCount(); got != 2 {
		t.Fatalf("wake count = %d, want 2: at most one wake may be in flight at a time", got)
	}

	g.fire(nil)
	frames := parseWire(t, w.wire())
	if len(frames) != 3 {
		t.Fatalf("got %d frame(s), want 3", len(frames))
	}
	for i, want := range [][]byte{[]byte("first"), []byte("second"), []byte("third")} {
		if !bytes.Equal(frames[i].payload, want) {
			t.Fatalf("frame %d = %q, want %q", i, frames[i].payload, want)
		}
	}
}

// #F4 disarm 的 generation 判定本身：陈旧的一代不得碰更新一代的状态。
func TestSenderDisarmOnlyRollsBackItsOwnGeneration(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{})

	if err := s.Send([]byte("first")); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	gen := s.wakeGen
	s.mu.Unlock()

	// 第一代的 callback 跑到了（take 复位 triggered），随后一次新的 send
	// arm 了第二代。
	s.flush()
	if err := s.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}

	// 现在第一代的 Wake 才返回错误。
	if n := s.disarm(gen); n != 0 {
		t.Fatalf("disarm of a stale generation reported %d stranded message(s), want 0", n)
	}
	s.mu.Lock()
	triggered, cur := s.triggered, s.wakeGen
	s.mu.Unlock()
	if cur == gen {
		t.Fatal("a second arm must bump the wake generation")
	}
	if !triggered {
		t.Fatal("disarm must not roll back an arm it does not own")
	}
}

// #F4 Wake 失败且队列还有消息时，已经没有任何人负责 flush 它们了：与其让它们
// 无声无息地烂在队列里，不如关连接，让业务层通过后续 Send 的 ErrConnClosed
// 观测到。
func TestSenderWakeFailureClosesConnectionInsteadOfStranding(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{})

	g.setWakeErr(errors.New("poller closed"))
	if err := s.Send([]byte("stranded")); err != nil {
		t.Fatalf("send should still accept the message: %v", err)
	}

	s.mu.Lock()
	triggered, queue := s.triggered, s.queue
	s.mu.Unlock()
	if triggered {
		t.Fatal("a failed wake must roll back its own arm")
	}
	if queue == nil || len(*queue) != 1 {
		t.Fatal("the message must stay queued")
	}
	if g.closeCount() == 0 {
		t.Fatal("a failed wake that leaves messages queued must close the connection, not strand them")
	}
}

// BenchmarkSenderCompressParallel 记录 zstd encoder 并发度的影响。
//
// EncodeAll 的实现是从 e.encoders channel 里取一个 encoder，而那个 channel 的
// 容量就等于 WithEncoderConcurrency：写死成 1 时所有 event loop 抢同一个
// encoder，压缩直接退化成全局串行。concurrency_1 与 concurrency_numcpu 的差值
// 就是这条串行瓶颈的代价。
func BenchmarkSenderCompressParallel(b *testing.B) {
	payload := make([]byte, 8*1024)
	for i := range payload {
		payload[i] = byte(i%251) ^ byte(i>>7)
	}

	serial, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		b.Fatal(err)
	}
	defer serial.Close()

	for _, tc := range []struct {
		name string
		e    *zstd.Encoder
	}{
		{"concurrency_1", serial},
		{"concurrency_numcpu", enc},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, 0, 8*1024)
				for pb.Next() {
					benchByteSink += len(tc.e.EncodeAll(payload, dst[:0]))
				}
			})
		})
	}
}

// BenchmarkSenderFlush 覆盖 callback -> pushTcp 的完整出站路径。
func BenchmarkSenderFlush(b *testing.B) {
	small := bytes.Repeat([]byte("m"), 96)
	large := bytes.Repeat([]byte("m"), 2048)

	for _, tc := range []struct {
		name string
		cfg  *SenderConfig
		msg  []byte
		n    int
	}{
		{"separate_64x96B", &SenderConfig{MaxBufferSize: 2 << 20}, small, 64},
		{"compound_64x96B", &SenderConfig{MaxBufferSize: 2 << 20, MaxClusterSize: 32 << 10}, small, 64},
		{"compound_compress_16x2KB", &SenderConfig{CompressThreshold: 1024, MaxBufferSize: 2 << 20, MaxClusterSize: 32 << 10}, large, 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s, w, _ := newTestSender(b, tc.cfg)
			buf := make([]sendMsg, tc.n)
			for i := range buf {
				buf[i] = compoundMsg(tc.msg)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.pushTcp(buf)
				w.mu.Lock()
				benchByteSink += w.stream.Len()
				w.stream.Reset()
				w.mu.Unlock()
			}
		})
	}
}
