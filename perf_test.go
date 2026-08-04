package gate

import (
	"bytes"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"testing/quick"
	"time"
	"unsafe"

	"github.com/gobwas/ws"
	"github.com/klauspost/compress/zstd"
	"github.com/panjf2000/gnet/v2"
)

// 这个文件里的每条用例都对应一处性能改动，作用是把"改动没有改变可观察行为"
// 钉死。判断标准统一是：把优化改回原样，对应的用例必须失败。

// ---------------------------------------------------------------- 活跃时间

// #P1 活跃时间从"每条消息一次 time.Now()"改成"每轮事件一次"。
// 语义必须不变：真正交付过消息就算活跃。
func TestOnTrafficMarksActivityOncePerBatch(t *testing.T) {
	var wire []byte
	for i := 0; i < 4; i++ {
		wire = append(wire, mkFrame([]byte("hello"), 0)...)
	}

	g := &inboundGnetConn{data: wire}
	h := &countingHandler{}
	conn := &Conn{conn: g, handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()

	// 把活跃时间推到很久以前，这样"有没有被刷新"是可判定的。
	stale := time.Now().Add(-time.Hour).UnixNano()
	conn.lastActive.Store(stale)

	if a := conn.onTraffic(); a != gnet.None {
		t.Fatalf("unexpected action %v", a)
	}
	if h.count != 4 {
		t.Fatalf("expected 4 delivered messages, got %d", h.count)
	}
	if conn.lastActive.Load() == stale {
		t.Fatal("delivering messages must refresh the activity timestamp")
	}
	if conn.sinceTouch != 0 {
		t.Fatal("the per-event activity counter must be reset when the batch ends")
	}
}

// #P1 时间戳必须在**进入 handler 之前**就已经发布出去。
//
// 上一条只在 onTraffic 返回之后检查，所以"整批处理完再刷新"照样能通过。而空闲
// 扫描跑在 gnet 自己的 ticker goroutine 上，和事件循环并发：批次结束才发布，
// 意味着整批期间扫描线程看到的都是上一批的时刻，一条正在处理消息的连接会被
// 判成空闲关掉。这里让 handler 自己回头看那个原子变量，直接钉住发布时机。
func TestActivityIsPublishedBeforeHandlerRuns(t *testing.T) {
	wire := mkFrame([]byte("hello"), 0)
	g := &inboundGnetConn{data: wire}

	var conn *Conn
	stale := time.Now().Add(-time.Hour).UnixNano()
	seen := make([]int64, 0, 1)
	h := &funcHandler{fn: func([]byte) {
		// handler 正在跑的这一刻，扫描线程能看到什么？
		seen = append(seen, conn.lastActive.Load())
	}}

	conn = &Conn{conn: g, handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()
	conn.lastActive.Store(stale)

	conn.onTraffic()

	if len(seen) != 1 {
		t.Fatalf("handler ran %d time(s), want 1", len(seen))
	}
	if seen[0] == stale {
		t.Fatal("the activity timestamp was still stale while the handler was running")
	}
}

// #P1 一批里的刷新次数必须有上界。
//
// 一次读事件能带回多少消息只受缓冲区大小约束（WebSocket 下默认 32MB），所以
// 一批里大量各自很短的 handler 累加起来照样可以超过 IdleTimeout——不需要谁真的
// 阻塞。只在第一条消息上发布的话，整批期间时间戳都不会再动。
func TestActivityRefreshesWithinLongBatch(t *testing.T) {
	const msgs = activityRefreshMessages*2 + 5

	var wire []byte
	for i := 0; i < msgs; i++ {
		wire = append(wire, mkFrame([]byte("m"), 0)...)
	}
	g := &inboundGnetConn{data: wire}

	var conn *Conn
	distinct := map[int64]struct{}{}
	h := &funcHandler{fn: func([]byte) {
		distinct[conn.lastActive.Load()] = struct{}{}
	}}
	conn = &Conn{conn: g, handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()

	conn.onTraffic()

	// msgs 条消息跨过 2 次刷新边界，所以 handler 至少应该观察到 3 个不同的时刻。
	// 只在第一条上发布的实现只会给出 1 个。
	if len(distinct) < 3 {
		t.Fatalf("the activity timestamp was refreshed %d time(s) across %d message(s); "+
			"staleness is not bounded within a batch", len(distinct), msgs)
	}
}

// #P1 只发半个帧不算活跃：slowloris 必须仍然会被 IdleTimeout 收走。
func TestOnTrafficDoesNotMarkActivityForPartialFrame(t *testing.T) {
	full := mkFrame(bytes.Repeat([]byte("x"), 32), 0)

	g := &inboundGnetConn{data: full[:3]} // 头齐了，body 不齐
	h := &countingHandler{}
	conn := &Conn{conn: g, handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()

	stale := time.Now().Add(-time.Hour).UnixNano()
	conn.lastActive.Store(stale)

	conn.onTraffic()
	if h.count != 0 {
		t.Fatalf("a partial frame must not be delivered, got %d", h.count)
	}
	if conn.lastActive.Load() != stale {
		t.Fatal("a partial frame must not count as activity")
	}
}

// #P1 WebSocket 走的是另一条 onTraffic，但共用同一个交付点，语义必须一致。
func TestWebSocketOnTrafficMarksActivity(t *testing.T) {
	wire := buildWSWire(t, mkFrame([]byte("ws-hello"), 0))

	g := &inboundGnetConn{data: wire}
	h := &countingHandler{}
	conn := &Conn{conn: g, handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()
	state := &wsConnState{conn: conn, upgraded: true}

	stale := time.Now().Add(-time.Hour).UnixNano()
	conn.lastActive.Store(stale)

	state.onTraffic()
	if h.count != 1 {
		t.Fatalf("expected 1 delivered message, got %d", h.count)
	}
	if conn.lastActive.Load() == stale {
		t.Fatal("websocket delivery must refresh the activity timestamp")
	}
}

// #P1 空闲扫描换成了整数比较，判定边界必须和原来的 idleFor 完全一致。
func TestIdleSinceMatchesIdleFor(t *testing.T) {
	now := time.Now()
	timeout := 30 * time.Second
	cutoff := now.UnixNano() - int64(timeout)

	for _, age := range []time.Duration{0, time.Second, timeout - time.Nanosecond, timeout, timeout + time.Second} {
		c := &Conn{}
		c.lastActive.Store(now.Add(-age).UnixNano())
		if got, want := c.idleSince(cutoff), c.idleFor(now) >= timeout; got != want {
			t.Fatalf("age=%v: idleSince=%v, idleFor-based=%v", age, got, want)
		}
	}
}

// ---------------------------------------------------------------- WS 帧头

// #P2 自己解 WebSocket 帧头替掉了 ws.ReadHeader（后者只吃 io.Reader，每帧要付
// 一个 bytes.Reader 加一块内部暂存区）。两者必须逐字段等价。
func TestParseWSHeaderMatchesReadHeader(t *testing.T) {
	lengths := []int{0, 1, 125, 126, 127, 65535, 65536}
	for _, masked := range []bool{true, false} {
		for _, op := range []ws.OpCode{ws.OpBinary, ws.OpText, ws.OpContinuation, ws.OpPing, ws.OpClose} {
			for _, n := range lengths {
				h := ws.Header{Fin: true, OpCode: op, Length: int64(n), Masked: masked}
				if masked {
					h.Mask = [4]byte{1, 2, 3, 4}
				}
				var buf bytes.Buffer
				if err := ws.WriteHeader(&buf, h); err != nil {
					t.Fatal(err)
				}
				raw := buf.Bytes()

				want, err := ws.ReadHeader(bytes.NewReader(raw))
				if err != nil {
					t.Fatalf("ReadHeader(%v): %v", h, err)
				}
				got, n2, ok, err := parseWSHeader(raw)
				if err != nil || !ok {
					t.Fatalf("parseWSHeader(%v): ok=%v err=%v", h, ok, err)
				}
				if got != want {
					t.Fatalf("header mismatch for %v:\n got %+v\nwant %+v", h, got, want)
				}
				if n2 != len(raw) {
					t.Fatalf("header length mismatch for %v: got %d want %d", h, n2, len(raw))
				}
			}
		}
	}
}

// #P2 数据不足必须报告"再等等"，而不是当成错误——原实现靠 ws.ReadHeader 返回
// io.ErrUnexpectedEOF 来表达这件事。
func TestParseWSHeaderReportsIncomplete(t *testing.T) {
	var buf bytes.Buffer
	if err := ws.WriteHeader(&buf, ws.Header{
		Fin: true, OpCode: ws.OpBinary, Length: 70000, Masked: true, Mask: [4]byte{9, 9, 9, 9},
	}); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()

	for i := 0; i < len(raw); i++ {
		_, _, ok, err := parseWSHeader(raw[:i])
		if ok || err != nil {
			t.Fatalf("prefix of %d byte(s): expected (ok=false, err=nil), got (ok=%v, err=%v)", i, ok, err)
		}
	}
}

// #P2 64 位长度的最高位必须为 0，与 ws.ReadHeader 的 ErrHeaderLengthMSB 一致：
// 放过它会让 int64 长度变成负数，后面的上限判断直接失效。
func TestParseWSHeaderRejectsLengthMSB(t *testing.T) {
	raw := []byte{0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 1}
	if _, _, _, err := parseWSHeader(raw); !errors.Is(err, ws.ErrHeaderLengthMSB) {
		t.Fatalf("expected ErrHeaderLengthMSB, got %v", err)
	}
}

// #P2 任意字节串上两个实现必须满足：
//
//	ws.ReadHeader 成功       -> parseWSHeader 也成功，且帧头逐字段相同
//	ws.ReadHeader 报硬错误   -> parseWSHeader 也报硬错误
//	ws.ReadHeader 报 EOF     -> parseWSHeader 要么"再等等"，要么已经看出这个头
//	                            无论后面再来多少字节都不可能合法（见下一条用例）
func TestParseWSHeaderQuickEquivalence(t *testing.T) {
	f := func(raw []byte) bool {
		got, n, ok, err := parseWSHeader(raw)
		want, rerr := ws.ReadHeader(bytes.NewReader(raw))

		switch {
		case rerr == nil:
			return ok && err == nil && got == want && n > 0 && n <= len(raw)
		case errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF):
			return !ok && (err == nil || errors.Is(err, ws.ErrHeaderLengthMSB))
		default:
			return !ok && err != nil
		}
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 50000}); err != nil {
		t.Fatal(err)
	}
}

// #P2 生产路径必须真的走新 parser。
//
// 上面几条只证明了 helper 与 ws.ReadHeader 等价；把 nextFrame 改回
// ws.ReadHeader、helper 原样留着的话它们全都照样通过。这条落在分配数上：
// ws.ReadHeader 每帧固定两次分配（一个 bytes.Reader 加一块内部暂存区）。
func TestWebSocketDecodeDoesNotAllocate(t *testing.T) {
	wire := buildWSWire(t, mkFrame(bytes.Repeat([]byte("x"), 512), 0))

	h := &countingHandler{}
	conn := &Conn{handler: h, codec: serverCodec(maxMessageSize)}
	conn.init()
	conn.markOpen()
	state := &wsConnState{conn: conn, upgraded: true}

	scratch := make([]byte, len(wire))
	requireNoAllocs(t, "websocket frame decoding", func() {
		copy(scratch, wire) // 去掩码是就地做的
		state.buf.Reset()
		state.gateBuf.Reset()
		state.fragmentBuf.reset()
		_, _ = state.buf.Write(scratch)
		if err := state.decodeMessages(); err != nil {
			t.Fatal(err)
		}
	})
	if h.count == 0 {
		t.Fatal("no message was delivered; the benchmark body is not exercising the parser")
	}
}

// #P2 一处有意的分歧，单独钉住。
//
// 长度字段已经完整读到、且最高位是 1 时，这个帧头无论后面再补多少字节都不可能
// 合法。ws.ReadHeader 会先要求把掩码也读齐（extra = 8 + 4），所以掩码缺字节时
// 它返回 io.ErrUnexpectedEOF——按 nextFrame 的约定就是"再等等"，于是这条必死的
// 连接要一直挂到对端把掩码补完才会被拒。
//
// parseWSHeader 在拿到长度的当下就判掉。方向只有这一个：ws.ReadHeader 报硬错误
// 的输入，parseWSHeader 一定也报同样的硬错误（上一条用例覆盖），所以这里只可能
// 更早失败，不可能误杀。
func TestParseWSHeaderFailsFastOnInvalidLength(t *testing.T) {
	// FIN+opcode, MASK|127, 8 字节长度（MSB=1），掩码只给了 3 字节。
	raw := []byte{0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 1, 0xaa, 0xbb, 0xcc}

	if _, rerr := ws.ReadHeader(bytes.NewReader(raw)); !errors.Is(rerr, io.ErrUnexpectedEOF) {
		t.Fatalf("precondition: expected ws.ReadHeader to want more data, got %v", rerr)
	}
	if _, _, ok, err := parseWSHeader(raw); ok || !errors.Is(err, ws.ErrHeaderLengthMSB) {
		t.Fatalf("expected an immediate ErrHeaderLengthMSB, got ok=%v err=%v", ok, err)
	}
}

// ---------------------------------------------------------------- 出站分配

// #P3 pushSeparate 的暂存区改成了池化对象，一次 flush 不应再有任何分配。
// 单条消息的分组此前还会因为变参调用额外构造一个 1 元素切片。
func TestPushSeparateDoesNotAllocate(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 2 << 20})
	buf := make([]sendMsg, 64)
	for i := range buf {
		buf[i] = compoundMsg(bytes.Repeat([]byte("m"), 96))
	}

	requireNoAllocs(t, "pushSeparate", func() { s.pushSeparate(buf) })
}

// #P3 单消息分组（cluster 只收到一条）走的是另一条分支，同样不该分配。
func TestPushTcpSingleMessageGroupDoesNotAllocate(t *testing.T) {
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 2 << 20, MaxClusterSize: 32 << 10})
	buf := []sendMsg{compoundMsg(bytes.Repeat([]byte("m"), 96))}

	requireNoAllocs(t, "a single-message group", func() { s.pushTcp(buf) })
}

// #P3 归还池对象之前必须清空：vb 里装的是 mcache 借来的 payload，留在池对象里
// 就等于让一块已经还回去的内存被下一次 flush 之前一直引用着。
func TestSeparateScratchIsClearedBeforeReuse(t *testing.T) {
	// CompressThreshold 必须开着，否则 vc 全程是空的，"清理 vc"那一半根本没被检查到。
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 8 << 20, CompressThreshold: 64})

	// 必须跨多个 chunk，而且最后一个 chunk 要比前面的短。
	//
	// pushSeparate 每个 chunk 都从 vb[:0] 重填，归还时 vb 的长度只是最后一个
	// chunk 的：33 条消息 = 满一个 32 条的 chunk（64 项）再加一个 1 条的
	// chunk（2 项）。只按长度清理的话，索引 2..63 上会留着第一个 chunk 那批
	// 已经还给 mcache 的 payload。一条消息的 flush 根本走不到这个形状。
	// 消息要够大且允许压缩，vc（压缩暂存区）才会跟着被填满——只用明文消息的话
	// vc 全程是空的，那一半的清理根本没被检查到。
	payload := bytes.Repeat([]byte("compress-me-"), 200)
	buf := make([]sendMsg, separateChunk+1)
	for i := range buf {
		buf[i] = sendMsg{maskPermit: maskZ, data: payload}
	}
	s.pushSeparate(buf)

	// sync.Pool.Get 不保证取回刚归还的那一个，所以一次性借出若干再逐个检查：
	// 只要池里还留着脏对象，总有一次会拿到它。
	scratches := make([]*separateScratch, 4)
	for i := range scratches {
		scratches[i] = getSeparateScratch()
	}
	defer func() {
		for _, sc := range scratches {
			putSeparateScratch(sc)
		}
	}()
	for n, sc := range scratches {
		for i, b := range sc.vb[:cap(sc.vb)] {
			if b != nil {
				t.Fatalf("scratch %d: vb[%d] still references %d byte(s) after being returned", n, i, len(b))
			}
		}
		for i, b := range sc.vc[:cap(sc.vc)] {
			if b != nil {
				t.Fatalf("scratch %d: vc[%d] still references %d byte(s) after being returned", n, i, len(b))
			}
		}
	}
}

// #P4 唤醒回调的方法值只在构造时求一次。
//
// 断言必须落在分配数上：只判 wakeCb 非 nil 的话，"字段留着、Wake 处仍写
// s.callback"这种回退照样通过——而那正是每次 arm 一个堆上闭包的写法。
// 顺带把整条 Send+flush 路径的零分配一起钉住。
func TestSenderSendFlushDoesNotAllocate(t *testing.T) {
	s, w, _ := newTestSender(t, &SenderConfig{MaxBufferSize: 8 << 20})
	payload := bytes.Repeat([]byte("x"), 96)

	requireNoAllocs(t, "Send+flush", func() {
		if err := s.Send(payload); err != nil {
			t.Fatal(err)
		}
		s.flush()
		w.mu.Lock()
		w.stream.Reset()
		w.mu.Unlock()
	})
}

// #P10 一条本身就大过整个配额的消息应当立刻被拒，而不是先完整复制一份再扔掉。
//
// 这条用例只钉语义边界和错误优先级——"少拷一次"本身没有任何可观察的行为差异
// （两条路径返回同一个 error），它的收益体现在 BenchmarkSenderRejectOversize 的
// ns/op 是否随 payload 增长。
func TestSenderRejectsMessageOverBudgetWithoutCopying(t *testing.T) {
	const limit = 1024
	s, _, _ := newTestSender(t, &SenderConfig{MaxBufferSize: limit})

	// 边界是"线路字节"，含 header：恰好占满配额的消息必须被接受。
	exact := make([]byte, limit-frameOverhead(limit))
	if err := s.Send(exact); err != nil {
		t.Fatalf("a message that exactly fills the budget must be accepted: %v", err)
	}
	s.flush()

	// 多一个字节就放不下了。
	if err := s.Send(make([]byte, len(exact)+1)); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("expected ErrSendQueueFull, got %v", err)
	}

	// 连接关闭优先于配额：业务通常按 ErrConnClosed 决定要不要重连。
	//
	// 这里命中的是 send 顶部那个 isClosed 检查。超配额分支里还有一个同样的检查，
	// 它只在"连接恰好在这两步之间被关掉"的竞态窗口里可达，测试没法稳定构造，
	// 所以那一行是纯防御——它存在的意义只是让两条路径的错误优先级一致。
	s.conn.markClosed()
	if err := s.Send(make([]byte, len(exact)+1)); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("a closed connection must report ErrConnClosed, got %v", err)
	}
}

// ---------------------------------------------------------------- 握手期唤醒

// #P5 连接还停在握手阶段时不该 arm：那一代唤醒醒来也只会在 take() 里原路返回。
func TestSenderDoesNotWakeWhileHandshaking(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{})
	s.conn.state.Store(uint32(stateHandshaking))

	for i := 0; i < 8; i++ {
		if err := s.Send([]byte("during-handshake")); err != nil {
			t.Fatal(err)
		}
	}
	if g.wakeCount() != 0 {
		t.Fatalf("handshaking connection armed %d useless wake(s)", g.wakeCount())
	}
}

// #P5 不 arm 但绝不能丢：握手完成后 notifyReady 的 flush 必须把它们全放出去，
// 而且之后的 Send 要恢复正常 arm。
func TestHandshakeQueuedMessagesFlushOnReady(t *testing.T) {
	s, w, g := newTestSender(t, &SenderConfig{})
	s.conn.state.Store(uint32(stateHandshaking))

	for _, m := range [][]byte{[]byte("first"), []byte("second")} {
		if err := s.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	if w.callCount() != 0 {
		t.Fatal("nothing may reach the socket before the handshake completes")
	}

	// 这正是 wsConnState.upgrade 的顺序：先 markOpen，再 notifyReady -> flush。
	s.conn.markOpen()
	if !s.conn.notifyReady() {
		t.Fatal("notifyReady must succeed")
	}

	requireMessages(t, frameMessages(t, parseWire(t, w.wire())[0], nil), []byte("first"))
	got := parseWire(t, w.wire())
	if len(got) != 2 {
		t.Fatalf("expected both queued messages on the wire, got %d frame(s)", len(got))
	}

	// 握手结束之后必须恢复正常唤醒，否则后续消息就真的没人管了。
	if err := s.Send([]byte("third")); err != nil {
		t.Fatal(err)
	}
	if g.wakeCount() != 1 {
		t.Fatalf("expected the post-handshake send to arm a wake, got %d", g.wakeCount())
	}
}

// #P5 关闭后的 Send 在入口就失败，不入队也不唤醒。
//
// 这条用例覆盖不到 arm 表达式里的 isClosed() 分支——那个分支要求"连接在入队之后、
// 读 writable() 之前被关掉"，测试没法稳定制造这个交错。它存在的意义是让
// isClosed() 这一项不会在"关闭中"的连接上把消息永久留在队列里：真到了那个交错，
// arm 会照常发生，那一代 callback 负责把队列排空并归还池化内存。
func TestSenderRejectsSendAfterClose(t *testing.T) {
	s, _, g := newTestSender(t, &SenderConfig{})
	s.conn.markClosed()

	if err := s.Send([]byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("expected ErrConnClosed, got %v", err)
	}
	if g.wakeCount() != 0 {
		t.Fatalf("a rejected send must not wake, got %d", g.wakeCount())
	}
}

// #P5 业务用 wrapper 包住内置 sender 时，握手期入队的消息一样要被排空。
//
// flush() 是包私有方法，嵌入 SenderI 的 wrapper 永远不满足那个接口，所以
// notifyReady 不能靠对 c.sender 做类型断言来找排空钩子——那样这批消息会一直
// 滞留到连接关闭。内置 sender 在构造时自己登记（见 Conn.flushHook）。
func TestWrappedSenderStillFlushesHandshakeQueue(t *testing.T) {
	g := &fakeGnetConn{}
	w := &captureWriter{}
	conn := &Conn{conn: g, outbound: w}
	conn.init()
	// WebSocket 的起点：还没 markOpen。

	inner := NewSenderBuilder(&SenderConfig{}).Build(conn)
	// 一个完全合法的埋点包装：嵌入接口，只覆盖它关心的方法。
	conn.sender = &countingSenderWrapper{SenderI: inner}

	if err := conn.SendNoEncrypt([]byte("queued-during-handshake")); err != nil {
		t.Fatal(err)
	}
	if w.callCount() != 0 {
		t.Fatal("nothing may reach the socket before the handshake completes")
	}

	conn.markOpen()
	if !conn.notifyReady() {
		t.Fatal("notifyReady must succeed")
	}

	frames := parseWire(t, w.wire())
	if len(frames) != 1 {
		t.Fatalf("the queued message was stranded: got %d frame(s) on the wire", len(frames))
	}
	requireMessages(t, frameMessages(t, frames[0], nil), []byte("queued-during-handshake"))
}

// countingSenderWrapper 模拟业务侧最常见的 SenderI 装饰器写法。
type countingSenderWrapper struct {
	SenderI
	sent int
}

func (w *countingSenderWrapper) SendNoEncrypt(b []byte) error {
	w.sent++
	return w.SenderI.SendNoEncrypt(b)
}

// ---------------------------------------------------------------- 连接计数

// #P6 不配上限时完全不碰 connCnt（它是所有 event loop 共享的一条 cache line）。
func TestReserveSlotSkipsCounterWhenUnlimited(t *testing.T) {
	e := &ev{c: &Config{}}
	for i := 0; i < 16; i++ {
		if !e.reserveSlot() {
			t.Fatal("an unlimited server must never reject a connection")
		}
	}
	if got := e.connCnt.Load(); got != 0 {
		t.Fatalf("connCnt must stay untouched when unlimited, got %d", got)
	}
	e.releaseSlot()
	if got := e.connCnt.Load(); got != 0 {
		t.Fatalf("releaseSlot must stay untouched when unlimited, got %d", got)
	}
}

// #P6 配了上限就必须照旧生效，而且并发占位不能冲破它。
func TestReserveSlotStillEnforcesLimitConcurrently(t *testing.T) {
	const limit = 32
	e := &ev{c: &Config{MaxConnections: limit}}

	var (
		wg      sync.WaitGroup
		granted int64
		mu      sync.Mutex
	)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := int64(0)
			for j := 0; j < 100; j++ {
				if e.reserveSlot() {
					local++
				}
			}
			mu.Lock()
			granted += local
			mu.Unlock()
		}()
	}
	wg.Wait()

	if granted != limit {
		t.Fatalf("granted %d slot(s), want exactly %d", granted, limit)
	}
}

// ---------------------------------------------------------------- client

// countingConn 数 Read 调用次数，用来验证入站系统调用被缓冲合并了。
//
// 读到底之后会挂住而不是返 EOF：否则 readLoop 立刻把 client 关掉，测试还没
// 来得及观察任何东西。Close 会放开它。
type countingConn struct {
	net.Conn

	mu    sync.Mutex
	rd    *bytes.Reader
	reads int
	out   bytes.Buffer

	closeOnce sync.Once
	closed    chan struct{}
}

func newCountingConn(wire []byte) *countingConn {
	return &countingConn{rd: bytes.NewReader(wire), closed: make(chan struct{})}
}

func (c *countingConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	c.reads++
	n, err := c.rd.Read(p)
	c.mu.Unlock()
	if errors.Is(err, io.EOF) {
		<-c.closed
		return 0, io.EOF
	}
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.Write(p)
}

func (c *countingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *countingConn) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// #P7 client 的 readLoop 之前直接对着裸 net.Conn 读，codec.readFrame 每帧要发
// 2~3 次 read。夹一层 bufio 之后一次 read 应该覆盖掉一整批帧。
func TestClientReadCoalescesSyscalls(t *testing.T) {
	const n = 64
	var wire []byte
	for i := 0; i < n; i++ {
		wire = append(wire, mkFrame([]byte("payload"), 0)...)
	}

	cc := newCountingConn(wire)
	got := make(chan int, 1)
	h := &collectHandler{done: got, want: n}

	c, err := newClientWithConn(cc, &ClientConfig{Addr: "x", Handler: h, SendQueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	select {
	case delivered := <-got:
		if delivered != n {
			t.Fatalf("delivered %d message(s), want %d", delivered, n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for messages")
	}

	// 每帧 2 次读是旧行为；这里给足余量，只要求"远小于逐帧读"。
	// 末尾还会多一次挂住的读，所以不做精确断言。
	if reads := cc.readCount(); reads > n/2 {
		t.Fatalf("readLoop issued %d read(s) for %d frame(s); buffering is not in effect", reads, n)
	}
}

// #P7 writeLoop 会把此刻已经排在队列里的消息攒成一次 writev。
func TestClientCollectFramesBatchesQueuedMessages(t *testing.T) {
	const queued = 40
	c := &Client{maxMessageSize: maxMessageSize, sendCh: make(chan outboundMsg, 256)}
	for i := 0; i < queued; i++ {
		c.sendCh <- newOutboundMsg([]byte("batched"))
	}

	first := <-c.sendCh
	frames, err := c.collectFrames(first, nil)
	if err != nil {
		t.Fatal(err)
	}

	// clientWriteBatch 封顶，剩下的留给下一轮。
	if want := min(queued, clientWriteBatch); len(frames) != want {
		t.Fatalf("collected %d frame(s) in one batch, want %d", len(frames), want)
	}
	if got, want := len(c.sendCh), queued-len(frames); got != want {
		t.Fatalf("%d message(s) left in the queue, want %d", got, want)
	}

	var wire []byte
	for _, f := range frames {
		wire = append(wire, f...)
	}
	for i, f := range parseWire(t, wire) {
		if f.z || f.c || f.e {
			t.Fatalf("frame %d must be a plain standalone frame, got z=%v c=%v e=%v", i, f.z, f.c, f.e)
		}
		if string(f.payload) != "batched" {
			t.Fatalf("frame %d payload = %q", i, f.payload)
		}
	}
}

// #P7 client 关闭后不再继续攒批。
//
// 关闭后队列里的消息本来就允许丢弃，但继续 drain 意味着继续调用业务的
// Cipher.Encrypt：一个昂贵的 Cipher 会把 Close 拖长最多 clientWriteBatch-1 次
// 加密的时间。这里只保证"发现关闭就停手"，剩下那个"取消息与关闭同时发生"的
// 瞬时窗口和 HEAD 一样宽（那时每条消息一次 Write，窗口同样是一条）。
func TestClientCollectFramesStopsAfterClose(t *testing.T) {
	c := &Client{
		maxMessageSize: maxMessageSize,
		sendCh:         make(chan outboundMsg, 64),
		closed:         make(chan struct{}),
	}
	for i := 0; i < 40; i++ {
		c.sendCh <- newOutboundMsg([]byte("batched"))
	}

	first := <-c.sendCh
	close(c.closed) // 模拟并发的 Close

	frames, err := c.collectFrames(first, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("collected %d frame(s) after close; the drain loop must stop", len(frames))
	}
}

// #P7 队列里只有一条时不能等：攒批完全由到达速率决定，不引入任何延迟。
func TestClientCollectFramesDoesNotWait(t *testing.T) {
	c := &Client{maxMessageSize: maxMessageSize, sendCh: make(chan outboundMsg, 8)}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if frames, err := c.collectFrames(newOutboundMsg([]byte("solo")), nil); err != nil {
			t.Error(err)
		} else if len(frames) != 1 {
			t.Errorf("collected %d frame(s) from an empty queue, want 1", len(frames))
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collectFrames blocked waiting for messages that had not arrived")
	}
}

// #P7 出站现在只分配一次：send 直接留出 header 空位，writeLoop 就地写头。
func TestClientEncodeOutboundDoesNotAllocate(t *testing.T) {
	c := &Client{maxMessageSize: maxMessageSize}
	buf := make([]byte, clientHeaderReserve+96)
	msg := outboundMsg{buf: buf}

	requireNoAllocs(t, "encodeOutbound", func() {
		if _, err := c.encodeOutbound(msg); err != nil {
			t.Fatal(err)
		}
	})
}

// #P7 预留空位的写头方式必须和原来"独立 header 数组再拼接"完全一致，
// 两种头长都要覆盖。
func TestClientEncodeOutboundMatchesWireFormat(t *testing.T) {
	for _, size := range []int{0, 1, moreHeaderSize - 1, moreHeaderSize, moreHeaderSize + 1} {
		payload := bytes.Repeat([]byte("q"), size)
		buf := make([]byte, clientHeaderReserve+size)
		copy(buf[clientHeaderReserve:], payload)

		c := &Client{maxMessageSize: maxMessageSize}
		frame, err := c.encodeOutbound(outboundMsg{buf: buf})
		if err != nil {
			t.Fatal(err)
		}

		f, n, ok, err := clientCodec(maxMessageSize, maxMessageSize).parse(frame)
		if err != nil || !ok {
			t.Fatalf("size=%d: parse failed ok=%v err=%v", size, ok, err)
		}
		if n != len(frame) {
			t.Fatalf("size=%d: frame is %d byte(s) but parse consumed %d", size, len(frame), n)
		}
		if !bytes.Equal(f.payload, payload) {
			t.Fatalf("size=%d: payload round-trip mismatch", size)
		}
		if f.z || f.c || f.e {
			t.Fatalf("size=%d: unexpected flags z=%v c=%v e=%v", size, f.z, f.c, f.e)
		}
	}
}

// #P7 解密提到了 deliver 里，"收到加密帧但没配 Cipher"仍然必须是协议错误。
func TestClientRejectsEncryptedFrameWithoutCipher(t *testing.T) {
	c := &Client{maxMessageSize: maxMessageSize, codec: clientCodec(maxMessageSize, maxMessageSize)}
	err := c.deliver(frame{payload: []byte("secret"), e: true})
	if !errors.Is(err, ErrCipherRequired) {
		t.Fatalf("expected ErrCipherRequired, got %v", err)
	}
}

// ---------------------------------------------------------------- 压缩配置

// #P8 encoder 按等级共享：同一个等级的两个 builder 不该各扛一份 encoder 池
// （Better 等级下每个池是 GOMAXPROCS × 约 4.3MB）。
func TestEncodersAreSharedPerLevel(t *testing.T) {
	a := NewSenderBuilder(&SenderConfig{CompressLevel: CompressFastest}).(*senderBuilder)
	b := NewSenderBuilder(&SenderConfig{CompressLevel: CompressFastest}).(*senderBuilder)
	if a.enc != b.enc {
		t.Fatal("two builders with the same level must share one encoder pool")
	}
	c := NewSenderBuilder(&SenderConfig{CompressLevel: CompressBest}).(*senderBuilder)
	if a.enc == c.enc {
		t.Fatal("different levels must not share an encoder pool")
	}
	// 零值必须仍然映射到 SpeedBetterCompression。断言映射本身，而不是
	// "默认 == enc"——后者是循环定义：enc 自己就是用 CompressBetter 造的，
	// 把默认改成 Fastest 也照样通过。
	if got := CompressLevel(0).zstd(); got != zstd.SpeedBetterCompression {
		t.Fatalf("the zero value maps to %v, want SpeedBetterCompression", got)
	}
	// 包级默认 builder 必须仍然用零值等级，不能被悄悄改成别的。
	if DefaultSenderBuilder.(*senderBuilder).c.CompressLevel != CompressBetter {
		t.Fatal("DefaultSenderBuilder must keep the zero-value compression level")
	}
	for _, tc := range []struct {
		lv   CompressLevel
		want zstd.EncoderLevel
	}{
		{CompressBetter, zstd.SpeedBetterCompression},
		{CompressFastest, zstd.SpeedFastest},
		{CompressBalanced, zstd.SpeedDefault},
		{CompressBest, zstd.SpeedBestCompression},
	} {
		if got := tc.lv.zstd(); got != tc.want {
			t.Fatalf("CompressLevel(%d) maps to %v, want %v", tc.lv, got, tc.want)
		}
	}
}

// #P8 换了等级之后，线路格式必须照旧：对端只认 zstd 帧，不认等级。
func TestCompressLevelRoundTrips(t *testing.T) {
	payload := bytes.Repeat([]byte("compress-me-please-"), 200)

	for _, lv := range []CompressLevel{CompressBetter, CompressFastest, CompressBalanced, CompressBest} {
		s, w, _ := newTestSender(t, &SenderConfig{
			CompressThreshold: 64,
			MaxBufferSize:     8 << 20,
			CompressLevel:     lv,
		})
		s.pushSeparate([]sendMsg{{maskPermit: maskZ, data: payload}})

		frames := parseWire(t, w.wire())
		if len(frames) != 1 {
			t.Fatalf("level %d: expected 1 frame, got %d", lv, len(frames))
		}
		if !frames[0].z {
			t.Fatalf("level %d: frame should be compressed", lv)
		}
		requireMessages(t, frameMessages(t, frames[0], nil), payload)
	}
}

// ---------------------------------------------------------------- 辅助

type countingHandler struct{ count int }

func (h *countingHandler) Handle([]byte) { h.count++ }
func (h *countingHandler) Close()        {}

type collectHandler struct {
	mu   sync.Mutex
	n    int
	want int
	done chan int
	sent bool
}

func (h *collectHandler) OnConnect(*Client) {}

func (h *collectHandler) OnMessage(*Client, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
	if h.n == h.want && !h.sent {
		h.sent = true
		h.done <- h.n
	}
}

func (h *collectHandler) OnClose(*Client, error) {}

func buildWSWire(tb testing.TB, payloads ...[]byte) []byte {
	tb.Helper()
	var out bytes.Buffer
	for _, p := range payloads {
		f := ws.MaskFrame(ws.NewFrame(ws.OpBinary, true, p))
		if err := ws.WriteFrame(&out, f); err != nil {
			tb.Fatal(err)
		}
	}
	return out.Bytes()
}

// newOutboundMsg 复刻 Client.send 的入队形态：预留 header 空位。
func newOutboundMsg(payload []byte) outboundMsg {
	buf := make([]byte, clientHeaderReserve+len(payload))
	copy(buf[clientHeaderReserve:], payload)
	return outboundMsg{buf: buf}
}

// #P9 WebSocket 出站的 iovec 暂存区也改成了池化对象。它此前是 wsOutbound 的常驻
// 字段：每条连接白扛一条最多 1+2*separateChunk 项的切片，十万连接约 150MB，
// 而这块内存只在一次 flush 期间有用。
func TestWSOutboundDoesNotRetainScratch(t *testing.T) {
	if got := unsafe.Sizeof(wsOutbound{}); got != unsafe.Sizeof((wsWriter)(nil)) {
		t.Fatalf("wsOutbound is %d bytes; it must hold nothing but the writer", got)
	}

	rec := &recordingWSWriter{}
	out := &wsOutbound{conn: rec}
	payload := bytes.Repeat([]byte("w"), 200)
	if err := out.writev([][]byte{payload[:100], payload[100:]}); err != nil {
		t.Fatal(err)
	}

	s := getWSScratch()
	defer putWSScratch(s)
	for i, b := range s.vec[:cap(s.vec)] {
		if b != nil {
			t.Fatalf("scratch vec[%d] still references %d byte(s) after being returned", i, len(b))
		}
	}
	// 帧内容不能因为换了暂存来源而改变。
	if rec.writevs != 1 {
		t.Fatalf("expected exactly one writev, got %d", rec.writevs)
	}
	if rec.lastIOV != 3 {
		t.Fatalf("expected header + 2 payload iovecs, got %d", rec.lastIOV)
	}
}

// requireNoAllocs 断言 f 在稳态下不分配内存。
//
// 两层防护：
//
//   - 开了竞态检测时直接跳过。插桩会改变逃逸分析并自带簿记分配，实测同一条
//     Send+flush 路径不带 -race 是 0、带 -race 稳定是 1，这个数在那种模式下
//     不代表被测代码。
//   - 取多轮里的最小值。这个包的测试进程里会有前面用例留下的 gnet 事件循环还在
//     跑，它们和被测代码共用同几个 sync.Pool，偶尔一次 Get 落空就会被算成一次
//     分配。噪声只会让计数变大不会变小，所以真实值就是最小值。
//
// 这仍然抓得住真正的回退：一条每次都分配的路径，任何一轮都不可能量到 0。
func requireNoAllocs(t *testing.T, what string, f func()) {
	t.Helper()
	if raceEnabled {
		t.Skipf("%s: allocation counts are distorted by race instrumentation", what)
	}
	if n := minAllocsPerRun(5, 200, f); n != 0 {
		t.Fatalf("%s allocated %.1f time(s) per run, want 0", what, n)
	}
}

func minAllocsPerRun(attempts, runs int, f func()) float64 {
	best := math.Inf(1)
	for i := 0; i < attempts; i++ {
		if n := testing.AllocsPerRun(runs, f); n < best {
			best = n
		}
		if best == 0 {
			break
		}
	}
	return best
}

// funcHandler 让用例把断言写进 Handle 内部，用来观察"handler 正在跑的那一刻"。
type funcHandler struct{ fn func([]byte) }

func (h *funcHandler) Handle(b []byte) { h.fn(b) }
func (h *funcHandler) Close()          {}
