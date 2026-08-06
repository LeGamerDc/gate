package gate

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"time"
)

// ─── Open：投递与回包 ───

func TestFSM_Open_DeliverAndEcho(t *testing.T) {
	h := newHarness(t)
	h.onMsg = func(msg []byte) error {
		h.msgs = append(h.msgs, append([]byte(nil), msg...))
		return h.c.out.send(append([]byte("echo:"), msg...), flagZ|flagC|flagE)
	}
	h.feedFrames([]byte("ping"))
	h.readable()

	if len(h.msgs) != 1 || string(h.msgs[0]) != "ping" {
		t.Fatalf("msgs=%q", h.msgs)
	}
	got := decodeStream(t, h.io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || string(got[0]) != "echo:ping" {
		t.Fatalf("echo=%q", got)
	}
	if h.opened != 1 || h.closed != 0 {
		t.Fatal(h.opened, h.closed)
	}
	// 回包走的是 in-loop 快路径：不应产生 dirty 唤醒。
	if h.fp.pending {
		t.Fatal("批内回包不该 notify")
	}

	h.c.requestClose(nil)
	h.l.step()
	if h.closed != 1 {
		t.Fatal("close 后应恰好一次 OnClose")
	}
	h.verifyConservation()
}

// FuzzGateCarrySplit 的确定性版：同一字节流在每个字节位置切开两段喂，
// 投递序列必须与一次性喂完全一致。含单发 2 字节空帧（复核修的死锁回归）。
func TestFSM_CarrySplitEveryPosition(t *testing.T) {
	msgs := [][]byte{[]byte("alpha"), {}, mkPayload(300), []byte("z")}
	var stream []byte
	for _, m := range msgs {
		bf, _ := buildFrame(append([]byte(nil), m...), 0, nil, false, nil, maxMessageSize)
		stream = append(stream, wire(&bf)...)
	}
	for cut := 1; cut < len(stream); cut++ {
		h := newHarness(t)
		h.io.feed(stream[:cut], stream[cut:])
		h.readable() // 预算内两段都会被读

		if len(h.msgs) != len(msgs) {
			t.Fatalf("cut=%d: %d msgs, want %d", cut, len(h.msgs), len(msgs))
		}
		for i := range msgs {
			if !bytes.Equal(h.msgs[i], msgs[i]) {
				t.Fatalf("cut=%d: 第 %d 条不一致", cut, i)
			}
		}
		h.c.requestClose(nil)
		h.l.step()
		h.verifyConservation()
	}
}

func TestFSM_SingleEmptyFrameNoDeadlock(t *testing.T) {
	h := newHarness(t)
	h.io.feed([]byte{0x00, 0x00}) // 2 字节空帧单发
	h.readable()
	if len(h.msgs) != 1 || len(h.msgs[0]) != 0 {
		t.Fatalf("空帧未投递: %q（2 字节即可定界 m=0 的帧）", h.msgs)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 大帧（> rbuf 64KB）直读：一次性申请整帧缓冲，分多次 read 填充。
func TestFSM_LargeFrameDirectRead(t *testing.T) {
	big := mkPayload(100 << 10)
	bf, _ := buildFrame(append([]byte(nil), big...), 0, nil, false, nil, maxMessageSize)
	frame := wire(&bf)

	h := newHarness(t)
	// 切成 64KB-1、1、3、其余 四段，跨多次读事件。
	cuts := []int{64<<10 - 1, 64 << 10, 64<<10 + 3}
	prev := 0
	for _, c := range cuts {
		h.io.feed(frame[prev:c])
		prev = c
	}
	h.io.feed(frame[prev:])
	for range 4 {
		h.readable()
	}
	if len(h.msgs) != 1 || !bytes.Equal(h.msgs[0], big) {
		t.Fatal("大帧未完整投递")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// ─── 关闭原因 ───

func TestFSM_ProtocolViolationCloses(t *testing.T) {
	h := newHarness(t)
	h.io.feed(rawFrame(flagZ, 3, false, []byte("abc"))) // 服务端不接受 z
	h.readable()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrProtocol) || !errors.Is(h.closeReason, ErrFlagNotAllowed) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

func TestFSM_PeerEOF(t *testing.T) {
	h := newHarness(t)
	h.io.readScript = append(h.io.readScript, readStep{}) // n=0 = EOF
	h.readable()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrPeerClosed) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// OnMessage 返回 error：同一批剩下的不再投递；返回前 Send 的仍被送出。
func TestFSM_OnMessageErrorStopsBatchButFlushes(t *testing.T) {
	reject := errors.New("bad login")
	h := newHarness(t)
	h.onMsg = func(msg []byte) error {
		h.msgs = append(h.msgs, append([]byte(nil), msg...))
		if len(h.msgs) == 2 {
			_ = h.c.out.send([]byte("rejected"), flagE) // 回一条拒绝消息再断开
			return reject
		}
		return nil
	}
	h.feedFrames([]byte("m1"), []byte("forged"), []byte("cmd-must-not-run"))
	h.readable()

	if len(h.msgs) != 2 {
		t.Fatalf("第一条判定失败后第三条仍被投递（安全语义破坏）: %q", h.msgs)
	}
	if h.closed != 1 || !errors.Is(h.closeReason, reject) {
		t.Fatalf("closed=%d reason=%v", h.closed, h.closeReason)
	}
	got := decodeStream(t, h.io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || string(got[0]) != "rejected" {
		t.Fatalf("拒绝消息未送出: %q", got)
	}
	h.verifyConservation()
}

func TestFSM_OnOpenRejectSkipsOnClose(t *testing.T) {
	h := newHarnessNoOpen(t)
	h.onOpen = func() error { return errors.New("blacklisted") }
	h.l.openConn(h.c)
	h.l.step()
	if h.opened != 1 || h.closed != 0 {
		t.Fatalf("OnOpen 拒绝不该配对 OnClose: opened=%d closed=%d", h.opened, h.closed)
	}
	h.verifyConservation()
}

func TestFSM_OnMessagePanicIsolated(t *testing.T) {
	h := newHarness(t)
	h.onMsg = func([]byte) error { panic("business bug") }
	h.feedFrames([]byte("boom"))
	h.readable()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrHandlerPanic) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// 写失败：丢弃已入队消息、立刻停止投递、OnClose 拿到底层错误。
func TestFSM_WriteErrorDiscards(t *testing.T) {
	h := newHarness(t)
	h.onMsg = func(msg []byte) error {
		return h.c.out.send([]byte("reply"), flagE)
	}
	h.io.script = []ioStep{{accept: 1, err: errConnResetStub}}
	h.feedFrames([]byte("go"))
	h.readable()
	if h.closed != 1 || !errors.Is(h.closeReason, errConnResetStub) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	if err := h.c.out.send([]byte("x"), flagE); !errors.Is(err, ErrConnClosed) {
		t.Fatal("写失败后 Send 应立即 ErrConnClosed")
	}
	h.verifyConservation()
}

// ─── Draining / linger（R15） ───

// Draining 期间不空转：阻塞后除可写事件外零次 writev。
func TestR15_DrainingNoBusyLoop(t *testing.T) {
	h := newHarness(t)
	_ = h.c.out.send(mkPayload(1000), flagE)
	// 写 10 字节即塞住；第二步兜住 close 前 Send 留下的 dirty 补写。
	h.io.script = []ioStep{{accept: 10, err: errEAGAIN}, {accept: 0, err: errEAGAIN}}
	h.c.requestClose(nil)
	h.l.step() // 进入 Draining，尝试写一次

	if h.c.state != stateDraining {
		t.Fatalf("state=%d", h.c.state)
	}
	base := h.io.writevN
	for range 5 {
		h.l.step() // 没有可写事件：绝不逐轮重试
	}
	if h.io.writevN != base {
		t.Fatalf("Draining 空转: %d 次额外 writev", h.io.writevN-base)
	}

	h.writable() // 可写事件 → 排空 → Detached → Closed
	if h.closed != 1 || h.closeReason != nil {
		t.Fatalf("closed=%d reason=%v", h.closed, h.closeReason)
	}
	got := decodeStream(t, h.io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || !bytes.Equal(got[0], mkPayload(1000)) {
		t.Fatal("尽力 flush 未把已入队消息送出")
	}
	h.verifyConservation()
}

func TestFSM_CloseLingerExpires(t *testing.T) {
	h := newHarness(t)
	_ = h.c.out.send(mkPayload(1000), flagE)
	h.io.script = []ioStep{{accept: 0, err: errEAGAIN}, {accept: 0, err: errEAGAIN}}
	h.c.requestClose(nil)
	h.l.step()
	if h.closed != 0 {
		t.Fatal("linger 未到期不该关闭")
	}
	h.advance(2 * time.Second) // CloseLinger 默认 1s
	h.l.step()
	if h.closed != 1 {
		t.Fatal("CloseLinger 到期应放弃排空直接 Detached")
	}
	h.verifyConservation()
}

func TestFSM_CloseLingerZeroImmediate(t *testing.T) {
	h := newHarness(t, func(c *loopConfig) { c.linger = 0 })
	_ = h.c.out.send(mkPayload(1000), flagE)
	h.io.script = []ioStep{{accept: 0, err: errEAGAIN}}
	h.c.requestClose(nil)
	h.l.step()
	if h.closed != 1 {
		t.Fatal("CloseLinger=0 表示立即关闭")
	}
	h.verifyConservation()
}

// ─── 超时（全走 fake clock） ───

func TestFSM_IdleTimeout(t *testing.T) {
	h := newHarness(t, func(c *loopConfig) { c.idle = int64(30 * time.Second) })
	h.advance(29 * time.Second)
	h.feedFrames([]byte("keepalive"))
	h.readable() // 交付消息 → idle touch
	h.advance(29 * time.Second)
	h.l.step()
	if h.closed != 0 {
		t.Fatal("touch 之后 29s 不该超时")
	}
	h.advance(2 * time.Second)
	h.l.step()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrIdleTimeout) {
		t.Fatalf("closed=%d reason=%v", h.closed, h.closeReason)
	}
	h.verifyConservation()
}

func TestFSM_StallTimeoutZeroProgress(t *testing.T) {
	h := newHarness(t)
	_ = h.c.out.send(mkPayload(5000), flagE)
	h.io.script = []ioStep{{accept: 0, err: errEAGAIN}}
	h.l.step() // flush → 阻塞 → 进 stall LRU
	h.advance(31 * time.Second)
	h.l.step()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrBackpressure) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// 盲区钉住：每 29 秒写出 1 字节可以永久重置 stall 计时（03 写明的已知盲区）。
// 将来加「最老消息年龄」判据时改这个用例的判据。
func TestFSM_StallBlindSpotTrickleDocumented(t *testing.T) {
	h := newHarness(t)
	_ = h.c.out.send(mkPayload(5000), flagE)
	h.io.script = []ioStep{{accept: 0, err: errEAGAIN}}
	h.l.step()
	for range 5 {
		h.advance(29 * time.Second)
		h.io.script = []ioStep{{accept: 1, err: errEAGAIN}} // 1 字节进度
		h.writable()
		if h.closed != 0 {
			t.Fatal("涓流进度重置了计时——盲区行为变了，去改文档或这个用例")
		}
	}
	h.c.requestClose(nil)
	h.advance(2 * time.Second)
	h.l.step()
	h.advance(time.Hour)
	h.l.step()
	if h.closed != 1 {
		t.Fatal("最终应能关闭")
	}
	h.verifyConservation()
}

// ─── Pause / resume / Post ───

func TestFSM_PauseMidBatchCarriesRemainder(t *testing.T) {
	var resume func()
	h := newHarness(t)
	h.onMsg = func(msg []byte) error {
		h.msgs = append(h.msgs, append([]byte(nil), msg...))
		if len(h.msgs) == 1 {
			resume = h.c.pause() // Pause 落在一批数据中间
		}
		return nil
	}
	// 三帧塞进同一次 read：Pause 必须落在一批数据中间。
	var batch []byte
	for _, m := range [][]byte{[]byte("m1"), []byte("m2"), []byte("m3")} {
		bf, _ := buildFrame(append([]byte(nil), m...), 0, nil, false, nil, maxMessageSize)
		batch = append(batch, wire(&bf)...)
	}
	h.io.feed(batch)
	h.readable()

	if len(h.msgs) != 1 {
		t.Fatalf("暂停后同批消息仍在投递: %q", h.msgs)
	}
	if h.c.in.carry == nil {
		t.Fatal("已读出未投递的字节应转 carry")
	}
	for range 3 {
		h.l.step() // 暂停期间不再投递
	}
	if len(h.msgs) != 1 {
		t.Fatal("暂停期间投递未停止")
	}

	resume()
	resume() // 幂等
	h.l.step()
	if len(h.msgs) != 3 || string(h.msgs[2]) != "m3" {
		t.Fatalf("resume 后 carry 未继续投递: %q", h.msgs)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

func TestFSM_MaxPauseTimeout(t *testing.T) {
	h := newHarness(t)
	h.onMsg = func([]byte) error { h.c.pause(); return nil }
	h.feedFrames([]byte("m"))
	h.readable()
	h.advance(61 * time.Second) // MaxPause 默认 60s
	h.l.step()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrPauseTimeout) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// 暂停不计入 idle：暂停 2 小时不触发 IdleTimeout，resume 后重新计时。
func TestFSM_PauseExcludedFromIdle(t *testing.T) {
	var resume func()
	h := newHarness(t, func(c *loopConfig) {
		c.idle = int64(30 * time.Second)
		c.maxPause = int64(4 * time.Hour)
	})
	h.onMsg = func([]byte) error { resume = h.c.pause(); return nil }
	h.feedFrames([]byte("m"))
	h.readable()
	h.advance(2 * time.Hour)
	h.l.step()
	if h.closed != 0 {
		t.Fatal("暂停期间不该按 idle 回收")
	}
	resume()
	h.l.step() // resume：重新挂回 idle LRU
	h.advance(31 * time.Second)
	h.l.step()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrIdleTimeout) {
		t.Fatalf("resume 后 idle 应重新计时: %v", h.closeReason)
	}
	h.verifyConservation()
}

// Post：暂停中排队、resume 后按序执行；关闭后不执行不报错。
func TestFSM_PostQueueAndDrop(t *testing.T) {
	var resume func()
	var ran []int
	h := newHarness(t)
	h.onMsg = func([]byte) error { resume = h.c.pause(); return nil }
	h.feedFrames([]byte("m"))
	h.readable()

	for i := range 3 {
		h.c.post(func() { ran = append(ran, i) })
	}
	h.l.step()
	if len(ran) != 0 {
		t.Fatal("暂停期间 Post 闭包不该执行")
	}
	resume()
	h.l.step()
	if len(ran) != 3 || ran[0] != 0 || ran[2] != 2 {
		t.Fatalf("resume 后应按投递顺序执行: %v", ran)
	}

	h.c.requestClose(nil)
	h.l.step()
	h.c.post(func() { ran = append(ran, 99) })
	h.l.step()
	if len(ran) != 3 {
		t.Fatal("已关闭连接的 Post 闭包不该执行")
	}
	h.verifyConservation()
}

// ─── AsyncDo ───

func TestFSM_AsyncDoSerialAndBusy(t *testing.T) {
	h := newHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var busyErr error
	h.onMsg = func([]byte) error {
		if err := h.c.asyncDo(func() {
			close(started)
			<-release
		}); err != nil {
			return err
		}
		busyErr = h.c.asyncDo(func() {}) // 同一连接同时只允许一个在途
		return nil
	}
	h.feedFrames([]byte("go"))
	h.readable()

	<-started // goroutine 在回调返回之后才启动
	if !errors.Is(busyErr, ErrAsyncBusy) {
		t.Fatalf("busyErr=%v", busyErr)
	}
	// 任务在途 = 串行域被占：投递暂停。
	h.feedFrames([]byte("queued"))
	h.readable()
	if len(h.msgs) != 0 {
		t.Fatal("AsyncDo 在途时不该投递新消息")
	}

	h.onMsg = nil // resume 之后按默认收集
	close(release)
	waitFor(t, func() bool { // resume 控制项经收件箱回 loop；恢复读兴趣后内核再报可读
		h.readable()
		return len(h.msgs) == 1
	})
	if string(h.msgs[0]) != "queued" {
		t.Fatalf("msgs=%q", h.msgs)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 06 迁移表难到达的边：Detached 时仍有在途 AsyncDo → resume 后才 OnClose。
func TestFSM_DetachedWaitsAsyncBeforeOnClose(t *testing.T) {
	h := newHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	h.onMsg = func([]byte) error {
		return h.c.asyncDo(func() { close(started); <-release })
	}
	h.feedFrames([]byte("go"))
	h.readable()
	<-started

	h.c.requestClose(errKicked) // 任务在途时关闭
	h.l.step()
	h.advance(2 * time.Second) // linger 到期 → Detached
	h.l.step()
	if h.c.state != stateDetached {
		t.Fatalf("state=%d, want Detached（拆资源不等业务代码）", h.c.state)
	}
	if !h.io.closed {
		t.Fatal("Detached 时 fd 必须已关")
	}
	if h.closed != 0 {
		t.Fatal("串行域未空闲，OnClose 不该已调用")
	}

	close(release)
	waitFor(t, func() bool {
		h.l.step()
		return h.closed == 1
	})
	if !errors.Is(h.closeReason, errKicked) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// ─── 陈旧事件（R5） ───

func TestR5_StaleTokenIgnored(t *testing.T) {
	h := newHarness(t)
	oldTok := makeToken(tokConn, h.c.gen, h.c.slotIdx)
	h.c.requestClose(nil)
	h.l.step() // 关闭：槽位 gen++

	h.io.feed([]byte{0x00, 0x00})
	h.fp.inject(event{tok: oldTok, read: true, write: true})
	h.l.step() // 陈旧事件必须被丢弃，不 panic、不投递
	if len(h.msgs) != 0 {
		t.Fatal("陈旧事件被处理了")
	}
	h.verifyConservation()
}

// ─── 辅助 ───

var (
	errEAGAIN        error = syscall.EAGAIN
	errConnResetStub error = syscall.ECONNRESET
	errKicked              = errors.New("kicked by admin")
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("waitFor 超时")
		}
		time.Sleep(time.Millisecond)
	}
}

// newHarnessNoOpen 与 newHarness 相同但不自动 openConn。
func newHarnessNoOpen(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, fp: newFakePoller(), io: &fakeIO{}}
	h.clock = int64(time.Hour)
	cfg := normalizeLoopConfig(Outbound{}, Limits{}, false)
	h.l = newLoop(h.fp, h.io, func() int64 { return h.clock }, cfg, encoderOptions{})
	cb := coreCallbacks{
		onOpen: func() error {
			h.opened++
			if h.onOpen != nil {
				return h.onOpen()
			}
			return nil
		},
		onMessage: func(msg []byte) error {
			h.msgs = append(h.msgs, append([]byte(nil), msg...))
			return nil
		},
		onClose: func(reason error) { h.closed++; h.closeReason = reason },
	}
	c, err := h.l.attach(3, cb)
	if err != nil {
		t.Fatal(err)
	}
	h.c = c
	return h
}
