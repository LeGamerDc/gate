package gate

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"
	"testing"
)

// closeSend 只置 closing，不做仲裁、不记 reason。生产路径的关闭一律走
// connCore.beginClose（它才是线性化点，见 06 线性化表），所以这个方法定义在
// 测试文件里——它不该被编译进生产包，也就不可能有人在生产路径上顺手用它，
// 把 reason 丢掉（那正是写失败路径上出现过的 bug）。
// 给没有 core 的 outbound 单元测试用。
func (o *outbound) closeSend() {
	o.mu.Lock()
	o.closing = true
	o.mu.Unlock()
}

// afterOutbound 是资源配平断言（T-D5 的组件级形态）：写完之后
// reservedWire == 0、stage 2 空、stage 1 空。
func afterOutbound(t *testing.T, o *outbound) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reservedWire != 0 {
		t.Fatalf("配平失败: reservedWire=%d, want 0", o.reservedWire)
	}
	if o.head != nil || o.tail != nil || o.stage2Bytes != 0 {
		t.Fatalf("配平失败: stage2Bytes=%d head=%v", o.stage2Bytes, o.head)
	}
	if len(o.q1) != 0 {
		t.Fatalf("配平失败: stage1 残留 %d 项", len(o.q1))
	}
}

// ─── O1 / O2 / FIFO ─────────────────────────────────────────────────────────

func TestO1_SendThenFlushDeliversAll(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	msgs := [][]byte{[]byte("alpha"), {}, mkPayload(5000), []byte("omega")}
	for _, m := range msgs {
		if err := o.send(m, flagZ|flagC|flagE); err != nil {
			t.Fatal(err)
		}
	}
	if st := loop.drainDirty(); st != writeIdle {
		t.Fatalf("status=%v", st)
	}
	got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != len(msgs) {
		t.Fatalf("%d msgs, want %d", len(got), len(msgs))
	}
	for i := range msgs {
		if !bytes.Equal(got[i], msgs[i]) {
			t.Fatalf("第 %d 条不一致", i)
		}
	}
	afterOutbound(t, o)
}

func TestO2_PartialWriteNewBytesOnlyAppend(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	_ = o.send([]byte("first-message"), flagE)
	// 第一次 writev 只收 3 字节并报 EAGAIN。
	io.script = []ioStep{{accept: 3, err: syscall.EAGAIN}}
	if st := loop.drainDirty(); st != writeBlocked {
		t.Fatal("want writeBlocked")
	}
	prefix := append([]byte(nil), io.wrote.Bytes()...)

	// 阻塞期间继续 Send：新字节只能出现在尾部（O2 严格 FIFO）。
	_ = o.send([]byte("second"), flagE)
	loop.drainDirty() // 脚本耗尽，全收
	full := io.wrote.Bytes()
	if !bytes.HasPrefix(full, prefix) {
		t.Fatal("已写出的前缀被改写")
	}
	got := decodeStream(t, full, nil, maxMessageSize)
	if len(got) != 2 || string(got[0]) != "first-message" || string(got[1]) != "second" {
		t.Fatalf("got %q", got)
	}
	afterOutbound(t, o)
}

// ─── O3 / O12：部分写在每个字节位置断一次，流水必须与一次性写出逐字节相同 ─────

func TestO3_PartialWriteNeverReencodes(t *testing.T) {
	// 混合负载：可合包组、不可合包大消息、barrier 后的加密帧——
	// 凑出 inline/pooled 交错、含 WS 帧头 chunk 的链。
	build := func(o *outbound) {
		_ = o.send([]byte("hello"), flagZ|flagC|flagE)
		_ = o.send([]byte("wo"), flagZ|flagC|flagE)
		_ = o.send(mkPayload(100), flagE) // 不可合包
		o.setCipher(&xorCipher{key: 0x11})
		_ = o.send([]byte("ciphered"), flagZ|flagC|flagE)
	}
	wsWrap := func(total int, dst []byte) int { // 编码期封帧桩：3 字节帧头
		dst[0], dst[1], dst[2] = 0xFA, byte(total>>8), byte(total)
		return 3
	}
	cfg := func(c *outConfig) {
		c.maxCluster = 64
		c.wsWrap = wsWrap
		c.wireSlack = wireSlackWS
	}

	// 基准：一次性全收。
	oRef, ioRef, loopRef := newTestOutbound(t, cfg)
	build(oRef)
	loopRef.drainDirty()
	want := append([]byte(nil), ioRef.wrote.Bytes()...)
	afterOutbound(t, oRef)

	// 在每个字节位置 p 断一次：收 p 字节并报 EAGAIN，可写事件后续写。
	for p := 0; p <= len(want); p++ {
		o, io, loop := newTestOutbound(t, cfg)
		build(o)
		io.script = []ioStep{{accept: p, err: syscall.EAGAIN}}
		loop.drainDirty()
		if st, err := o.write(); err != nil || st != writeIdle {
			t.Fatalf("p=%d: 续写 st=%v err=%v", p, st, err)
		}
		if !bytes.Equal(io.wrote.Bytes(), want) {
			t.Fatalf("p=%d: 流水与一次性写出不同\n got %x\nwant %x", p, io.wrote.Bytes(), want)
		}
		afterOutbound(t, o)
	}
}

// ─── O5：三段配平 ───────────────────────────────────────────────────────────

func TestO5_BudgetChargeRefundConserves(t *testing.T) {
	sender, _ := pairGCM([16]byte{9})
	o, _, loop := newTestOutbound(t, func(c *outConfig) {
		c.compressThreshold = 32
	})
	o.setCipher(sender)

	compressible := bytes.Repeat([]byte("gate"), 200) // 压缩省出差额
	if err := o.send(compressible, flagZ|flagC|flagE); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	charged := o.reservedWire
	o.mu.Unlock()
	want := frameSize(len(compressible)+sender.Overhead()) + wireSlackTCP
	if charged != want {
		t.Fatalf("charge=%d want %d", charged, want)
	}

	// 编码后（未写出）：reservedWire 必须精确等于 stage2 的未写字节。
	items := o.take(false, false)
	if err := o.encodeItems(items); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	after := o.reservedWire
	o.mu.Unlock()
	if after != o.stage2Bytes {
		t.Fatalf("编码退还后 reservedWire=%d, stage2=%d", after, o.stage2Bytes)
	}
	if after >= charged {
		t.Fatal("压缩+AEAD 编码后应退还差额")
	}

	// 写出后归零。
	if st, err := o.write(); st != writeIdle || err != nil {
		t.Fatal(st, err)
	}
	afterOutbound(t, o)
	_ = loop
}

func TestO5_AEADGrowthCoveredByCharge(t *testing.T) {
	// 不可压缩 + AEAD：实际字节 = 明文 + Overhead + 头，charge 必须 ≥ 它。
	sender, _ := pairGCM([16]byte{7})
	o, io, loop := newTestOutbound(t)
	o.setCipher(sender)
	msg := mkPayload(100)
	_ = o.send(msg, flagZ|flagC|flagE)
	loop.drainDirty()
	wire := io.wrote.Len()
	if wire != frameSize(100+16) {
		t.Fatalf("wire=%d want %d", wire, frameSize(116))
	}
	afterOutbound(t, o)
}

// ─── O6 / O7：frameSize 口径与 compound 计量 ────────────────────────────────

func TestO6_EmptyMessageCostsQuota(t *testing.T) {
	o, _, _ := newTestOutbound(t, func(c *outConfig) {
		c.maxBuffer = 64
	})
	// 空消息 charge = frameSize(0)+slack = 6：64 字节预算只放得下 10 条。
	n := 0
	for {
		if err := o.send(nil, flagZ|flagC|flagE); err != nil {
			if !errors.Is(err, ErrSendQueueFull) {
				t.Fatal(err)
			}
			break
		}
		n++
		if n > 100 {
			t.Fatal("空消息不消耗配额：Send 循环可以涨到 OOM（W7）")
		}
	}
	if n != 64/(frameSize(0)+wireSlackTCP) {
		t.Fatalf("n=%d", n)
	}
}

func TestO7_CompoundMeteringAndClusterCap(t *testing.T) {
	o, io, loop := newTestOutbound(t, func(c *outConfig) {
		c.maxCluster = 8 // 每条空消息 frameSize=2 ⇒ 每组恰好 4 条
	})
	for range 10 {
		_ = o.send(nil, flagZ|flagC|flagE)
	}
	loop.drainDirty()

	// 线路上应是 4+4+2 的三个 compound。
	c := clientCodec(maxMessageSize, maxMessageSize)
	stream := io.wrote.Bytes()
	var groups []int
	for len(stream) > 0 {
		f, n, ok, err := c.parse(stream)
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
		if !f.c() {
			t.Fatal("空消息组应编码为 compound")
		}
		if len(f.payload) > 8 {
			t.Fatalf("compound body %d 超过 MaxCluster（O7 口径漏了子头）", len(f.payload))
		}
		cnt := 0
		if err := c.deliver(f, nil, nil, func(msg []byte, _ bool) error {
			cnt++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		groups = append(groups, cnt)
		stream = stream[n:]
	}
	if fmt.Sprint(groups) != "[4 4 2]" {
		t.Fatalf("分组 %v, want [4 4 2]", groups)
	}
	afterOutbound(t, o)
}

// ─── O8：写失败先关 outbound ────────────────────────────────────────────────

func TestO8_WriteErrorClosesOutboundFirst(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	var fatalErr error
	o.fatal = func(err error) {
		fatalErr = err
		// 失败瞬间并发的 Send 必须已经吃到 ErrConnClosed（先关 outbound 再关连接）。
		if err := o.send([]byte("racing"), flagE); !errors.Is(err, ErrConnClosed) {
			t.Errorf("fatal 回调期间 Send = %v, want ErrConnClosed", err)
		}
	}
	_ = o.send([]byte("doomed"), flagE)
	io.script = []ioStep{{accept: 2, err: syscall.ECONNRESET}}
	if st := loop.drainDirty(); st != writeFailed {
		t.Fatal("want writeFailed")
	}
	if !errors.Is(fatalErr, syscall.ECONNRESET) {
		t.Fatalf("fatal=%v", fatalErr)
	}
	afterOutbound(t, o)
	// 后续写是 no-op。
	if st, err := o.write(); st != writeIdle || err != nil {
		t.Fatal(st, err)
	}
}

// ─── O9 / SendFrame ─────────────────────────────────────────────────────────

func TestO9_SendFrameChecksBeforeRetain(t *testing.T) {
	f, err := newFrame([]byte("broadcast"), Outbound{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	o, io, loop := newTestOutbound(t)
	o.setCipher(&xorCipher{key: 1})
	if err := o.sendFrame(f); !errors.Is(err, ErrCipherConflict) {
		t.Fatalf("配了 Cipher 的连接: %v, want ErrCipherConflict", err)
	}
	o.setCipher(nil) // nil 关闭加密后恢复可用
	if err := o.sendFrame(f); err != nil {
		t.Fatal(err)
	}
	loop.drainDirty()
	got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || string(got[0]) != "broadcast" {
		t.Fatalf("got %q", got)
	}
	afterOutbound(t, o)

	closed, _, _ := newTestOutbound(t)
	closed.closeSend()
	if err := closed.sendFrame(f); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
}

// ─── O10 / O11 ──────────────────────────────────────────────────────────────

func TestO10_FlushIdempotentOnEmptyQueue(t *testing.T) {
	o, io, _ := newTestOutbound(t)
	for range 3 {
		if st := o.flush(false, false); st != writeIdle {
			t.Fatal(st)
		}
	}
	if io.wrote.Len() != 0 || io.writevN != 0 {
		t.Fatal("空队列 flush 不该产生任何写")
	}
}

func TestO11_NotFlushableSuppressesArm(t *testing.T) {
	o, _, loop := newTestOutbound(t)
	o.setFlushable(false)
	_ = o.send([]byte("during-handshake"), flagE)
	if len(loop.dirty) != 0 || loop.notifies != 0 {
		t.Fatal("不可写时不该 arm")
	}
	o.setFlushable(true)
	_ = o.send([]byte("after"), flagE)
	if len(loop.dirty) != 1 {
		t.Fatal("恢复可写后应正常 arm")
	}
}

// ─── O13：cipher barrier ────────────────────────────────────────────────────

func TestO13_BarrierSplitsEpochsAndGroups(t *testing.T) {
	key := [16]byte{42}
	sender, recv := pairGCM(key)
	o, io, loop := newTestOutbound(t, func(c *outConfig) {
		c.maxCluster = 1 << 10 // 合包全开：barrier 必须切断分组
	})
	_ = o.send([]byte("plain-a"), flagZ|flagC|flagE)
	_ = o.send([]byte("plain-b"), flagZ|flagC|flagE)
	o.setCipher(sender)
	_ = o.send([]byte("sealed-c"), flagZ|flagC|flagE)
	_ = o.send([]byte("sealed-d"), flagZ|flagC|flagE)
	loop.drainDirty()

	// 前半段明文 compound，后半段密文 compound；逐帧检查 e 位。
	stream := io.wrote.Bytes()
	cPlain := clientCodec(maxMessageSize, maxMessageSize)
	f1, n1, ok, err := cPlain.parse(stream)
	if !ok || err != nil || !f1.c() || f1.e() {
		t.Fatalf("第一帧: c=%v e=%v err=%v", f1.c(), f1.e(), err)
	}
	var part1 [][]byte
	_ = cPlain.deliver(f1, nil, nil, func(m []byte, _ bool) error {
		part1 = append(part1, append([]byte(nil), m...))
		return nil
	})
	cEnc := clientCodec(maxMessageSize, maxMessageSize)
	cEnc.requireEncrypt = true
	f2, n2, ok, err := cEnc.parse(stream[n1:])
	if !ok || err != nil || !f2.c() || !f2.e() {
		t.Fatalf("第二帧: c=%v e=%v err=%v", f2.c(), f2.e(), err)
	}
	var part2 [][]byte
	if err := cEnc.deliver(f2, recv, nil, func(m []byte, _ bool) error {
		part2 = append(part2, append([]byte(nil), m...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n1+n2 != len(stream) {
		t.Fatal("应恰好两帧")
	}
	if fmt.Sprintf("%s%s", part1[0], part1[1]) != "plain-aplain-b" ||
		fmt.Sprintf("%s%s", part2[0], part2[1]) != "sealed-csealed-d" {
		t.Fatalf("epoch 切分错误: %q %q", part1, part2)
	}
	afterOutbound(t, o)
}

func TestO13_DoubleBarrierAndEmptyFlush(t *testing.T) {
	a := &xorCipher{key: 1}
	b := &xorCipher{key: 2}
	o, io, loop := newTestOutbound(t)
	o.setCipher(a)
	o.setCipher(b) // 连续两个 barrier
	loop.drainDirty()
	if io.wrote.Len() != 0 {
		t.Fatal("只有 barrier 的 flush 不该产出字节")
	}
	if o.encCipher != Cipher(b) {
		t.Fatal("encCipher 应推进到最后一个 barrier")
	}
	_ = o.send([]byte("x"), flagZ|flagC|flagE)
	loop.drainDirty()
	rx := &xorCipher{key: 2}
	got := decodeStream(t, io.wrote.Bytes(), rx, maxMessageSize)
	if len(got) != 1 || string(got[0]) != "x" {
		t.Fatalf("got %q", got)
	}
	afterOutbound(t, o)
}

// ─── O14：dirty 发布纪律 ────────────────────────────────────────────────────

func TestO14_ArmOncePerCycle(t *testing.T) {
	o, _, loop := newTestOutbound(t)
	for range 5 {
		_ = o.send([]byte("m"), flagE)
	}
	if len(loop.dirty) != 1 || loop.notifies != 1 {
		t.Fatalf("dirty=%d notify=%d, want 1/1", len(loop.dirty), loop.notifies)
	}
	loop.drainDirty() // 消费后 armed 清零
	_ = o.send([]byte("again"), flagE)
	if len(loop.dirty) != 1 {
		t.Fatal("新一轮应重新 arm 恰好一次")
	}
	loop.drainDirty()
	afterOutbound(t, o)
}

func TestO14_InLoopSuppressesArmUntilBatchEnd(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	o.beginInLoop()
	_ = o.send([]byte("reply"), flagE) // OnMessage 里的回包
	if len(loop.dirty) != 0 {
		t.Fatal("inLoop 期间不该 arm（批收尾必然 flush）")
	}
	if st := o.flush(false, true); st != writeIdle { // 批收尾：清 inLoop + flush
		t.Fatal(st)
	}
	if io.wrote.Len() == 0 {
		t.Fatal("批收尾 flush 应写出回包")
	}
	// 收尾之后的 Send 恢复正常 arm。
	_ = o.send([]byte("later"), flagE)
	if len(loop.dirty) != 1 {
		t.Fatal("批收尾后应恢复 arm")
	}
	loop.drainDirty()
	afterOutbound(t, o)
}

// ─── 准入与 Writable ────────────────────────────────────────────────────────

func TestAdmission_TooLargeWithCipherOverhead(t *testing.T) {
	o, _, _ := newTestOutbound(t, func(c *outConfig) {
		c.maxMessage = 110
	})
	o.setCipher(mustNewGCM())
	msg := mkPayload(100)
	// frameSize(100+16)=118 > 110：贴着上限的明文在 AEAD 下必须被 Send 拒绝。
	if err := o.send(msg, flagE); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v", err)
	}
	if err := o.sendFunc(100, nil); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("SendFunc: %v", err)
	}
}

func TestWritable_HighWater(t *testing.T) {
	o, _, _ := newTestOutbound(t, func(c *outConfig) {
		c.maxBuffer = 1 << 20
		c.highWater = 64
	})
	if !o.isWritable() {
		t.Fatal("空队列应 writable")
	}
	_ = o.send(mkPayload(100), flagE)
	if o.isWritable() {
		t.Fatal("越过高水位应转 false")
	}
	o.flush(false, false) // 写光后恢复
	if !o.isWritable() {
		t.Fatal("排空后应恢复 writable")
	}
}

func mustNewGCM() Cipher {
	c, _ := pairGCM([16]byte{1})
	return c
}
