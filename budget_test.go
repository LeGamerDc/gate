package gate

import (
	"errors"
	"testing"
)

// WS 下一个 WS 帧可以装下远超 rbuf 的 gate 字节：投递预算截断后剩下的
// carry 会大于 rbuf。旧实现把 carry 拷进 rbuf 再解析，直接切片越界 panic，
// 把一个完全合法的客户端打成 ErrHandlerPanic。
func TestP0_WSCarryMayExceedRbuf(t *testing.T) {
	h := newWSHarness(t, func(c *loopConfig) {
		c.maxPending = 8 << 20 // 放开 MaxPending，专测 carry 上界本身
	})
	// 一个 WS 帧里塞 3000 条小 gate 帧：超过每读事件 1024 条的投递预算，
	// 剩余部分（远大于 64KB rbuf）必须能安全地留在 carry 里。
	msgs := make([][]byte, 3000)
	for i := range msgs {
		msgs[i] = mkPayload(60)
	}
	gate := gateWire(t, msgs...)
	if len(gate) <= rbufSize {
		t.Fatalf("语料太小（%d），构造不出超 rbuf 的 carry", len(gate))
	}
	h.io.feed(wsClientFrame(true, wsOpBinary, gate, testMask))

	for range 8 {
		h.readable()
		if h.closed != 0 {
			t.Fatalf("连接被关闭: %v", h.closeReason)
		}
	}
	if len(h.msgs) != len(msgs) {
		t.Fatalf("投递 %d 条, want %d", len(h.msgs), len(msgs))
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 投递预算是每读事件的：WS 把一个读事件切成多段 emit 时不得被重置。
func TestP0_DeliverBudgetPerReadEvent(t *testing.T) {
	h := newWSHarness(t, func(c *loopConfig) { c.maxPending = 8 << 20 })
	msgs := make([][]byte, 1500)
	for i := range msgs {
		msgs[i] = []byte{byte(i)}
	}
	gate := gateWire(t, msgs...)
	// 切成 30 段 emit：局部计数会被每段重置，连接级计数不会。
	seg := len(gate) / 30
	var raw []byte
	for off := 0; off < len(gate); off += seg {
		end := min(off+seg, len(gate))
		fin := end == len(gate)
		op := byte(wsOpContinuation)
		if off == 0 {
			op = wsOpBinary
		}
		raw = append(raw, wsClientFrame(fin, op, gate[off:end], testMask)...)
	}
	h.io.feed(raw)
	h.readable()
	if len(h.msgs) > deliverBudget {
		t.Fatalf("一个读事件投递了 %d 条, 预算是 %d（每段 emit 重置了计数）",
			len(h.msgs), deliverBudget)
	}
	if len(h.msgs) == 0 {
		t.Fatal("一条都没投递")
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// 预算截断留下的 carry 同样受 MaxPending 封顶（它正是「已读出未投递」）。
func TestP0_BudgetStashRespectsMaxPending(t *testing.T) {
	h := newWSHarness(t, func(c *loopConfig) { c.maxPending = 4096 })
	msgs := make([][]byte, 2000)
	for i := range msgs {
		msgs[i] = mkPayload(40)
	}
	h.io.feed(wsClientFrame(true, wsOpBinary, gateWire(t, msgs...), testMask))
	h.readable()
	if h.closed != 1 || !errors.Is(h.closeReason, ErrPendingOverflow) {
		t.Fatalf("closed=%d reason=%v, want ErrPendingOverflow", h.closed, h.closeReason)
	}
	h.verifyConservation()
}

// ─── 全局出站预算（MaxOutboundBytes）───

func TestQuota_ServerBudgetRejectsBeyondGlobalLimit(t *testing.T) {
	budget := newServerBudget(300 << 10) // 全局 300KB
	q1, q2 := newQuotaLease(budget), newQuotaLease(budget)

	// 第一个 loop 租一段（leaseChunk = 256KB）。
	if !q1.acquire(1024) {
		t.Fatal("首次 acquire 应成功")
	}
	// 第二个 loop 只剩不到一段，但仍应能按需租到剩余部分。
	if !q2.acquire(16 << 10) {
		t.Fatal("剩余预算内的 acquire 应成功")
	}
	// 继续要一大块：全局已耗尽。
	if q2.acquire(1 << 20) {
		t.Fatal("越过 MaxOutboundBytes 的 acquire 必须失败")
	}
	// 归还之后又能租到。
	q1.release(1024)
	q1.release(200 << 10)
	if !q2.acquire(64 << 10) {
		t.Fatal("归还后应能重新租到")
	}
}

func TestQuota_UnlimitedNeverBlocks(t *testing.T) {
	q := newQuotaLease(newServerBudget(0)) // Unlimited
	for range 100 {
		if !q.acquire(1 << 20) {
			t.Fatal("Unlimited 不该拒绝")
		}
	}
	q.release(1 << 20)
}

// 全局预算耗尽时 Send 返回 ErrSendQueueFull（消息从未入队，帧流没有洞），
// 而不是关闭连接；排空之后恢复。
func TestQuota_SendRejectedWhenGlobalExhausted(t *testing.T) {
	o, io, loop := newTestOutbound(t, func(c *outConfig) { c.maxBuffer = Unlimited })
	o.env.quota = newQuotaLease(newServerBudget(64 << 10))

	msg := mkPayload(4096)
	sent := 0
	for {
		err := o.send(msg, flagE)
		if err == nil {
			sent++
			continue
		}
		if !errors.Is(err, ErrSendQueueFull) {
			t.Fatalf("err=%v, want ErrSendQueueFull", err)
		}
		break
	}
	if sent == 0 {
		t.Fatal("一条都没发出去")
	}
	loop.drainDirty() // 写出后配额归还
	if io.wrote.Len() == 0 {
		t.Fatal("已入队的消息应被送出")
	}
	if err := o.send(msg, flagE); err != nil {
		t.Fatalf("排空后应恢复: %v", err)
	}
	loop.drainDirty()
	afterOutbound(t, o)
}

// 配额与 reservedWire 必须同步：所有退还路径都过 refundLocked。
func TestQuota_ConservesAcrossAllRefundPaths(t *testing.T) {
	budget := newServerBudget(1 << 20)
	start := budget.remaining.Load()

	o, _, loop := newTestOutbound(t, func(c *outConfig) { c.compressThreshold = 32 })
	o.env.quota = newQuotaLease(budget)

	_ = o.send(mkPayload(500), flagZ|flagC|flagE)                        // 正常写出
	_ = o.sendFunc(64, func(b []byte) (int, error) { return 8, nil })    // commit 缩水
	_ = o.sendFunc(64, func(b []byte) (int, error) { return 0, errEOF }) // abort
	loop.drainDirty()

	_ = o.send(mkPayload(500), flagE)
	o.discard() // 丢弃路径

	// 配平的口径是「server 未租出的 + 本 loop 未用的 == 总量」：
	// 租约本来就允许字节停在 loop 手里（这正是近似的来源），
	// 但它们必须仍在账上。
	if got := budget.remaining.Load() + o.env.quota.avail.Load(); got != start {
		t.Fatalf("配额未配平: %d, want %d（漏了 %d）", got, start, start-got)
	}
}

var errEOF = errors.New("fill aborted")
