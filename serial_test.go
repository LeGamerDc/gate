package gate

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// AsyncDo 注册之后回调走 error / panic 出口：按 01 D18 **不启动** f
// （「连接要关时干脆不启动，省掉一次注定没用的 RPC」），但必须把注册
// 撤销干净——注册了却既不启动也不取消的话，没有任何东西会送出 resume，
// 连接进 Detached 后永远挂在 pendingClose 上，OnClose 永不触发。
func TestSerial_AsyncDoCancelledOnErrorExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit func() error
	}{
		{"回调返回error", func() error { return errors.New("reject after async") }},
		{"回调panic", func() error { panic("boom after async") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var ran atomic.Bool
			h.onMsg = func([]byte) error {
				if err := h.c.asyncDo(func() { ran.Store(true) }); err != nil {
					t.Fatal(err)
				}
				return tc.exit()
			}
			h.feedFrames([]byte("go"))
			h.readable()

			// 连接必须能一路走完关闭，不卡在 pendingClose 上。
			waitFor(t, func() bool {
				h.advance(2 * time.Second)
				h.l.step()
				return h.closed == 1
			})
			if ran.Load() {
				t.Fatal("连接要关时不该启动 AsyncDo 的 f（01 D18）")
			}
			if h.c.asyncBusy || h.c.asyncFn != nil {
				t.Fatal("注册未撤销干净")
			}
			h.verifyConservation()
		})
	}
}

// AsyncDo(nil)：不占用 asyncBusy，否则没人会送出它的 resume。
func TestSerial_AsyncDoNilIsNoop(t *testing.T) {
	h := newHarness(t)
	h.onMsg = func([]byte) error { return h.c.asyncDo(nil) }
	h.feedFrames([]byte("go"))
	h.readable()
	if h.c.asyncBusy || h.c.pauseDepth != 0 {
		t.Fatalf("AsyncDo(nil) 不该占用串行域: busy=%v depth=%d", h.c.asyncBusy, h.c.pauseDepth)
	}
	// 后续消息照常投递。
	h.feedFrames([]byte("next"))
	h.onMsg = nil
	h.readable()
	if len(h.msgs) != 1 {
		t.Fatalf("msgs=%d", len(h.msgs))
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// OnOpen 里 Pause：tnode 已在 pause 链上，openConn 不得再把它挂进 idle 链
// （同一个侵入式节点属于两条链会直接覆盖指针）。
func TestSerial_PauseInOnOpenKeepsLRUExclusive(t *testing.T) {
	h := newHarnessNoOpen(t)
	var resume func()
	h.onOpen = func() error {
		resume = h.c.pause()
		return nil
	}
	h.l.openConn(h.c)

	if h.l.pauseLRU.empty() {
		t.Fatal("Pause 后应在 pause 链上")
	}
	if !h.l.idleLRU.empty() {
		t.Fatal("暂停中的连接不该同时在 idle 链上（链表结构会被覆盖）")
	}
	assertLRUIntact(t, &h.l.pauseLRU, 1)
	assertLRUIntact(t, &h.l.idleLRU, 0)

	resume()
	h.l.step()
	if !h.l.pauseLRU.empty() || h.l.idleLRU.empty() {
		t.Fatal("resume 后应回到 idle 链")
	}
	assertLRUIntact(t, &h.l.idleLRU, 1)

	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// OnOpen 里 AsyncDo：同样不得双挂，且任务结束后连接照常可用。
func TestSerial_AsyncDoInOnOpen(t *testing.T) {
	h := newHarnessNoOpen(t)
	ran := make(chan struct{})
	h.onOpen = func() error {
		return h.c.asyncDo(func() { close(ran) })
	}
	h.l.openConn(h.c)
	<-ran
	assertLRUIntact(t, &h.l.pauseLRU, 1)
	assertLRUIntact(t, &h.l.idleLRU, 0)

	h.feedFrames([]byte("after"))
	waitFor(t, func() bool {
		h.readable()
		return len(h.msgs) == 1
	})
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// resume 放行排队的 Post 时，若某个 Post 再次 Pause，剩下的必须留到下一次
// resume——否则它们会与新的异步任务并发跑，串行域被打破。
func TestSerial_ResumeStopsWhenPostPausesAgain(t *testing.T) {
	h := newHarness(t)
	var resume1 func()
	h.onMsg = func([]byte) error { resume1 = h.c.pause(); return nil }
	h.feedFrames([]byte("go"))
	h.readable()

	var order []int
	var resume2 func()
	h.c.post(func() { order = append(order, 1); resume2 = h.c.pause() })
	h.c.post(func() { order = append(order, 2) })
	h.c.post(func() { order = append(order, 3) })
	h.l.step()
	if len(order) != 0 {
		t.Fatal("暂停期间 Post 不该执行")
	}

	resume1()
	h.l.step()
	if len(order) != 1 || order[0] != 1 {
		t.Fatalf("第一个 Post 重新 Pause 后，剩余 Post 仍被执行: %v", order)
	}
	if h.c.pauseDepth != 1 {
		t.Fatalf("pauseDepth=%d, want 1", h.c.pauseDepth)
	}
	if h.c.curInterest&interestRead != 0 {
		t.Fatal("重新暂停后不该恢复读兴趣")
	}
	assertLRUIntact(t, &h.l.pauseLRU, 1)
	assertLRUIntact(t, &h.l.idleLRU, 0)

	resume2()
	h.l.step()
	if len(order) != 3 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("第二次 resume 后应按序放完剩余 Post: %v", order)
	}
	h.c.requestClose(nil)
	h.l.step()
	h.verifyConservation()
}

// Detached 之后到达的 resume 必须是 no-op：链表与计数在 detach 时已结清。
func TestSerial_ResumeAfterDetachIsNoop(t *testing.T) {
	h := newHarness(t)
	var resume func()
	h.onMsg = func([]byte) error { resume = h.c.pause(); return nil }
	h.feedFrames([]byte("go"))
	h.readable()

	h.c.requestClose(nil)
	h.l.step()
	h.advance(2 * time.Second)
	h.l.step() // linger 到期 → Detached → Closed
	if h.closed != 1 {
		t.Fatalf("closed=%d", h.closed)
	}
	paused := h.l.stats.connsPaused.Load()

	resume()
	h.l.step()
	if got := h.l.stats.connsPaused.Load(); got != paused {
		t.Fatalf("Detached 后的 resume 改了 ConnsPaused: %d → %d", paused, got)
	}
	if got := h.l.stats.connsPaused.Load(); got < 0 {
		t.Fatalf("ConnsPaused 变成负数: %d", got)
	}
	h.verifyConservation()
}

// assertLRUIntact 检查链表的双向指针自洽且长度符合预期。
func assertLRUIntact(t *testing.T, l *lruList, want int) {
	t.Helper()
	n := 0
	for p := l.root.next; p != &l.root; p = p.next {
		if p.next.prev != p || p.prev.next != p {
			t.Fatalf("LRU 双向指针不自洽（节点被挂进了两条链？）")
		}
		n++
		if n > want+8 {
			t.Fatal("LRU 出现环或长度失控")
		}
	}
	if n != want {
		t.Fatalf("LRU 长度 %d, want %d", n, want)
	}
}
