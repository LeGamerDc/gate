package gate

import "testing"

func TestInboxBacklogDoesNotLoseCoalescedWakeup(t *testing.T) {
	h := newHarness(t)
	ran := false
	// This producer runs after the original inbox list was detached, but before
	// its wakeup is disarmed. Its notify coalesces with the original event.
	h.l.box.push(func() {
		h.l.box.push(func() { ran = true })
		h.l.wk.maybeNotify()
	})
	for range 2 * inboxBudget {
		h.l.box.push(func() {})
	}
	h.l.wk.maybeNotify()
	for range 2 {
		h.l.step()
	}
	if !ran && h.l.nextTimeout() != 0 {
		t.Fatalf("queued work stranded after backlog drained: timeout=%v armed=%v backlog=%v head=%v", h.l.nextTimeout(), h.l.wk.armed.Load(), h.l.boxBacklog != nil, h.l.box.head.Load() != nil)
	}
	h.l.step()
	if !ran {
		t.Fatal("queued work did not run after the nonblocking continuation")
	}
}
