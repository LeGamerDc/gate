package gate

import (
	"sync"
	"testing"
	"time"
)

// fakePoller：第 3 层 harness 的事件源（07 L0「单步 loop 驱动」）。
// wait 不阻塞——一次性吐出测试注入的事件；notify 置位后在下一次 wait 变成
// notify 事件。整个 harness 单线程，每个用例都是确定性重放。
type fakePoller struct {
	mu       sync.Mutex // notify 可从别的 goroutine 来（resume/AsyncDo 完成）
	queue    []event
	pending  bool // notify 置位
	timeouts []time.Duration
	dels     []int
	mods     map[int]interest
}

func newFakePoller() *fakePoller {
	return &fakePoller{mods: map[int]interest{}}
}

func (p *fakePoller) add(fd int, _ token, want interest) error {
	p.mods[fd] = want
	return nil
}

func (p *fakePoller) mod(fd int, _ token, want interest) error {
	p.mods[fd] = want
	return nil
}

func (p *fakePoller) del(fd int) error {
	p.dels = append(p.dels, fd)
	delete(p.mods, fd)
	return nil
}

func (p *fakePoller) wait(out []event, timeout time.Duration) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.timeouts = append(p.timeouts, timeout)
	n := 0
	if p.pending {
		p.pending = false
		out[n] = event{tok: notifyToken}
		n++
	}
	for n < len(out) && len(p.queue) > 0 {
		out[n] = p.queue[0]
		p.queue = p.queue[1:]
		n++
	}
	return n, nil
}

func (p *fakePoller) notify() error {
	p.mu.Lock()
	p.pending = true
	p.mu.Unlock()
	return nil
}

func (p *fakePoller) close() error { return nil }

func (p *fakePoller) inject(e event) {
	p.mu.Lock()
	p.queue = append(p.queue, e)
	p.mu.Unlock()
}

// ─── harness ───

type harness struct {
	t  *testing.T
	l  *loop
	fp *fakePoller
	io *fakeIO
	c  *connCore

	clock int64

	opened      int
	closed      int
	closeReason error
	msgs        [][]byte
	onMsg       func(msg []byte) error // 可覆写；默认收集
	onOpen      func() error
}

type harnessOpt func(*loopConfig)

func newHarness(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	h := &harness{t: t, fp: newFakePoller(), io: &fakeIO{}}
	h.clock = int64(time.Hour) // 从非零起步，便于看出「未初始化的时刻」

	cfg := normalizeLoopConfig(Outbound{}, Limits{}, false)
	for _, opt := range opts {
		opt(&cfg)
	}
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
			if h.onMsg != nil {
				return h.onMsg(msg)
			}
			h.msgs = append(h.msgs, append([]byte(nil), msg...))
			return nil
		},
		onClose: func(reason error) {
			h.closed++
			h.closeReason = reason
		},
	}
	c, err := h.l.attach(3, cb)
	if err != nil {
		t.Fatal(err)
	}
	h.c = c
	h.l.openConn(c)
	return h
}

func (h *harness) advance(d time.Duration) { h.clock += int64(d) }

// readable 注入一个读事件并跑一轮迭代。
func (h *harness) readable() {
	h.fp.inject(event{tok: makeToken(tokConn, h.c.gen, h.c.slotIdx), read: true})
	h.l.step()
}

// writable 注入一个可写事件并跑一轮迭代。
func (h *harness) writable() {
	h.fp.inject(event{tok: makeToken(tokConn, h.c.gen, h.c.slotIdx), write: true})
	h.l.step()
}

// feedFrames 把消息编码成线路帧喂给 fake io（单帧一步）。
func (h *harness) feedFrames(msgs ...[]byte) {
	for _, m := range msgs {
		bf, err := buildFrame(append([]byte(nil), m...), 0, nil, false, nil, maxMessageSize)
		if err != nil {
			h.t.Fatal(err)
		}
		h.io.feed(wire(&bf))
	}
}

// verifyConservation 是 07 L3 的 afterEach：字节所有权配平 + 结构清空。
// 在连接完全关闭后调用。
func (h *harness) verifyConservation() {
	h.t.Helper()
	l, c := h.l, h.c
	if c.state != stateClosed && c.state != stateDetached {
		h.t.Fatalf("配平检查要求连接已关闭, state=%d", c.state)
	}
	c.out.mu.Lock()
	if c.out.reservedWire != 0 {
		h.t.Fatalf("reservedWire=%d", c.out.reservedWire)
	}
	if c.out.head != nil || len(c.out.q1) != 0 {
		h.t.Fatal("出站队列残留")
	}
	c.out.mu.Unlock()
	if c.in.carry != nil || c.in.body != nil {
		h.t.Fatal("入站缓冲残留")
	}
	for name, lru := range map[string]*lruList{
		"idle": &l.idleLRU, "hs": &l.hsLRU, "pause": &l.pauseLRU,
		"stall": &l.stallLRU, "linger": &l.lingerLRU,
	} {
		if !lru.empty() {
			h.t.Fatalf("LRU %s 非空", name)
		}
	}
	if l.nconns != 0 {
		h.t.Fatalf("nconns=%d", l.nconns)
	}
	if len(l.free) != len(l.slots) {
		h.t.Fatal("槽位未全部归还")
	}
	if !h.io.closed {
		h.t.Fatal("fd 未关闭")
	}
}
