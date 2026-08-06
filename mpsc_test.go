package gate

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestDirtyListFIFO(t *testing.T) {
	var l dirtyList
	a, b, c := &outbound{}, &outbound{}, &outbound{}
	l.push(a)
	l.push(b)
	l.push(c)
	var got []*outbound
	for o := l.drain(); o != nil; o = o.dirtyNext {
		got = append(got, o)
	}
	if len(got) != 3 || got[0] != a || got[1] != b || got[2] != c {
		t.Fatal("drain 应按 push 顺序返回")
	}
	if l.drain() != nil {
		t.Fatal("二次 drain 应为空")
	}
}

func TestInboxConcurrentPushOrder(t *testing.T) {
	const producers, per = 8, 200
	var b inbox
	var seen [producers]int32
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				b.push(func() {
					// 单生产者内 FIFO：i 必须等于该生产者已见计数。
					if seen[p] != int32(i) {
						t.Errorf("producer %d 乱序: got %d want %d", p, i, seen[p])
					}
					seen[p]++
				})
			}
		}()
	}
	wg.Wait()
	total := 0
	for n := b.drain(); n != nil; n = n.next {
		n.fn()
		total++
	}
	if total != producers*per {
		t.Fatalf("drain %d 项, want %d", total, producers*per)
	}
}

type countingNotifier struct {
	poller
	n atomic.Int32
}

func (c *countingNotifier) notify() error {
	c.n.Add(1)
	return nil
}

// R7：N 次投递合并为一次 notify——只有空→非空的那个生产者真正调 notify。
func TestWakerCoalescesNotify(t *testing.T) {
	p := &countingNotifier{}
	w := &waker{p: p}
	for range 100 {
		w.maybeNotify()
	}
	if p.n.Load() != 1 {
		t.Fatalf("notify 调了 %d 次, want 1", p.n.Load())
	}
	w.disarm() // 消费侧先清标志再 drain
	w.maybeNotify()
	if p.n.Load() != 2 {
		t.Fatal("disarm 后第一次投递应再次 notify")
	}
}

// 丢事件回归：disarm 必须发生在 drain 之前。模拟「drain 完才 disarm」的错误
// 顺序会让 drain 后 push 的项目等不到唤醒——这里验证正确顺序下不会丢。
func TestWakerDisarmBeforeDrain(t *testing.T) {
	p := &countingNotifier{}
	w := &waker{p: p}
	var b inbox

	w.maybeNotify() // 第一轮投递
	b.push(func() {})

	// loop 侧：先 disarm 再 drain。
	w.disarm()
	drained := 0
	for n := b.drain(); n != nil; n = n.next {
		drained++
	}
	// drain 之后新的投递必须能触发新一次 notify。
	b.push(func() {})
	w.maybeNotify()
	if p.n.Load() != 2 {
		t.Fatalf("disarm 后的投递未触发 notify（丢事件）")
	}
	if drained != 1 {
		t.Fatal(drained)
	}
}
