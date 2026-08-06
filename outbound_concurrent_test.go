package gate

import (
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// syncFakeLoop 是并发用例里的 dirty 链：postDirty 来自任意 goroutine。
type syncFakeLoop struct {
	mu    sync.Mutex
	dirty []*outbound
}

func (l *syncFakeLoop) postDirty(o *outbound) {
	l.mu.Lock()
	l.dirty = append(l.dirty, o)
	l.mu.Unlock()
}

func (l *syncFakeLoop) maybeNotify() {}

func (l *syncFakeLoop) pop() *outbound {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.dirty) == 0 {
		return nil
	}
	o := l.dirty[0]
	l.dirty = l.dirty[1:]
	return o
}

// 多 goroutine 并发 Send，单一 loop goroutine 排空：
//   - 每个成功返回的 Send 都必须出现在线路上（O1）；
//   - 同一 goroutine 的消息严格 FIFO（01「发送顺序」）；
//   - 结束后预算配平。
//
// 跨线程路径只入队与标脏，flush/write 全部发生在 loop goroutine 上（O4）。
func TestOutbound_ConcurrentSendersLinearize(t *testing.T) {
	const senders, perSender = 4, 500

	loop := &syncFakeLoop{}
	io := &fakeIO{}
	cfg := normalizeOutConfig(Outbound{MaxBuffer: Unlimited, MaxCluster: 256}, Limits{}, false)
	o := newOutbound(cfg, io, 3, newLoopEnv(encoderOptions{}), loop)

	var wg sync.WaitGroup
	var sent atomic.Int64
	for s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perSender {
				var m [8]byte
				binary.BigEndian.PutUint32(m[:4], uint32(s))
				binary.BigEndian.PutUint32(m[4:], uint32(i))
				if s%2 == 0 {
					if err := o.send(m[:], flagZ|flagC|flagE); err != nil {
						t.Errorf("send: %v", err)
						return
					}
				} else {
					if err := o.sendFunc(8, func(b []byte) (int, error) {
						return copy(b, m[:]), nil
					}); err != nil {
						t.Errorf("sendFunc: %v", err)
						return
					}
				}
				sent.Add(1)
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	// loop goroutine（当前 goroutine）：排空 dirty 直到发送者收工且队列干净。
	for {
		if d := loop.pop(); d != nil {
			d.flush(true, false)
			continue
		}
		select {
		case <-done:
			if d := loop.pop(); d != nil {
				d.flush(true, false)
				continue
			}
			o.flush(false, false)
			goto verify
		default:
			runtime.Gosched()
		}
	}

verify:
	got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
	if int64(len(got)) != sent.Load() {
		t.Fatalf("线路上 %d 条, 成功入队 %d 条（O1: 不静默丢消息）", len(got), sent.Load())
	}
	next := make([]uint32, senders)
	for _, m := range got {
		s := binary.BigEndian.Uint32(m[:4])
		i := binary.BigEndian.Uint32(m[4:])
		if i != next[s] {
			t.Fatalf("sender %d 乱序: got seq %d, want %d", s, i, next[s])
		}
		next[s]++
	}
	afterOutbound(t, o)
}
