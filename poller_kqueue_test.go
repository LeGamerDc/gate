//go:build darwin

package gate

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newTestPoller(t *testing.T) poller {
	t.Helper()
	p, err := newPoller()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.close() })
	return p
}

// socketPair 返回一对已设非阻塞的 UNIX 流套接字。
func socketPair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if err := unix.SetNonblock(fd, true); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	return fds[0], fds[1]
}

func waitOne(t *testing.T, p poller, timeout time.Duration) []event {
	t.Helper()
	out := make([]event, 8)
	n, err := p.wait(out, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return out[:n]
}

func TestPoller_ReadEvent(t *testing.T) {
	p := newTestPoller(t)
	a, b := socketPair(t)
	tok := makeToken(tokConn, 7, 3)
	if err := p.add(a, tok, interestRead); err != nil {
		t.Fatal(err)
	}
	if evs := waitOne(t, p, 0); len(evs) != 0 {
		t.Fatal("无数据不该有事件")
	}
	if _, err := unix.Write(b, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	evs := waitOne(t, p, time.Second)
	if len(evs) != 1 || evs[0].tok != tok || !evs[0].read || evs[0].write {
		t.Fatalf("evs=%+v", evs)
	}
}

// R14：同一 fd 读写同时就绪，wait 必须聚合成一个 event。
func TestR14_KqueueAggregatesReadWrite(t *testing.T) {
	p := newTestPoller(t)
	a, b := socketPair(t)
	tok := makeToken(tokConn, 1, 1)
	// a 的发送缓冲空 ⇒ 可写；b 写入 ⇒ a 可读。两个 filter 同时就绪。
	if _, err := unix.Write(b, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := p.add(a, tok, interestRead|interestWrite); err != nil {
		t.Fatal(err)
	}
	evs := waitOne(t, p, time.Second)
	if len(evs) != 1 {
		t.Fatalf("读写分报未聚合: %d 个事件 %+v", len(evs), evs)
	}
	if !evs[0].read || !evs[0].write || evs[0].tok != tok {
		t.Fatalf("evs[0]=%+v", evs[0])
	}
}

// Pause 的机制基础：mod 摘掉读兴趣后不再有读事件，恢复后事件回来。
func TestPoller_ModTogglesReadInterest(t *testing.T) {
	p := newTestPoller(t)
	a, b := socketPair(t)
	tok := makeToken(tokConn, 2, 9)
	if err := p.add(a, tok, interestRead); err != nil {
		t.Fatal(err)
	}
	if err := p.mod(a, tok, 0); err != nil { // 摘掉读兴趣（Pause）
		t.Fatal(err)
	}
	if _, err := unix.Write(b, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if evs := waitOne(t, p, 50*time.Millisecond); len(evs) != 0 {
		t.Fatalf("已摘读兴趣仍收到事件: %+v", evs)
	}
	if err := p.mod(a, tok, interestRead); err != nil { // resume
		t.Fatal(err)
	}
	evs := waitOne(t, p, time.Second)
	if len(evs) != 1 || !evs[0].read {
		t.Fatalf("恢复读兴趣后未收到事件: %+v", evs)
	}
}

func TestPoller_DelStopsEvents(t *testing.T) {
	p := newTestPoller(t)
	a, b := socketPair(t)
	tok := makeToken(tokConn, 3, 4)
	if err := p.add(a, tok, interestRead); err != nil {
		t.Fatal(err)
	}
	if err := p.del(a); err != nil {
		t.Fatal(err)
	}
	if err := p.del(a); err != nil { // 重复 del：ENOENT 被吞掉
		t.Fatalf("重复 del: %v", err)
	}
	if _, err := unix.Write(b, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if evs := waitOne(t, p, 50*time.Millisecond); len(evs) != 0 {
		t.Fatalf("del 之后仍收到事件: %+v", evs)
	}
}

func TestPoller_NotifyWakesBlockedWait(t *testing.T) {
	p := newTestPoller(t)
	go func() {
		time.Sleep(20 * time.Millisecond)
		for range 3 { // 幂等：连发多次只保证至少醒一次
			_ = p.notify()
		}
	}()
	start := time.Now()
	evs := waitOne(t, p, 5*time.Second)
	if time.Since(start) > time.Second {
		t.Fatal("notify 未及时唤醒 wait")
	}
	if len(evs) != 1 || evs[0].tok.kind() != tokNotify {
		t.Fatalf("evs=%+v", evs)
	}
	// EV_CLEAR：送达即复位，无重复事件。
	if evs := waitOne(t, p, 20*time.Millisecond); len(evs) != 0 {
		t.Fatalf("notify 事件未复位: %+v", evs)
	}
}

func TestPoller_HupOnPeerClose(t *testing.T) {
	p := newTestPoller(t)
	a, b := socketPair(t)
	tok := makeToken(tokConn, 5, 6)
	if err := p.add(a, tok, interestRead); err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(b)
	evs := waitOne(t, p, time.Second)
	if len(evs) != 1 || !evs[0].hup || !evs[0].read {
		t.Fatalf("对端关闭应报 hup+read（仍要走一次 read 拿原因）: %+v", evs)
	}
}

func TestPoller_WaitTimeout(t *testing.T) {
	p := newTestPoller(t)
	start := time.Now()
	if evs := waitOne(t, p, 30*time.Millisecond); len(evs) != 0 {
		t.Fatal(evs)
	}
	if d := time.Since(start); d < 20*time.Millisecond || d > 500*time.Millisecond {
		t.Fatalf("timeout 偏差过大: %v", d)
	}
}
