package gate

import (
	"errors"
	"net/netip"
	"testing"
)

type funcHandler[S any] struct {
	open func(*Conn[S]) (S, error)
	msg  func(*Conn[S], []byte) error
	clsd func(*Conn[S], error)
}

func (h funcHandler[S]) OnOpen(c *Conn[S]) (S, error) {
	if h.open != nil {
		return h.open(c)
	}
	var zero S
	return zero, nil
}
func (h funcHandler[S]) OnMessage(c *Conn[S], m []byte) error {
	if h.msg != nil {
		return h.msg(c, m)
	}
	return nil
}
func (h funcHandler[S]) OnClose(c *Conn[S], reason error) {
	if h.clsd != nil {
		h.clsd(c, reason)
	}
}

// 壳/核分离：Detached 后 core 为 nil，需要活连接的方法全部 ✗（合法性表），
// 而 ID/Remote/Handshake 读壳上的不可变字段，任何状态——包括 OnClose 里——可用。
func TestShell_LegalityAfterDetach(t *testing.T) {
	fp := newFakePoller()
	io := &fakeIO{}
	l := newLoop(fp, io, monotonicNow(), normalizeLoopConfig(Outbound{}, Limits{}, false), encoderOptions{})

	remote := netip.MustParseAddrPort("203.0.113.7:5555")
	var closeSeen error
	var idInClose uint64
	handler := funcHandler[string]{
		open: func(c *Conn[string]) (string, error) { return "session-state", nil },
		clsd: func(c *Conn[string], reason error) {
			closeSeen = reason
			idInClose = c.ID() // OnClose 里打日志：壳字段必须可用
			if c.Remote() != remote {
				t.Error("OnClose 里 Remote 不可用")
			}
			if c.State != "session-state" {
				t.Error("OnClose 期间 State 应仍然有效")
			}
		},
	}

	core, err := l.attach(3, coreCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	shell := bindConn(handler, core, 42, remote, nil)
	l.openConn(core)

	if shell.ID() != 42 || shell.Remote() != remote || shell.Handshake() != nil {
		t.Fatal("壳字段不符")
	}
	if err := shell.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if !shell.Writable() {
		t.Fatal("Open 下应 writable")
	}

	shell.Close(errKicked)
	shell.Close(errors.New("second reason ignored")) // 幂等，第一个 reason 生效
	l.step()

	if !errors.Is(closeSeen, errKicked) || idInClose != 42 {
		t.Fatalf("closeSeen=%v id=%d", closeSeen, idInClose)
	}
	if shell.core.Load() != nil {
		t.Fatal("Detached 后 core 应为 nil")
	}

	// 合法性表 ✗ 格：全部 ErrConnClosed / no-op / false。
	if err := shell.Send([]byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if err := shell.SendAlone([]byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if err := shell.SendFunc(4, nil); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	f, _ := newFrame([]byte("b"), Outbound{}, nil)
	if err := shell.SendFrame(f); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if err := shell.AsyncDo(func() {}); !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if shell.Writable() {
		t.Fatal("关闭后 Writable 应为 false")
	}
	resume := shell.Pause() // ✗³：返回 no-op resume
	resume()
	shell.SetCipher(&xorCipher{}) // no-op
	ran := false
	shell.Post(func(*Conn[string]) { ran = true })
	l.step()
	if ran {
		t.Fatal("已关闭连接的 Post 闭包不该执行")
	}
	// 已入队的 hello 在 close 前排空（尽力 flush）。
	got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || string(got[0]) != "hello" {
		t.Fatalf("got %q", got)
	}
}
