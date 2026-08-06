package gate

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
)

// 走公开 API 建一条 harness 连接（内部测试常直接调 core，会掩盖公开路径的缺陷）。
func newShellHarness(t *testing.T, h Handler[string]) (*harness, *Conn[string]) {
	t.Helper()
	hh := newHarnessNoOpen(t)
	shell := bindShell(hh, h, nil)
	hh.l.openConn(hh.c)
	return hh, shell
}

// bindShell 把泛型壳绑上已有的 harness 连接，并保留 harness 对 OnClose 的记录
// （bindConn 会整体覆写回调，直接用它会让 h.closed / h.closeReason 永远不动）。
func bindShell(h *harness, handler Handler[string], hs *Handshake) *Conn[string] {
	shell := bindConn(handler, h.c, 1, netip.MustParseAddrPort("127.0.0.1:1"), hs)
	base := h.c.cb.onClose
	h.c.cb.onClose = func(reason error) {
		h.closed++
		h.closeReason = reason
		base(reason)
	}
	return shell
}

// 01 D15 的硬保证：Close 返回之后 Send 一定报错。
// Close 的线性化点必须是「取得 outbound 锁并置 closing」，而不是等 loop
// 处理完控制项——否则 OnMessage 里 `c.Close(nil); c.Send(x)` 会成功入队。
func TestClose_SendAfterCloseAlwaysFails(t *testing.T) {
	var sendErr error
	var closeSeen error
	h, _ := newShellHarness(t, funcHandler[string]{
		msg: func(c *Conn[string], m []byte) error {
			c.Close(nil)
			sendErr = c.Send([]byte("after-close"))
			return nil
		},
		clsd: func(c *Conn[string], reason error) { closeSeen = reason },
	})
	h.feedFrames([]byte("go"))
	h.readable()
	h.l.step()

	if !errors.Is(sendErr, ErrConnClosed) {
		t.Fatalf("Close 返回后 Send = %v, want ErrConnClosed（01 D15 硬保证）", sendErr)
	}
	if closeSeen != nil {
		t.Fatalf("reason=%v, want nil", closeSeen)
	}
	got := decodeStream(t, h.io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 0 {
		t.Fatalf("Close 之后的消息上了线路: %q", got)
	}
	h.verifyConservation()
}

// SendFunc 的 fill 里调用公开 Close：commit 必须失败，消息不得入队。
func TestClose_DuringSendFuncFillViaPublicAPI(t *testing.T) {
	h, shell := newShellHarness(t, funcHandler[string]{})
	err := shell.SendFunc(16, func(b []byte) (int, error) {
		shell.Close(errKicked)
		return copy(b, "wasted"), nil
	})
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("fill 期间 Close 后 commit = %v, want ErrConnClosed", err)
	}
	h.l.step()
	if h.io.wrote.Len() != 0 {
		t.Fatal("fill 期间关闭的消息不该上线路")
	}
	if !errors.Is(h.closeReason, errKicked) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// 第一个 reason 生效——包括 nil：Close(nil) 之后的错误不得替换它。
func TestClose_FirstReasonWinsIncludingNil(t *testing.T) {
	h, shell := newShellHarness(t, funcHandler[string]{})
	shell.Close(nil)
	shell.Close(errKicked)
	h.c.requestClose(ErrIdleTimeout)
	h.l.closeLocal(h.c, ErrProtocol)
	h.l.step()
	if h.closed != 1 || h.closeReason != nil {
		t.Fatalf("closed=%d reason=%v, want 1/nil", h.closed, h.closeReason)
	}
	h.verifyConservation()
}

// 并发关闭：无论谁赢，reason 必须来自赢得仲裁的那一个，且恰好一次 OnClose。
func TestClose_ConcurrentArbitration(t *testing.T) {
	for range 200 {
		h, shell := newShellHarness(t, funcHandler[string]{})
		errA := errors.New("A")
		errB := errors.New("B")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); shell.Close(errA) }()
		go func() { defer wg.Done(); shell.Close(errB) }()
		wg.Wait()
		h.l.step()
		if h.closed != 1 {
			t.Fatalf("OnClose 调了 %d 次", h.closed)
		}
		if !errors.Is(h.closeReason, errA) && !errors.Is(h.closeReason, errB) {
			t.Fatalf("reason=%v 不是任何一个关闭者给的", h.closeReason)
		}
		// 赢家的 reason 定了之后，Send 一定已经关闭。
		if err := shell.Send([]byte("x")); !errors.Is(err, ErrConnClosed) {
			t.Fatalf("Send=%v", err)
		}
	}
}

// W13：close 帧与并发 Send 的竞争——close 帧必须是线路上的最后一段。
// 旧实现里 wsOnDrain 先入队 close 帧、再置 closing，中间的 Send 会排到它后面。
func TestW13_NoDataFrameAfterCloseFrame(t *testing.T) {
	for range 100 {
		h := newWSHarness(t)
		shell := bindShell(h, funcHandler[string]{}, &Handshake{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if shell.Send([]byte("racing")) != nil {
					return
				}
			}
		}()
		shell.Close(nil)
		wg.Wait()
		h.l.step()

		frames := decodeServerWS(t, h.io.wrote.Bytes())
		for i, f := range frames {
			if f.op == wsOpClose && i != len(frames)-1 {
				t.Fatalf("close 帧后面还有 %d 个帧（W13）", len(frames)-1-i)
			}
		}
	}
}

// 对端发起的关闭同样不能被数据帧越过：旧实现先普通入队回帧、再 closeLocal，
// 两者之间并发的 Send 会排到 close 帧后面（W13）。
func TestW13_PeerCloseNotOvertakenByData(t *testing.T) {
	for range 100 {
		h := newWSHarness(t)
		shell := bindShell(h, funcHandler[string]{}, &Handshake{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if shell.Send([]byte("racing")) != nil {
					return
				}
			}
		}()
		// 对端 close（1000）与并发 Send 撞在一起。
		h.io.feed(wsClientFrame(true, wsOpClose, []byte{0x03, 0xE8}, testMask))
		h.readable()
		wg.Wait()
		h.l.step()

		frames := decodeServerWS(t, h.io.wrote.Bytes())
		sawClose := false
		for _, f := range frames {
			if sawClose && f.op != wsOpClose {
				t.Fatal("对端关闭的回帧之后还有数据帧（W13）")
			}
			if f.op == wsOpClose {
				sawClose = true
			}
		}
		if !sawClose {
			t.Fatal("对端 close 未回帧（RFC 6455 §5.5.1 要求回一个）")
		}
		if !errors.Is(h.closeReason, ErrPeerClosed) {
			t.Fatalf("reason=%v", h.closeReason)
		}
	}
}

// 对端不带状态码（1005）时，回帧也不带。
func TestW13_PeerCloseWithoutCodeEchoesEmpty(t *testing.T) {
	h := newWSHarness(t)
	h.io.feed(wsClientFrame(true, wsOpClose, nil, testMask))
	h.readable()
	h.l.step()
	frames := decodeServerWS(t, h.io.wrote.Bytes())
	if len(frames) != 1 || frames[0].op != wsOpClose || len(frames[0].payload) != 0 {
		t.Fatalf("frames=%v", frames)
	}
	h.verifyConservation()
}

// 写失败之后不发 close 帧、不再尝试任何写（05 最后一行 + O8）。
func TestW10_NoCloseFrameAfterWriteError(t *testing.T) {
	h := newWSHarness(t)
	_ = h.c.out.send([]byte("doomed"), flagE)
	h.io.script = []ioStep{{accept: 3, err: errConnResetStub}}
	h.l.step()

	if h.c.state != stateClosed && h.c.state != stateDetached {
		t.Fatalf("state=%d", h.c.state)
	}
	frames := decodeServerWSLoose(h.io.wrote.Bytes())
	for _, f := range frames {
		if f.op == wsOpClose {
			t.Fatal("帧流已有洞时不该再发 close 帧")
		}
	}
	before := h.io.writevN
	h.writable()
	h.l.step()
	if h.io.writevN != before {
		t.Fatal("写失败后仍在尝试写")
	}
	if !errors.Is(h.closeReason, errConnResetStub) {
		t.Fatalf("reason=%v", h.closeReason)
	}
	h.verifyConservation()
}

// decodeServerWSLoose 解析可能被截断的服务端流水（写失败场景）。
func decodeServerWSLoose(b []byte) []wsSrvFrame {
	var out []wsSrvFrame
	for len(b) >= 2 {
		op := b[0] & 0x0F
		n := int(b[1] & 0x7F)
		off := 2
		switch n {
		case 126:
			if len(b) < 4 {
				return out
			}
			n = int(b[2])<<8 | int(b[3])
			off = 4
		case 127:
			return out
		}
		if len(b) < off+n {
			return out
		}
		out = append(out, wsSrvFrame{op: op, payload: b[off : off+n]})
		b = b[off+n:]
	}
	return out
}
