package gate

import (
	"bytes"
	"errors"
	"testing"
)

func reservedOf(o *outbound) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reservedWire
}

// fill 的四种返回逐种对 01 的表（07 L2「SendFunc 三阶段」）。
func TestSendFunc_FillOutcomes(t *testing.T) {
	t.Run("正常提交前k字节", func(t *testing.T) {
		o, io, loop := newTestOutbound(t)
		err := o.sendFunc(64, func(b []byte) (int, error) {
			if len(b) != 64 {
				t.Fatalf("fill 拿到 %d 字节, want 64", len(b))
			}
			copy(b, "zerocopy")
			return 8, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		loop.drainDirty()
		got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
		if len(got) != 1 || string(got[0]) != "zerocopy" {
			t.Fatalf("got %q", got)
		}
		afterOutbound(t, o)
	})

	t.Run("k越界", func(t *testing.T) {
		o, _, _ := newTestOutbound(t)
		for _, k := range []int{-1, 65} {
			err := o.sendFunc(64, func(b []byte) (int, error) { return k, nil })
			if !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("k=%d: %v", k, err)
			}
			if reservedOf(o) != 0 {
				t.Fatalf("k=%d: 预算未退还", k)
			}
		}
	})

	t.Run("error原样返回", func(t *testing.T) {
		o, io, _ := newTestOutbound(t)
		boom := errors.New("marshal failed")
		err := o.sendFunc(64, func(b []byte) (int, error) { return 3, boom })
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
		if reservedOf(o) != 0 || io.wrote.Len() != 0 {
			t.Fatal("失败的 fill 不该留下任何痕迹")
		}
	})

	t.Run("panic走恢复屏障", func(t *testing.T) {
		o, _, _ := newTestOutbound(t)
		var panicked error
		o.onPanic = func(err error) { panicked = err }
		err := o.sendFunc(64, func(b []byte) (int, error) { panic("boom") })
		if !errors.Is(err, ErrHandlerPanic) {
			t.Fatal(err)
		}
		if !errors.Is(panicked, ErrHandlerPanic) {
			t.Fatal("panic 应按 OnMessage panic 处理（宿主关连接）")
		}
		if reservedOf(o) != 0 {
			t.Fatal("panic 后预算未退还")
		}
	})
}

// fill 期间 Close：commit 返回 ErrConnClosed，消息未入队，预算退还。
func TestSendFunc_CloseDuringFill(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	err := o.sendFunc(32, func(b []byte) (int, error) {
		if reservedOf(o) == 0 {
			t.Fatal("fill 期间预算应已扣减（未提交的缓冲也计入预算）")
		}
		o.closeSend() // Close 不等待 fill
		copy(b, "wasted")
		return 6, nil
	})
	if !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if reservedOf(o) != 0 {
		t.Fatal("预算未退还")
	}
	loop.drainDirty()
	if io.wrote.Len() != 0 {
		t.Fatal("fill 期间关闭的消息不该上线路")
	}
}

// commit 在队列 barrier 之后线性化：fill 期间 SetCipher，消息用新密钥编码。
func TestSendFunc_CommitAfterBarrierUsesNewCipher(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	err := o.sendFunc(16, func(b []byte) (int, error) {
		o.setCipher(&xorCipher{key: 0x55}) // fill 期间换密钥
		copy(b, "late-bind")
		return 9, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	loop.drainDirty()
	rx := &xorCipher{key: 0x55}
	got := decodeStream(t, io.wrote.Bytes(), rx, maxMessageSize)
	if len(got) != 1 || !bytes.Equal(got[0], []byte("late-bind")) {
		t.Fatalf("got %q", got)
	}
	afterOutbound(t, o)
}

// n=0 的 SendFunc：空消息也占 2 字节头的配额。
func TestSendFunc_ZeroLength(t *testing.T) {
	o, io, loop := newTestOutbound(t)
	if err := o.sendFunc(0, func(b []byte) (int, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	loop.drainDirty()
	got := decodeStream(t, io.wrote.Bytes(), nil, maxMessageSize)
	if len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("got %q", got)
	}
	afterOutbound(t, o)
}
