//go:build darwin || linux

package gate

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// A detached connection no longer owns the buffers held by concurrent fill
// callbacks. Each callback must retain its reservation until it returns.
func TestSendFunc_DiscardPreservesInFlightReservations(t *testing.T) {
	boom := errors.New("fill failed")
	for _, tc := range []struct {
		name string
		fill func([]byte) (int, error)
		want error
	}{
		{"success", func([]byte) (int, error) { return 6, nil }, ErrConnClosed},
		{"error", func([]byte) (int, error) { return 0, boom }, boom},
		{"negative", func([]byte) (int, error) { return -1, nil }, ErrInvalidLength},
		{"too-long", func([]byte) (int, error) { return 33, nil }, ErrInvalidLength},
		{"panic", func([]byte) (int, error) { panic("fill panic") }, ErrHandlerPanic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, _, _ := newTestOutbound(t)
			b := newServerBudget(4096)
			o.env.quota = newQuotaLease(b)
			baseline := poolOutstanding()
			if err := o.send([]byte("queued"), flagE); err != nil {
				t.Fatal(err)
			}
			const fills = 4
			entered := make(chan struct{}, fills)
			release := make(chan struct{})
			results := make(chan error, fills)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			for range fills {
				go func() {
					results <- o.sendFunc(32, func(buf []byte) (int, error) { entered <- struct{}{}; <-release; return tc.fill(buf) })
				}()
			}
			for range fills {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("fill did not start")
				}
			}
			o.discard()
			o.discard() // failLoop followed by detach must not release a reservation twice.
			want := fills * o.charge(32, nil)
			if got := reservedOf(o); got != want {
				t.Errorf("in-flight reserved=%d want=%d", got, want)
			}
			if got := o.env.quota.outstanding.Load(); got != int64(want) {
				t.Errorf("outstanding=%d want=%d", got, want)
			}
			if got := o.env.stats.outboundQueued.Load(); got != int64(want) {
				t.Errorf("queued=%d want=%d", got, want)
			}
			unblock()
			for range fills {
				select {
				case err := <-results:
					if !errors.Is(err, tc.want) {
						t.Errorf("got %v want %v", err, tc.want)
					}
				case <-time.After(time.Second):
					t.Fatal("fill did not complete")
				}
			}
			afterOutbound(t, o)
			if o.fillReserved != 0 || o.env.quota.outstanding.Load() != 0 || o.env.stats.outboundQueued.Load() != 0 || b.remaining.Load() != 4096 {
				t.Fatalf("unbalanced fill=%d outstanding=%d queued=%d remaining=%d", o.fillReserved, o.env.quota.outstanding.Load(), o.env.stats.outboundQueued.Load(), b.remaining.Load())
			}
			if got := poolOutstanding(); got != baseline {
				t.Fatalf("pool balance=%d want=%d", got, baseline)
			}
		})
	}
}

// Exercise the same accounting lifetime through a real socket and public API.
func TestServer_SendFuncDetachDuringFill(t *testing.T) {
	opened := make(chan *Conn[None], 1)
	closed := make(chan struct{})
	s, err := Listen(Options[None]{Addr: "127.0.0.1:0", Loops: 1, Limits: Limits{MaxOutboundBytes: 4096}, Handler: funcHandler[None]{
		open: func(c *Conn[None]) (None, error) { opened <- c; return None{}, nil }, clsd: func(*Conn[None], error) { close(closed) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Shutdown(context.Background()); _ = s.Wait() }()
	sock, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	c := <-opened
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	result := make(chan error, 1)
	go func() {
		result <- c.SendFunc(32, func([]byte) (int, error) { close(started); <-release; return 6, nil })
	}()
	<-started
	c.Close(nil)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("OnClose timeout")
	}
	unblock()
	if err := <-result; !errors.Is(err, ErrConnClosed) {
		t.Fatal(err)
	}
	if got := s.Stats().OutboundQueued; got != 0 {
		t.Fatalf("queued=%d", got)
	}
	if got := s.budget.remaining.Load(); got != 4096 {
		t.Fatalf("remaining=%d", got)
	}
}

func TestWS_HandshakeAdmissionFailureDoesNotOpen(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(map[bool]string{false: "connection", true: "global"}[global], func(t *testing.T) {
			h, run := hsHarness(t, &WebSocketOptions{})
			if global {
				b := newServerBudget(1 << 20)
				b.remaining.Store(0)
				h.c.out.env.quota = newQuotaLease(b)
			} else {
				h.c.out.cfg.maxBuffer = 64
			}
			run(hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/"))
			if h.opened != 0 || h.c.opened {
				t.Fatal("OnOpen called without accepted 101")
			}
			if h.io.wrote.Len() != 0 {
				t.Fatalf("non-HTTP bytes sent: %x", h.io.wrote.Bytes())
			}
			if !errors.Is(h.c.closeReason(), ErrSendQueueFull) {
				t.Fatalf("reason=%v", h.c.closeReason())
			}
			h.verifyConservation()
		})
	}
}

func TestServer_WSHandshakeAdmissionFailure(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(map[bool]string{false: "connection", true: "global"}[global], func(t *testing.T) {
			opened := make(chan struct{}, 1)
			opts := Options[None]{Addr: "127.0.0.1:0", Loops: 1, WebSocket: &WebSocketOptions{}, Handler: funcHandler[None]{open: func(c *Conn[None]) (None, error) { opened <- struct{}{}; return None{}, c.Send([]byte("welcome")) }}}
			if global {
				opts.Limits.MaxOutboundBytes = 64
			} else {
				opts.Outbound.MaxBuffer = 64
			}
			s, err := Listen(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Shutdown(context.Background()); _ = s.Wait() }()
			conn, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			if _, err := conn.Write(hsRequest("dGhlIHNhbXBsZSBub25jZQ==", "/")); err != nil {
				t.Fatal(err)
			}
			var buf [512]byte
			n, err := conn.Read(buf[:])
			if n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("want EOF without 101/business bytes: n=%d err=%v data=%x", n, err, buf[:n])
			}
			select {
			case <-opened:
				t.Fatal("OnOpen called after 101 admission failed")
			default:
			}
			waitCond(t, func() bool { return s.conns.Load() == 0 && s.handshaking.Load() == 0 })
			if st := s.Stats(); st.ConnsOpen != 0 || st.ConnsHandshaking != 0 || st.OutboundQueued != 0 {
				t.Fatalf("stats=%+v", st)
			}
		})
	}
}

// Manual read pauses do not own the serial domain, and must not delay OnClose
// once the actual AsyncDo worker has finished. Late resumes remain harmless.
func TestServer_AsyncCloseDoesNotWaitForManualPauses(t *testing.T) {
	opened := make(chan *Conn[None], 1)
	closed := make(chan struct{})
	resumes := make(chan []func(), 1)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s, err := Listen(Options[None]{Addr: "127.0.0.1:0", Loops: 1, Handler: funcHandler[None]{
		open: func(c *Conn[None]) (None, error) {
			resumes <- []func(){c.Pause(), c.Pause()}
			err := c.AsyncDo(func() { close(started); <-release })
			opened <- c
			return None{}, err
		}, clsd: func(*Conn[None], error) { close(closed) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	sock, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	c := <-opened
	<-started
	rs := <-resumes
	defer func() {
		unblock()
		for _, r := range rs {
			r()
		}
		_ = s.Shutdown(context.Background())
		_ = s.Wait()
	}()
	c.Close(nil)
	waitCond(t, func() bool { return c.core.Load() == nil })
	select {
	case <-closed:
		t.Fatal("OnClose overlapped AsyncDo")
	default:
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("manual Pause delayed OnClose after AsyncDo ended")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = s.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait delayed by manual Pause")
	}
	for _, r := range rs {
		r()
		r()
	}
}
