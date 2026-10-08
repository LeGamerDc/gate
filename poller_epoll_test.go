//go:build linux

package gate

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

// Paused reads must remain disabled even when the peer disconnects. In
// particular, level-triggered RDHUP/HUP must not spin the reactor until resume.
func TestEpollPausedPeerCloseDoesNotSpin(t *testing.T) {
	for _, fullClose := range []bool{false, true} {
		name := "half_close"
		if fullClose {
			name = "full_close"
		}
		t.Run(name, func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			peerOpen := true
			defer func() {
				if peerOpen {
					_ = unix.Close(fds[1])
				}
			}()
			p, err := newPoller()
			if err != nil {
				_ = unix.Close(fds[0])
				t.Fatal(err)
			}
			defer p.close()
			l := newLoop(p, sysIO{}, monotonicNow(), normalizeLoopConfig(Outbound{}, Limits{}, false), encoderOptions{})
			c, err := l.attach(fds[0], coreCallbacks{})
			if err != nil {
				_ = unix.Close(fds[0])
				t.Fatal(err)
			}
			defer l.detach(c)
			l.openConn(c)
			c.pause()
			if fullClose {
				if err := unix.Close(fds[1]); err != nil {
					t.Fatal(err)
				}
				peerOpen = false
			} else if err := unix.Shutdown(fds[1], unix.SHUT_WR); err != nil {
				t.Fatal(err)
			}

			n, err := p.wait(l.events, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !fullClose && n != 0 {
				t.Fatalf("paused half-close returned %d events", n)
			}
			if fullClose && n > 1 {
				t.Fatalf("full close returned %d events; want at most one", n)
			}
			for i := range n {
				if l.events[i].read {
					l.connReadable(c)
				}
			}
			if c.state != stateOpen {
				t.Fatalf("paused connection closed before resume: %v", c.state)
			}
			for range 3 {
				n, err = p.wait(l.events, 0)
				if err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("paused socket keeps reporting peer close: %+v", l.events[:n])
				}
			}

			// Rearming read interest must observe the deferred EOF.
			l.resumeOnLoop(c)
			n, err = p.wait(l.events, 0)
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 || !l.events[0].read {
				t.Fatalf("resume did not expose EOF: %+v", l.events[:n])
			}
			l.connReadable(c)
			if c.state != stateClosed || !errors.Is(c.closeReason(), ErrPeerClosed) {
				t.Fatalf("resume did not close for EOF: state=%v reason=%v", c.state, c.closeReason())
			}
		})
	}
}

func TestEpollWriteOnlyPeerCloseReportsReadHint(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	p, err := newPoller()
	if err != nil {
		_ = unix.Close(fds[1])
		t.Fatal(err)
	}
	defer p.close()
	tok := makeToken(tokConn, 1, 1)
	if err := p.add(fds[0], tok, interestWrite); err != nil {
		_ = unix.Close(fds[1])
		t.Fatal(err)
	}
	if err := unix.Close(fds[1]); err != nil {
		t.Fatal(err)
	}
	events := make([]event, 2)
	n, err := p.wait(events, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || events[0].tok != tok || !events[0].hup || !events[0].read || !events[0].write {
		t.Fatalf("write-only peer hangup must expose both EOF and write failure: %+v", events[:n])
	}
}
