//go:build linux

package gate

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"testing"
	"time"
)

func TestServer_LateResumeCannotWriteReusedFD(t *testing.T) {
	resumes := make(chan func(), 1)
	opened := make(chan *Conn[None], 1)
	s, err := Listen(Options[None]{Addr: "127.0.0.1:0", Loops: 1, Handler: funcHandler[None]{open: func(c *Conn[None]) (None, error) { resumes <- c.Pause(); opened <- c; return None{}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	<-opened
	resume := <-resumes
	old := s.loops[0].p.(*epollPoller).efd
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = s.Wait()
	// Obtain unrelated eventfds in this test process until the old descriptor is reused.
	var fds []int
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	reused := false
	for i := 0; i < 32; i++ {
		fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
		if err != nil {
			t.Fatal(err)
		}
		fds = append(fds, fd)
		if fd == old {
			reused = true
			break
		}
	}
	if !reused {
		t.Skip("could not reuse descriptor")
	}
	resume()
	var buf [8]byte
	n, err := unix.Read(old, buf[:])
	if n > 0 || !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("late resume wrote to unrelated reused fd %d: n=%d err=%v bytes=%x", old, n, err, buf)
	}
}
