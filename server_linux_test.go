//go:build linux

package gate

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

// Isolate the descriptor limit from other tests and the go test driver.
func TestServer_ListenFailureClosesReserveFDs(t *testing.T) {
	const childEnv = "GATE_TEST_LISTEN_FAILURE_FDS"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestServer_ListenFailureClosesReserveFDs$", "-test.count=1")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("subprocess: %v\n%s", err, out)
		}
		return
	}
	if runtime.NumCPU() < 2 {
		t.Skip("requires two reactor loops")
	}
	// Initialize Go's netpoll descriptors before taking the baseline.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	countFDs := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	baseline := countFDs()
	var original unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
		t.Fatal(err)
	}
	limited := original
	limited.Cur = min(original.Cur, 64)
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limited); err != nil {
		t.Fatal(err)
	}
	defer unix.Setrlimit(unix.RLIMIT_NOFILE, &original)
	var held []int
	defer func() {
		for _, fd := range held {
			unix.Close(fd)
		}
	}()
	for {
		fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, fd)
	}
	if len(held) < 6 {
		t.Fatal("insufficient descriptor headroom for regression")
	}
	// Two listeners + first loop's epoll/eventfd/reserve + second epoll.
	// The second loop's eventfd allocation then fails deterministically.
	for range 6 {
		unix.Close(held[len(held)-1])
		held = held[:len(held)-1]
	}
	_, err = Listen(Options[string]{Addr: "127.0.0.1:0", Loops: 2, Handler: funcHandler[string]{}})
	if !errors.Is(err, unix.EMFILE) {
		t.Fatalf("Listen error = %v, want EMFILE", err)
	}
	for _, fd := range held {
		unix.Close(fd)
	}
	held = nil
	if got := countFDs(); got != baseline {
		t.Fatalf("failed Listen leaked descriptors: before=%d after=%d", baseline, got)
	}
}
