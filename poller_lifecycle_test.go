//go:build darwin || linux

package gate

import (
	"sync"
	"testing"
)

// notify is the sole poller operation allowed from arbitrary goroutines.
// Closing it must serialize with all in-flight notifications and be idempotent.
func TestPoller_NotifyConcurrentClose(t *testing.T) {
	for range 20 {
		p, err := newPoller()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 100 {
					if err := p.notify(); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		close(start)
		if err := p.close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("notify raced with close: %v", err)
		}
		if err := p.notify(); err != nil {
			t.Fatalf("late notify: %v", err)
		}
		if err := p.close(); err != nil {
			t.Fatalf("second close: %v", err)
		}
	}
}
