//go:build darwin || linux

package gate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// 混合流失压力（07 第 5 层的本机版）：并发建连/断连、大小消息混发、
// 随机中途断开（走 ErrPeerClosed/Reset 路径），全程可跑 -race。
// 收尾断言的是**配平**而不是吞吐：所有水位计数与 server 级计数必须归零——
// 这轮修复里大半个 P0 都是「某条路径上少配对了一次」。
func TestSoakMixedChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("soak：-short 下跳过")
	}
	env := startEcho(t, func(o *Options[string]) {
		o.Loops = 4
		o.Outbound.MaxCluster = 512
		o.Outbound.CompressThreshold = 128
		o.Limits.Idle = 5 * time.Second
	}, nil)

	var wg sync.WaitGroup
	for w := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for round := range 12 {
				conn, err := net.DialTimeout("tcp", env.addr, 3*time.Second)
				if err != nil {
					t.Errorf("dial: %v", err)
					return
				}
				cl := &tcpClient{t: t, conn: conn, cdc: clientCodec(maxMessageSize, maxMessageSize)}
				n := 1 + rng.Intn(20)
				for i := range n {
					size := rng.Intn(3000)
					if rng.Intn(20) == 0 {
						size = 70 << 10 // 偶尔来个大帧
					}
					cl.sendMsgs(append([]byte(fmt.Sprintf("%d-%d:", w, i)), mkPayload(size)...))
				}
				if round%3 == 0 {
					_ = conn.Close() // 随机中途断开：走 ErrPeerClosed/Reset 路径
					continue
				}
				cl.readMsgs(n)
				_ = conn.Close()
			}
		}()
	}
	wg.Wait()

	waitCond(t, func() bool { return env.srv.Stats().ConnsOpen == 0 })
	st := env.srv.Stats()
	if st.OutboundQueued != 0 || st.PendingInbound != 0 || st.ConnsOverHighWater != 0 || st.ConnsPaused != 0 {
		t.Fatalf("水位未归零: %+v", st)
	}
	if env.srv.live.Load() != 0 || env.srv.conns.Load() != 0 || env.srv.handshaking.Load() != 0 {
		t.Fatalf("计数未归零: live=%d conns=%d hs=%d",
			env.srv.live.Load(), env.srv.conns.Load(), env.srv.handshaking.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := env.srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("stats: in=%d out=%d bytesIn=%d bytesOut/raw=%d/%d frames=%d poolMiss=%d",
		st.MessagesIn, st.MessagesOut, st.BytesIn, st.BytesOut, st.BytesOutRaw, st.FramesOut, st.PoolMiss)
}
