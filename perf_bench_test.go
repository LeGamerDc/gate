package gate

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
	"github.com/panjf2000/gnet/v2"
)

// 这个文件只装"整条链路"级别的基准：单点微基准放在 bench_test.go / sender_test.go，
// 这里覆盖 onTraffic / Send / WS onTraffic / client 出站这几条端到端路径，
// 用来量化优化前后的差异。

// ---------------------------------------------------------------- 入站（TCP）

// inboundGnetConn 复现 gnet 在稳态下的 Peek/Discard 语义：数据已经在事件循环的
// 读缓冲里，Peek(-1) 直接返回一段切片，Discard 推进游标。
type inboundGnetConn struct {
	gnet.Conn

	data []byte
	off  int
}

func (c *inboundGnetConn) Peek(n int) ([]byte, error) {
	rest := c.data[c.off:]
	if n < 0 || n > len(rest) {
		return rest, nil
	}
	return rest[:n], nil
}

func (c *inboundGnetConn) Discard(n int) (int, error) {
	c.off += n
	return n, nil
}

func (c *inboundGnetConn) InboundBuffered() int { return len(c.data) - c.off }

func (c *inboundGnetConn) Next(n int) ([]byte, error) {
	if n < 0 || n > len(c.data)-c.off {
		n = len(c.data) - c.off
	}
	b := c.data[c.off : c.off+n]
	c.off += n
	return b, nil
}

func (c *inboundGnetConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *inboundGnetConn) Writev(b [][]byte) (int, error) {
	n := 0
	for _, x := range b {
		n += len(x)
	}
	return n, nil
}
func (c *inboundGnetConn) Wake(gnet.AsyncCallback) error { return nil }
func (c *inboundGnetConn) Close() error                  { return nil }
func (c *inboundGnetConn) OutboundBuffered() int         { return 0 }

// BenchmarkConnOnTraffic 覆盖服务端 TCP 入站的完整一跳：
// Peek -> codec.parse -> deliver -> handler -> Discard。
//
// batch 是一次 epoll 事件里携带的消息条数：真实网关在有一定负载时，一次读事件
// 通常能带回多条消息，这正是 gate 相对 goroutine-per-conn 的收益来源。
func BenchmarkConnOnTraffic(b *testing.B) {
	for _, tc := range []struct {
		name  string
		size  int
		batch int
	}{
		{"1x64B", 64, 1},
		{"16x64B", 64, 16},
		{"16x1KB", 1024, 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var wire []byte
			for i := 0; i < tc.batch; i++ {
				wire = append(wire, mkFrame(bytes.Repeat([]byte("x"), tc.size), 0)...)
			}

			g := &inboundGnetConn{data: wire}
			handler := &benchConnHandler{}
			conn := &Conn{conn: g, handler: handler, codec: serverCodec(maxMessageSize)}
			conn.init()
			conn.markOpen()

			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			for b.Loop() {
				g.off = 0
				if a := conn.onTraffic(); a != gnet.None {
					b.Fatalf("unexpected action %v", a)
				}
			}
			benchByteSink += handler.total
		})
	}
}

// ---------------------------------------------------------------- 入站（WS）

// BenchmarkWSOnTraffic 覆盖 WebSocket 入站的完整一跳，包含 gnet.Next -> w.buf
// 的那次拷贝、WS 帧头解析、去掩码，以及内层 gate 帧的解析与投递。
func BenchmarkWSOnTraffic(b *testing.B) {
	for _, tc := range []struct {
		name  string
		size  int
		batch int
	}{
		{"1x64B", 64, 1},
		{"16x64B", 64, 16},
		{"16x1KB", 1024, 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			frames := make([][]byte, tc.batch)
			for i := range frames {
				frames[i] = mkFrame(bytes.Repeat([]byte("x"), tc.size), 0)
			}
			wire := buildWebSocketWireFrames(b, frames...)

			g := &inboundGnetConn{data: wire}
			handler := &benchConnHandler{}
			conn := &Conn{conn: g, handler: handler, codec: serverCodec(maxMessageSize)}
			conn.init()
			conn.markOpen()
			state := &wsConnState{conn: conn, upgraded: true}

			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			for b.Loop() {
				g.off = 0
				// 每轮都要重新灌一份：去掩码是就地进行的。
				copy(g.data, wire)
				if a := state.onTraffic(); a != gnet.None {
					b.Fatalf("unexpected action %v", a)
				}
			}
			benchByteSink += handler.total
		})
	}
}

// ---------------------------------------------------------------- 出站入队

// BenchmarkSenderSend 只测业务侧的入队开销：拷贝 + 一次锁 + 首次入队的 Wake。
// parallel 变体量的是多 producer goroutine 打同一条连接时 sender.mu 的伸缩性。
func BenchmarkSenderSend(b *testing.B) {
	for _, size := range []int{64, 1024} {
		payload := bytes.Repeat([]byte("s"), size)

		b.Run(fmt.Sprintf("serial_%dB", size), func(b *testing.B) {
			s, w, _ := newTestSender(b, &SenderConfig{MaxClusterSize: 32 << 10})
			b.ReportAllocs()
			for b.Loop() {
				if err := s.Send(payload); err != nil {
					b.Fatal(err)
				}
				// 立刻排空，避免队列无限增长把分配统计带偏。
				s.flush()
				// captureWriter 的 stream 也必须清掉：它会一直增长，而它的扩容
				// 会被算进 B/op，把 sender 自己的分配量整个淹掉。
				w.mu.Lock()
				benchByteSink += w.stream.Len()
				w.stream.Reset()
				w.mu.Unlock()
			}
		})

		// parallel 量的是多 producer goroutine 打同一条连接时 sender.mu 的伸缩性。
		//
		// 排空的 goroutine 每轮之间要 park 一下，不能死转：RunParallel 本身会开满
		// GOMAXPROCS 个 worker，再加一个满负荷自旋的消费者就是超额订阅，量出来的
		// 一多半是调度噪声而不是锁争用。park 期间队列会涨，但 MaxBufferSize 兜着。
		b.Run(fmt.Sprintf("parallel_%dB", size), func(b *testing.B) {
			s, w, _ := newTestSender(b, &SenderConfig{
				MaxClusterSize: 32 << 10,
				MaxBufferSize:  64 << 20,
			})
			done := make(chan struct{})
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for {
					s.flush()
					w.mu.Lock()
					w.stream.Reset()
					w.mu.Unlock()
					select {
					case <-done:
						return
					case <-time.After(50 * time.Microsecond):
					}
				}
			}()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := s.Send(payload); err != nil && !errors.Is(err, ErrSendQueueFull) {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			close(done)
			<-drained
		})
	}
}

// BenchmarkSenderRejectOversize 量的是"一条永远放不下的消息"被拒绝的成本。
//
// 此前是先从 mcache 取一块、完整拷一遍、拿到锁发现放不下、再原样归还：默认
// MaxBufferSize 2MB 下，一条 32MB 的合法协议消息就是白搬 32MB 内存带宽。
// 现在应当与 payload 大小无关——ns/op 在各个尺寸上应该基本持平。
func BenchmarkSenderRejectOversize(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			s, _, _ := newTestSender(b, &SenderConfig{MaxBufferSize: 1024})
			payload := make([]byte, size)

			b.ReportAllocs()
			for b.Loop() {
				if err := s.Send(payload); !errors.Is(err, ErrSendQueueFull) {
					b.Fatalf("expected ErrSendQueueFull, got %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------- client 出站

func BenchmarkClientEncodeOutbound(b *testing.B) {
	for _, size := range []int{64, 1024} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			c := &Client{maxMessageSize: maxMessageSize}
			payload := bytes.Repeat([]byte("c"), size)

			b.ReportAllocs()
			for b.Loop() {
				// 覆盖 send 分配 + writeLoop 编码这两步的总成本。
				buf := make([]byte, clientHeaderReserve+len(payload))
				copy(buf[clientHeaderReserve:], payload)
				frame, err := c.encodeOutbound(outboundMsg{buf: buf})
				if err != nil {
					b.Fatal(err)
				}
				benchByteSink += len(frame)
			}
		})
	}
}

// ---------------------------------------------------------------- 单点成本

// BenchmarkTouch 量的是每条消息刷新一次活跃时间的成本。time.Now 在 darwin/linux
// 上都要走一次 vDSO/commpage，放在每消息路径上并不便宜。
func BenchmarkTouch(b *testing.B) {
	c := &Conn{}
	b.Run("time.Now+atomic.Store", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c.touch()
		}
	})
	b.Run("atomic.Store_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c.lastActive.Store(1)
		}
	})
}

// BenchmarkDeliverIsolation 量的是每条消息一个 defer+recover 的成本。
func BenchmarkDeliverIsolation(b *testing.B) {
	h := &benchConnHandler{}
	c := &Conn{handler: h}
	c.init()
	msg := bytes.Repeat([]byte("d"), 64)

	b.Run("with_recover", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := c.deliverOne(msg, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("direct_call", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c.handler.Handle(msg)
		}
	})
	benchByteSink += h.total
}

// BenchmarkZstdLevel 比较压缩等级对出站压缩路径的影响。
// 当前默认是 SpeedBetterCompression，对实时游戏流量未必是正确的取舍。
func BenchmarkZstdLevel(b *testing.B) {
	// 构造一份"游戏协议 batch"风格的数据：重复的结构 + 少量变化的字段。
	var raw []byte
	for i := 0; i < 64; i++ {
		raw = append(raw, fmt.Sprintf(
			`{"op":%d,"uid":%d,"x":%d,"y":%d,"hp":%d,"buffs":[1,2,3]}`,
			i%7, 100000+i, i*13%997, i*29%997, 100-i%100)...)
	}

	for _, lv := range []struct {
		name string
		l    zstd.EncoderLevel
	}{
		{"fastest", zstd.SpeedFastest},
		{"default", zstd.SpeedDefault},
		{"better", zstd.SpeedBetterCompression},
	} {
		e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(lv.l), zstd.WithEncoderConcurrency(1))
		if err != nil {
			b.Fatal(err)
		}
		ratio := float64(len(e.EncodeAll(raw, nil))) / float64(len(raw))
		b.Run(lv.name, func(b *testing.B) {
			dst := make([]byte, 0, len(raw))
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				benchByteSink += len(e.EncodeAll(raw, dst[:0]))
			}
			b.ReportMetric(ratio, "ratio")
		})
		e.Close()
	}
}

// TestStructSizes 给按连接（或按队列项）分配的热路径结构体钉上尺寸上限。
//
// 只打印是不够的：字段顺序的回退不会让任何别的用例变红，而它的代价是按连接数
// 线性放大的。上限写成"当前值"，任何变大都必须是一次显式决定；变小则只要顺手
// 把数字调下来。数值针对 64 位平台，32 位上一律更小，所以这里只在 64 位断言。
func TestStructSizes(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("size expectations are written for 64-bit platforms")
	}
	for _, tc := range []struct {
		name string
		got  uintptr
		max  uintptr
	}{
		{"sendMsg", unsafe.Sizeof(sendMsg{}), 32},
		{"Conn", unsafe.Sizeof(Conn{}), 184},
		{"sender", unsafe.Sizeof(sender{}), 80},
		{"frame", unsafe.Sizeof(frame{}), 32},
		{"codec", unsafe.Sizeof(codec{}), 24},
		{"wsConnState", unsafe.Sizeof(wsConnState{}), 328},
		{"wsOutbound", unsafe.Sizeof(wsOutbound{}), 16},
	} {
		t.Logf("%-12s = %d bytes (max %d)", tc.name, tc.got, tc.max)
		if tc.got > tc.max {
			t.Errorf("%s grew to %d bytes, over the %d-byte budget", tc.name, tc.got, tc.max)
		}
	}
	_ = time.Now
}
