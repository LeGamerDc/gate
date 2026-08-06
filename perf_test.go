package gate

import (
	"testing"
	"time"
)

// 07 第 6 层：性能护栏分两级——「硬断言」（0 allocs/op，回归即失败）
// 与「趋势」（吞吐基准，夜间比对）。这里是硬断言；分级池先预热，
// GC 清池后的重建分配不算回归（01「稳态零堆分配」的定义）。

func warmPools() {
	for _, n := range []int{64, 256, 1024, 4096, 64 << 10} {
		for range 8 {
			poolPut(poolGet(n))
		}
	}
}

// 入站：读 → 解析 → 投递 → handler。
func TestZeroAlloc_InboundPath(t *testing.T) {
	warmPools()
	h := newHarness(t)
	sink := 0
	h.onMsg = func(msg []byte) error { sink += len(msg); return nil }
	frame := gateWire(t, mkPayload(200))
	script := []readStep{{data: frame}}

	h.io.readScript = script
	h.l.connReadable(h.c) // 预热一次（q1spare、iov 等按需成型）

	allocs := testing.AllocsPerRun(500, func() {
		h.io.readScript = script[:1]
		script[0].data = frame
		h.l.connReadable(h.c)
	})
	if allocs != 0 {
		t.Fatalf("入站路径 %v allocs/op, want 0", allocs)
	}
}

// 出站：Send 入队 → flush（拼包、帧头、writev 暂存）→ 写出。
func TestZeroAlloc_SendFlushPath(t *testing.T) {
	warmPools()
	o, io, loop := newTestOutbound(t, func(c *outConfig) { c.maxCluster = 256 })
	io.discard = true
	msg := mkPayload(100)

	for range 8 { // 预热：q1spare、chunk slab、iov
		_ = o.send(msg, flagZ|flagC|flagE)
		_ = o.send(msg, flagZ|flagC|flagE)
		loop.drainDirty()
	}
	allocs := testing.AllocsPerRun(500, func() {
		_ = o.send(msg, flagZ|flagC|flagE)
		_ = o.send(msg, flagZ|flagC|flagE) // 两条 → compound 路径
		loop.drainDirty()
	})
	if allocs != 0 {
		t.Fatalf("Send+flush %v allocs/op, want 0", allocs)
	}
}

// Overhead()==0 的加密全程原地：入站解密 + 出站加密都不许分配。
func TestZeroAlloc_InPlaceCipher(t *testing.T) {
	warmPools()
	h := newHarness(t)
	sink := 0
	h.onMsg = func(msg []byte) error { sink += len(msg); return nil }
	h.c.setCipher(&xorCipher{key: 0x11})
	h.l.step() // 消化 barrier

	sender := &xorCipher{key: 0x11}
	plain := mkPayload(150)
	var hd [maxHeaderSize]byte
	hn := putHeader(hd[:], len(plain), flagE)
	body := sender.Seal(make([]byte, 0, len(plain)), plain, hd[:hn])
	frame := append(append([]byte{}, hd[:hn]...), body...)
	master := append([]byte(nil), frame...)
	script := []readStep{{data: frame}}

	h.io.readScript = script
	h.l.connReadable(h.c) // 预热

	allocs := testing.AllocsPerRun(500, func() {
		copy(frame, master) // 原地解密会覆写，重放前恢复
		h.io.readScript = script[:1]
		script[0].data = frame
		h.l.connReadable(h.c)
	})
	if allocs != 0 {
		t.Fatalf("原地加密路径 %v allocs/op, want 0", allocs)
	}
}

// WS 入站：帧层去掩码 + gate 帧循环。
func TestZeroAlloc_WSInbound(t *testing.T) {
	warmPools()
	h := newWSHarness(t)
	sink := 0
	h.onMsg = func(msg []byte) error { sink += len(msg); return nil }
	raw := wsClientFrame(true, wsOpBinary, gateWire(t, mkPayload(200)), testMask)
	master := append([]byte(nil), raw...)
	script := []readStep{{data: raw}}

	h.io.readScript = script
	h.l.connReadable(h.c) // 预热

	allocs := testing.AllocsPerRun(500, func() {
		copy(raw, master) // 去掩码是原地的，重放前恢复
		h.io.readScript = script[:1]
		script[0].data = raw
		h.l.connReadable(h.c)
	})
	if allocs != 0 {
		t.Fatalf("WS 入站 %v allocs/op, want 0", allocs)
	}
}

// ─── 吞吐基准（趋势级，不做硬断言）───

func BenchmarkInboundEcho(b *testing.B) {
	h := newHarness(&testing.T{})
	h.io.discard = true
	h.onMsg = func(msg []byte) error {
		return h.c.out.send(msg, flagZ|flagC|flagE)
	}
	frame := gateWire(&testing.T{}, mkPayload(128))
	script := []readStep{{data: frame}}
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		h.io.readScript = script[:1]
		script[0].data = frame
		h.l.connReadable(h.c)
	}
}

func BenchmarkSendCompoundFlush(b *testing.B) {
	o, io, loop := newTestOutbound(&testing.T{}, func(c *outConfig) { c.maxCluster = 1024 })
	io.discard = true
	msg := mkPayload(64)
	b.ReportAllocs()
	b.SetBytes(int64(8 * len(msg)))
	for b.Loop() {
		for range 8 {
			_ = o.send(msg, flagZ|flagC|flagE)
		}
		loop.drainDirty()
	}
}

func BenchmarkCodecParseDeliver(b *testing.B) {
	c := serverCodec(maxMessageSize)
	frame := gateWire(&testing.T{}, mkPayload(128))
	sink := 0
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		f, _, ok, err := c.parse(frame)
		if !ok || err != nil {
			b.Fatal(ok, err)
		}
		_ = c.deliver(f, nil, nil, func(m []byte, _ bool) error { sink += len(m); return nil })
	}
	_ = sink
}

// 07 第 3 层要求的「每个池借出计数 == 归还计数」。它是进程级计数器，
// 并行存在的 server loop 会让它漂移，所以不放进 harness 的 afterEach，
// 而是用一个独占的用例跑一遍有代表性的负载再断言。
//
// 只在 -tags gatedebug 下有数（默认构建里计数函数是空的——它们在热路径上）。
func TestPoolBalanceAcrossWorkload(t *testing.T) {
	if !debugAccounting {
		t.Skip("需要 -tags gatedebug")
	}
	before := poolOutstanding()

	// TCP：小帧、空帧、大帧直读、合包、压缩、AEAD、部分写、carry 切分。
	h := newHarness(t, func(c *loopConfig) {
		c.out.maxCluster = 256
		c.out.compressThreshold = 64
	})
	sender, recv := pairGCM([16]byte{5})
	h.c.setCipher(recv)
	h.l.step()
	h.onMsg = func(msg []byte) error { return h.c.out.send(msg, flagZ|flagC|flagE) }

	var stream []byte
	for _, m := range [][]byte{{}, mkPayload(10), mkPayload(3000), mkPayload(100 << 10)} {
		bf, err := buildFrame(append([]byte(nil), m...), 0, nil, false, sender, maxMessageSize)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, wire(&bf)...)
	}
	for i := 0; i < len(stream); i += 7000 {
		h.io.feed(stream[i:min(i+7000, len(stream))])
	}
	h.io.script = []ioStep{{accept: 13, err: errEAGAIN}}
	for range 30 {
		h.readable()
		h.writable()
	}
	h.c.requestClose(nil)
	h.advance(2 * time.Second)
	h.l.step()
	h.verifyConservation()

	// WS：握手、流式解帧、控制帧、carry。
	hw := newWSHarness(t)
	hw.io.feed(wsClientFrame(true, wsOpPing, []byte("ka"), testMask))
	hw.io.feed(wsClientFrame(true, wsOpBinary, gateWire(t, mkPayload(500), mkPayload(80<<10)), testMask))
	for range 6 {
		hw.readable()
	}
	hw.c.requestClose(nil)
	hw.advance(2 * time.Second)
	hw.l.step()
	hw.verifyConservation()

	if got := poolOutstanding(); got != before {
		t.Fatalf("分级池借还不配平: %d → %d（漏还 %d 块）", before, got, got-before)
	}
}
