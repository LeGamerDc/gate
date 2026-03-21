package common

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type CounterSnapshot struct {
	TxMessages  uint64
	TxRawBytes  uint64
	TxWireBytes uint64

	RxMessages  uint64
	RxFrames    uint64
	RxRawBytes  uint64
	RxWireBytes uint64

	MismatchErrors uint64
	ReadErrors     uint64
	WriteErrors    uint64
}

func (c CounterSnapshot) Delta(prev CounterSnapshot) CounterSnapshot {
	return CounterSnapshot{
		TxMessages:     c.TxMessages - prev.TxMessages,
		TxRawBytes:     c.TxRawBytes - prev.TxRawBytes,
		TxWireBytes:    c.TxWireBytes - prev.TxWireBytes,
		RxMessages:     c.RxMessages - prev.RxMessages,
		RxFrames:       c.RxFrames - prev.RxFrames,
		RxRawBytes:     c.RxRawBytes - prev.RxRawBytes,
		RxWireBytes:    c.RxWireBytes - prev.RxWireBytes,
		MismatchErrors: c.MismatchErrors - prev.MismatchErrors,
		ReadErrors:     c.ReadErrors - prev.ReadErrors,
		WriteErrors:    c.WriteErrors - prev.WriteErrors,
	}
}

type LatencySummary struct {
	Samples uint64
	Dropped uint64
	Avg     time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
}

type Stats struct {
	txMessages  atomic.Uint64
	txRawBytes  atomic.Uint64
	txWireBytes atomic.Uint64

	rxMessages  atomic.Uint64
	rxFrames    atomic.Uint64
	rxRawBytes  atomic.Uint64
	rxWireBytes atomic.Uint64

	mismatchErrors atomic.Uint64
	readErrors     atomic.Uint64
	writeErrors    atomic.Uint64

	latency *latencyRecorder
}

func NewStats(latencyBuffer int) *Stats {
	return &Stats{
		latency: newLatencyRecorder(latencyBuffer),
	}
}

func (s *Stats) Close() {
	s.latency.Close()
}

func (s *Stats) RecordTx(rawBytes, wireBytes int) {
	s.txMessages.Add(1)
	s.txRawBytes.Add(uint64(rawBytes))
	s.txWireBytes.Add(uint64(wireBytes))
}

func (s *Stats) RecordRx(rawBytes, wireBytes, frames, messages int) {
	if messages > 0 {
		s.rxMessages.Add(uint64(messages))
		s.rxRawBytes.Add(uint64(rawBytes))
	}
	if frames > 0 {
		s.rxFrames.Add(uint64(frames))
		s.rxWireBytes.Add(uint64(wireBytes))
	}
}

func (s *Stats) RecordMismatch() {
	s.mismatchErrors.Add(1)
}

func (s *Stats) RecordReadError() {
	s.readErrors.Add(1)
}

func (s *Stats) RecordWriteError() {
	s.writeErrors.Add(1)
}

func (s *Stats) RecordLatency(d time.Duration) {
	s.latency.Record(d)
}

func (s *Stats) Counters() CounterSnapshot {
	return CounterSnapshot{
		TxMessages:     s.txMessages.Load(),
		TxRawBytes:     s.txRawBytes.Load(),
		TxWireBytes:    s.txWireBytes.Load(),
		RxMessages:     s.rxMessages.Load(),
		RxFrames:       s.rxFrames.Load(),
		RxRawBytes:     s.rxRawBytes.Load(),
		RxWireBytes:    s.rxWireBytes.Load(),
		MismatchErrors: s.mismatchErrors.Load(),
		ReadErrors:     s.readErrors.Load(),
		WriteErrors:    s.writeErrors.Load(),
	}
}

func (s *Stats) TakeIntervalLatency() LatencySummary {
	return s.latency.TakeInterval()
}

func (s *Stats) TotalLatency() LatencySummary {
	return s.latency.Total()
}

type latencyRecorder struct {
	ch      chan time.Duration
	closeCh chan struct{}
	done    chan struct{}

	droppedTotal    atomic.Uint64
	droppedInterval atomic.Uint64

	mu       sync.Mutex
	total    latencyHistogram
	interval latencyHistogram
}

func newLatencyRecorder(buffer int) *latencyRecorder {
	if buffer <= 0 {
		buffer = 65536
	}

	r := &latencyRecorder{
		ch:       make(chan time.Duration, buffer),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
		total:    newLatencyHistogram(),
		interval: newLatencyHistogram(),
	}
	go r.loop()
	return r
}

func (r *latencyRecorder) Record(d time.Duration) {
	select {
	case r.ch <- d:
	default:
		r.droppedTotal.Add(1)
		r.droppedInterval.Add(1)
	}
}

func (r *latencyRecorder) TakeInterval() LatencySummary {
	r.mu.Lock()
	defer r.mu.Unlock()

	summary := r.interval.summary(r.droppedInterval.Swap(0))
	r.interval.reset()
	return summary
}

func (r *latencyRecorder) Total() LatencySummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total.summary(r.droppedTotal.Load())
}

func (r *latencyRecorder) Close() {
	close(r.closeCh)
	<-r.done
}

func (r *latencyRecorder) loop() {
	defer close(r.done)
	for {
		select {
		case d := <-r.ch:
			r.record(d)
		case <-r.closeCh:
			for {
				select {
				case d := <-r.ch:
					r.record(d)
				default:
					return
				}
			}
		}
	}
}

func (r *latencyRecorder) record(d time.Duration) {
	r.mu.Lock()
	r.total.record(d)
	r.interval.record(d)
	r.mu.Unlock()
}

var latencyUpperBoundsMicros = buildLatencyBounds()

type latencyHistogram struct {
	counts []uint64
	count  uint64
	sumUs  uint64
}

func newLatencyHistogram() latencyHistogram {
	return latencyHistogram{
		counts: make([]uint64, len(latencyUpperBoundsMicros)),
	}
}

func (h *latencyHistogram) record(d time.Duration) {
	us := max(int64(1), d.Microseconds())
	idx := sort.Search(len(latencyUpperBoundsMicros), func(i int) bool {
		return us <= latencyUpperBoundsMicros[i]
	})
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
	h.count++
	h.sumUs += uint64(us)
}

func (h *latencyHistogram) reset() {
	clear(h.counts)
	h.count = 0
	h.sumUs = 0
}

func (h *latencyHistogram) summary(dropped uint64) LatencySummary {
	if h.count == 0 {
		return LatencySummary{Dropped: dropped}
	}
	return LatencySummary{
		Samples: h.count,
		Dropped: dropped,
		Avg:     time.Duration(h.sumUs/h.count) * time.Microsecond,
		P50:     h.quantile(0.50),
		P95:     h.quantile(0.95),
		P99:     h.quantile(0.99),
	}
}

func (h *latencyHistogram) quantile(q float64) time.Duration {
	target := uint64(math.Ceil(float64(h.count) * q))
	if target == 0 {
		target = 1
	}

	var cumulative uint64
	for i, c := range h.counts {
		cumulative += c
		if cumulative >= target {
			return time.Duration(latencyUpperBoundsMicros[i]) * time.Microsecond
		}
	}
	return time.Duration(latencyUpperBoundsMicros[len(latencyUpperBoundsMicros)-1]) * time.Microsecond
}

func buildLatencyBounds() []int64 {
	var bounds []int64
	appendRange := func(start, end, step int64) {
		for v := start; v <= end; v += step {
			bounds = append(bounds, v)
		}
	}

	appendRange(10, 1000, 10)
	appendRange(1100, 10000, 100)
	appendRange(11000, 100000, 1000)
	appendRange(110000, 1000000, 10000)
	appendRange(1100000, 10000000, 100000)
	bounds = append(bounds, math.MaxInt64)
	return bounds
}
