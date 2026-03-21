package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/LeGamerDc/gate/benchmark/common"
)

func main() {
	cfg := loadConfig()
	if err := cfg.validate(); err != nil {
		log.Fatalf("invalid benchmark config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stats := common.NewStats(cfg.latencyBuffer)
	defer stats.Close()

	var (
		measureStart atomic.Int64
		measureEnd   atomic.Int64
		errOnce      sync.Once
		runErr       error
		wg           sync.WaitGroup
	)

	fail := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			runErr = err
			cancel()
		})
	}

	workers := make([]*worker, 0, cfg.connections)
	for i := 0; i < cfg.connections; i++ {
		conn, err := net.Dial("tcp", cfg.addr)
		if err != nil {
			log.Fatalf("connect worker %d: %v", i, err)
		}

		template, err := common.BuildPayloadTemplate(cfg.payloadSize, cfg.payloadMode, int64(i)+1)
		if err != nil {
			log.Fatalf("payload template worker %d: %v", i, err)
		}

		w := &worker{
			id:           uint32(i + 1),
			conn:         conn,
			template:     template,
			stats:        stats,
			encrypt:      cfg.encrypt,
			cipher:       common.XORCipher{Key: 0x5a},
			inflight:     int64(cfg.inflight),
			measureStart: &measureStart,
			measureEnd:   &measureEnd,
			fail:         fail,
		}
		workers = append(workers, w)
	}

	for _, w := range workers {
		wg.Add(2)
		go func(w *worker) {
			defer wg.Done()
			w.writeLoop(runCtx, cfg, len(workers))
		}(w)
		go func(w *worker) {
			defer wg.Done()
			w.readLoop(runCtx)
		}(w)
	}

	if cfg.warmup > 0 {
		log.Printf("warming up for %s", cfg.warmup)
		select {
		case <-time.After(cfg.warmup):
		case <-runCtx.Done():
		}
	}

	start := time.Now()
	measureStart.Store(start.UnixNano())
	log.Printf(
		"benchmark started addr=%s connections=%d payload=%dB duration=%s rate=%s inflight=%d encrypt=%v mode=%s",
		cfg.addr,
		cfg.connections,
		cfg.payloadSize,
		cfg.duration,
		cfg.rateLabel(),
		cfg.inflight,
		cfg.encrypt,
		cfg.payloadMode,
	)

	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		reportLoop(runCtx, stats, start)
	}()

	select {
	case <-time.After(cfg.duration):
	case <-runCtx.Done():
	}

	measureEnd.Store(time.Now().UnixNano())
	cancel()

	if cfg.drain > 0 {
		log.Printf("draining replies for %s", cfg.drain)
		time.Sleep(cfg.drain)
	}

	for _, w := range workers {
		_ = w.conn.Close()
	}
	wg.Wait()
	<-reportDone

	totalCounters := stats.Counters()
	totalLatency := stats.TotalLatency()
	printSummary(cfg.duration, totalCounters, totalLatency)

	if runErr == nil {
		switch {
		case totalCounters.MismatchErrors > 0:
			runErr = errors.New("reply verification failed")
		case totalCounters.ReadErrors > 0:
			runErr = errors.New("read loop failed")
		case totalCounters.WriteErrors > 0:
			runErr = errors.New("write loop failed")
		}
	}

	if runErr != nil {
		log.Printf("benchmark failed: %v", runErr)
		os.Exit(1)
	}
}

type config struct {
	addr          string
	connections   int
	payloadSize   int
	duration      time.Duration
	warmup        time.Duration
	drain         time.Duration
	rate          int
	inflight      int
	encrypt       bool
	payloadMode   string
	latencyBuffer int
}

func loadConfig() config {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:8081", "server address")
	flag.IntVar(&cfg.connections, "connections", 128, "number of TCP connections")
	flag.IntVar(&cfg.payloadSize, "payload-size", 256, "payload size in bytes, minimum 24")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "measurement duration")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "warmup duration before measurement")
	flag.DurationVar(&cfg.drain, "drain", 2*time.Second, "wait time after sending stops to collect in-flight replies")
	flag.IntVar(&cfg.rate, "rate", 0, "target aggregate send rate in messages per second, 0 means send as fast as possible")
	flag.IntVar(&cfg.inflight, "inflight", 128, "max in-flight requests per connection, <=0 disables the limit")
	flag.BoolVar(&cfg.encrypt, "encrypt", false, "encrypt requests and expect encrypted replies")
	flag.StringVar(&cfg.payloadMode, "payload-mode", "repeat", "payload body mode: repeat or random")
	flag.IntVar(&cfg.latencyBuffer, "latency-buffer", 65536, "latency sample channel size")
	flag.Parse()
	return cfg
}

func (c config) validate() error {
	switch {
	case c.addr == "":
		return errors.New("addr must not be empty")
	case c.connections <= 0:
		return errors.New("connections must be > 0")
	case c.payloadSize < common.PayloadMetaSize:
		return fmt.Errorf("payload-size must be >= %d", common.PayloadMetaSize)
	case c.duration <= 0:
		return errors.New("duration must be > 0")
	}
	if c.payloadMode != "repeat" && c.payloadMode != "random" {
		return errors.New("payload-mode must be repeat or random")
	}
	return nil
}

func (c config) rateLabel() string {
	if c.rate <= 0 {
		return "max"
	}
	return fmt.Sprintf("%d msg/s", c.rate)
}

type worker struct {
	id       uint32
	conn     net.Conn
	template []byte
	stats    *common.Stats
	encrypt  bool
	cipher   common.XORCipher
	inflight int64

	pending atomic.Int64

	measureStart *atomic.Int64
	measureEnd   *atomic.Int64
	fail         func(error)
}

func (w *worker) writeLoop(ctx context.Context, cfg config, workerCount int) {
	payload := append([]byte(nil), w.template...)
	frame := make([]byte, common.EncodedFrameSize(len(payload)))
	seq := uint64(0)

	interval := perWorkerInterval(cfg.rate, workerCount)
	nextSend := time.Now()
	if interval > 0 {
		nextSend = nextSend.Add(time.Duration(int64(interval) * int64(w.id-1) / int64(workerCount)))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if w.inflight > 0 && w.pending.Load() >= w.inflight {
			if !sleepContext(ctx, 50*time.Microsecond) {
				return
			}
			continue
		}

		if interval > 0 {
			now := time.Now()
			if now.Before(nextSend) {
				if !sleepContext(ctx, time.Until(nextSend)) {
					return
				}
				continue
			}
			nextSend = nextSend.Add(interval)
		}

		seq++
		sentNano := time.Now().UnixNano()
		common.FillPayloadMeta(payload, w.id, seq, sentNano)

		flag := byte(0)
		if w.encrypt {
			w.cipher.Encrypt(payload)
			flag |= common.MaskE
		}

		n, err := common.EncodeFrame(frame, payload, flag)
		if w.encrypt {
			w.cipher.Decrypt(payload)
		}
		if err != nil {
			w.stats.RecordWriteError()
			w.fail(err)
			return
		}
		if err := common.WriteAll(w.conn, frame[:n]); err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			w.stats.RecordWriteError()
			w.fail(err)
			return
		}

		w.pending.Add(1)
		if inMeasureWindow(sentNano, w.measureStart, w.measureEnd) {
			w.stats.RecordTx(len(payload), n)
		}
	}
}

func (w *worker) readLoop(ctx context.Context) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		w.fail(err)
		return
	}
	defer dec.Close()

	for {
		frame, err := common.ReadFrame(w.conn, common.MaxMessageSize)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			w.stats.RecordReadError()
			w.fail(err)
			return
		}

		messages, rawBytes, err := w.consumeTopFrame(frame, dec)
		if err != nil {
			w.stats.RecordMismatch()
			w.fail(err)
			return
		}
		if messages > 0 {
			w.stats.RecordRx(rawBytes, frame.WireBytes, 1, messages)
		}
	}
}

func (w *worker) consumeTopFrame(frame common.Frame, dec *zstd.Decoder) (int, int, error) {
	data := frame.Payload
	if frame.E {
		w.cipher.Decrypt(data)
	}
	if frame.Z {
		var err error
		data, err = dec.DecodeAll(data, nil)
		if err != nil {
			return 0, 0, err
		}
	}
	if !frame.C {
		ok, err := w.consumeMessage(data)
		if err != nil || !ok {
			return 0, 0, err
		}
		return 1, len(data), nil
	}

	var (
		messages int
		rawBytes int
	)
	for len(data) > 0 {
		sub, n, err := common.ConsumeFrame(data, common.MaxMessageSize)
		if err != nil {
			return 0, 0, err
		}
		if sub.Z || sub.C || sub.E {
			return 0, 0, errors.New("compound sub-frame contains unexpected flags")
		}
		ok, err := w.consumeMessage(sub.Payload)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			messages++
			rawBytes += len(sub.Payload)
		}
		data = data[n:]
	}
	return messages, rawBytes, nil
}

func (w *worker) consumeMessage(data []byte) (bool, error) {
	connID, _, sentNano, err := common.ParsePayloadMeta(data)
	if err != nil {
		return false, err
	}
	if connID != w.id {
		return false, fmt.Errorf("unexpected conn id %d for worker %d", connID, w.id)
	}
	if !common.VerifyPayloadBody(data, w.template) {
		return false, errors.New("payload body mismatch")
	}

	w.pending.Add(-1)
	if !inMeasureWindow(sentNano, w.measureStart, w.measureEnd) {
		return false, nil
	}

	w.stats.RecordLatency(time.Since(time.Unix(0, sentNano)))
	return true, nil
}

func perWorkerInterval(rate int, workers int) time.Duration {
	if rate <= 0 || workers <= 0 {
		return 0
	}
	perWorkerRate := float64(rate) / float64(workers)
	if perWorkerRate <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / perWorkerRate)
}

func inMeasureWindow(sentNano int64, start, end *atomic.Int64) bool {
	s := start.Load()
	if s == 0 || sentNano < s {
		return false
	}
	e := end.Load()
	return e == 0 || sentNano <= e
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func reportLoop(ctx context.Context, stats *common.Stats, start time.Time) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	prevCounters := stats.Counters()
	prevTime := start

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			counters := stats.Counters()
			delta := counters.Delta(prevCounters)
			latency := stats.TakeIntervalLatency()
			printInterval(now.Sub(start), now.Sub(prevTime), delta, latency)
			prevCounters = counters
			prevTime = now
		}
	}
}

func printInterval(elapsed, interval time.Duration, counters common.CounterSnapshot, latency common.LatencySummary) {
	if interval <= 0 {
		return
	}

	txRate := float64(counters.TxMessages) / interval.Seconds()
	rxRate := float64(counters.RxMessages) / interval.Seconds()
	rawMB := mibPerSecond(counters.RxRawBytes, interval)
	wireMB := mibPerSecond(counters.RxWireBytes, interval)
	ratio := safeRatio(counters.RxWireBytes, counters.RxRawBytes)
	compound := safeRatio(counters.RxMessages, counters.RxFrames)

	log.Printf(
		"[%5.1fs] tx=%8.0f msg/s rx=%8.0f msg/s raw=%7.2f MiB/s wire=%7.2f MiB/s ratio=%4.2f compound=%4.2f avg=%8s p50=%8s p95=%8s p99=%8s sample_drop=%d err(w/r/m)=%d/%d/%d",
		elapsed.Seconds(),
		txRate,
		rxRate,
		rawMB,
		wireMB,
		ratio,
		compound,
		formatDuration(latency.Avg),
		formatDuration(latency.P50),
		formatDuration(latency.P95),
		formatDuration(latency.P99),
		latency.Dropped,
		counters.WriteErrors,
		counters.ReadErrors,
		counters.MismatchErrors,
	)
}

func printSummary(duration time.Duration, counters common.CounterSnapshot, latency common.LatencySummary) {
	log.Printf("summary window=%s", duration)
	log.Printf(
		"totals tx=%d rx=%d rx_frames=%d tx_raw=%.2f MiB rx_raw=%.2f MiB rx_wire=%.2f MiB ratio=%.2f compound=%.2f inflight_gap=%d",
		counters.TxMessages,
		counters.RxMessages,
		counters.RxFrames,
		bytesToMiB(counters.TxRawBytes),
		bytesToMiB(counters.RxRawBytes),
		bytesToMiB(counters.RxWireBytes),
		safeRatio(counters.RxWireBytes, counters.RxRawBytes),
		safeRatio(counters.RxMessages, counters.RxFrames),
		int64(counters.TxMessages)-int64(counters.RxMessages),
	)
	log.Printf(
		"latency avg=%s p50=%s p95=%s p99=%s samples=%d dropped=%d errors(w/r/m)=%d/%d/%d",
		formatDuration(latency.Avg),
		formatDuration(latency.P50),
		formatDuration(latency.P95),
		formatDuration(latency.P99),
		latency.Samples,
		latency.Dropped,
		counters.WriteErrors,
		counters.ReadErrors,
		counters.MismatchErrors,
	)
}

func mibPerSecond(bytes uint64, interval time.Duration) float64 {
	if interval <= 0 {
		return 0
	}
	return bytesToMiB(bytes) / interval.Seconds()
}

func bytesToMiB(bytes uint64) float64 {
	return float64(bytes) / 1024.0 / 1024.0
}

func safeRatio(num, den uint64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func formatDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return "0"
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
