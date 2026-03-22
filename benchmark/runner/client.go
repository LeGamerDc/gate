package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/LeGamerDc/gate/benchmark/common"
)

const (
	DefaultClientConnections    = 128
	DefaultClientPayloadSize    = 256
	DefaultClientDuration       = 10 * time.Second
	DefaultClientWarmup         = 2 * time.Second
	DefaultClientDrain          = 2 * time.Second
	DefaultClientInflight       = 128
	DefaultClientPayloadMode    = "repeat"
	DefaultClientLatencyBuffer  = 65536
	DefaultClientReportInterval = time.Second
)

// ClientConfig controls the benchmark client runner.
type ClientConfig struct {
	Addr           string
	Connections    int
	PayloadSize    int
	Duration       time.Duration
	Warmup         time.Duration
	Drain          time.Duration
	Rate           int
	Inflight       int
	Encrypt        bool
	PayloadMode    string
	LatencyBuffer  int
	ReportInterval time.Duration
	Logger         Logger
}

// ClientResult is the aggregated result returned by RunClient.
type ClientResult struct {
	Window   time.Duration
	Counters common.CounterSnapshot
	Latency  common.LatencySummary
}

// RunClient runs the benchmark client until the configured measurement window
// completes or ctx is canceled.
func RunClient(ctx context.Context, cfg ClientConfig) (ClientResult, error) {
	var result ClientResult

	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.Validate(); err != nil {
		return result, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stats := common.NewStats(cfg.LatencyBuffer)

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

	workers := make([]*clientWorker, 0, cfg.Connections)
	closeWorkers := func() {
		for _, w := range workers {
			_ = w.conn.Close()
		}
	}

	for i := 0; i < cfg.Connections; i++ {
		conn, err := net.Dial("tcp", cfg.Addr)
		if err != nil {
			closeWorkers()
			stats.Close()
			return result, fmt.Errorf("connect worker %d: %w", i, err)
		}

		template, err := common.BuildPayloadTemplate(cfg.PayloadSize, cfg.PayloadMode, int64(i)+1)
		if err != nil {
			_ = conn.Close()
			closeWorkers()
			stats.Close()
			return result, fmt.Errorf("payload template worker %d: %w", i, err)
		}

		w := &clientWorker{
			id:           uint32(i + 1),
			conn:         conn,
			template:     template,
			stats:        stats,
			encrypt:      cfg.Encrypt,
			cipher:       common.XORCipher{Key: 0x5a},
			inflight:     int64(cfg.Inflight),
			measureStart: &measureStart,
			measureEnd:   &measureEnd,
			fail:         fail,
		}
		workers = append(workers, w)
	}

	for _, w := range workers {
		wg.Add(2)
		go func(w *clientWorker) {
			defer wg.Done()
			w.writeLoop(runCtx, cfg, len(workers))
		}(w)
		go func(w *clientWorker) {
			defer wg.Done()
			w.readLoop(runCtx)
		}(w)
	}

	if cfg.Warmup > 0 {
		logf(cfg.Logger, "warming up for %s", cfg.Warmup)
		select {
		case <-time.After(cfg.Warmup):
		case <-runCtx.Done():
		}
	}

	start := time.Now()
	measureStart.Store(start.UnixNano())
	logf(
		cfg.Logger,
		"benchmark started addr=%s connections=%d payload=%dB duration=%s rate=%s inflight=%d encrypt=%v mode=%s",
		cfg.Addr,
		cfg.Connections,
		cfg.PayloadSize,
		cfg.Duration,
		cfg.rateLabel(),
		cfg.Inflight,
		cfg.Encrypt,
		cfg.PayloadMode,
	)

	reportDone := make(chan struct{})
	if cfg.ReportInterval > 0 && cfg.Logger != nil {
		go func() {
			defer close(reportDone)
			reportLoop(runCtx, cfg.Logger, cfg.ReportInterval, stats, start)
		}()
	} else {
		close(reportDone)
	}

	timer := time.NewTimer(cfg.Duration)
	select {
	case <-timer.C:
	case <-runCtx.Done():
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	measureEnd.Store(time.Now().UnixNano())
	cancel()

	if cfg.Drain > 0 {
		logf(cfg.Logger, "draining replies for %s", cfg.Drain)
		select {
		case <-time.After(cfg.Drain):
		case <-ctx.Done():
		}
	}

	closeWorkers()
	wg.Wait()
	<-reportDone

	stats.Close()

	result = ClientResult{
		Counters: stats.Counters(),
		Latency:  stats.TotalLatency(),
	}
	if startNano, endNano := measureStart.Load(), measureEnd.Load(); startNano > 0 && endNano >= startNano {
		result.Window = time.Duration(endNano - startNano)
	}

	printSummary(cfg.Logger, result)

	if runErr == nil {
		switch {
		case result.Counters.MismatchErrors > 0:
			runErr = errors.New("reply verification failed")
		case result.Counters.ReadErrors > 0:
			runErr = errors.New("read loop failed")
		case result.Counters.WriteErrors > 0:
			runErr = errors.New("write loop failed")
		case ctx.Err() != nil:
			runErr = ctx.Err()
		}
	}

	return result, runErr
}

// Validate normalizes optional settings and checks the config.
func (c *ClientConfig) Validate() error {
	if c.Addr == "" {
		c.Addr = DefaultServerAddr
	}
	switch {
	case c.Connections <= 0:
		return errors.New("connections must be > 0")
	case c.PayloadSize < common.PayloadMetaSize:
		return fmt.Errorf("payload-size must be >= %d", common.PayloadMetaSize)
	case c.Duration <= 0:
		return errors.New("duration must be > 0")
	case c.ReportInterval < 0:
		return errors.New("report-interval must be >= 0")
	}
	if c.PayloadMode == "" {
		c.PayloadMode = DefaultClientPayloadMode
	}
	if c.PayloadMode != "repeat" && c.PayloadMode != "random" {
		return errors.New("payload-mode must be repeat or random")
	}
	if c.LatencyBuffer <= 0 {
		c.LatencyBuffer = DefaultClientLatencyBuffer
	}
	return nil
}

func (c ClientConfig) rateLabel() string {
	if c.Rate <= 0 {
		return "max"
	}
	return fmt.Sprintf("%d msg/s", c.Rate)
}

type clientWorker struct {
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

func (w *clientWorker) writeLoop(ctx context.Context, cfg ClientConfig, workerCount int) {
	payload := append([]byte(nil), w.template...)
	frame := make([]byte, common.EncodedFrameSize(len(payload)))
	seq := uint64(0)

	interval := perWorkerInterval(cfg.Rate, workerCount)
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

func (w *clientWorker) readLoop(ctx context.Context) {
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

func (w *clientWorker) consumeTopFrame(frame common.Frame, dec *zstd.Decoder) (int, int, error) {
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

func (w *clientWorker) consumeMessage(data []byte) (bool, error) {
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

func reportLoop(ctx context.Context, logger Logger, interval time.Duration, stats *common.Stats, start time.Time) {
	ticker := time.NewTicker(interval)
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
			printInterval(logger, now.Sub(start), now.Sub(prevTime), delta, latency)
			prevCounters = counters
			prevTime = now
		}
	}
}

func printInterval(logger Logger, elapsed, interval time.Duration, counters common.CounterSnapshot, latency common.LatencySummary) {
	if logger == nil || interval <= 0 {
		return
	}

	txRate := float64(counters.TxMessages) / interval.Seconds()
	rxRate := float64(counters.RxMessages) / interval.Seconds()
	rawMB := mibPerSecond(counters.RxRawBytes, interval)
	wireMB := mibPerSecond(counters.RxWireBytes, interval)
	ratio := safeRatio(counters.RxWireBytes, counters.RxRawBytes)
	compound := safeRatio(counters.RxMessages, counters.RxFrames)

	logger.Printf(
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

func printSummary(logger Logger, result ClientResult) {
	if logger == nil {
		return
	}

	logger.Printf("summary window=%s", result.Window)
	logger.Printf(
		"totals tx=%d rx=%d rx_frames=%d tx_raw=%.2f MiB rx_raw=%.2f MiB rx_wire=%.2f MiB ratio=%.2f compound=%.2f inflight_gap=%d",
		result.Counters.TxMessages,
		result.Counters.RxMessages,
		result.Counters.RxFrames,
		bytesToMiB(result.Counters.TxRawBytes),
		bytesToMiB(result.Counters.RxRawBytes),
		bytesToMiB(result.Counters.RxWireBytes),
		safeRatio(result.Counters.RxWireBytes, result.Counters.RxRawBytes),
		safeRatio(result.Counters.RxMessages, result.Counters.RxFrames),
		int64(result.Counters.TxMessages)-int64(result.Counters.RxMessages),
	)
	logger.Printf(
		"latency avg=%s p50=%s p95=%s p99=%s samples=%d dropped=%d errors(w/r/m)=%d/%d/%d",
		formatDuration(result.Latency.Avg),
		formatDuration(result.Latency.P50),
		formatDuration(result.Latency.P95),
		formatDuration(result.Latency.P99),
		result.Latency.Samples,
		result.Latency.Dropped,
		result.Counters.WriteErrors,
		result.Counters.ReadErrors,
		result.Counters.MismatchErrors,
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
