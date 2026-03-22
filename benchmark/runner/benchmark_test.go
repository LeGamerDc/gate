package runner

import (
	"context"
	"testing"
	"time"
)

const (
	benchmarkWarmup     = 100 * time.Millisecond
	benchmarkPerOp      = 250 * time.Millisecond
	benchmarkDrain      = 100 * time.Millisecond
	benchmarkTimeoutPad = 5 * time.Second
)

func BenchmarkRunnerRoundTrip(b *testing.B) {
	for _, tc := range []struct {
		name   string
		server ServerConfig
		client ClientConfig
	}{
		{
			name: "plain_512B",
			server: ServerConfig{
				Addr:              "127.0.0.1:0",
				LoopCount:         1,
				CompressThreshold: -1,
				MaxClusterSize:    -1,
				MaxBufferSize:     DefaultServerMaxBufferSize,
			},
			client: ClientConfig{
				Connections:    16,
				PayloadSize:    512,
				Rate:           0,
				Inflight:       32,
				PayloadMode:    DefaultClientPayloadMode,
				LatencyBuffer:  DefaultClientLatencyBuffer,
				ReportInterval: 0,
			},
		},
		{
			name: "encrypt_512B",
			server: ServerConfig{
				Addr:              "127.0.0.1:0",
				LoopCount:         1,
				CompressThreshold: -1,
				MaxClusterSize:    -1,
				MaxBufferSize:     DefaultServerMaxBufferSize,
				Encrypt:           true,
			},
			client: ClientConfig{
				Connections:    16,
				PayloadSize:    512,
				Rate:           0,
				Inflight:       32,
				Encrypt:        true,
				PayloadMode:    DefaultClientPayloadMode,
				LatencyBuffer:  DefaultClientLatencyBuffer,
				ReportInterval: 0,
			},
		},
		{
			name: "compress_8KiB",
			server: ServerConfig{
				Addr:              "127.0.0.1:0",
				LoopCount:         1,
				CompressThreshold: 1,
				MaxClusterSize:    DefaultServerMaxClusterSize,
				MaxBufferSize:     DefaultServerMaxBufferSize,
			},
			client: ClientConfig{
				Connections:    8,
				PayloadSize:    8 * 1024,
				Rate:           0,
				Inflight:       16,
				PayloadMode:    DefaultClientPayloadMode,
				LatencyBuffer:  DefaultClientLatencyBuffer,
				ReportInterval: 0,
			},
		},
		{
			name: "encrypt_compress_8KiB",
			server: ServerConfig{
				Addr:              "127.0.0.1:0",
				LoopCount:         1,
				CompressThreshold: 1,
				MaxClusterSize:    DefaultServerMaxClusterSize,
				MaxBufferSize:     DefaultServerMaxBufferSize,
				Encrypt:           true,
			},
			client: ClientConfig{
				Connections:    8,
				PayloadSize:    8 * 1024,
				Rate:           0,
				Inflight:       16,
				Encrypt:        true,
				PayloadMode:    DefaultClientPayloadMode,
				LatencyBuffer:  DefaultClientLatencyBuffer,
				ReportInterval: 0,
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			srv := startTestBenchmarkServer(b, tc.server)

			cfg := tc.client
			cfg.Addr = srv.Addr()
			cfg.Warmup = benchmarkWarmup
			cfg.Duration = benchmarkPerOp * time.Duration(maxInt(b.N, 1))
			cfg.Drain = benchmarkDrain

			timeout := cfg.Warmup + cfg.Duration + cfg.Drain + benchmarkTimeoutPad
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			b.ReportAllocs()
			b.ResetTimer()
			result, err := RunClient(ctx, cfg)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}

			assertRunnerResult(b, result)
			reportBenchmarkMetrics(b, result)
		})
	}
}

func reportBenchmarkMetrics(b *testing.B, result ClientResult) {
	if result.Window <= 0 {
		return
	}

	windowSeconds := result.Window.Seconds()
	b.SetBytes(int64(result.Counters.RxRawBytes / uint64(maxInt(b.N, 1))))
	b.ReportMetric(float64(result.Counters.TxMessages)/windowSeconds, "tx-msg/s")
	b.ReportMetric(float64(result.Counters.RxMessages)/windowSeconds, "rx-msg/s")
	b.ReportMetric(bytesToMiB(result.Counters.RxRawBytes)/windowSeconds, "rx-MiB/s")
	b.ReportMetric(bytesToMiB(result.Counters.RxWireBytes)/windowSeconds, "wire-MiB/s")
	b.ReportMetric(safeRatio(result.Counters.RxWireBytes, result.Counters.RxRawBytes), "wire/raw")
	b.ReportMetric(safeRatio(result.Counters.RxMessages, result.Counters.RxFrames), "msg/frame")
	b.ReportMetric(float64(result.Latency.Avg.Microseconds()), "lat-avg-us")
	b.ReportMetric(float64(result.Latency.P50.Microseconds()), "lat-p50-us")
	b.ReportMetric(float64(result.Latency.P95.Microseconds()), "lat-p95-us")
	b.ReportMetric(float64(result.Latency.P99.Microseconds()), "lat-p99-us")
	b.ReportMetric(float64(result.Latency.Dropped), "lat-drop")
}

func maxInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
