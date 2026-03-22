package runner

import (
	"context"
	"testing"
	"time"
)

func TestRunnerPlainRoundTrip(t *testing.T) {
	srv := startTestBenchmarkServer(t, ServerConfig{
		Addr:              "127.0.0.1:0",
		LoopCount:         1,
		CompressThreshold: -1,
		MaxClusterSize:    -1,
		MaxBufferSize:     DefaultServerMaxBufferSize,
	})

	result, err := RunClient(context.Background(), ClientConfig{
		Addr:           srv.Addr(),
		Connections:    4,
		PayloadSize:    256,
		Duration:       300 * time.Millisecond,
		Warmup:         50 * time.Millisecond,
		Drain:          150 * time.Millisecond,
		Rate:           200,
		Inflight:       8,
		PayloadMode:    DefaultClientPayloadMode,
		LatencyBuffer:  1024,
		ReportInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRunnerResult(t, result)
}

func TestRunnerEncryptedCompressedRoundTrip(t *testing.T) {
	srv := startTestBenchmarkServer(t, ServerConfig{
		Addr:              "127.0.0.1:0",
		LoopCount:         1,
		CompressThreshold: 1,
		MaxClusterSize:    DefaultServerMaxClusterSize,
		MaxBufferSize:     DefaultServerMaxBufferSize,
		Encrypt:           true,
	})

	result, err := RunClient(context.Background(), ClientConfig{
		Addr:           srv.Addr(),
		Connections:    2,
		PayloadSize:    8 * 1024,
		Duration:       300 * time.Millisecond,
		Warmup:         50 * time.Millisecond,
		Drain:          150 * time.Millisecond,
		Rate:           100,
		Inflight:       4,
		Encrypt:        true,
		PayloadMode:    DefaultClientPayloadMode,
		LatencyBuffer:  1024,
		ReportInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRunnerResult(t, result)
}

func startTestBenchmarkServer(tb testing.TB, cfg ServerConfig) interface{ Addr() string } {
	tb.Helper()

	srv, err := StartServer(cfg)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			tb.Fatalf("stop benchmark server: %v", err)
		}
		if err := srv.Wait(); err != nil {
			tb.Fatalf("wait benchmark server: %v", err)
		}
	})

	return srv
}

func assertRunnerResult(tb testing.TB, result ClientResult) {
	tb.Helper()

	if result.Window <= 0 {
		tb.Fatal("expected positive measurement window")
	}
	if result.Counters.TxMessages == 0 {
		tb.Fatal("expected transmitted messages")
	}
	if result.Counters.RxMessages == 0 {
		tb.Fatal("expected received messages")
	}
	if result.Counters.MismatchErrors != 0 {
		tb.Fatalf("unexpected mismatch errors: %d", result.Counters.MismatchErrors)
	}
	if result.Counters.ReadErrors != 0 {
		tb.Fatalf("unexpected read errors: %d", result.Counters.ReadErrors)
	}
	if result.Counters.WriteErrors != 0 {
		tb.Fatalf("unexpected write errors: %d", result.Counters.WriteErrors)
	}
	if result.Latency.Samples == 0 {
		tb.Fatal("expected latency samples")
	}
}
