package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/LeGamerDc/gate/benchmark/runner"
)

func main() {
	cfg := loadConfig()
	cfg.Logger = log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if _, err := runner.RunClient(ctx, cfg); err != nil {
		log.Printf("benchmark failed: %v", err)
		os.Exit(1)
	}
}

func loadConfig() runner.ClientConfig {
	var cfg runner.ClientConfig
	flag.StringVar(&cfg.Addr, "addr", runner.DefaultServerAddr, "server address")
	flag.IntVar(&cfg.Connections, "connections", runner.DefaultClientConnections, "number of TCP connections")
	flag.IntVar(&cfg.PayloadSize, "payload-size", runner.DefaultClientPayloadSize, "payload size in bytes, minimum 24")
	flag.DurationVar(&cfg.Duration, "duration", runner.DefaultClientDuration, "measurement duration")
	flag.DurationVar(&cfg.Warmup, "warmup", runner.DefaultClientWarmup, "warmup duration before measurement")
	flag.DurationVar(&cfg.Drain, "drain", runner.DefaultClientDrain, "wait time after sending stops to collect in-flight replies")
	flag.IntVar(&cfg.Rate, "rate", 0, "target aggregate send rate in messages per second, 0 means send as fast as possible")
	flag.IntVar(&cfg.Inflight, "inflight", runner.DefaultClientInflight, "max in-flight requests per connection, <=0 disables the limit")
	flag.BoolVar(&cfg.Encrypt, "encrypt", false, "encrypt requests and expect encrypted replies")
	flag.StringVar(&cfg.PayloadMode, "payload-mode", runner.DefaultClientPayloadMode, "payload body mode: repeat or random")
	flag.IntVar(&cfg.LatencyBuffer, "latency-buffer", runner.DefaultClientLatencyBuffer, "latency sample channel size")
	flag.DurationVar(&cfg.ReportInterval, "report-interval", runner.DefaultClientReportInterval, "interval for periodic progress logs, 0 disables it")
	flag.Parse()
	return cfg
}
