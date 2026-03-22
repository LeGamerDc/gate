package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/LeGamerDc/gate/benchmark/runner"
)

func main() {
	var cfg runner.ServerConfig
	var pprofAddr string

	flag.StringVar(&cfg.Addr, "addr", runner.DefaultServerAddr, "listen address")
	flag.IntVar(&cfg.LoopCount, "loops", runner.DefaultServerLoopCount, "gnet event loop count")
	flag.IntVar(&cfg.CompressThreshold, "compress-threshold", runner.DefaultServerCompressThreshold, "compress payloads larger than this size, <=0 disables compression")
	flag.IntVar(&cfg.MaxClusterSize, "max-cluster-size", runner.DefaultServerMaxClusterSize, "compound reply limit in bytes, <=0 disables compounding")
	flag.IntVar(&cfg.MaxBufferSize, "max-buffer-size", runner.DefaultServerMaxBufferSize, "per-connection outbound buffer limit in bytes, <=0 disables the limit")
	flag.BoolVar(&cfg.Encrypt, "encrypt", false, "encrypt replies and expect encrypted requests")
	flag.StringVar(&pprofAddr, "pprof-addr", "", "optional pprof listen address, empty disables it")
	flag.Parse()

	if pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof stopped: %v", err)
			}
		}()
	}

	cfg.Logger = log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runner.RunServer(ctx, cfg); err != nil {
		log.Fatalf("benchmark server: %v", err)
	}
}
