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
	"time"

	"github.com/LeGamerDc/gate"
	"github.com/LeGamerDc/gate/benchmark/common"
)

func main() {
	var (
		addr              = flag.String("addr", "127.0.0.1:8081", "listen address")
		loops             = flag.Int("loops", 1, "gnet event loop count")
		compressThreshold = flag.Int("compress-threshold", 1024, "compress payloads larger than this size, <=0 disables compression")
		maxClusterSize    = flag.Int("max-cluster-size", 32*1024, "compound reply limit in bytes, <=0 disables compounding")
		maxBufferSize     = flag.Int("max-buffer-size", 2*1024*1024, "per-connection outbound buffer limit in bytes, <=0 disables the limit")
		encrypt           = flag.Bool("encrypt", false, "encrypt replies and expect encrypted requests")
		pprofAddr         = flag.String("pprof-addr", "", "optional pprof listen address, empty disables it")
	)
	flag.Parse()

	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("pprof stopped: %v", err)
			}
		}()
	}

	var cipher gate.Cipher
	if *encrypt {
		cipher = common.XORCipher{Key: 0x5a}
	}

	srv, err := gate.StartServer(&gate.Config{
		Addr:      *addr,
		LoopCount: *loops,
		CHB: handlerBuilder{
			cipher: cipher,
		},
		SB: gate.NewSenderBuilder(&gate.SenderConfig{
			CompressThreshold: *compressThreshold,
			MaxBufferSize:     *maxBufferSize,
			MaxClusterSize:    *maxClusterSize,
		}),
		Logger: stdLogger{},
	})
	if err != nil {
		log.Fatalf("start benchmark server: %v", err)
	}

	log.Printf(
		"benchmark server listening on %s (loops=%d compress-threshold=%d max-cluster-size=%d max-buffer-size=%d encrypt=%v)",
		srv.Addr(),
		*loops,
		*compressThreshold,
		*maxClusterSize,
		*maxBufferSize,
		*encrypt,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(shutdownCtx); err != nil {
		log.Fatalf("stop benchmark server: %v", err)
	}
	if err := srv.Wait(); err != nil {
		log.Fatalf("wait benchmark server: %v", err)
	}
}

type handlerBuilder struct {
	cipher gate.Cipher
}

func (b handlerBuilder) Build(conn *gate.Conn) gate.ConnHandler {
	if b.cipher != nil {
		conn.UpdateCipher(b.cipher)
	}
	return &echoHandler{conn: conn}
}

type echoHandler struct {
	conn *gate.Conn
}

func (h *echoHandler) Handle(raw []byte) {
	msg := append([]byte(nil), raw...)
	if err := h.conn.Send(msg); err != nil {
		h.conn.Close()
	}
}

func (h *echoHandler) Close() {}

type stdLogger struct{}

func (stdLogger) Debugf(format string, args ...interface{}) { log.Printf(format, args...) }
func (stdLogger) Infof(format string, args ...interface{})  { log.Printf(format, args...) }
func (stdLogger) Warnf(format string, args ...interface{})  { log.Printf(format, args...) }
func (stdLogger) Errorf(format string, args ...interface{}) { log.Printf(format, args...) }
func (stdLogger) Fatalf(format string, args ...interface{}) { log.Printf(format, args...) }
