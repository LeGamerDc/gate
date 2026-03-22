package runner

import (
	"context"
	"time"

	"github.com/LeGamerDc/gate"
	"github.com/LeGamerDc/gate/benchmark/common"
)

const (
	DefaultServerAddr              = "127.0.0.1:8081"
	DefaultServerLoopCount         = 1
	DefaultServerCompressThreshold = 1024
	DefaultServerMaxClusterSize    = 32 * 1024
	DefaultServerMaxBufferSize     = 2 * 1024 * 1024
)

// ServerConfig controls the benchmark echo server.
type ServerConfig struct {
	Addr              string
	LoopCount         int
	CompressThreshold int
	MaxClusterSize    int
	MaxBufferSize     int
	Encrypt           bool
	Logger            Logger
}

func (c *ServerConfig) fillDefaults() {
	if c.Addr == "" {
		c.Addr = DefaultServerAddr
	}
	if c.LoopCount <= 0 {
		c.LoopCount = DefaultServerLoopCount
	}
}

// StartServer starts the benchmark echo server and returns the running gate
// server handle.
func StartServer(cfg ServerConfig) (*gate.Server, error) {
	cfg.fillDefaults()

	var cipher gate.Cipher
	if cfg.Encrypt {
		cipher = common.XORCipher{Key: 0x5a}
	}

	srv, err := gate.StartServer(&gate.Config{
		Addr:      cfg.Addr,
		LoopCount: cfg.LoopCount,
		CHB: handlerBuilder{
			cipher: cipher,
		},
		SB: gate.NewSenderBuilder(&gate.SenderConfig{
			CompressThreshold: cfg.CompressThreshold,
			MaxBufferSize:     cfg.MaxBufferSize,
			MaxClusterSize:    cfg.MaxClusterSize,
		}),
		Logger: GateLogger{Logger: cfg.Logger},
	})
	if err != nil {
		return nil, err
	}

	logf(
		cfg.Logger,
		"benchmark server listening on %s (loops=%d compress-threshold=%d max-cluster-size=%d max-buffer-size=%d encrypt=%v)",
		srv.Addr(),
		cfg.LoopCount,
		cfg.CompressThreshold,
		cfg.MaxClusterSize,
		cfg.MaxBufferSize,
		cfg.Encrypt,
	)
	return srv, nil
}

// RunServer starts the benchmark server and blocks until ctx is canceled, then
// shuts the server down gracefully.
func RunServer(ctx context.Context, cfg ServerConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}

	srv, err := StartServer(cfg)
	if err != nil {
		return err
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(shutdownCtx); err != nil {
		return err
	}
	return srv.Wait()
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
	if err := h.conn.Send(raw); err != nil {
		h.conn.Close()
	}
}

func (h *echoHandler) Close() {}
