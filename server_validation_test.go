package gate

import "testing"

func TestStartServerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	validHandler := testHandlerBuilder{build: func(conn *Conn) ConnHandler {
		return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
	}}

	tests := []struct {
		name string
		cfg  *Config
	}{
		{name: "nil", cfg: nil},
		{name: "missing_handler", cfg: &Config{SB: DefaultSenderBuilder, Logger: noopTestLogger{}}},
		{name: "missing_sender", cfg: &Config{CHB: validHandler, Logger: noopTestLogger{}}},
		{name: "missing_logger", cfg: &Config{CHB: validHandler, SB: DefaultSenderBuilder}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := StartServer(tc.cfg); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestStartWebSocketServerRejectsNilConfig(t *testing.T) {
	t.Parallel()

	if _, err := StartWebSocketServer(nil); err == nil {
		t.Fatal("expected validation error")
	}
}
