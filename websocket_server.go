package gate

import "errors"

// WebSocketConfig is kept as an alias for callers that prefer a websocket-
// specific name, but websocket mode is now configured through the shared
// Config type.
type WebSocketConfig = Config

// StartWebSocketEventLoop starts gate in websocket transport mode and blocks
// until it exits.
func StartWebSocketEventLoop(c *Config) error {
	s, err := StartWebSocketServer(c)
	if err != nil {
		return err
	}
	return s.Wait()
}

// StartWebSocketServer starts gate in websocket transport mode. This is a thin
// wrapper over StartServer so websocket and raw TCP share the same server
// implementation and Config.
func StartWebSocketServer(c *Config) (*Server, error) {
	if c == nil {
		return nil, errors.New("config must set")
	}
	cfg := *c
	cfg.Transport = ServerTransportWebSocket
	return StartServer(&cfg)
}
