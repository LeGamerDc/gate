package runner

// Logger is the minimal logging surface used by the benchmark runner.
type Logger interface {
	Printf(format string, args ...interface{})
}

// GateLogger adapts a Printf-style logger to gate's logging interface.
type GateLogger struct {
	Logger Logger
}

func (l GateLogger) Debugf(format string, args ...interface{}) { logf(l.Logger, format, args...) }
func (l GateLogger) Infof(format string, args ...interface{})  { logf(l.Logger, format, args...) }
func (l GateLogger) Warnf(format string, args ...interface{})  { logf(l.Logger, format, args...) }
func (l GateLogger) Errorf(format string, args ...interface{}) { logf(l.Logger, format, args...) }
func (l GateLogger) Fatalf(format string, args ...interface{}) { logf(l.Logger, format, args...) }

func logf(logger Logger, format string, args ...interface{}) {
	if logger != nil {
		logger.Printf(format, args...)
	}
}
