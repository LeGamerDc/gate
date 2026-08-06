package gate

import (
	"log/slog"

	"go.uber.org/zap"
)

// Field 是 zap.Field 的类型别名（零成本、完全互换）。zap.Field 是调用方的
// 选型而不是 gate 的实现选型——gate 换 reactor 不会导致它变（01 D16）。
type Field = zap.Field

// Logger 只有三级、没有 printf 风格：gate 的每一条日志都应该是可执行的信号。
// *zap.Logger 直接满足它，Log: zap.L() 即可。
type Logger interface {
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// SlogLogger 是 slog 用户的适配器。
func SlogLogger(l *slog.Logger) Logger { return slogLogger{l} }

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) convert(fields []Field) []any {
	if len(fields) == 0 {
		return nil
	}
	enc := zapcoreEncoder{}
	args := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		k, v := enc.pair(f)
		args = append(args, k, v)
	}
	return args
}

func (s slogLogger) Info(msg string, fields ...Field)  { s.l.Info(msg, s.convert(fields)...) }
func (s slogLogger) Warn(msg string, fields ...Field)  { s.l.Warn(msg, s.convert(fields)...) }
func (s slogLogger) Error(msg string, fields ...Field) { s.l.Error(msg, s.convert(fields)...) }

// zapcoreEncoder 把 zap.Field 拍平成 slog 的 K/V。只做常见类型，其余走 Any。
type zapcoreEncoder struct{}

func (zapcoreEncoder) pair(f Field) (string, any) {
	switch {
	case f.Interface != nil:
		return f.Key, f.Interface
	case f.String != "":
		return f.Key, f.String
	default:
		return f.Key, f.Integer
	}
}

// nopLogger 在未配置 Log 时使用：库不该往别人的 stdout 里写。
type nopLogger struct{}

func (nopLogger) Info(string, ...Field)  {}
func (nopLogger) Warn(string, ...Field)  {}
func (nopLogger) Error(string, ...Field) {}
