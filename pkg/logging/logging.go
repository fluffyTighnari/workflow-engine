// Package logging provides the minimal leveled logger used by the engine,
// worker pool, and async facade.
package logging

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
)

// Logger is the small logging surface used across the project.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Default returns the process-wide slog logger.
func Default() Logger {
	return NewSlog(slog.Default())
}

// Discard returns a logger that drops every record. It is useful in tests.
func Discard() Logger {
	return NewSlog(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// NewText returns a text slog logger writing to w at the given level.
func NewText(w io.Writer, level slog.Level) Logger {
	return NewSlog(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: level,
	})))
}

// NewSlog adapts a *slog.Logger. A nil logger falls back to Default.
func NewSlog(logger *slog.Logger) Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return slogLogger{logger: logger}
}

// FromStandard adapts a standard-library *log.Logger. Levels are encoded
// as key/value fields in the log line.
func FromStandard(logger *log.Logger) Logger {
	if logger == nil {
		return Default()
	}
	return standardLogger{logger: logger}
}

type slogLogger struct {
	logger *slog.Logger
}

func (l slogLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

func (l slogLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

func (l slogLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

func (l slogLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

type standardLogger struct {
	logger *log.Logger
}

func (l standardLogger) Debug(msg string, args ...any) {
	l.log("debug", msg, args...)
}

func (l standardLogger) Info(msg string, args ...any) {
	l.log("info", msg, args...)
}

func (l standardLogger) Warn(msg string, args ...any) {
	l.log("warn", msg, args...)
}

func (l standardLogger) Error(msg string, args ...any) {
	l.log("error", msg, args...)
}

func (l standardLogger) log(level string, msg string, args ...any) {
	if len(args) == 0 {
		l.logger.Printf("level=%s msg=%q", level, msg)
		return
	}
	l.logger.Printf(
		"level=%s msg=%q %s",
		level,
		msg,
		formatFields(args),
	)
}

func formatFields(args []any) string {
	var b strings.Builder

	for i := 0; i < len(args); i += 2 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}

		fmt.Fprintf(&b, "%v=", args[i])
		if i+1 < len(args) {
			fmt.Fprintf(&b, "%v", args[i+1])
		} else {
			b.WriteString("<missing>")
		}
	}

	return b.String()
}
