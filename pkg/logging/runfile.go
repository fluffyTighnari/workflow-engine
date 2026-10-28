package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RunFileLogger writes every record to a base logger and also appends
// records carrying a "run_id" field to a per-run file:
//
//	<dir>/<run_id>.log
//
// It is safe for concurrent use.
type RunFileLogger struct {
	base Logger
	dir  string
}

// NewRunFileLogger creates a run-scoped logger. Records without a run_id
// are only written to base.
func NewRunFileLogger(base Logger, dir string) (*RunFileLogger, error) {
	if base == nil {
		base = Default()
	}
	if dir == "" {
		dir = filepath.Join("logs", "runs")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create run log dir %q: %w", dir, err)
	}

	return &RunFileLogger{base: base, dir: dir}, nil
}

// Dir returns the directory containing per-run log files.
func (l *RunFileLogger) Dir() string {
	return l.dir
}

// Close is a no-op because files are opened per record.
func (l *RunFileLogger) Close() error {
	return nil
}

func (l *RunFileLogger) Debug(msg string, args ...any) {
	l.log("debug", msg, args...)
}

func (l *RunFileLogger) Info(msg string, args ...any) {
	l.log("info", msg, args...)
}

func (l *RunFileLogger) Warn(msg string, args ...any) {
	l.log("warn", msg, args...)
}

func (l *RunFileLogger) Error(msg string, args ...any) {
	l.log("error", msg, args...)
}

func (l *RunFileLogger) log(level string, msg string, args ...any) {
	l.writeBase(level, msg, args...)

	runID := fieldString(args, "run_id")
	if runID == "" {
		return
	}

	line := fmt.Sprintf(
		"%s level=%s msg=%q %s\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		level,
		msg,
		formatFields(args),
	)
	if err := l.appendRunLine(runID, line); err != nil {
		// Avoid a recursive per-run write: this message has no run_id field.
		l.base.Error("run log write failed",
			"log_dir", l.dir,
			"file_run_id", runID,
			"err", err,
		)
	}
}

func (l *RunFileLogger) writeBase(level string, msg string, args ...any) {
	switch level {
	case "debug":
		l.base.Debug(msg, args...)
	case "info":
		l.base.Info(msg, args...)
	case "warn":
		l.base.Warn(msg, args...)
	default:
		l.base.Error(msg, args...)
	}
}

func (l *RunFileLogger) appendRunLine(runID string, line string) error {
	path := filepath.Join(l.dir, safeRunFilename(runID)+".log")

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(line)
	return err
}

func fieldString(args []any, key string) string {
	for i := 0; i+1 < len(args); i += 2 {
		if fmt.Sprint(args[i]) == key {
			return fmt.Sprint(args[i+1])
		}
	}
	return ""
}

func safeRunFilename(runID string) string {
	var b strings.Builder
	for _, r := range runID {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}

	name := b.String()
	if name == "" {
		return "unknown"
	}
	if len(name) > 128 {
		name = name[:128]
	}
	return name
}
