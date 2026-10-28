package logging

import (
	"bytes"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFromStandardIncludesLevelAndFields(t *testing.T) {
	var buf bytes.Buffer
	logger := FromStandard(log.New(&buf, "", 0))

	logger.Info("run created", "run_id", "r1", "step_count", 2)
	logger.Error("step failed", "run_id", "r1", "err", "boom")

	out := buf.String()
	for _, want := range []string{
		"level=info",
		`msg="run created"`,
		"run_id=r1",
		"step_count=2",
		"level=error",
		`msg="step failed"`,
		"err=boom",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestSlogAdapterAndDiscard(t *testing.T) {
	var buf bytes.Buffer
	NewText(&buf, slog.LevelDebug).Debug("cache miss", "run_id", "r1")

	if !strings.Contains(buf.String(), "cache miss") {
		t.Fatalf("slog output missing message:\n%s", buf.String())
	}

	Discard().Info("this must not panic")
	NewSlog(nil).Info("nil logger falls back to default")
}

func TestRunFileLoggerWritesPerRunFiles(t *testing.T) {
	dir := t.TempDir()
	logger, err := NewRunFileLogger(Discard(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()

	logger.Info("step started", "run_id", "run-1", "step_id", "A")
	logger.Error("step failed", "run_id", "run-1", "err", "boom")
	logger.Info("definition saved", "workflow_id", "demo")

	content, err := os.ReadFile(filepath.Join(dir, "run-1.log"))
	if err != nil {
		t.Fatalf("read run log: %v", err)
	}

	out := string(content)
	for _, want := range []string{
		"step started",
		"step failed",
		"run_id=run-1",
		"step_id=A",
		"err=boom",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("run log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "definition saved") {
		t.Errorf("record without run_id must not enter run log:\n%s", out)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("run log dir has %d files, want 1", len(entries))
	}
}

func TestSafeRunFilename(t *testing.T) {
	got := safeRunFilename("../run/1")
	if strings.ContainsAny(got, `/\.`) {
		t.Fatalf("unsafe run filename: %q", got)
	}
	if got == "" {
		t.Fatal("run filename must not be empty")
	}
}
