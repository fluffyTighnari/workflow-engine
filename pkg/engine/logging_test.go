package engine

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
)

func TestEngineLogsRunAndStepLifecycle(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)

	var buf bytes.Buffer
	e.SetLogger(logging.NewText(&buf, slog.LevelDebug))

	run, def := createRunForPipeline(t, e)
	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"run created",
		"run_id=" + run.ID,
		"step started",
		"step_id=A",
		"step succeeded",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q:\n%s", want, out)
		}
	}
}
