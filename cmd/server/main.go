package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/store"
	"github.com/fluffyTighnari/workflow-engine/pkg/worker"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

const (
	queueCapacity      = 100
	defaultWorkerCount = 4
)

func main() {
	appLogger, closeLogs, err := setupLogging()
	if err != nil {
		log.Fatalf("setup logging: %v", err)
	}
	defer closeLogs()

	m := metrics.New()

	// 使用 SQLite 持久化（带内存缓存层）。
	eng, err := engine.NewSQLite("workflow.db")
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	eng.SetLogger(appLogger)
	eng.SetMetrics(m)
	defer eng.Close()

	// 装配消息队列与 worker 池。
	q := queue.NewBoundedQueue(queueCapacity)
	pool := worker.NewPool(
		q,
		eng,
		workerCountFromEnv(),
		worker.NewRunLocker(),
	)
	pool.SetStructuredLogger(appLogger)
	pool.SetMetrics(m)
	pool.Start()
	defer pool.Stop()
	defer q.Close()

	// 崩溃恢复：启动时扫描未完成 run，生成 recover job 入队。
	recoverUnfinished(context.Background(), q, eng)

	mux := http.NewServeMux()

	// POST /workflows — 保存工作流定义
	mux.HandleFunc("POST /workflows", func(w http.ResponseWriter, r *http.Request) {
		var def workflow.WorkflowDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eng.SaveDefinition(r.Context(), def); err != nil {
			switch {
			case errors.Is(err, engine.ErrInvalidDefinition):
				http.Error(w, err.Error(), http.StatusBadRequest)
			case errors.Is(err, store.ErrDefinitionExists):
				http.Error(w, err.Error(), http.StatusConflict)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"id":      def.ID,
			"version": def.Version,
			"saved":   true,
		})
	})

	// GET /workflows/{id}/versions/{version} — 查询指定版本定义
	mux.HandleFunc("GET /workflows/{id}/versions/{version}", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		versionStr := r.PathValue("version")

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}

		def, err := eng.GetDefinition(r.Context(), workflowID, version)
		if err != nil {
			http.Error(w, "workflow version not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(def)
	})

	// POST /workflows/{id}/versions/{version}/runs — 创建执行实例并异步入队
	mux.HandleFunc("POST /workflows/{id}/versions/{version}/runs", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		versionStr := r.PathValue("version")

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}

		var req struct {
			Input map[string]any `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// 先加载 definition
		def, err := eng.GetDefinition(r.Context(), workflowID, version)
		if err != nil {
			http.Error(w, "workflow version not found", http.StatusNotFound)
			return
		}

		// 创建 run（持久化 PENDING）
		run, err := eng.CreateRun(r.Context(), def, req.Input)
		if err != nil {
			m.Submissions.Inc("rejected")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 入队执行任务（队列满时阻塞，保证任务不丢）
		job := queue.Job{
			ID:        run.ID,
			RunID:     run.ID,
			Type:      queue.JobExecute,
			CreatedAt: time.Now().UTC(),
		}
		if err := q.Enqueue(r.Context(), job); err != nil {
			m.Submissions.Inc("enqueue_failed")
			appLogger.Error("run enqueue failed",
				"run_id", run.ID,
				"err", err,
			)
			http.Error(w, fmt.Sprintf("enqueue run: %v", err),
				http.StatusServiceUnavailable)
			return
		}

		m.Submissions.Inc("accepted")
		appLogger.Info("run accepted",
			"run_id", run.ID,
			"workflow_id", workflowID,
			"workflow_version", version,
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"run_id":   run.ID,
			"status":   run.Status,
			"accepted": true,
		})
	})

	// GET /runs/{run_id} — 查询执行状态和各步骤结果
	mux.HandleFunc("GET /runs/{run_id}", func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		run, err := eng.GetRun(r.Context(), runID)
		if err != nil {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(run)
	})

	// POST /runs/{run_id}/execute — 触发执行（异步入队，202 Accepted）
	mux.HandleFunc("POST /runs/{run_id}/execute", func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		// 1. 加载 run
		run, err := eng.GetRun(r.Context(), runID)
		if err != nil {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		// 2. 已终态不可再触发
		if run.Status == workflow.RunSucceeded ||
			run.Status == workflow.RunFailed {
			http.Error(w, fmt.Sprintf("run %q is already terminal (%s)",
				runID, run.Status), http.StatusConflict)
			return
		}

		// 3. 入队执行任务（RUNNING 的 run 由 worker 自动转恢复）
		job := queue.Job{
			ID:        runID,
			RunID:     runID,
			Type:      queue.JobExecute,
			CreatedAt: time.Now().UTC(),
		}
		if err := q.Enqueue(r.Context(), job); err != nil {
			appLogger.Error("run re-enqueue failed",
				"run_id", runID,
				"err", err,
			)
			http.Error(w, fmt.Sprintf("enqueue run: %v", err),
				http.StatusServiceUnavailable)
			return
		}

		appLogger.Info("run re-enqueued",
			"run_id", runID,
			"status", run.Status,
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"run_id":   runID,
			"status":   run.Status,
			"accepted": true,
		})
	})

	// 健康检查
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// 最小指标快照入口，不依赖 Prometheus。
	mux.HandleFunc("GET /debug/metrics", func(w http.ResponseWriter, r *http.Request) {
		m.QueueDepth.Set(int64(q.Len()))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.Snapshot())
	})

	loggedMux := logMiddleware(mux)
	instrumentedMux := instrumentMiddleware(loggedMux, m)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: instrumentedMux,
	}

	// 优雅关闭：先停止 HTTP，再停 worker 池，让当前 Job 收尾。
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		pool.Stop()
		q.Close()
	}()

	log.Println("workflow-engine server listening on :8080")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// setupLogging wires the standard logger to stderr + logs/workflow.log and
// returns a logger that also writes per-run files under logs/runs/.
// WORKFLOW_LOG_DIR overrides the default logs/ directory.
func setupLogging() (logging.Logger, func(), error) {
	logDir := os.Getenv("WORKFLOW_LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create log dir %q: %w", logDir, err)
	}

	mainPath := filepath.Join(logDir, "workflow.log")
	file, err := os.OpenFile(
		mainPath,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open main log %q: %w", mainPath, err)
	}

	log.SetOutput(io.MultiWriter(os.Stderr, file))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	base := logging.FromStandard(log.Default())
	runLogger, err := logging.NewRunFileLogger(
		base,
		filepath.Join(logDir, "runs"),
	)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	log.Printf("logging: main=%s runs=%s", mainPath, runLogger.Dir())

	cleanup := func() {
		_ = runLogger.Close()
		_ = file.Close()
	}
	return runLogger, cleanup, nil
}

// workerCountFromEnv reads WORKFLOW_WORKERS, falling back to the default.
func workerCountFromEnv() int {
	raw := os.Getenv("WORKFLOW_WORKERS")
	if raw == "" {
		return defaultWorkerCount
	}

	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		log.Printf(
			"invalid WORKFLOW_WORKERS=%q, using %d",
			raw,
			defaultWorkerCount,
		)
		return defaultWorkerCount
	}
	return count
}

// recoverUnfinished 启动时扫描 PENDING/RUNNING 的 run，
// 为每个未完成任务生成 recover job 入队（崩溃恢复）。
func recoverUnfinished(ctx context.Context, q queue.Queue, eng *engine.Engine) {
	ids, err := eng.ListRunsByStatus(ctx,
		workflow.RunPending, workflow.RunRunning)
	if err != nil {
		log.Printf("recover: scan unfinished runs: %v", err)
		return
	}

	enqueued := 0
	for _, id := range ids {
		job := queue.Job{
			ID:        id,
			RunID:     id,
			Type:      queue.JobRecover,
			CreatedAt: time.Now().UTC(),
		}
		if err := q.Enqueue(ctx, job); err != nil {
			log.Printf("recover: enqueue %s: %v", id, err)
			continue
		}
		enqueued++
	}
	log.Printf("recover: re-enqueued %d unfinished run(s)", enqueued)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

// instrumentMiddleware records low-cardinality HTTP metrics.
func instrumentMiddleware(
	next http.Handler,
	m *metrics.Metrics,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.HTTPInFlight.Inc()
		defer m.HTTPInFlight.Dec()

		started := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.HTTPRequests.Inc(
			r.Method,
			route,
			strconv.Itoa(recorder.status),
		)
		m.HTTPDuration.ObserveSeconds(
			time.Since(started).Seconds(),
			r.Method,
			route,
		)
	})
}

// logMiddleware 记录每个请求的方法和路径。
func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if len(path) > 80 {
			path = path[:80] + "..."
		}
		log.Printf("%s %s", r.Method, path)
		next.ServeHTTP(w, r)
	})
}
