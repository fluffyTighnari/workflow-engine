# Changelog

## 2026-10-28 — 最小进程内 Metrics

### 设计

- 新增 `pkg/metrics`，不依赖 Prometheus：
  - `Counter`（atomic）
  - `Gauge`（atomic）
  - `Histogram`（固定 bucket、count、sum）
  - `CounterVec` / `HistogramVec`（低基数标签）
  - `Metrics.Snapshot()` 返回 JSON 友好快照
- 与 logger 一样通过依赖注入共享：
  - `Engine.SetMetrics` / `Engine.Metrics`
  - `Worker.SetMetrics` / `Worker.Metrics`
  - `Pool.SetMetrics` / `Pool.Metrics`
  - `async.WithMetrics` / `AsyncEngine.Metrics` / `AsyncEngine.MetricsSnapshot`
- 默认每个 Engine/Pool 创建自己的 `metrics.New()`；AsyncEngine 和 Server 显式让 Engine 与 Pool 共享同一个 registry。

### 指标与插入点

Run 级：

- `workflow_runs_created_total`
  - `Engine.CreateRun` 成功返回前
- `workflow_runs_finished_total{status}`
  - `Engine.recordRunTerminal`
- `workflow_run_failures_total{failure_kind}`
  - `Engine.recordRunTerminal`，仅 FAILED
- `workflow_run_duration_seconds{status}`
  - `Engine.recordRunTerminal`，从 `run.CreatedAt` 到终态
- `workflow_run_execution_duration_seconds{status}`
  - `Engine.RunWorkflow` defer，从开始执行到返回
- `workflow_run_queue_wait_seconds`
  - `Worker.executeRun`，从 `run.CreatedAt` 到 Worker 开始执行

Step 级：

- `workflow_steps_started_total{step_type}`
  - `Engine.ExecuteStep`，Executor 查找成功后、输入解析前
- `workflow_steps_finished_total{step_type,status}`
  - `Engine.recordStepFinished`，仅在终态成功持久化后
- `workflow_step_failures_total{step_type,failure_kind}`
  - `Engine.recordStepFinished`，仅 FAILED
- `workflow_step_duration_seconds{step_type,status}`
  - `Engine.recordStepFinished`
- `workflow_steps_in_flight`
  - `Engine.ExecuteStep` 开始 +1，defer -1

Job / Worker 级：

- `workflow_jobs_started_total{job_type}`
  - `Worker.handle`，拿到 Run 锁后
- `workflow_jobs_finished_total{job_type,status}`
  - `Worker.handle` defer
- `workflow_job_duration_seconds{job_type}`
  - `Worker.handle` defer
- `workflow_jobs_duplicate_total{job_type}`
  - `Worker.handle`，TryLock 失败
- `workflow_jobs_panic_total`
  - `Worker.handle` panic recover
- `workflow_workers_busy`
  - `Worker.handle` 开始 +1 / defer -1
- `workflow_workers_total`
  - `worker.NewPool` / `Pool.SetMetrics`
- `workflow_queue_depth`
  - 快照时从 `Queue.Len()` 更新

提交 / HTTP：

- `workflow_submissions_total{result}`
  - `AsyncEngine.SubmitRun`
  - Server 创建 Run 入口
- `workflow_http_requests_total{method,route,status}`
  - Server `instrumentMiddleware`
- `workflow_http_request_duration_seconds{method,route}`
  - Server `instrumentMiddleware`
- `workflow_http_in_flight`
  - Server `instrumentMiddleware`

### 暴露

- Server 新增 `GET /debug/metrics`，返回 JSON 快照。
- `AsyncEngine.MetricsSnapshot()` 会先更新 queue depth，再返回快照。
- 固定 duration bucket：
  `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300` 秒。

### 文件变化

- 新增 `pkg/metrics/metrics.go`
- 新增 `pkg/metrics/metrics_test.go`
- 新增 `pkg/engine/metrics.go`
- 新增 `pkg/engine/metrics_test.go`
- 新增 `pkg/worker/metrics_test.go`
- 修改 `pkg/engine/engine.go`
- 修改 `pkg/engine/factory.go`
- 修改 `pkg/engine/execute.go`
- 修改 `pkg/engine/recover.go`
- 修改 `pkg/worker/worker.go`
- 修改 `pkg/worker/pool.go`
- 修改 `pkg/async/async.go`
- 修改 `pkg/async/async_test.go`
- 修改 `cmd/server/main.go`
- 修改 `CHANGELOG.md`

## 2026-10-27 — 可观测性基础日志

### Logger

- 新增 `pkg/logging`：
  - `Logger` 接口：`Debug` / `Info` / `Warn` / `Error`
  - 默认基于 `log/slog`
  - `NewSlog` / `NewText` / `Default`
  - `FromStandard`：兼容标准库 `*log.Logger`
  - `Discard`：测试静默 logger
- Engine 增加 `SetLogger`，默认使用 `logging.Default()`。
- Worker 保留 `SetLogger(*log.Logger)` 兼容接口，新增 `SetStructuredLogger`。
- WorkerPool 增加 `SetStructuredLogger`，`SetLogger(*log.Logger)` 继续可用。
- AsyncEngine 增加 `WithLogger` 选项。

### 新增日志

- Engine：
  - Run 创建成功/失败
  - Definition 保存成功/冲突/失败
  - Executor 注册
  - Step 开始、成功、失败、取消、输入解析失败
  - Step 持久化失败与状态回滚
  - Run 执行开始/成功/取消/死锁
  - Run 最终状态持久化失败
  - 崩溃恢复开始、RUNNING Step 重置、修复持久化失败、恢复完成/失败
- Worker：
  - Job 开始、重复 Job 跳过
  - Job 加载 Run/Definition 失败
  - Run 执行完成、终态失败、可重试错误、引擎错误
  - Recovery 完成/失败
  - panic 恢复
  - 增加 `duration_ms`
- WorkerPool：
  - 启动、停止
- AsyncEngine：
  - Run 提交成功/创建失败/入队失败
  - Recovery 扫描与入队失败
  - Close

### 单次 Run 日志

- 新增 `logging.RunFileLogger`：
  - 所有记录继续写入主日志；
  - 带 `run_id` 的记录额外追加到 `logs/runs/<run_id>.log`；
  - 文件按记录打开/追加，避免长期占用大量文件描述符。
- `cmd/server` 默认日志布局：
  - `logs/workflow.log`：全部日志
  - `logs/runs/<run_id>.log`：单次 Run 的完整日志
- 支持 `WORKFLOW_LOG_DIR` 覆盖日志目录。
- Server 的 Run 创建/入队/重新入队日志现在带 `run_id`。
- 新增 `scripts/runlog.sh`：
  - `./scripts/runlog.sh <run_id>` 直接 tail 单次 Run 日志。
- 新增 `logs/README.md` 说明日志位置和覆盖方式。
- 新增 `.gitignore` 忽略运行时日志和本地 SQLite 文件。

### 日志字段约定

- 常用字段：
  - `run_id`
  - `workflow_id`
  - `workflow_version`
  - `step_id`
  - `step_type`
  - `worker_id`
  - `job_type`
  - `status`
  - `duration_ms`
  - `failure_kind`
  - `err`
  - `state_rolled_back`
- 不记录完整 workflow input/output。
- Store 层暂不新增日志；错误由 Engine/Worker 统一记录，避免重复。

### 文件变化

- 新增 `pkg/logging/logging.go`
- 新增 `pkg/logging/runfile.go`
- 新增 `pkg/logging/logging_test.go`
- 新增 `pkg/engine/logging_test.go`
- 新增 `logs/README.md`
- 新增 `scripts/runlog.sh`
- 新增 `.gitignore`
- 修改 `cmd/server/main.go`
- 修改 `pkg/engine/engine.go`
- 修改 `pkg/engine/execute.go`
- 修改 `pkg/engine/recover.go`
- 修改 `pkg/engine/factory.go`
- 修改 `pkg/worker/worker.go`
- 修改 `pkg/worker/pool.go`
- 修改 `pkg/async/async.go`
- 修改 `CHANGELOG.md`

## 2026-10-27 — 多 Worker 并发执行

### 行为

- 支持同一进程内启动多个 Worker。
- 不同 Run 可以并行执行。
- 同一个 Run 通过进程内 `RunLocker` 保证同一时刻只被一个 Worker 处理。
- 重复的 `JobExecute` / `JobRecover`：
  - 如果目标 Run 正在执行，后来者直接跳过；
  - 如果 Run 已终态，重新加载后跳过。
- Worker 增加 panic 隔离：单个 Job 的 Executor panic 不会带崩 Worker，
  锁会通过 defer 释放，Run 可由后续 `JobRecover` 修复。
- 当前仍然只支持单进程；跨进程互斥和数据库租约暂未实现。

### 队列

- `BoundedQueue` 不再关闭 jobs channel，改用独立 `done` channel：
  - `Close` 与 `Enqueue` 并发时不会再出现 `send on closed channel` panic；
  - `Close` 后仍会先消费完已入队任务，再返回 `ErrClosed`。

### API

- 新增 `worker.RunLocker` / `worker.MemoryRunLocker` / `worker.NewRunLocker`。
- 新增 `worker.Pool`：
  - `worker.NewPool(queue, engine, workerCount, locker)`
  - `Pool.Start()`
  - `Pool.Stop()`
  - `Pool.Size()`
- 新增 `worker.NewWithLocker`；`worker.New` 继续可用，并创建独立 locker。
- `worker.Worker` 增加：
  - `SetID`
  - panic 恢复
  - 重复 Run 跳过日志
- `async.AsyncEngine` 改为持有 `worker.Pool`：
  - 原有 `NewSQLite` / `NewMemory` 保持单 Worker 默认行为；
  - 新增 `NewSQLiteWithOptions` / `NewMemoryWithOptions`；
  - 新增 `WithWorkers(n)`；
  - 新增 `WorkerCount()`。
- `cmd/server` 默认使用 4 个 Worker，可通过 `WORKFLOW_WORKERS` 覆盖。

### 文件变化

- 修改 `pkg/queue/queue.go`
- 修改 `pkg/queue/queue_test.go`
- 修改 `pkg/worker/worker.go`
- 新增 `pkg/worker/lock.go`
- 新增 `pkg/worker/lock_test.go`
- 新增 `pkg/worker/pool.go`
- 新增 `pkg/worker/pool_test.go`
- 修改 `pkg/async/async.go`
- 修改 `pkg/async/async_test.go`
- 修改 `cmd/server/main.go`
- 修改 `CHANGELOG.md`

### 测试

- 不同 Run 在 2 个 Worker 下可以同时进入 Executor。
- 同一个 Run 重复入队 20 次，Executor 只执行 1 次。
- 同一 Run 的 `JobExecute` + `JobRecover` 并发投递，只执行 1 次。
- Executor panic 后 Worker 继续工作，Run 锁释放，可通过 recover job 恢复。
- RunLocker 并发竞争时只有一个成功者。
- 队列并发 Close/Enqueue 不 panic。
- `go test -race ./pkg/queue ./pkg/worker ./pkg/async` 通过。

## 2026-10-26 — 一致性与缓存策略修复

### 定义不可变

- `SaveDefinition` 改为 insert-only：
  - 同一个 `(workflow_id, version)` 已存在时返回 `store.ErrDefinitionExists`；
  - 不再覆盖已保存的定义。
- 定义校验下沉到 `workflow.ValidateDefinition`，Engine 与所有内置 Store 的
  `SaveDefinition` 都会校验：
  - Engine 保持返回 `engine.ErrInvalidDefinition`；
  - Store 直接调用时返回 `store.ErrInvalidDefinition`。
- `Engine.CreateRun` 不再无条件 upsert 定义：
  - 定义不存在时保存；
  - 定义已存在且内容一致时复用；
  - 定义已存在但内容不同时返回 `store.ErrDefinitionExists`。
- `POST /workflows` 对非法定义返回 `400`，对已存在的版本返回 `409`。

### 缓存写入策略

- `CachedStore` 的所有写操作只写持久化层：
  - `SaveDefinition`
  - `CreateRun`
  - `SaveExecutionState`
- 写入成功后只废弃本实例对应的缓存条目：
  - 定义：`(workflow_id, version)`
  - Run：`run_id`
- 缓存只在读取 miss 时从持久化层回填。
- 当前明确只支持单实例：
  - 同一个 SQLite 文件只能由一个 `CachedStore`/进程写入；
  - 不支持跨实例或跨进程缓存失效；
  - 不设置 TTL；
  - 如果需要多进程共享数据库，应直接使用 `SQLiteStore`，或后续引入版本号/乐观锁与缓存失效机制。

### 存储后端语义对齐

- `MemoryStore.SaveDefinition` 与 SQLite 一致：重复版本返回 `ErrDefinitionExists`。
- `MemoryStore.CreateRun` 与 SQLite 一致：重复 Run ID 返回 `ErrRunExists`，不再静默覆盖。
- `MemoryStore.SaveExecutionState` 与 SQLite 一致：
  - 只更新目标 Step；
  - 只更新 Run 级状态字段；
  - 不再用调用方的整份 Run 覆盖存储。
- `MemoryStore` 的 Definition 读写改为深拷贝，避免调用方修改 `Steps`、`Config`、`Inputs` 等字段污染内部状态。

### 测试

新增/更新测试覆盖：

- 定义 insert-only 与冲突错误；
- Engine 定义校验；
- 同一版本 + 相同定义可创建多个 Run；
- 同一版本 + 不同定义被拒绝；
- CachedStore 写入只落持久化、读取 miss 回填；
- 冷缓存下 `CreateRun` 不再出现“已持久化但返回错误”；
- MemoryStore 定义深拷贝、重复 Run 拒绝、Step 级更新语义；
- SQLite 旧 schema 自动迁移。
