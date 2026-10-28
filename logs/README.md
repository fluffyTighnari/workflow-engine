# Runtime logs

`cmd/server` writes logs here by default:

- `logs/workflow.log` — all application logs
- `logs/runs/<run_id>.log` — every log record carrying that `run_id`

Override the directory with:

```bash
WORKFLOW_LOG_DIR=/path/to/logs go run ./cmd/server
```

Follow one run:

```bash
./scripts/runlog.sh <run_id>
```

The directory is intentionally kept in the repository so the runtime log
location is discoverable. Actual log files are ignored by `.gitignore`.
