#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <run_id> [log_dir]" >&2
  exit 1
fi

run_id=$1
log_dir=${2:-logs}
file="$log_dir/runs/$run_id.log"

if [ ! -f "$file" ]; then
  echo "no log file for run $run_id: $file" >&2
  echo "hint: set WORKFLOW_LOG_DIR if the server uses a different directory" >&2
  exit 1
fi

echo "tailing $file"
tail -n +1 -f "$file"
