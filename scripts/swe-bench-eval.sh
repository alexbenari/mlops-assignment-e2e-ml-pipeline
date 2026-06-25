#!/usr/bin/env bash
# Evaluate a preds.json with the SWE-bench harness and write the summary report
# plus per-instance logs/reports into OUTPUT_DIR.
#
# The harness writes its logs to ./logs/run_evaluation/<run_id>/... and the summary
# report to --report_dir, both relative to CWD, so we cd into OUTPUT_DIR. Uses the
# project .venv python directly (precise CWD control); works the same in a Docker
# container where the venv lives at <repo>/.venv.
set -euo pipefail

# Resolve repo root from this script's own location.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Parameters (env-overridable).
DATASET_NAME="${DATASET_NAME:-princeton-nlp/SWE-bench_Verified}"
SPLIT="${SPLIT:-test}"
MAX_WORKERS="${MAX_WORKERS:-4}"
RUN_ID="${RUN_ID:-eval}"
PREDICTIONS_PATH="${PREDICTIONS_PATH:?PREDICTIONS_PATH is required}"
OUTPUT_DIR="${OUTPUT_DIR:-run-eval}"

# predictions_path must be absolute because we cd into OUTPUT_DIR below.
PREDICTIONS_PATH="$(readlink -f "$PREDICTIONS_PATH")"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo "[eval] dataset=$DATASET_NAME split=$SPLIT workers=$MAX_WORKERS run_id=$RUN_ID preds=$PREDICTIONS_PATH out=$OUTPUT_DIR"

"$REPO_ROOT/.venv/bin/python" -m swebench.harness.run_evaluation \
    --dataset_name "$DATASET_NAME" \
    --split "$SPLIT" \
    --predictions_path "$PREDICTIONS_PATH" \
    --max_workers "$MAX_WORKERS" \
    --run_id "$RUN_ID" \
    --report_dir .
