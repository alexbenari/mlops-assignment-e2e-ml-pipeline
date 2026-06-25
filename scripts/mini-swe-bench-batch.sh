#!/usr/bin/env bash
# Run the mini-swe-agent batch over a SWE-bench subset and write per-instance
# trajectories + preds.json into OUTPUT_DIR.
#
# Parameterized via environment variables so the SAME script works from Airflow
# (subprocess) now and from a DockerOperator container later. Uses `uv run` so it
# resolves the project .venv in both environments.
set -euo pipefail

# Resolve repo root from this script's own location so it runs from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Make uv discoverable regardless of how the parent process was launched.
export PATH="$HOME/.local/bin:$PATH"

# Load credentials (NEBIUS_API_KEY) from the repo .env if present. mini-swe-agent
# only auto-loads its global config .env, not the repo one, so we do it here.
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

# Parameters (env-overridable; defaults suit a small smoke run).
SUBSET="${SUBSET:-verified}"
SPLIT="${SPLIT:-test}"
MODEL="${MODEL:-nebius/moonshotai/Kimi-K2.6}"
WORKERS="${WORKERS:-4}"
TASK_SLICE="${TASK_SLICE:-0:3}"
COST_LIMIT="${COST_LIMIT:-0}"
OUTPUT_DIR="${OUTPUT_DIR:-trajectories}"
# Base agent config. Passing any -c drops the built-in default, so we pass this
# explicitly and then layer the cost_limit override on top.
CONFIG="${CONFIG:-mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml}"

mkdir -p "$OUTPUT_DIR"

# Cost tracking is unreliable with Nebius; ignore errors so runs don't abort.
export MSWEA_COST_TRACKING="${MSWEA_COST_TRACKING:-ignore_errors}"

echo "[batch] subset=$SUBSET split=$SPLIT model=$MODEL workers=$WORKERS slice='${TASK_SLICE}' cost_limit=$COST_LIMIT out=$OUTPUT_DIR"

uv run mini-extra swebench \
    --subset "$SUBSET" \
    --split "$SPLIT" \
    --model "$MODEL" \
    --workers "$WORKERS" \
    --config "$CONFIG" \
    --config "agent.cost_limit=$COST_LIMIT" \
    ${TASK_SLICE:+--slice "$TASK_SLICE"} \
    -o "$OUTPUT_DIR"
