"""Build a run's resolved configuration and on-disk layout.

Standard-library only (runs under Airflow's interpreter). See pipeline/__init__.py.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# SWE-bench subset keyword -> HuggingFace dataset name expected by the eval harness
# (swebench.harness.run_evaluation --dataset_name). The agent itself takes the
# bare subset keyword (--subset verified); only evaluation needs the full path.
SUBSET_TO_DATASET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}

CONFIG_FILENAME = "config.json"


def generate_run_id() -> str:
    """Sortable, unique run id: UTC timestamp + short random suffix."""
    return f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"


def build_run_config(params: dict[str, Any]) -> dict[str, Any]:
    """Normalize Airflow params into a fully-resolved, serializable run config.

    Generates a run_id when the param is empty, and resolves the eval dataset
    name from the subset. This dict is the single source of truth written to
    runs/<run-id>/config.json and reused by the agent and eval tasks.
    """
    run_id = str(params.get("run_id") or "").strip() or generate_run_id()
    subset = params["subset"]
    return {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "split": params["split"],
        "subset": subset,
        "dataset_name": SUBSET_TO_DATASET.get(subset, subset),
        "workers": int(params["workers"]),
        "model": params["model"],
        "task_slice": str(params.get("task_slice") or "").strip(),
        "cost_limit": float(params.get("cost_limit") or 0),
    }


def load_run_config(run_dir: Path) -> dict[str, Any]:
    """Read the persisted config.json from a run dir (the authoritative config)."""
    return json.loads((Path(run_dir) / CONFIG_FILENAME).read_text(encoding="utf-8"))


def prepare_run_dir(run_config: dict[str, Any], runs_root: Path) -> Path:
    """Create runs/<run-id>/ (with run-agent/ and run-eval/) and write config.json.

    Returns the run directory path. Idempotent: safe to re-run for the same id.
    """
    run_dir = Path(runs_root) / run_config["run_id"]
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
    (run_dir / CONFIG_FILENAME).write_text(
        json.dumps(run_config, indent=2) + "\n", encoding="utf-8"
    )
    return run_dir
