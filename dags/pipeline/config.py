"""Build a run's resolved configuration and on-disk layout.

Standard-library only (runs under Airflow's interpreter). See pipeline/__init__.py.
"""
from __future__ import annotations

import json
import os
import re
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


def validate_run_config(run_config: dict[str, Any], project_root: Path) -> None:
    """Pre-flight checks for real (non-transient) config problems. Raises ValueError.

    Runs in prepare_run before any expensive task, so misconfigurations fail fast
    instead of surfacing mid-run (e.g. an auth error after pulling a multi-GB image).
    """
    project_root = Path(project_root)

    # Auth: the agent needs NEBIUS_API_KEY, from the environment or the repo .env that the
    # batch script sources. Check for a non-empty value in either place.
    has_key = bool(os.environ.get("NEBIUS_API_KEY"))
    env_file = project_root / ".env"
    if not has_key and env_file.exists():
        has_key = any(
            line.strip().startswith("NEBIUS_API_KEY=") and line.strip() != "NEBIUS_API_KEY="
            for line in env_file.read_text(encoding="utf-8").splitlines()
        )
    if not has_key:
        raise ValueError("NEBIUS_API_KEY not set (environment or repo .env); the agent cannot authenticate")

    # Dataset must resolve (subset is a known SWE-bench key or an explicit dataset path).
    if not run_config.get("dataset_name"):
        raise ValueError(f"could not resolve a dataset for subset={run_config.get('subset')!r}")

    # Workers must be positive.
    if int(run_config.get("workers", 0)) < 1:
        raise ValueError(f"workers must be >= 1, got {run_config.get('workers')!r}")

    # task_slice, if given, must look like a Python slice spec ('0:3', ':5', '0:', '0:6:2', '5').
    task_slice = (run_config.get("task_slice") or "").strip()
    if task_slice and not re.fullmatch(r"-?\d*(?::-?\d*){0,2}", task_slice):
        raise ValueError(f"task_slice {task_slice!r} is not a valid slice spec (e.g. '0:3')")


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
