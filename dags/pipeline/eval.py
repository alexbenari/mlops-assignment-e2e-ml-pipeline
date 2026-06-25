"""Evaluate a preds.json with the SWE-bench harness (layer-3 workload via scripts/).

Standard-library only; runs under Airflow's interpreter. The harness runs in the
project .venv via scripts/swe-bench-eval.sh. No swebench import here.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

EVAL_SCRIPT = Path("scripts") / "swe-bench-eval.sh"


def run_swebench_eval(
    run_dir: Path, run_config: dict[str, Any], preds_path: Path, project_root: Path
) -> Path:
    """Evaluate preds_path, writing logs + summary report into run_dir/run-eval/.

    Returns the run-eval directory. Raises if the script fails or no summary
    report json is produced.
    """
    run_dir = Path(run_dir)
    project_root = Path(project_root)
    preds_path = Path(preds_path)
    eval_dir = run_dir / "run-eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    env = {
        **os.environ,
        "DATASET_NAME": str(run_config["dataset_name"]),
        "SPLIT": str(run_config["split"]),
        "MAX_WORKERS": str(run_config["workers"]),
        "RUN_ID": str(run_config["run_id"]),
        "PREDICTIONS_PATH": str(preds_path),
        "OUTPUT_DIR": str(eval_dir),
    }
    subprocess.run(
        ["bash", str(project_root / EVAL_SCRIPT)],
        cwd=str(project_root),
        env=env,
        check=True,
    )

    reports = list(eval_dir.glob("*.json"))
    if not reports:
        raise FileNotFoundError(f"eval finished but no summary report json in {eval_dir}")
    return eval_dir
