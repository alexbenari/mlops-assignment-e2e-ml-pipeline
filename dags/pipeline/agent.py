"""Run the mini-swe-agent batch (the layer-3 workload) by shelling out to scripts/.

Standard-library only; runs under Airflow's interpreter. The actual agent runs in
the project .venv via scripts/mini-swe-bench-batch.sh (which uses `uv run`). We
pass params as environment variables and never import swebench/minisweagent here.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

BATCH_SCRIPT = Path("scripts") / "mini-swe-bench-batch.sh"


def run_agent_batch(run_dir: Path, run_config: dict[str, Any], project_root: Path) -> Path:
    """Run the agent batch with run_config params, writing into run_dir/run-agent/.

    Returns the path to the produced preds.json. Raises if the script exits non-zero
    or preds.json is missing afterwards.
    """
    run_dir = Path(run_dir)
    project_root = Path(project_root)
    output_dir = run_dir / "run-agent"
    output_dir.mkdir(parents=True, exist_ok=True)

    env = {
        **os.environ,
        "SUBSET": str(run_config["subset"]),
        "SPLIT": str(run_config["split"]),
        "MODEL": str(run_config["model"]),
        "WORKERS": str(run_config["workers"]),
        "TASK_SLICE": str(run_config.get("task_slice", "")),
        "COST_LIMIT": str(run_config.get("cost_limit", 0)),
        "OUTPUT_DIR": str(output_dir),
    }
    subprocess.run(
        ["bash", str(project_root / BATCH_SCRIPT)],
        cwd=str(project_root),
        env=env,
        check=True,
    )

    preds_path = output_dir / "preds.json"
    if not preds_path.is_file():
        raise FileNotFoundError(f"agent batch finished but no preds.json at {preds_path}")
    return preds_path
