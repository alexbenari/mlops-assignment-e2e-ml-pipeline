"""Log a run to MLflow by shelling out to scripts/log_mlflow.py in the project venv.

Standard-library only; runs under Airflow's interpreter. The mlflow client lives in
the project .venv (not Airflow's env), so we invoke it via `uv run python`.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

LOG_SCRIPT = Path("scripts") / "log_mlflow.py"


def log_mlflow_run(
    run_dir: Path,
    project_root: Path,
    tracking_uri: str | None = None,
    experiment: str | None = None,
) -> None:
    """Log params/metrics/artifacts for run_dir to MLflow. Raises if logging fails."""
    project_root = Path(project_root)
    env = {**os.environ}
    # Ensure uv is discoverable regardless of how Airflow was launched.
    env["PATH"] = f"{Path.home() / '.local' / 'bin'}{os.pathsep}{env.get('PATH', '')}"
    if tracking_uri:
        env["MLFLOW_TRACKING_URI"] = tracking_uri
    if experiment:
        env["MLFLOW_EXPERIMENT"] = experiment

    subprocess.run(
        ["uv", "run", "python", str(project_root / LOG_SCRIPT), "--run-dir", str(run_dir)],
        cwd=str(project_root),
        env=env,
        check=True,
    )
