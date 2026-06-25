"""Log a pipeline run to MLflow directly (mlflow client imported lazily).

Runs under Airflow's interpreter, which has the mlflow client available:
  - standalone:      run-airflow-standalone.sh adds it via `--with mlflow-skinny`
  - docker-compose:  baked into the Airflow image (docker/Dockerfile.airflow)
mlflow is imported inside the function so DAG *parsing* never requires it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

# config.json keys -> MLflow params; metrics.json keys -> MLflow metrics.
PARAM_KEYS = ["run_id", "split", "subset", "dataset_name", "workers", "model", "task_slice", "cost_limit"]
METRIC_KEYS = ["submitted", "completed", "resolved", "unresolved", "empty_patch", "errors",
               "resolve_rate", "api_calls", "total_cost"]
ARTIFACT_FILES = ["config.json", "metrics.json", "manifest.json"]


def log_mlflow_run(run_dir: Path, tracking_uri: str | None = None, experiment: str | None = None) -> None:
    """Log params, metrics, provenance tags, and the small JSON files to MLflow.

    Reads config.json + metrics.json (+ manifest.json) from run_dir. Raises if the
    tracking server is unreachable (the run's results are already on disk).
    """
    import mlflow  # lazy: needed only when this task runs, not at DAG parse

    run_dir = Path(run_dir)
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    manifest_path = run_dir / "manifest.json"
    manifest: dict[str, Any] = (
        json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    )

    mlflow.set_tracking_uri(tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    exp = mlflow.set_experiment(experiment or os.environ.get("MLFLOW_EXPERIMENT", "evaluate_agent"))

    # Idempotent: if a run for this pipeline run_id already exists (e.g. this task is being
    # retried after a transient MLflow failure), reuse it instead of creating a duplicate.
    run_id = config.get("run_id")
    existing = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=f"tags.`mlflow.runName` = '{run_id}'",
        max_results=1,
        output_format="list",
    )
    start_kwargs = {"run_id": existing[0].info.run_id} if existing else {"run_name": run_id}

    with mlflow.start_run(**start_kwargs):
        mlflow.log_params({k: config[k] for k in PARAM_KEYS if k in config})
        mlflow.log_metrics(
            {k: float(metrics[k]) for k in METRIC_KEYS if isinstance(metrics.get(k), (int, float))}
        )
        mlflow.set_tag("run_dir", str(run_dir))
        for k, v in (manifest.get("provenance") or {}).items():
            if v:
                mlflow.set_tag(k, v)
        remote_uri = (manifest.get("storage") or {}).get("remote_uri")
        if remote_uri:
            mlflow.set_tag("remote_uri", remote_uri)
        for fn in ARTIFACT_FILES:
            p = run_dir / fn
            if p.exists():
                mlflow.log_artifact(str(p))
