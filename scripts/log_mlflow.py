"""Log one pipeline run to MLflow. Runs in the project venv (needs the mlflow client).

Reads config.json + metrics.json (+ manifest.json) from a run dir and logs params,
metrics, provenance tags, and the small JSON files as artifacts to the configured
MLflow tracking server. Invoked by pipeline/mlflow_log.py via `uv run python`.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import mlflow

# Which config.json keys become MLflow params, and which metrics.json keys become metrics.
PARAM_KEYS = ["run_id", "split", "subset", "dataset_name", "workers", "model", "task_slice", "cost_limit"]
METRIC_KEYS = ["submitted", "completed", "resolved", "unresolved", "empty_patch", "errors",
               "resolve_rate", "api_calls", "total_cost"]
ARTIFACT_FILES = ["config.json", "metrics.json", "manifest.json"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Log a pipeline run dir to MLflow.")
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--tracking-uri",
                    default=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    ap.add_argument("--experiment",
                    default=os.environ.get("MLFLOW_EXPERIMENT", "evaluate_agent"))
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    manifest_path = run_dir / "manifest.json"

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name=config.get("run_id")) as run:
        mlflow.log_params({k: config[k] for k in PARAM_KEYS if k in config})
        mlflow.log_metrics(
            {k: float(metrics[k]) for k in METRIC_KEYS
             if isinstance(metrics.get(k), (int, float))}
        )
        mlflow.set_tag("run_dir", str(run_dir))

        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
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

        print(f"[mlflow] logged run_id={config.get('run_id')} "
              f"mlflow_run_id={run.info.run_id} experiment={args.experiment} uri={args.tracking_uri}")


if __name__ == "__main__":
    main()
