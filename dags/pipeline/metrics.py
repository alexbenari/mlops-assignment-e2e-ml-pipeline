"""Collect metrics from eval output and write metrics.json + manifest.json.

Standard-library only (runs under Airflow's interpreter). Reads the SWE-bench
summary report and (best-effort) the agent trajectories; never imports swebench.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

METRICS_FILENAME = "metrics.json"
MANIFEST_FILENAME = "manifest.json"


def find_eval_report(eval_dir: Path) -> Path:
    """The SWE-bench summary report is the single top-level *.json in run-eval/."""
    reports = sorted(Path(eval_dir).glob("*.json"))
    if not reports:
        raise FileNotFoundError(f"no eval summary report (*.json) in {eval_dir}")
    return reports[0]


def _collect_agent_stats(agent_dir: Path) -> dict[str, Any]:
    """Best-effort aggregate of api_calls / cost from per-instance trajectories."""
    api_calls = 0
    cost = 0.0
    n = 0
    for traj in Path(agent_dir).glob("*/*.traj.json"):
        try:
            stats = (json.loads(traj.read_text()).get("info") or {}).get("model_stats") or {}
            api_calls += int(stats.get("api_calls") or 0)
            cost += float(stats.get("instance_cost") or 0.0)
            n += 1
        except Exception:
            continue
    return {"agent_instances": n, "api_calls": api_calls, "total_cost": round(cost, 6)}


def collect_metrics(eval_dir: Path, agent_dir: Path | None = None) -> dict[str, Any]:
    """Parse the eval summary report into headline metrics; optionally fold in
    agent-side operational stats from the trajectories in agent_dir.
    """
    report = json.loads(find_eval_report(eval_dir).read_text(encoding="utf-8"))
    submitted = int(report.get("submitted_instances", 0))
    resolved = int(report.get("resolved_instances", 0))
    metrics: dict[str, Any] = {
        "submitted": submitted,
        "completed": int(report.get("completed_instances", 0)),
        "resolved": resolved,
        "unresolved": int(report.get("unresolved_instances", 0)),
        "empty_patch": int(report.get("empty_patch_instances", 0)),
        "errors": int(report.get("error_instances", 0)),
        "resolve_rate": round(resolved / submitted, 4) if submitted else 0.0,
        "resolved_ids": report.get("resolved_ids", []),
    }
    if agent_dir is not None:
        metrics.update(_collect_agent_stats(agent_dir))
    return metrics


def write_metrics(metrics: dict[str, Any], run_dir: Path) -> Path:
    """Write metrics.json into the run dir."""
    path = Path(run_dir) / METRICS_FILENAME
    path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    return path


def _provenance(project_root: Path) -> dict[str, Any]:
    """Best-effort code provenance: git commit + workload package versions."""
    prov: dict[str, Any] = {"git_commit": None}
    try:
        prov["git_commit"] = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        pass
    try:
        venv_py = Path(project_root) / ".venv" / "bin" / "python"
        out = subprocess.run(
            [str(venv_py), "-c",
             "import importlib.metadata as m;print(m.version('mini-swe-agent'));print(m.version('swebench'))"],
            capture_output=True, text=True, check=True,
        ).stdout.split()
        prov["mini_swe_agent"], prov["swebench"] = out[0], out[1]
    except Exception:
        pass
    return prov


def write_manifest(
    run_dir: Path, run_config: dict[str, Any], metrics: dict[str, Any], project_root: Path
) -> Path:
    """Write manifest.json: an index of the run's artifacts + provenance + storage.

    Artifact pointers are relative to run_dir so the manifest stays valid if the
    folder is moved or zipped.
    """
    run_dir = Path(run_dir)
    try:
        eval_report_rel = str(find_eval_report(run_dir / "run-eval").relative_to(run_dir))
    except Exception:
        eval_report_rel = None

    manifest = {
        "run_id": run_config.get("run_id"),
        "created_at": run_config.get("created_at"),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed",
        "artifacts": {
            "config": "config.json",
            "predictions": "run-agent/preds.json",
            "trajectories": "run-agent",
            "agent_log": "run-agent/minisweagent.log",
            "eval_report": eval_report_rel,
            "eval_logs": "run-eval/logs/run_evaluation",
            "metrics": "metrics.json",
        },
        "storage": {"local_path": str(run_dir), "remote_uri": None},
        "provenance": _provenance(project_root),
        "headline_metrics": {
            "resolved": metrics.get("resolved"),
            "submitted": metrics.get("submitted"),
            "resolve_rate": metrics.get("resolve_rate"),
        },
    }
    path = run_dir / MANIFEST_FILENAME
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path
