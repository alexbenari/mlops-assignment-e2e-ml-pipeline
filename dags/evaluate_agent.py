"""Airflow DAG: run mini-swe-agent on a SWE-bench subset and evaluate the patches.

This file is parsed by Airflow's interpreter (the uvx apache-airflow env), so the
imports here stay light. The heavy workload (mini-swe-agent, swebench) is shelled
out to the project .venv from the pipeline.* helpers / scripts. See dags/pipeline/.

Pipeline (built incrementally): prepare_run -> run_agent -> run_eval -> summarize_and_log.
This slice implements params + prepare_run.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = Path(__file__).resolve().parent
# dags/ is normally on sys.path under Airflow; ensure it for local parsing/tests too.
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from pipeline.agent import run_agent_batch  # noqa: E402
from pipeline.config import build_run_config, load_run_config, prepare_run_dir  # noqa: E402
from pipeline.eval import run_swebench_eval  # noqa: E402
from pipeline.metrics import collect_metrics, write_manifest, write_metrics  # noqa: E402
from pipeline.mlflow_log import log_mlflow_run  # noqa: E402

# All experiment values are DAG-level params so they appear in the "Trigger DAG
# w/ config" form. No hard-coded experiment values live in the task bodies.
PARAMS = {
    "split": Param(
        "test",
        type="string",
        enum=["test", "dev"],
        title="Dataset split",
        description="SWE-bench split to pull instances from.",
    ),
    "subset": Param(
        "verified",
        type="string",
        enum=["verified", "lite", "full"],
        title="SWE-bench subset",
        description="Which SWE-bench dataset subset to run against.",
    ),
    "workers": Param(
        4,
        type="integer",
        minimum=1,
        maximum=64,
        title="Workers",
        description="Parallel workers for both the agent batch and the evaluation.",
    ),
    "model": Param(
        "nebius/moonshotai/Kimi-K2.6",
        type="string",
        title="Model",
        description="litellm model id powering the agent (nebius/... routes via Nebius Token Factory).",
    ),
    "task_slice": Param(
        "0:3",
        type=["null", "string"],
        title="Task slice",
        description="Python-style slice over sorted instance ids, e.g. '0:3'. Blank = all instances.",
    ),
    "run_id": Param(
        None,
        type=["null", "string"],
        title="Run ID",
        description="Optional. Leave blank to auto-generate (UTC timestamp + short hash).",
    ),
    "cost_limit": Param(
        0,
        type="number",
        minimum=0,
        title="Cost limit (USD)",
        description="Per-instance cost cap for the agent. 0 disables the limit.",
    ),
}


@dag(
    dag_id="evaluate_agent",
    description="Run mini-swe-agent on a SWE-bench subset and evaluate the patches.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params=PARAMS,
    tags=["mlops", "swebench", "evaluation"],
)
def evaluate_agent():
    @task
    def prepare_run() -> str:
        """Resolve params into a run config and create runs/<run-id>/ with config.json.

        Returns the run directory as a string (an XCom) for downstream tasks.
        """
        params = get_current_context()["params"]
        run_config = build_run_config(params)
        run_dir = prepare_run_dir(run_config, runs_root=PROJECT_ROOT / "runs")
        print(f"[prepare_run] run_id={run_config['run_id']} run_dir={run_dir}")
        return str(run_dir)

    @task
    def run_agent(run_dir: str) -> str:
        """Run the mini-swe-agent batch; writes run-agent/ and returns preds.json path."""
        run_config = load_run_config(run_dir)
        preds_path = run_agent_batch(Path(run_dir), run_config, PROJECT_ROOT)
        print(f"[run_agent] preds={preds_path}")
        return str(preds_path)

    @task
    def run_eval(run_dir: str, preds_path: str) -> str:
        """Evaluate preds.json with SWE-bench; writes run-eval/ and returns its path."""
        run_config = load_run_config(run_dir)
        eval_dir = run_swebench_eval(Path(run_dir), run_config, Path(preds_path), PROJECT_ROOT)
        print(f"[run_eval] eval_dir={eval_dir}")
        return str(eval_dir)

    @task
    def summarize_and_log(run_dir: str, eval_dir: str) -> str:
        """Parse eval results into metrics.json and write manifest.json.

        (MLflow logging is added in a later slice.) Returns the manifest path.
        """
        run_config = load_run_config(run_dir)
        agent_dir = Path(run_dir) / "run-agent"
        metrics = collect_metrics(Path(eval_dir), agent_dir=agent_dir)
        write_metrics(metrics, Path(run_dir))
        manifest_path = write_manifest(Path(run_dir), run_config, metrics, PROJECT_ROOT)
        print(f"[summarize_and_log] metrics={metrics}")
        print(f"[summarize_and_log] manifest={manifest_path}")
        # Log to MLflow last: metrics.json + manifest.json are already on disk, so a
        # tracking-server outage doesn't lose the run's results (just fails this task).
        log_mlflow_run(Path(run_dir), PROJECT_ROOT)
        print("[summarize_and_log] logged to MLflow")
        return str(manifest_path)

    run_dir = prepare_run()
    preds_path = run_agent(run_dir)
    eval_dir = run_eval(run_dir, preds_path)
    summarize_and_log(run_dir, eval_dir)


evaluate_agent()
