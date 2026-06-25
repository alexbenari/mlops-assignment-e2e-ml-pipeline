"""Airflow DAG: run mini-swe-agent on a SWE-bench subset and evaluate the patches.

    prepare_run (Python) -> run_agent (DockerOperator) -> run_eval (DockerOperator)
      -> summarize_and_log (Python + MLflow)

run_agent and run_eval run inside the project image (mlops-eval:local) for execution
isolation, via DockerOperator talking to the host Docker daemon. The mounted Docker
socket lets those containers launch the SWE-bench instance containers (docker-out-of-
docker). prepare_run/summarize_and_log stay as light Python tasks (no isolation needed).

Parsed by Airflow's interpreter (the uvx apache-airflow env), so heavy imports are
avoided. The Docker provider must be present in that env -- see run-airflow-standalone.sh
(`uv tool run --with apache-airflow-providers-docker ...`).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag, get_current_context, task
from docker.types import Mount

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = Path(__file__).resolve().parent
# dags/ is normally on sys.path under Airflow; ensure it for local parsing/tests too.
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from pipeline.config import build_run_config, prepare_run_dir  # noqa: E402
from pipeline.metrics import collect_metrics, write_manifest, write_metrics  # noqa: E402
from pipeline.mlflow_log import log_mlflow_run  # noqa: E402

IMAGE = "mlops-eval:local"
RUNS_DIR = PROJECT_ROOT / "runs"

# Bind mounts shared by both container tasks:
#  - the host Docker socket, so the agent/eval can launch SWE-bench instance containers
#    (docker-out-of-docker) against the host daemon;
#  - the runs/ tree, so outputs land on the host at runs/<run-id>/...;
#  - the repo .env (read-only), which the batch script sources for NEBIUS_API_KEY.
COMMON_MOUNTS = [
    Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
    Mount(source=str(RUNS_DIR), target="/runs", type="bind"),
    Mount(source=str(PROJECT_ROOT / ".env"), target="/mlops-assignment/.env", type="bind", read_only=True),
]


class _ScriptDockerOperator(DockerOperator):
    """DockerOperator that does NOT treat a '.sh' command as a template file.

    DockerOperator's default template_ext includes '.sh'/'.bash', so a command string
    ending in '.sh' makes Airflow try to read it as a file from the dags folder
    (TemplateNotFound). Clearing template_ext disables that; Jinja rendering of
    template_fields (e.g. `environment`) is unaffected.
    """

    template_ext: tuple[str, ...] = ()


def _cfg(key: str) -> str:
    """Jinja snippet that pulls one field from the resolved run config (the dict
    prepare_run returns as its XCom). Used to template DockerOperator env vars."""
    return "{{ ti.xcom_pull(task_ids='prepare_run')['" + key + "'] }}"


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
    # Baseline for every task: retry transient failures (API blips, flaky container
    # start) with exponential backoff. Tasks are safe to retry -- run_agent resumes
    # from preds.json, run_eval just regenerates reports.
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
)
def evaluate_agent():
    # retries=0: prepare_run is fast and pure (params + mkdir); a failure is a real bug,
    # not a transient blip, and a retry would mint a new run_id / orphan run dir.
    @task(execution_timeout=timedelta(minutes=5), retries=0)
    def prepare_run() -> dict:
        """Resolve params into a run config and create runs/<run-id>/ with config.json.

        Returns the resolved run config dict (XCom) consumed by the downstream tasks.
        """
        params = get_current_context()["params"]
        run_config = build_run_config(params)
        prepare_run_dir(run_config, runs_root=RUNS_DIR)
        print(f"[prepare_run] run_id={run_config['run_id']}")
        return run_config

    run_config = prepare_run()

    run_agent = _ScriptDockerOperator(
        task_id="run_agent",
        image=IMAGE,
        command="bash /mlops-assignment/scripts/mini-swe-bench-batch.sh",
        mounts=COMMON_MOUNTS,
        mount_tmp_dir=False,
        auto_remove="force",
        execution_timeout=timedelta(hours=4),
        environment={
            "SUBSET": _cfg("subset"),
            "SPLIT": _cfg("split"),
            "MODEL": _cfg("model"),
            "WORKERS": _cfg("workers"),
            "TASK_SLICE": _cfg("task_slice"),
            "COST_LIMIT": _cfg("cost_limit"),
            "OUTPUT_DIR": "/runs/" + _cfg("run_id") + "/run-agent",
        },
    )

    run_eval = _ScriptDockerOperator(
        task_id="run_eval",
        image=IMAGE,
        command="bash /mlops-assignment/scripts/swe-bench-eval.sh",
        mounts=COMMON_MOUNTS,
        mount_tmp_dir=False,
        auto_remove="force",
        execution_timeout=timedelta(hours=2),
        environment={
            "DATASET_NAME": _cfg("dataset_name"),
            "SPLIT": _cfg("split"),
            "MAX_WORKERS": _cfg("workers"),
            "RUN_ID": _cfg("run_id"),
            "PREDICTIONS_PATH": "/runs/" + _cfg("run_id") + "/run-agent/preds.json",
            "OUTPUT_DIR": "/runs/" + _cfg("run_id") + "/run-eval",
        },
    )

    @task(execution_timeout=timedelta(minutes=10))
    def summarize_and_log(run_config: dict) -> str:
        """Parse eval results into metrics.json, write manifest.json, log to MLflow.

        Derives paths from run_id (the container tasks wrote into runs/<run-id>/).
        Metrics + manifest are written to disk before MLflow, so a tracking-server
        outage doesn't lose the run's results (just fails this task).
        """
        run_id = run_config["run_id"]
        run_dir = RUNS_DIR / run_id
        metrics = collect_metrics(run_dir / "run-eval", agent_dir=run_dir / "run-agent")
        write_metrics(metrics, run_dir)
        write_manifest(run_dir, run_config, metrics, PROJECT_ROOT)
        print(f"[summarize_and_log] metrics={metrics}")
        log_mlflow_run(run_dir)
        print("[summarize_and_log] logged to MLflow")
        return str(run_dir)

    summary = summarize_and_log(run_config)

    run_config >> run_agent >> run_eval >> summary


evaluate_agent()
