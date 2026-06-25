# Evaluation pipeline for coding-agent experiments — Report

An Airflow pipeline that runs **mini-swe-agent** on a subset of **SWE-bench** tasks,
evaluates the produced patches with the SWE-bench harness, writes a reproducible
`runs/<run-id>/` artifact tree, and logs parameters/metrics to **MLflow**.

```
prepare_run ──► run_agent ──► run_eval ──► summarize_and_log
 (config.json)   (preds.json)   (reports)   (metrics.json, manifest.json, MLflow)
```

## Architecture

Two layers, deliberately separated:

- **Orchestration (Airflow).** The DAG and the light helpers in
  `dags/pipeline/`. They read params, build the run config, create the run dir,
  parse results, and log to MLflow. They do not import `swebench` / `minisweagent`.
- **Workload (the project environment).** The agent and the evaluation run in the
  `mlops-eval:local` image via the scripts in
  `scripts/`. This is the heavy code with the ML dependencies.

Airflow runs in its own environment, the `apache/airflow` image. It does **not** contain the
workload dependencies. The orchestration layer performs the workload operations through **`DockerOperator`** launching `mlops-eval:local`. This is done through the `scripts/*.sh` files (which are `COPY`'d into the
Docker image). This is the single source of truth for "how to run the agent/eval".

### Components

| Piece | Role |
|---|---|
| `dags/evaluate_agent.py` | The DAG: params, the four tasks, retries/timeouts |
| `dags/pipeline/` | Orchestration helpers: `config`, `metrics`, `mlflow_log` |
| `scripts/mini-swe-bench-batch.sh` | Run the agent batch → trajectories + `preds.json` |
| `scripts/swe-bench-eval.sh` | Evaluate `preds.json` → logs + summary report |
| `Dockerfile` → `mlops-eval:local` | Workload image (mini-swe-agent + swebench), run by DockerOperator |
| `docker-compose.yaml` | Control plane: Postgres + MLflow + Airflow |

## Pipeline tasks

| Task | Type | What it does |
|---|---|---|
| `prepare_run` | Python | Resolve params → `run_config`; create `runs/<run-id>/` + `config.json` |
| `run_agent` | DockerOperator | Run mini-swe-agent on the subset → `run-agent/{preds.json, <id>/<id>.traj.json}` |
| `run_eval` | DockerOperator | Evaluate `preds.json` with SWE-bench → `run-eval/{logs/, <model>.<run-id>.json}` |
| `summarize_and_log` | Python | Parse reports → `metrics.json`; write `manifest.json`; log to MLflow |

Tasks retry once (`retries=1`, exponential backoff) except `prepare_run` (`retries=0` — a
failure there is a real bug, not transient). Each has a per-task `execution_timeout`
(agent 4h, eval 2h, prepare 5m, summarize 10m). Retries are safe: `run_agent` resumes from
an existing `preds.json`, and `summarize_and_log`'s MLflow logging is idempotent (a retry
reuses the same run instead of duplicating it). `prepare_run` also runs pre-flight
validation (NEBIUS_API_KEY present, params sane) and raises `AirflowFailException` on a bad
config, so misconfigurations fail fast before the expensive Docker tasks run.

## Configuration (DAG params)

All experiment values are DAG-level params, surfaced in "Trigger DAG w/ config" — no
hard-coded experiment values in the task bodies:

| Param | Default | Notes |
|---|---|---|
| `split` | `test` | dataset split |
| `subset` | `verified` | `verified` / `lite` / `full` |
| `workers` | `4` | parallelism for agent + eval |
| `model` | `nebius/moonshotai/Kimi-K2.6` | litellm model id |
| `task_slice` | `0:3` | slice of sorted instance ids; blank = all |
| `run_id` | *(blank)* | auto-generated `YYYYMMDD-HHMMSS-<hash>` if empty |
| `cost_limit` | `0` | per-instance USD cap; 0 = disabled |

## Run artifact layout

```
runs/<run-id>/
├── config.json        # resolved run config (the single source of truth)
├── run-agent/
│   ├── preds.json     # {instance_id: {model_name_or_path, instance_id, model_patch}}
│   ├── <instance>/<instance>.traj.json   # full agent trajectory
│   └── minisweagent.log
├── run-eval/
│   ├── <model>.<run-id>.json             # SWE-bench summary report
│   └── logs/run_evaluation/<run-id>/<model>/<instance>/{report.json, patch.diff, test_output.txt, ...}
├── metrics.json       # resolved / resolve_rate / counts / api_calls
└── manifest.json      # index of artifacts + provenance (git commit) + storage location
```

A committed example lives at `runs/20260625-155405-0af1496f/`.

## How to run

### A) Standalone (easy mode)
```bash
cp .env.example .env          # add your NEBIUS_API_KEY
uv sync
docker build -t mlops-eval:local .             # workload image (run_agent/run_eval use DockerOperator in both modes)
uv tool run mlflow server --port 5000          # MLflow (separate terminal)
bash run-airflow-standalone.sh                 # Airflow at http://localhost:8080 (admin/admin)
```
Trigger `evaluate_agent` from the UI with e.g. `{"task_slice": "0:1", "workers": 1}`.
(Docker must be running — both deployments launch the agent/eval as containers.)

### B) docker-compose (production-style)
```bash
cp .env.example .env          # set NEBIUS_API_KEY, HOST_PROJECT_DIR, AIRFLOW_UID, DOCKER_GID
docker build -t mlops-eval:local .             # the workload image
docker compose up -d                           # postgres + mlflow + airflow
```
- Airflow UI → http://localhost:8080 (`admin` / `admin`)
- MLflow UI → http://localhost:5000

The Airflow container talks to the host Docker daemon (mounted socket) to launch the
workload image as a sibling container (docker-out-of-docker). The repo is mounted at the
**same path** inside the container so `runs/` written by Airflow and the DockerOperator's
host-path mounts agree.

## MLflow tracking

Each run logs params (`config.json`), metrics (`metrics.json`), provenance tags
(git commit), and the small JSON artifacts. Runs are comparable in the MLflow UI.
See `screenshots/mlflow_runs.png`.

## A completed run

`run_id = 20260625-155405-0af1496f` (compose stack, `{"task_slice":"0:1","workers":1}`):

| metric | value |
|---|---|
| instance | `astropy__astropy-12907` |
| resolved | 1 / 1 |
| resolve_rate | 1.0 |
| api_calls | 14 |
| model | `nebius/moonshotai/Kimi-K2.6` |
| git_commit | `046a64d` |

Evidence: `screenshots/airflow_dag.png` (all four tasks green) and
`screenshots/mlflow_runs.png` (logged metrics).

## Reproduce / rerun by run-id

- **Inspect** a run: everything is under `runs/<run-id>/`; `manifest.json` indexes it.
- **Re-evaluate** existing predictions without re-running the agent:
  ```bash
  PREDICTIONS_PATH=runs/<run-id>/run-agent/preds.json OUTPUT_DIR=/tmp/re-eval \
    RUN_ID=<run-id> bash scripts/swe-bench-eval.sh
  ```
- **Re-run the whole pipeline**: trigger the DAG with `{"run_id": "<new-or-blank>", ...}`.
  A blank `run_id` auto-generates a fresh one; reusing a `run_id` resumes the agent batch
  (it skips instances already in `preds.json`).

## Remote storage (S3) — not enabled, but can be done as described below

Artifacts are kept locally under `runs/<run-id>/`. `manifest.json` already carries a
`storage.remote_uri` slot (currently `null`). To enable durable/shared storage:
1. Add a `log_artifacts_to_s3` step (or extend `summarize_and_log`) that uploads
   `runs/<run-id>/` to `s3://<bucket>/runs/<run-id>/` and sets `manifest.storage.remote_uri`.
2. Tag the MLflow run with that URI (the code already logs `remote_uri` when present).
3. Point MLflow's `--artifacts-destination` at the same bucket for managed artifacts.


