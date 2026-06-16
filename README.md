# Home assignment: Evaluation pipeline for coding-agent experiments

**What**: Home assignment.

**Where**: Nebius Academy course [AI Performance Engineering](https://academy.nebius.com/ai-engineering-il), MLOps module, lecture #6, "End-to-end ML pipeline".

**Author**: Simon Karasik.

**Learning objective**: Get hands-on experience turning an ad-hoc coding-agent evaluation script into an automated, observable, versioned, and durable Airflow pipeline with a structured data footprint: datasets, artifacts, metadata, metrics, logs, and trajectories.

**Inspired by**: https://github.com/GlebBerjoskin/mlops-assignment

---

## Legend

Imagine you are an MLOps engineer on a team that builds better coding agents. Think Claude Code, Codex, Cursor, OpenCode, mini-swe-agent, and similar systems.

Agent quality depends on two broad things:

1. **Harness**: the agent loop, prompts, tools, skills, retries, subagents, context management, and execution environment.
2. **Model**: the LLM that powers the harness, including decoding parameters and fine-tuned variants.

Your researchers want to experiment with both. Typical research loops look like this:

1. tweak a prompt or harness setting -> run the agent -> evaluate generated patches
2. fine-tune a model -> deploy it -> run the agent -> evaluate generated patches

Quality is measured on [SWE-bench](https://www.swebench.com/)-like tasks: the agent receives a real GitHub issue inside an isolated environment, tries to solve it, produces a patch, and the patch is judged by real unit tests.

Right now the researchers have several scripts on one VM. Someone SSHes in, runs them by hand, waits, copies logs, and pastes numbers into a doc. One experiment at a time. No queue. No durable run history. No reliable way to answer "which config produced this result?" or "why did this run fail?"

So, the team needs your help to turn these ad-hoc scripts into reliable, multi-user pipelines.

## Task

You are provided with ad-hoc scripts in `scripts/` to run [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) and evaluate the results using [SWE-bench](https://github.com/swe-bench/SWE-bench).

Sample outputs of `scripts/mini-swe-bench-batch.sh` and `scripts/swe-bench-eval.sh` are available in `sample/`.

Your goal is to turn these ad-hoc scripts from `scripts/` into a proper, configurable Airflow pipeline that implements the basic  `run-agent -> run-evaluation` workflow: run `mini-swe-agent` on a subset of SWE-bench instances and evaluate the results.

As a starting point with Airflow, you are provided with `run-airflow-standalone.sh` and a dag in `dags/` that re-implements `scripts/mini-swe-bench-single.sh`.

**Airflow pipeline requirements**:
- Configurable from Airflow parameters: `--split`, `--subset`, `--worker`. No hard-code.
- All run artifacts are properly structured. E.g.,
```
runs/
  <<run-id>>/
    run-agent/
      astropy__astropy-12907/
      preds.json
    run-eval/
```
- Run artifacts are saved to a remote long-term storage, such as Object Storage (S3).
- It's possible to re-construct the run based on the produced `<<run-id>>` folder: input SWE-bench tasks, configuration, output trajectories, etc. Basically, you can just send a directory to someone -- and they will be able to grab the whole picture.
- Airflow pipelines uses `DockerOperator` to run the scripts in isolated environments, instead of calling `uv run`. `Dockerfile` for the project is provided. In large-scale production, `DockerOperator` can be replaced with `KubernetesPodOperator`.
- Each run metrics and parameters are logged to `MLflow`, one can easily compare different runs.

**Deployment**
1. Airflow is deployed locally on a VM using `docker compose`: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#running-airflow-in-docker
2. MLflow is deployed locally as a part of the same `docker-compose.yaml`.

Ultimately, the pipeline may look like: `run-mini-swe-agent` -> `swe-bench-eval` -> `log-artifacts-to-s3` -> `log-metrics-to-mlflow`.

---

## Why This Matters

By the end of the assignment you should be able to:

- Model an ML experiment as a pipeline with explicit inputs, outputs, retries, and dependencies.
- Use Airflow for orchestration instead of manual shell ordering.
- Track experiment configs, datasets, model IDs, metrics, artifacts, and logs in MLflow.
- Run coding-agent evaluations in user-provided Docker images and collect reproducible outputs.
- Deploy and use the mini-swe-agent trajectory viewer to inspect what happened inside an agent run.
- Compare multiple experiments without losing track of which code, prompt, dataset, and model produced each result.

If done carefully, this assignment teaches the practical MLOps discipline that research code usually lacks: durability, repeatability, provenance, and operational visibility.

---

## Prerequisites

- A CPU VM with 8 CPU, 32 GB RAM, public IP. Can be created in Nebius.
- `NEBIUS_API_KEY` for Nebius Token Factory

You do not need a GPU VM for the orchestration parts. The inference part is handled by managed APIs.

Create a VM with 8 CPU, 32 GB RAM, public IP. Add your public SSH key.

For simplicity, add this VM to your `~/.ssh/config`, for instance:

```
Host sbkarasik-academy-playground
  HostName 89.169.100.8
  User sbkarasik
  ForwardAgent yes
```

Connect to the VM. 

Install the basic tools:
```bash
# uv 
curl -LsSf https://astral.sh/uv/install.sh | sh

# Docker
# Add Docker's official GPG key:
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Let your user use `docker` without `sudo`
sudo usermod -aG docker "$USER"
sudo newgrp docker
```

Set up the starter repo:

```bash
git clone <repo-url>
cd <repo-folder>
uv sync
cp .env.example .env
```


Install the dependencies:
```bash
uv sync
```

Activate the venv: `source .venv/bin/activate`.

Add your `NEBIUS_API_KEY` to `.env`.

**Check your setup**:
- Run the script: `bash scripts/mini-swe-bench-single.sh`
- Via Airflow:
  - Run the Airflow: `bash run-airflow-standalone`
  - Forward port `8080` -- this is where Airflow is running.
    - VSCode/Cursor may do it automatically for you.
    - Plain SSH: `ssh -L 8080:localhost:8080 <user>@<vm-host>`.
  - Open it: http://localhost:8080
  - Try running the example DAG `mini-swe-bench-single`.


Congratulations! You are all set.

## Final Deliverables

By the end of the mandatory assignment, your repo should contain:

| File or directory | What it is |
|---|---|
| `REPORT.md` | Final writeup with architecture, run instructions, experiment results, artifact layout, and rerun notes |
| `docker-compose.yaml` | VM deployment for Airflow and MLflow |
| `Dockerfile` | Execution image used by Airflow `DockerOperator` tasks |
| `dags/` | Configurable Airflow DAG for `run-mini-swe-agent -> swe-bench-eval -> log-artifacts-to-s3 -> log-metrics-to-mlflow` |
| `scripts/` | Runnable agent and evaluation entrypoints used by the DAG |
| `.env.example` | Non-secret environment template for Airflow, MLflow, S3/Object Storage, and inference credentials |
| `runs/` manifest or sample run folder | Structured run evidence, for example `runs/<run-id>/run-agent/...` and `runs/<run-id>/run-eval/...` |
| S3/Object Storage references | Long-term artifact location for full run outputs |
| MLflow experiment runs | Logged parameters, metrics, run IDs, and artifact references for completed evaluations |
| `screenshots/airflow_dag.png` | Airflow UI showing the completed evaluation pipeline |
| `screenshots/mlflow_runs.png` | MLflow UI showing logged evaluation runs and metrics |
| `screenshots/object_storage_artifacts.png` | Object Storage UI, CLI output, or equivalent evidence showing uploaded run artifacts |

The repository should be enough to deploy the stack on a fresh VM, trigger an evaluation from Airflow parameters, and find the complete evidence for a run by its `run-id`.

If full artifacts are too large to commit, commit a small manifest or example folder and include the remote artifact URI in `REPORT.md`.

---

## Grading

We care more about engineering judgment and traceability than about one lucky metric. A weak result with excellent provenance and analysis is better than a pasted number nobody can reproduce.

| Area | Weight | What a strong submission shows |
|---|---:|---|
| **Configurable Airflow DAG** | 35% | The DAG implements the `run-agent -> run-evaluation` workflow, exposes `split`, `subset`, and `worker` as parameters, avoids hard-coded experiment values, and can be triggered reliably from the Airflow UI. A strong standalone Airflow solution is acceptable here. |
| **Artifact structure and reproducibility** | 20% | Each run writes a structured `runs/<run-id>/` tree and includes enough inputs, outputs, trajectories, predictions, logs, and reports to reconstruct the run. Extra credit within this area for uploading artifacts to S3/Object Storage. |
| **MLflow tracking** | 15% | Runs log parameters, metrics, run IDs, and artifact references so multiple evaluations can be compared in the MLflow UI. |
| **Execution isolation** | 10% | Agent and evaluation work run in a documented, repeatable environment. `DockerOperator` with the project `Dockerfile` is the preferred production-style solution, but a clear standalone Airflow implementation without `DockerOperator` can still receive most of the credit if it is reproducible. |
| **Docker Compose deployment** | 10% | Airflow and MLflow can run from `docker-compose.yaml` with documented setup and required environment variables. The Compose deployment should support the pipeline rather than become the main point of the assignment. |
| **Report and reproducibility** | 10% | `REPORT.md` explains the architecture, how to trigger a run, where artifacts live, how to rerun by `run-id`, and what happened in at least one completed evaluation. |

