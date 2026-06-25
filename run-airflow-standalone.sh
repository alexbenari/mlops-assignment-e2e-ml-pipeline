set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

# --with apache-airflow-providers-docker: makes DockerOperator importable in Airflow's
# (uvx) environment, so the evaluate_agent DAG can run the agent/eval in containers.
uv tool run --with apache-airflow-providers-docker apache-airflow standalone
