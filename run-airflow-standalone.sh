set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

# Extra packages in Airflow's (uvx) environment:
#   - apache-airflow-providers-docker: DockerOperator (run the agent/eval in containers)
#   - mlflow-skinny: summarize_and_log logs params/metrics to the MLflow tracking server
uv tool run \
    --with apache-airflow-providers-docker \
    --with mlflow-skinny \
    apache-airflow standalone
