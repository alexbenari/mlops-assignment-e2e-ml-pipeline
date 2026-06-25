-- Runs once on first Postgres init. Airflow uses the default 'airflow' database
-- (POSTGRES_DB); MLflow gets its own database in the same Postgres instance.
CREATE DATABASE mlflow;
