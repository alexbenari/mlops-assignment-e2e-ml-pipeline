"""Light-weight orchestration helpers for the evaluate_agent DAG.

These modules are imported by the DAG and therefore run under Airflow's
interpreter (the uvx apache-airflow env), NOT the project .venv. Keep them
standard-library only: do not import swebench / minisweagent here. The heavy
workload is shelled out to the project venv via scripts/ in agent.py / eval.py.
"""
