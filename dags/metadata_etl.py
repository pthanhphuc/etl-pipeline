"""The single manually-authored Airflow entry point for this project."""

from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.pipeline import load_source, transform_source


def _transform(**context: object) -> str:
    dag_run = context["dag_run"]
    source_prefix = dag_run.conf.get("source_prefix")
    if not isinstance(source_prefix, str) or set(dag_run.conf) != {"source_prefix"}:
        raise ValueError("dag_run.conf must contain only a string source_prefix")
    result = transform_source(source_prefix)
    return result.run_id


def _load(**context: object) -> None:
    dag_run = context["dag_run"]
    source_prefix = dag_run.conf.get("source_prefix")
    if not isinstance(source_prefix, str) or set(dag_run.conf) != {"source_prefix"}:
        raise ValueError("dag_run.conf must contain only a string source_prefix")
    run_id = context["ti"].xcom_pull(task_ids="transform_data")
    load_source(source_prefix, run_id=run_id)


with DAG(
    dag_id="metadata_etl",
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_runs=16,
) as dag:
    transform_data = PythonOperator(task_id="transform_data", python_callable=_transform)
    load_data_into_db = PythonOperator(task_id="load_data_into_db", python_callable=_load)
    transform_data >> load_data_into_db
