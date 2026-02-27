"""Hourly Feast feature materialization DAG."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="feature_materialization",
    description="Materialize Feast features to the online store every hour.",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "fraud-ml-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["fraud", "feast", "features"],
)
def feature_materialization() -> None:

    materialize = BashOperator(
        task_id="feast_materialize_incremental",
        bash_command=(
            "cd /opt/airflow/feature_repo && "
            "feast materialize-incremental $(date -u +'%Y-%m-%dT%H:%M:%S')"
        ),
        env={"FEAST_REGISTRY_PATH": "./registry.db"},
        append_env=True,
    )

    materialize


feature_materialization()
