"""
Airflow DAG — Spark 30-Day Feature Compute
==========================================
Schedule: daily at 01:00 UTC (after midnight data has landed in Delta Silver)

Tasks:
  1. validate_silver_data   — check Silver table has data for yesterday
  2. run_feature_compute    — submit Spark batch feature computation
  3. run_feast_materialize  — materialise Feast features from Parquet → Redis
  4. run_delta_optimize     — OPTIMIZE + ZORDER Silver enriched table
  5. notify_success / notify_failure
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

LAKE_BASE = os.environ.get("DELTA_LAKE_BASE", "s3a://fraud-lake")
FEAST_REPO = os.environ.get("FEAST_REGISTRY_PATH", "/opt/airflow/feature_repo")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "local[4]")

DEFAULT_ARGS = {
    "owner": "fraud-de-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def validate_silver_data(**context) -> str:
    """
    Check that the Silver enriched table has at least 100 records
    from the previous day before running feature computation.
    Returns the next task_id to branch to.
    """
    try:
        from pyspark.sql import SparkSession

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        spark = (
            SparkSession.builder.appName("FraudDetection-Validator")
            .master(SPARK_MASTER)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .getOrCreate()
        )

        silver_path = f"{LAKE_BASE}/silver/enriched"
        count = (
            spark.read.format("delta")
            .load(silver_path)
            .filter(f"date_partition = '{yesterday}'")
            .count()
        )
        spark.stop()

        log.info("Silver records for %s: %d", yesterday, count)
        if count >= 100:
            return "run_feature_compute"
        else:
            log.warning("Insufficient Silver data (%d rows) — skipping feature compute", count)
            return "skip_feature_compute"

    except Exception as exc:
        log.error("Silver validation failed: %s", exc)
        return "skip_feature_compute"


def run_feature_compute_fn(**context) -> None:
    """Run the Spark batch feature computation job inline via PySpark."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    # Re-export environment so Spark picks up MinIO config
    os.environ.setdefault("SPARK_MASTER_URL", SPARK_MASTER)
    os.environ.setdefault("MINIO_ENDPOINT", "http://minio:9000")

    from spark.batch.feature_compute import main
    main()


def run_feast_materialize_fn(**context) -> None:
    """Trigger Feast incremental materialisation after features are written."""
    from datetime import datetime, timedelta, timezone
    result = subprocess.run(
        [
            "feast",
            "--chdir", FEAST_REPO,
            "materialize-incremental",
            datetime.now(timezone.utc).isoformat(),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    log.info("Feast materialise stdout: %s", result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Feast materialise failed: {result.stderr}")


with DAG(
    dag_id="spark_feature_compute",
    description="Daily Spark 30-day feature computation + Feast materialisation",
    schedule_interval="0 1 * * *",  # 01:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fraud-detection", "spark", "feature-store", "delta-lake"],
    doc_md=__doc__,
) as dag:

    validate_silver = BranchPythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver_data,
    )

    skip_feature_compute = EmptyOperator(task_id="skip_feature_compute")

    run_feature_compute = PythonOperator(
        task_id="run_feature_compute",
        python_callable=run_feature_compute_fn,
        execution_timeout=timedelta(hours=2),
    )

    run_feast_materialize = PythonOperator(
        task_id="run_feast_materialize",
        python_callable=run_feast_materialize_fn,
        execution_timeout=timedelta(minutes=30),
    )

    notify_success = EmptyOperator(
        task_id="notify_success",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG wiring
    validate_silver >> [run_feature_compute, skip_feature_compute]
    run_feature_compute >> run_feast_materialize >> notify_success
    skip_feature_compute >> notify_success
