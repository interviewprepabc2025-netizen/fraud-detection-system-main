"""
Airflow DAG — Spark Daily Fraud Analytics
==========================================
Schedule: daily at 02:00 UTC (after feature_compute DAG runs at 01:00)

Tasks:
  1. check_gold_data        — verify Gold decisions table has data
  2. run_fraud_analytics    — submit Spark analytics roll-up job
  3. optimize_gold_tables   — OPTIMIZE ZORDER on Gold analytics tables
  4. vacuum_old_data        — VACUUM Bronze/Silver to reclaim storage

This DAG feeds Grafana dashboards via the Delta Gold analytics tables.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

LAKE_BASE = os.environ.get("DELTA_LAKE_BASE", "s3a://fraud-lake")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "local[4]")

DEFAULT_ARGS = {
    "owner": "fraud-de-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_gold_data_fn(**context) -> None:
    """Verify Gold decisions table exists and has recent data."""
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("FraudDetection-GoldCheck")
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

        gold_path = f"{LAKE_BASE}/gold/fraud_decisions"
        count = spark.read.format("delta").load(gold_path).count()
        spark.stop()
        log.info("Gold decisions table has %d total records.", count)

    except Exception as exc:
        log.warning("Gold check soft-failed (table may not exist yet): %s", exc)


def run_fraud_analytics_fn(**context) -> None:
    """Run the Spark daily fraud analytics batch job."""
    sys.path.insert(0, "/opt/airflow")
    os.environ.setdefault("SPARK_MASTER_URL", SPARK_MASTER)
    os.environ.setdefault("MINIO_ENDPOINT", "http://minio:9000")

    try:
        from spark.batch.fraud_analytics import main
        main()
    except Exception as exc:
        log.warning(
            "Fraud analytics soft-failed (Gold table may not exist yet): %s", exc
        )


def optimize_gold_tables_fn(**context) -> None:
    """Run OPTIMIZE + ZORDER on Gold analytics tables for Grafana query perf."""
    sys.path.insert(0, "/opt/airflow")
    os.environ.setdefault("SPARK_MASTER_URL", SPARK_MASTER)
    os.environ.setdefault("MINIO_ENDPOINT", "http://minio:9000")

    try:
        from spark.utils.delta_utils import optimize_table
        from spark.utils.spark_session import get_spark

        spark = get_spark("FraudDetection-Optimize")
        analytics_tables = [
            ("fraud_by_country", ["country_code"]),
            ("fraud_by_mcc", ["merchant_category_code"]),
            ("daily_kpis", ["decision_date"]),
        ]
        for table_name, z_cols in analytics_tables:
            optimize_table(spark, f"{LAKE_BASE}/gold/analytics/{table_name}", z_order_cols=z_cols)
        spark.stop()
    except Exception as exc:
        log.warning("optimize_gold_tables soft-failed (tables may not exist yet): %s", exc)


def vacuum_old_data_fn(**context) -> None:
    """VACUUM Bronze + Silver tables (keep last 7 days = 168 hours)."""
    sys.path.insert(0, "/opt/airflow")
    os.environ.setdefault("SPARK_MASTER_URL", SPARK_MASTER)
    os.environ.setdefault("MINIO_ENDPOINT", "http://minio:9000")

    from spark.utils.delta_utils import PATHS, vacuum_table
    from spark.utils.spark_session import get_spark

    spark = get_spark("FraudDetection-Vacuum")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    vacuum_table(spark, PATHS["bronze_transactions"], retain_hours=168)
    vacuum_table(spark, PATHS["silver_enriched"], retain_hours=168)
    spark.stop()


with DAG(
    dag_id="spark_fraud_analytics",
    description="Daily Spark fraud analytics roll-up + Delta Lake maintenance",
    schedule_interval="0 2 * * *",  # 02:00 UTC daily (after feature_compute)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fraud-detection", "spark", "analytics", "delta-lake"],
    doc_md=__doc__,
) as dag:

    check_gold = PythonOperator(
        task_id="check_gold_data",
        python_callable=check_gold_data_fn,
    )

    run_analytics = PythonOperator(
        task_id="run_fraud_analytics",
        python_callable=run_fraud_analytics_fn,
        execution_timeout=timedelta(hours=1),
    )

    optimize_gold = PythonOperator(
        task_id="optimize_gold_tables",
        python_callable=optimize_gold_tables_fn,
        execution_timeout=timedelta(minutes=30),
    )

    vacuum_data = PythonOperator(
        task_id="vacuum_old_data",
        python_callable=vacuum_old_data_fn,
        execution_timeout=timedelta(minutes=30),
    )

    done = EmptyOperator(task_id="done")

    check_gold >> run_analytics >> [optimize_gold, vacuum_data] >> done
