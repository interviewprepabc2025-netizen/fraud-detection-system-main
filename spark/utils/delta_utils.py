"""
Delta Lake utility helpers — table initialisation, OPTIMIZE, VACUUM, etc.

Lakehouse layer paths (all on MinIO via s3a://):
  Bronze  → s3a://fraud-lake/bronze/transactions/      raw ingest, partitioned by date
  Silver  → s3a://fraud-lake/silver/enriched/           feature-joined transactions
            s3a://fraud-lake/silver/velocity_windows/   1-hour windowed aggregates
  Gold    → s3a://fraud-lake/gold/fraud_decisions/      supervisor decisions
            s3a://fraud-lake/gold/fraud_analytics/      daily analytics roll-ups
  Quarantine → s3a://fraud-lake/quarantine/             DQ-failed records
"""

import os
import logging
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

LAKE_BASE = os.environ.get("DELTA_LAKE_BASE", "s3a://fraud-lake")

# ── Canonical table paths ─────────────────────────────────────────────────────
PATHS = {
    "bronze_transactions": f"{LAKE_BASE}/bronze/transactions",
    "silver_enriched": f"{LAKE_BASE}/silver/enriched",
    "silver_velocity_windows": f"{LAKE_BASE}/silver/velocity_windows",
    "gold_decisions": f"{LAKE_BASE}/gold/fraud_decisions",
    "gold_analytics": f"{LAKE_BASE}/gold/fraud_analytics",
    "quarantine": f"{LAKE_BASE}/quarantine",
}

# ── Checkpoint paths (Structured Streaming) ───────────────────────────────────
CHECKPOINTS = {
    "enricher_main": f"{LAKE_BASE}/checkpoints/enricher_main",
    "enricher_windows": f"{LAKE_BASE}/checkpoints/enricher_windows",
    "decision_sink": f"{LAKE_BASE}/checkpoints/decision_sink",
}


def table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at *path*."""
    return DeltaTable.isDeltaTable(spark, path)


def upsert_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_key: str,
) -> None:
    """
    Merge *source_df* into the Delta table at *target_path* using *merge_key*
    as the match column. Performs SCD-1 (latest-write-wins) semantics.
    If the target table doesn't exist yet, creates it from the source.
    """
    if not table_exists(spark, target_path):
        source_df.write.format("delta").save(target_path)
        log.info("Created new Delta table at %s", target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("tgt")
        .merge(source_df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("Upserted %d rows into %s", source_df.count(), target_path)


def optimize_table(spark: SparkSession, path: str, z_order_cols: Optional[List[str]] = None) -> None:
    """
    Run OPTIMIZE on a Delta table (compacts small files).
    Optionally applies Z-ORDER to improve data-skipping on high-cardinality columns.
    """
    if not table_exists(spark, path):
        log.warning("OPTIMIZE skipped — table not found at %s", path)
        return

    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
        log.info("OPTIMIZE ZORDER BY (%s) on %s", cols, path)
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        log.info("OPTIMIZE on %s", path)


def vacuum_table(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    """
    Run VACUUM on a Delta table, removing files older than *retain_hours*.
    Default retention is 7 days (168 h).
    """
    if not table_exists(spark, path):
        log.warning("VACUUM skipped — table not found at %s", path)
        return

    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    log.info("VACUUM (retain %dh) on %s", retain_hours, path)


def get_table_history(spark: SparkSession, path: str, limit: int = 10) -> DataFrame:
    """Return the last *limit* entries from a Delta table's transaction log."""
    dt = DeltaTable.forPath(spark, path)
    return dt.history(limit)
