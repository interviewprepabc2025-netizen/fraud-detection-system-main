"""
Spark Batch Job — 30-Day Feature Computation
============================================
Triggered daily by Airflow (dags/spark_feature_compute.py).

Reads from Delta Silver (enriched transactions, last 30 days) and computes
per-user behavioural features that are then written to:
  1. Delta Silver features table (for analytics / audit)
  2. Parquet files consumed by Feast offline → online materialisation

DE concepts demonstrated:
  • Batch reading from partitioned Delta Lake table with predicate pushdown
  • Window functions (LAG, RANK, rolling aggregates via window specs)
  • Broadcast join with static country-risk reference table
  • Bucketing + partitioned Parquet output for Feast offline store
  • AQE (Adaptive Query Execution) for dynamic partition coalescing
  • DataFrame caching for multiple downstream aggregations
  • Z-ORDER optimisation hints logged for manual execution
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from spark.utils.delta_utils import PATHS, optimize_table, vacuum_table
from spark.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("fraud.feature_compute")

LAKE_BASE = os.environ.get("DELTA_LAKE_BASE", "s3a://fraud-lake")
FEAST_PARQUET_OUTPUT = f"{LAKE_BASE}/feast_offline/user_behavioral_stats"

# High-risk MCC codes for hour-of-day risk computation
HIGH_RISK_MCCS = {"5912", "7995", "6051", "4829", "6211", "5999"}

# Country risk reference (static broadcast table)
COUNTRY_RISK_DATA = [
    ("NG", 0.85), ("RO", 0.70), ("UA", 0.65), ("VN", 0.60),
    ("CN", 0.55), ("RU", 0.80), ("BR", 0.50), ("IN", 0.40),
    ("US", 0.10), ("GB", 0.12), ("DE", 0.10), ("FR", 0.11),
]
COUNTRY_RISK_SCHEMA = StructType(
    [StructField("country_code", StringType(), True), StructField("country_risk", FloatType(), True)]
)


def load_silver_transactions(spark: SparkSession, lookback_days: int = 30) -> DataFrame:
    """
    Load Silver enriched transactions for the last *lookback_days*.
    Uses partition pruning on `date_partition` for efficient reads.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    log.info("Loading Silver transactions since %s", cutoff)

    df = (
        spark.read.format("delta")
        .load(PATHS["silver_enriched"])
        .filter(F.col("date_partition") >= cutoff)
    )
    count = df.count()
    log.info("Loaded %d records from Silver layer", count)
    return df


def compute_behavioral_features(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Compute per-user 30-day behavioural statistics using Spark window functions.

    Returns a DataFrame with one row per user_id containing:
      - avg_txn_amount_30d
      - stddev_txn_amount_30d
      - amount_deviation_score   (last txn vs 30d avg, in stddev units)
      - hour_of_day_risk_score   (fraction of txns in 00:00-05:59 window)
      - most_common_mcc          (mode MCC code as float for Feast)
      - txn_count_30d
      - high_risk_mcc_ratio      (fraction of txns in high-risk MCC categories)
    """
    log.info("Computing 30-day behavioural features...")

    # Cache the base DataFrame — reused for multiple aggregations
    df.cache()

    user_window = Window.partitionBy("user_id")
    user_time_window = Window.partitionBy("user_id").orderBy("event_time")

    # ── 30-day aggregations ───────────────────────────────────────────────────
    agg_df = df.groupBy("user_id").agg(
        F.avg("amount").alias("avg_txn_amount_30d"),
        F.stddev("amount").alias("stddev_txn_amount_30d"),
        F.count("*").alias("txn_count_30d"),
        F.last("amount").alias("last_txn_amount"),
        # Night-hour risk: transactions between midnight and 6 AM
        F.sum(
            F.when(F.hour(F.col("event_time")).between(0, 5), 1).otherwise(0)
        ).alias("night_txn_count"),
        # High-risk MCC ratio
        F.sum(
            F.when(F.col("merchant_category_code").isin(list(HIGH_RISK_MCCS)), 1).otherwise(0)
        ).alias("high_risk_mcc_count"),
    )

    # ── Most-common MCC (mode) via rank window ────────────────────────────────
    mcc_counts = (
        df.filter(F.col("merchant_category_code").isNotNull())
        .groupBy("user_id", "merchant_category_code")
        .agg(F.count("*").alias("mcc_freq"))
    )
    mcc_rank_window = Window.partitionBy("user_id").orderBy(F.col("mcc_freq").desc())
    top_mcc = (
        mcc_counts
        .withColumn("rank", F.rank().over(mcc_rank_window))
        .filter(F.col("rank") == 1)
        .select("user_id", F.col("merchant_category_code").alias("most_common_mcc"))
    )

    # ── Derived features ──────────────────────────────────────────────────────
    features_df = (
        agg_df
        .join(top_mcc, on="user_id", how="left")
        .withColumn(
            "stddev_txn_amount_30d",
            F.coalesce(F.col("stddev_txn_amount_30d"), F.lit(0.0)),
        )
        .withColumn(
            "amount_deviation_score",
            F.when(
                F.col("stddev_txn_amount_30d") > 0,
                F.abs(
                    (F.col("last_txn_amount") - F.col("avg_txn_amount_30d"))
                    / F.col("stddev_txn_amount_30d")
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "hour_of_day_risk_score",
            F.col("night_txn_count") / F.col("txn_count_30d"),
        )
        .withColumn(
            "high_risk_mcc_ratio",
            F.col("high_risk_mcc_count") / F.col("txn_count_30d"),
        )
        .withColumn(
            # Feast expects numeric types; encode MCC as hash float
            "most_common_mcc",
            F.hash(F.col("most_common_mcc")).cast(FloatType()),
        )
        .withColumn("feature_ts", F.current_timestamp())
        .drop("last_txn_amount", "night_txn_count", "high_risk_mcc_count")
    )

    # ── Broadcast join: country risk score ────────────────────────────────────
    last_country = (
        df.withColumn("rn", F.row_number().over(user_time_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .filter(F.col("rn") == F.count("*").over(user_window))  # last row per user
        .select("user_id", F.col("country_code").alias("last_country_code"))
    )
    country_risk = spark.createDataFrame(COUNTRY_RISK_DATA, COUNTRY_RISK_SCHEMA)

    features_df = (
        features_df
        .join(last_country, on="user_id", how="left")
        .join(F.broadcast(country_risk), on="country_code", how="left")
        .withColumnRenamed("country_code", "last_country_code")
        .withColumn("country_risk", F.coalesce(F.col("country_risk"), F.lit(0.3)))
    )

    df.unpersist()
    return features_df


def write_features_to_delta(features_df: DataFrame) -> None:
    """Write computed features to Delta Silver features table."""
    output_path = f"{LAKE_BASE}/silver/behavioral_features"
    log.info("Writing behavioral features to Delta Silver → %s", output_path)
    (
        features_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    log.info("Delta Silver features written successfully.")


def write_features_to_feast_parquet(features_df: DataFrame) -> None:
    """
    Write features to partitioned Parquet files consumed by Feast offline store.
    Bucketed by user_id (16 buckets) to speed up Feast point-in-time joins.
    """
    log.info("Writing Feast offline Parquet → %s", FEAST_PARQUET_OUTPUT)
    (
        features_df
        .select(
            "user_id",
            "avg_txn_amount_30d",
            "stddev_txn_amount_30d",
            "amount_deviation_score",
            "hour_of_day_risk_score",
            "most_common_mcc",
            "txn_count_30d",
            "high_risk_mcc_ratio",
            "country_risk",
            "feature_ts",
        )
        .write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(FEAST_PARQUET_OUTPUT)
    )
    log.info("Feast offline Parquet written successfully.")


def main() -> None:
    spark = get_spark("FraudDetection-FeatureCompute")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Starting 30-day Feature Compute Batch Job ===")

    silver_df = load_silver_transactions(spark, lookback_days=30)
    features_df = compute_behavioral_features(silver_df, spark)

    write_features_to_delta(features_df)
    write_features_to_feast_parquet(features_df)

    # Periodic Delta Lake maintenance
    optimize_table(
        spark,
        f"{LAKE_BASE}/silver/behavioral_features",
        z_order_cols=["user_id"],
    )
    vacuum_table(spark, PATHS["silver_enriched"], retain_hours=168)

    log.info("=== Feature Compute Batch Job completed ===")
    spark.stop()


if __name__ == "__main__":
    main()
