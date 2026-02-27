"""
Spark Batch Job — Daily Fraud Analytics Roll-up (Gold Layer)
============================================================
Triggered daily by Airflow (dags/spark_fraud_analytics.py).

Reads from:
  • Delta Gold — fraud_decisions (supervisor outcomes)
  • Delta Silver — enriched transactions

Produces Gold analytics tables:
  • fraud_by_country       — fraud rate, avg risk score, decision counts by country
  • fraud_by_mcc           — fraud rate and avg risk score by merchant category
  • fraud_by_hour          — hourly fraud pattern (UTC)
  • fraud_velocity_summary — top users by transaction velocity and risk
  • daily_kpis             — system-level KPIs for SLA monitoring

DE concepts:
  • Multi-table Delta Lake read with predicate pushdown
  • Complex window functions (NTILE for risk percentile buckets)
  • Pivoting / cross-tabulation for fraud pattern analysis
  • OPTIMIZE + ZORDER on Gold analytics tables
  • Schema evolution via Delta Lake mergeSchema option
  • Writing analytics as both Delta and JSON summary for Grafana
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.utils.delta_utils import PATHS, optimize_table, vacuum_table
from spark.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("fraud.analytics")

LAKE_BASE = os.environ.get("DELTA_LAKE_BASE", "s3a://fraud-lake")
ANALYTICS_BASE = f"{LAKE_BASE}/gold/analytics"


def load_decisions(spark: SparkSession, lookback_days: int = 7) -> DataFrame:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    log.info("Loading Gold decisions since %s", cutoff)
    return (
        spark.read.format("delta")
        .load(PATHS["gold_decisions"])
        .filter(F.col("decision_date") >= cutoff)
    )


def load_silver_enriched(spark: SparkSession, lookback_days: int = 7) -> DataFrame:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    return (
        spark.read.format("delta")
        .load(PATHS["silver_enriched"])
        .filter(F.col("date_partition") >= cutoff)
    )


# ── Analytics computations ────────────────────────────────────────────────────

def compute_fraud_by_country(decisions: DataFrame, enriched: DataFrame) -> DataFrame:
    """Fraud rate, avg risk score, and decision distribution by country."""
    log.info("Computing fraud by country...")
    joined = decisions.join(
        enriched.select("transaction_id", "country_code"),
        on="transaction_id",
        how="left",
    )
    return (
        joined
        .groupBy("country_code")
        .agg(
            F.count("*").alias("total_decisions"),
            F.sum(F.when(F.col("action") == "decline", 1).otherwise(0)).alias("declines"),
            F.sum(F.when(F.col("action") == "review", 1).otherwise(0)).alias("reviews"),
            F.sum(F.when(F.col("action") == "approve", 1).otherwise(0)).alias("approvals"),
            F.avg("final_risk_score").alias("avg_risk_score"),
            F.max("final_risk_score").alias("max_risk_score"),
            F.avg("total_latency_ms").alias("avg_latency_ms"),
        )
        .withColumn(
            "fraud_rate",
            F.col("declines") / F.col("total_decisions"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.col("fraud_rate").desc())
    )


def compute_fraud_by_mcc(decisions: DataFrame, enriched: DataFrame) -> DataFrame:
    """Fraud rate and avg risk score by merchant category code."""
    log.info("Computing fraud by MCC...")
    joined = decisions.join(
        enriched.select("transaction_id", "merchant_category_code"),
        on="transaction_id",
        how="left",
    )
    return (
        joined
        .groupBy("merchant_category_code")
        .agg(
            F.count("*").alias("total_decisions"),
            F.sum(F.when(F.col("action") == "decline", 1).otherwise(0)).alias("declines"),
            F.avg("final_risk_score").alias("avg_risk_score"),
        )
        .withColumn("fraud_rate", F.col("declines") / F.col("total_decisions"))
        .withColumn("computed_at", F.current_timestamp())
        .filter(F.col("merchant_category_code").isNotNull())
        .orderBy(F.col("fraud_rate").desc())
    )


def compute_fraud_by_hour(decisions: DataFrame) -> DataFrame:
    """Fraud pattern by UTC hour — identifies peak fraud windows."""
    log.info("Computing fraud by hour of day...")
    return (
        decisions
        .withColumn("hour_utc", F.hour(F.col("event_time")))
        .groupBy("hour_utc")
        .agg(
            F.count("*").alias("total_decisions"),
            F.sum(F.when(F.col("action") == "decline", 1).otherwise(0)).alias("declines"),
            F.avg("final_risk_score").alias("avg_risk_score"),
        )
        .withColumn("fraud_rate", F.col("declines") / F.col("total_decisions"))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("hour_utc")
    )


def compute_velocity_summary(enriched: DataFrame) -> DataFrame:
    """
    Top users by transaction velocity with risk percentile buckets.
    Uses NTILE window function to compute risk quartiles.
    """
    log.info("Computing velocity summary...")
    user_stats = (
        enriched
        .groupBy("user_id")
        .agg(
            F.count("*").alias("txn_count_7d"),
            F.sum("amount").alias("amount_sum_7d"),
            F.avg("amount").alias("avg_amount_7d"),
            F.avg("ip_fraud_score").alias("avg_ip_fraud_score"),
            F.max("txn_count_1h").alias("peak_txn_count_1h"),
        )
    )
    risk_window = Window.orderBy(F.col("avg_ip_fraud_score").desc())
    return (
        user_stats
        .withColumn("risk_percentile_bucket", F.ntile(4).over(risk_window))  # Q1–Q4
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.col("txn_count_7d").desc())
        .limit(10_000)  # top 10k users
    )


def compute_daily_kpis(decisions: DataFrame) -> DataFrame:
    """
    System-level KPIs for SLA monitoring:
      - Total decisions, fraud rate, avg latency, p95 latency
      - SLA breach count (latency > 200 ms)
    """
    log.info("Computing daily KPIs...")
    percentile_udf = F.percentile_approx("total_latency_ms", 0.95)
    return (
        decisions
        .withColumn("decision_date", F.to_date(F.col("event_time")))
        .groupBy("decision_date")
        .agg(
            F.count("*").alias("total_decisions"),
            F.sum(F.when(F.col("action") == "decline", 1).otherwise(0)).alias("total_declines"),
            F.sum(F.when(F.col("action") == "review", 1).otherwise(0)).alias("total_reviews"),
            F.avg("final_risk_score").alias("avg_risk_score"),
            F.avg("total_latency_ms").alias("avg_latency_ms"),
            percentile_udf.alias("p95_latency_ms"),
            F.sum(F.when(F.col("total_latency_ms") > 200, 1).otherwise(0)).alias("sla_breaches"),
        )
        .withColumn(
            "fraud_rate",
            F.col("total_declines") / F.col("total_decisions"),
        )
        .withColumn(
            "sla_breach_rate",
            F.col("sla_breaches") / F.col("total_decisions"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.col("decision_date").desc())
    )


def write_analytics_table(df: DataFrame, name: str) -> None:
    path = f"{ANALYTICS_BASE}/{name}"
    log.info("Writing analytics table %s → %s", name, path)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    optimize_table(df.sparkSession, path)


def main() -> None:
    spark = get_spark("FraudDetection-FraudAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Starting Daily Fraud Analytics Batch Job ===")

    decisions = load_decisions(spark, lookback_days=7)
    enriched = load_silver_enriched(spark, lookback_days=7)

    # Cache both DataFrames — used in multiple downstream aggregations
    decisions.cache()
    enriched.cache()

    write_analytics_table(compute_fraud_by_country(decisions, enriched), "fraud_by_country")
    write_analytics_table(compute_fraud_by_mcc(decisions, enriched), "fraud_by_mcc")
    write_analytics_table(compute_fraud_by_hour(decisions), "fraud_by_hour")
    write_analytics_table(compute_velocity_summary(enriched), "velocity_summary")
    write_analytics_table(compute_daily_kpis(decisions), "daily_kpis")

    decisions.unpersist()
    enriched.unpersist()

    # Periodic Gold maintenance
    vacuum_table(spark, PATHS["gold_decisions"], retain_hours=336)  # 14 days

    log.info("=== Fraud Analytics Batch Job completed ===")
    spark.stop()


if __name__ == "__main__":
    main()
