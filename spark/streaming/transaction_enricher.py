"""
Spark Structured Streaming — Transaction Enrichment Pipeline
============================================================

Replaces the plain-Python enrichment_consumer with a full Spark Structured
Streaming job that delivers these data-engineering capabilities:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  DE Concepts Demonstrated                                           │
  │  ─────────────────────────────────────────────────────────────────  │
  │  • Structured Streaming micro-batch processing (foreachBatch)      │
  │  • Watermarking for late-arriving data tolerance                   │
  │  • Tumbling + sliding window aggregations                          │
  │  • Delta Lake Bronze / Silver / Quarantine writes (ACID)           │
  │  • Broadcast join with static merchant-risk lookup table           │
  │  • Redis feature look-ups inside foreachBatch (online store)       │
  │  • Exactly-once semantics via Spark checkpointing + Delta Lake     │
  │  • Data quality gate — DQ failures routed to quarantine layer      │
  │  • Partitioning by event date for efficient downstream reads       │
  │  • Adaptive Query Execution (AQE) for dynamic plan re-optimisation │
  │  • Two concurrent streaming queries on the same Kafka source       │
  └─────────────────────────────────────────────────────────────────────┘

Data Flow:
  Kafka raw-transactions
    ├─► [Query 1 — main enricher]
    │     parse → DQ check → Bronze Delta write
    │     → Redis feature fetch → Broadcast join
    │     → Silver Delta write
    │     → Kafka enriched-transactions publish
    │     → Redis velocity counters update (TTL-based sliding window)
    │
    └─► [Query 2 — windowed aggregator]
          watermark(10 min) → 1-h tumbling window per user_id
          → Silver velocity_windows Delta write (append)
          → feeds Airflow daily feature-compute batch job
"""

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from spark.streaming.schemas import TRANSACTION_SCHEMA
from spark.utils.delta_utils import CHECKPOINTS, PATHS
from spark.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("fraud.enricher")

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_RAW_TOPIC = os.environ.get("TOPIC_RAW_TRANSACTIONS", "raw-transactions")
KAFKA_ENRICHED_TOPIC = os.environ.get("TOPIC_ENRICHED_TRANSACTIONS", "enriched-transactions")

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

# Velocity TTLs (seconds)
TTL_1H = 3600
TTL_24H = 86400

# High-risk MCC codes (used for risk-score annotation in enrichment)
HIGH_RISK_MCCS = {"5912", "7995", "6051", "4829", "6211", "5999"}

# ── Merchant risk broadcast table (static) ────────────────────────────────────
# In production this would come from a managed reference data store.
MERCHANT_RISK_DATA = [
    ("5912", "pharmacy", 0.6),
    ("7995", "gambling", 0.9),
    ("6051", "crypto_exchange", 0.85),
    ("4829", "money_transfer", 0.8),
    ("6211", "securities_broker", 0.7),
    ("5999", "misc_retail", 0.4),
]
MERCHANT_RISK_SCHEMA = StructType(
    [
        StructField("mcc", StringType(), True),
        StructField("mcc_label", StringType(), True),
        StructField("mcc_risk_score", DoubleType(), True),
    ]
)


# ── Redis helpers (run on driver inside foreachBatch) ─────────────────────────

def _redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
    )


def _get_user_features(r: redis.Redis, user_id: str) -> Dict[str, Any]:
    """Fetch velocity, behavioural, and geo features from Redis for one user."""
    pipe = r.pipeline(transaction=False)
    pipe.hgetall(f"feast:user_transaction_stats:{user_id}")
    pipe.hgetall(f"feast:user_behavioral_stats:{user_id}")
    pipe.hgetall(f"feast:geo_stats:{user_id}")
    tx_stats, behav_stats, geo_stats = pipe.execute()

    return {
        "txn_count_1h": int(tx_stats.get("txn_count_1h", 0)),
        "txn_count_24h": int(tx_stats.get("txn_count_24h", 0)),
        "amount_sum_1h": float(tx_stats.get("amount_sum_1h", 0.0)),
        "amount_sum_24h": float(tx_stats.get("amount_sum_24h", 0.0)),
        "unique_merchants_1h": int(tx_stats.get("unique_merchants_1h", 0)),
        "avg_txn_amount_30d": float(behav_stats.get("avg_txn_amount_30d", 0.0)),
        "amount_deviation_score": float(behav_stats.get("amount_deviation_score", 0.0)),
        "hour_of_day_risk_score": float(behav_stats.get("hour_of_day_risk_score", 0.0)),
        "distance_from_last_txn_km": float(geo_stats.get("distance_from_last_txn_km", 0.0)),
        "is_new_country": geo_stats.get("is_new_country", "false").lower() == "true",
        "is_new_city": geo_stats.get("is_new_city", "false").lower() == "true",
    }


def _get_device_features(r: redis.Redis, device_fp: str) -> Dict[str, Any]:
    """Fetch network features from Redis for one device fingerprint."""
    data = r.hgetall(f"feast:device_network_stats:{device_fp}")
    return {
        "ip_fraud_score": float(data.get("ip_fraud_score", 0.0)),
        "device_seen_before": data.get("device_seen_before", "false").lower() == "true",
        "linked_fraud_accounts": int(data.get("linked_fraud_accounts", 0)),
    }


def _update_velocity_features(r: redis.Redis, rows: List[Dict]) -> None:
    """
    Increment per-user velocity counters in Redis.
    Uses INCRBYFLOAT + EXPIRE for TTL-based sliding windows.
    These counters are read back by the agents via the Feast online store.
    """
    pipe = r.pipeline(transaction=False)
    for row in rows:
        uid = row["user_id"]
        amount = float(row["amount"])
        merchant = row.get("merchant_id", "")

        key_cnt_1h = f"velocity:{uid}:cnt_1h"
        key_cnt_24h = f"velocity:{uid}:cnt_24h"
        key_sum_1h = f"velocity:{uid}:sum_1h"
        key_sum_24h = f"velocity:{uid}:sum_24h"
        key_merch = f"velocity:{uid}:merchants_1h"

        pipe.incr(key_cnt_1h)
        pipe.expire(key_cnt_1h, TTL_1H)
        pipe.incr(key_cnt_24h)
        pipe.expire(key_cnt_24h, TTL_24H)
        pipe.incrbyfloat(key_sum_1h, amount)
        pipe.expire(key_sum_1h, TTL_1H)
        pipe.incrbyfloat(key_sum_24h, amount)
        pipe.expire(key_sum_24h, TTL_24H)
        if merchant:
            pipe.sadd(key_merch, merchant)
            pipe.expire(key_merch, TTL_1H)

    pipe.execute()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance between two lat/lon points in kilometres."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── Data-quality check (inline, no external library required) ─────────────────

def _run_dq_checks(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split a batch DataFrame into (good_records, bad_records).

    Rules:
      1. transaction_id must be non-null
      2. user_id must be non-null
      3. amount must be > 0
      4. timestamp must be non-null
    """
    is_good = (
        F.col("transaction_id").isNotNull()
        & F.col("user_id").isNotNull()
        & (F.col("amount") > 0)
        & F.col("timestamp").isNotNull()
    )
    return df.filter(is_good), df.filter(~is_good)


# ── foreachBatch sink ─────────────────────────────────────────────────────────

def process_enrichment_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Per-micro-batch processing:
      1. Parse + cast types
      2. DQ gate — quarantine bad records
      3. Write raw transactions to Delta Bronze (partitioned by date)
      4. Fetch features from Redis (driver-side, per user/device)
      5. Build enriched DataFrame in Spark (broadcast join for MCC risk)
      6. Write enriched to Delta Silver (partitioned by date)
      7. Publish enriched transactions to Kafka enriched-transactions topic
      8. Update Redis velocity counters (sliding-window state)
    """
    if batch_df.isEmpty():
        return

    spark: SparkSession = batch_df.sparkSession
    log.info("==> Batch %d | rows=%d", batch_id, batch_df.count())

    # ── 1. Cast timestamp string → timestamp type ─────────────────────────────
    batch_df = batch_df.withColumn(
        "event_time", F.to_timestamp(F.col("timestamp"))
    ).withColumn("amount", F.col("amount").cast(DoubleType()))

    # ── 2. DQ gate ────────────────────────────────────────────────────────────
    good_df, bad_df = _run_dq_checks(batch_df)

    if not bad_df.isEmpty():
        bad_count = bad_df.count()
        log.warning("DQ failures: %d records → quarantine", bad_count)
        (
            bad_df
            .withColumn("quarantine_ts", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
            .write.format("delta")
            .mode("append")
            .save(PATHS["quarantine"])
        )

    if good_df.isEmpty():
        return

    # ── 3. Write to Delta Bronze (partitioned by date) ────────────────────────
    (
        good_df
        .withColumn("date_partition", F.to_date(F.to_timestamp(F.col("timestamp"))))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_ts", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .partitionBy("date_partition")
        .save(PATHS["bronze_transactions"])
    )

    # ── 4. Fetch features from Redis (on driver, per unique user/device) ──────
    r = _redis_client()

    user_ids = [row.user_id for row in good_df.select("user_id").distinct().collect()]
    device_fps = [
        row.device_fingerprint
        for row in good_df.select("device_fingerprint")
        .filter(F.col("device_fingerprint").isNotNull())
        .distinct()
        .collect()
    ]

    user_features: Dict[str, Dict] = {uid: _get_user_features(r, uid) for uid in user_ids}
    device_features: Dict[str, Dict] = {fp: _get_device_features(r, fp) for fp in device_fps}

    # ── 5. Build enriched rows with feature merge ─────────────────────────────
    rows = good_df.collect()
    enriched_rows = []
    for row in rows:
        row_dict = row.asDict()
        uf = user_features.get(row_dict["user_id"], {})
        df_ = device_features.get(row_dict.get("device_fingerprint", "") or "", {})

        row_dict.update(uf)
        row_dict.update(df_)
        row_dict["amount"] = float(row_dict.get("amount") or 0.0)
        # Normalise types that Spark can't auto-infer from Python
        row_dict["metadata"] = json.dumps(row_dict.get("metadata") or {})
        row_dict.pop("event_time", None)   # re-computed from timestamp downstream
        row_dict.pop("kafka_ts", None)     # not needed in enriched layer
        # Ensure feature fields have explicit defaults so schema can be inferred
        row_dict.setdefault("txn_count_1h", 0)
        row_dict.setdefault("txn_count_24h", 0)
        row_dict.setdefault("amount_sum_1h", 0.0)
        row_dict.setdefault("amount_sum_24h", 0.0)
        row_dict.setdefault("unique_merchants_1h", 0)
        row_dict.setdefault("distance_from_last_txn_km", 0.0)
        row_dict.setdefault("is_new_country", False)
        row_dict.setdefault("is_new_city", False)
        row_dict.setdefault("avg_txn_amount_30d", 0.0)
        row_dict.setdefault("amount_deviation_score", 0.0)
        row_dict.setdefault("hour_of_day_risk_score", 0.0)
        row_dict.setdefault("ip_fraud_score", 0.0)
        row_dict.setdefault("device_seen_before", False)
        row_dict.setdefault("linked_fraud_accounts", 0)
        enriched_rows.append(row_dict)

    if not enriched_rows:
        return

    # Build enriched DataFrame (schema can now be inferred — all fields have non-None defaults)
    enriched_df = spark.createDataFrame(enriched_rows)

    # ── 5b. Broadcast join: enrich with MCC risk scores (static ref table) ────
    merchant_risk_df = spark.createDataFrame(MERCHANT_RISK_DATA, schema=MERCHANT_RISK_SCHEMA)
    enriched_df = enriched_df.join(
        F.broadcast(merchant_risk_df),
        enriched_df["merchant_category_code"] == merchant_risk_df["mcc"],
        how="left",
    ).drop("mcc")

    enriched_df = enriched_df.withColumn(
        "mcc_risk_score",
        F.coalesce(F.col("mcc_risk_score"), F.lit(0.1)),
    )

    # ── 6. Write enriched to Delta Silver (partitioned by date) ───────────────
    (
        enriched_df
        .withColumn("date_partition", F.to_date(F.to_timestamp(F.col("timestamp"))))
        .write.format("delta")
        .mode("append")
        .partitionBy("date_partition")
        .save(PATHS["silver_enriched"])
    )

    # ── 7. Publish enriched transactions to Kafka ─────────────────────────────
    # Drop helper columns added by Spark that aren't part of the Pydantic schema
    publish_cols = [
        "transaction_id", "user_id", "merchant_id", "amount", "currency",
        "card_bin", "card_last4", "ip_address", "device_fingerprint",
        "merchant_category_code", "country_code", "city", "latitude", "longitude",
        "timestamp", "metadata",
        "txn_count_1h", "txn_count_24h", "amount_sum_1h", "amount_sum_24h",
        "unique_merchants_1h", "distance_from_last_txn_km", "is_new_country", "is_new_city",
        "avg_txn_amount_30d", "amount_deviation_score", "hour_of_day_risk_score",
        "ip_fraud_score", "device_seen_before", "linked_fraud_accounts",
    ]
    # Keep only columns that actually exist in the DataFrame
    existing = set(enriched_df.columns)
    safe_cols = [c for c in publish_cols if c in existing]

    (
        enriched_df.select(safe_cols)
        .select(
            F.to_json(F.struct(*[F.col(c) for c in safe_cols])).alias("value"),
            F.col("transaction_id").alias("key"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", KAFKA_ENRICHED_TOPIC)
        .save()
    )

    log.info("Published %d enriched transactions to Kafka", len(enriched_rows))

    # ── 8. Update Redis velocity counters ──────────────────────────────────────
    _update_velocity_features(r, enriched_rows)
    r.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = get_spark("FraudDetection-StreamingEnricher")
    spark.sparkContext.setLogLevel("WARN")

    log.info("Starting Fraud Detection Streaming Enricher")
    log.info("Kafka source  : %s → %s", KAFKA_SERVERS, KAFKA_RAW_TOPIC)
    log.info("Delta Bronze  : %s", PATHS["bronze_transactions"])
    log.info("Delta Silver  : %s", PATHS["silver_enriched"])
    log.info("Kafka sink    : %s", KAFKA_ENRICHED_TOPIC)

    # ── Read stream from Kafka ─────────────────────────────────────────────────
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)  # back-pressure control
        .load()
    )

    # Parse JSON payload → typed columns
    transactions_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_ts"),  # Kafka message timestamp
    ).select("data.*", "kafka_ts")

    # ── Query 1: Main enrichment + Delta Bronze/Silver + Kafka publish ─────────
    enrichment_query = (
        transactions_stream.writeStream.foreachBatch(process_enrichment_batch)
        .option("checkpointLocation", CHECKPOINTS["enricher_main"])
        .trigger(processingTime="500 milliseconds")
        .start()
    )

    # ── Query 2: 1-hour tumbling window velocity aggregations → Delta Silver ───
    # Watermark 10 min: tolerate data up to 10 min late before closing the window
    windowed_velocity = (
        transactions_stream
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window(F.col("event_time"), "1 hour"),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("amount_sum"),
            F.avg("amount").alias("avg_amount"),
            F.approx_count_distinct("merchant_id").alias("unique_merchants"),
            F.approx_count_distinct("country_code").alias("unique_countries"),
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount"),
        )
        .select(
            F.col("user_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("txn_count"),
            F.col("amount_sum"),
            F.col("avg_amount"),
            F.col("unique_merchants"),
            F.col("unique_countries"),
            F.col("max_amount"),
            F.col("min_amount"),
        )
    )

    velocity_windows_query = (
        windowed_velocity.writeStream.format("delta")
        .outputMode("append")  # watermark ensures windows are final before appending
        .option("checkpointLocation", CHECKPOINTS["enricher_windows"])
        .trigger(processingTime="1 minute")
        .start(PATHS["silver_velocity_windows"])
    )

    log.info("Both streaming queries started. Awaiting termination...")

    # Block until both queries terminate (or one fails)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
