"""
PySpark schema definitions for the fraud detection streaming pipeline.
All schemas mirror the Pydantic models in shared/utils/models.py so that
the Spark layer can deserialise Kafka JSON payloads without field ambiguity.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Raw transaction (produced by transaction_producer.py) ─────────────────────
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=False),  # producer sends quoted "324.54"; cast to Double in job
        StructField("currency", StringType(), nullable=True),
        StructField("card_bin", StringType(), nullable=True),
        StructField("card_last4", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("device_fingerprint", StringType(), nullable=True),
        StructField("merchant_category_code", StringType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        # ISO-8601 string from Kafka; cast to TimestampType in the job
        StructField("timestamp", StringType(), nullable=False),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)

# ── Enriched transaction (after feature join) ─────────────────────────────────
ENRICHED_TRANSACTION_SCHEMA = StructType(
    TRANSACTION_SCHEMA.fields
    + [
        # Velocity features
        StructField("txn_count_1h", LongType(), nullable=True),
        StructField("txn_count_24h", LongType(), nullable=True),
        StructField("amount_sum_1h", DoubleType(), nullable=True),
        StructField("amount_sum_24h", DoubleType(), nullable=True),
        StructField("unique_merchants_1h", LongType(), nullable=True),
        # Geolocation features
        StructField("distance_from_last_txn_km", DoubleType(), nullable=True),
        StructField("is_new_country", BooleanType(), nullable=True),
        StructField("is_new_city", BooleanType(), nullable=True),
        # Behavioural features
        StructField("avg_txn_amount_30d", DoubleType(), nullable=True),
        StructField("amount_deviation_score", DoubleType(), nullable=True),
        StructField("hour_of_day_risk_score", DoubleType(), nullable=True),
        # Network features
        StructField("ip_fraud_score", DoubleType(), nullable=True),
        StructField("device_seen_before", BooleanType(), nullable=True),
        StructField("linked_fraud_accounts", LongType(), nullable=True),
    ]
)

# ── Supervisor decision (written by supervisor agent to agent-verdicts) ────────
SUPERVISOR_DECISION_SCHEMA = StructType(
    [
        StructField("decision_id", StringType(), nullable=False),
        StructField("transaction_id", StringType(), nullable=False),
        StructField("final_risk_score", DoubleType(), nullable=True),
        StructField("risk_level", StringType(), nullable=True),
        StructField("action", StringType(), nullable=True),
        StructField("reasoning", StringType(), nullable=True),
        StructField("total_latency_ms", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=False),
    ]
)

# ── Windowed velocity aggregation output ──────────────────────────────────────
VELOCITY_WINDOW_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("txn_count", LongType(), nullable=True),
        StructField("amount_sum", DoubleType(), nullable=True),
        StructField("avg_amount", DoubleType(), nullable=True),
        StructField("unique_merchants", LongType(), nullable=True),
        StructField("unique_countries", LongType(), nullable=True),
    ]
)
