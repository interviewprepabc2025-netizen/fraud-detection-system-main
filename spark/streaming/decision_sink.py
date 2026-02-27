"""
Spark Structured Streaming — Fraud Decision Sink (Gold Layer)
=============================================================

Reads supervisor decisions from the Kafka `agent-verdicts` topic and writes
them to the Delta Lake Gold layer, replacing the plain-Python storage_consumer.

DE concepts:
  • Structured Streaming with foreachBatch
  • Delta Lake upsert (MERGE) for idempotent exactly-once writes
  • Data partitioning by decision date + risk_level
  • Derived metrics column (risk_bucket) computed in Spark
"""

import json
import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from spark.streaming.schemas import SUPERVISOR_DECISION_SCHEMA
from spark.utils.delta_utils import CHECKPOINTS, PATHS, upsert_into_delta
from spark.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("fraud.decision_sink")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VERDICTS_TOPIC = os.environ.get("TOPIC_AGENT_VERDICTS", "agent-verdicts")


def _add_derived_columns(df: DataFrame) -> DataFrame:
    """Add analytical columns computed from the raw decision fields."""
    risk_bucket = (
        F.when(F.col("final_risk_score") >= 0.75, F.lit("high"))
        .when(F.col("final_risk_score") >= 0.40, F.lit("medium"))
        .otherwise(F.lit("low"))
    )
    return df.withColumn("risk_bucket", risk_bucket).withColumn(
        "decision_date", F.to_date(F.col("event_time"))
    )


def process_decisions_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Persist supervisor decisions to Delta Gold.

    Steps:
      1. Filter: only process messages that contain `final_risk_score`
         (the supervisor publishes a superset message type; individual agent
         verdicts also go to the same topic but lack this field).
      2. Add derived/analytical columns.
      3. Delta MERGE (upsert) — idempotent re-processing if checkpoint restarts.
      4. Partition by decision_date + risk_level for efficient downstream reads.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession

    supervisor_df = batch_df.filter(F.col("final_risk_score").isNotNull())
    if supervisor_df.isEmpty():
        return

    enriched = _add_derived_columns(supervisor_df)
    count = enriched.count()
    log.info("Batch %d → %d supervisor decisions", batch_id, count)

    # Upsert into Gold Delta (idempotent; safe for at-least-once Kafka delivery)
    upsert_into_delta(
        spark=spark,
        source_df=enriched,
        target_path=PATHS["gold_decisions"],
        merge_key="decision_id",
    )


def main() -> None:
    spark = get_spark("FraudDetection-DecisionSink")
    spark.sparkContext.setLogLevel("WARN")

    log.info("Starting Fraud Detection Decision Sink")
    log.info("Kafka source : %s → %s", KAFKA_SERVERS, KAFKA_VERDICTS_TOPIC)
    log.info("Delta Gold   : %s", PATHS["gold_decisions"])

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_VERDICTS_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    decisions_stream = raw_stream.select(
        F.from_json(
            F.col("value").cast("string"), SUPERVISOR_DECISION_SCHEMA
        ).alias("data")
    ).select(
        "data.*",
        F.to_timestamp(F.col("data.timestamp")).alias("event_time"),
    )

    sink_query = (
        decisions_stream.writeStream.foreachBatch(process_decisions_batch)
        .option("checkpointLocation", CHECKPOINTS["decision_sink"])
        .trigger(processingTime="2 seconds")
        .start()
    )

    log.info("Decision sink streaming query started. Awaiting termination...")
    sink_query.awaitTermination()


if __name__ == "__main__":
    main()
