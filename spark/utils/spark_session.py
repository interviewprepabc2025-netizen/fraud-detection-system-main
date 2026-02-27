"""
SparkSession factory with Delta Lake + S3A (MinIO) configuration.
All Spark jobs import get_spark() to obtain a consistently configured session.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession


def get_spark(app_name: str, master: Optional[str] = None) -> SparkSession:
    """
    Build (or retrieve) a SparkSession configured for:
      - Delta Lake (DeltaSparkSessionExtension + DeltaCatalog)
      - S3A file system pointing to MinIO
      - Adaptive Query Execution
      - Kryo serialiser
    """
    master_url = master or os.environ.get("SPARK_MASTER_URL", "local[4]")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        # Ensure Python version matches container
        .config("spark.pyspark.python", "python3")
        .config("spark.pyspark.driver.python", "python3")
        # ── Delta Lake ────────────────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── S3A / MinIO ───────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Performance ───────────────────────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # ── Streaming ─────────────────────────────────────────────────────────
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )

    return builder.getOrCreate()
