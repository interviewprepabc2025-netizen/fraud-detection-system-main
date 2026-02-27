#!/bin/bash
# ---------------------------------------------------------------------------
# Spark multi-purpose entrypoint (apache/spark:3.5.3 base)
#
# Modes (controlled by SPARK_DAEMON env var):
#   SPARK_DAEMON=master   → start Spark standalone master
#   SPARK_DAEMON=worker   → start Spark standalone worker
#   JOB_SCRIPT is set     → wait for deps, then run spark-submit
# ---------------------------------------------------------------------------
set -e

SPARK_BIN=/opt/spark/bin

# ── Cluster daemon modes ───────────────────────────────────────────────────────
if [[ "${SPARK_DAEMON}" == "master" ]]; then
  echo "==> [entrypoint] Starting Spark Master..."
  exec "${SPARK_BIN}/spark-class" org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
fi

if [[ "${SPARK_DAEMON}" == "worker" ]]; then
  echo "==> [entrypoint] Starting Spark Worker → ${SPARK_MASTER_URL}"
  # Wait for master
  until nc -z spark-master 7077 2>/dev/null; do sleep 2; done
  exec "${SPARK_BIN}/spark-class" org.apache.spark.deploy.worker.Worker \
    --cores "${SPARK_WORKER_CORES:-2}" \
    --memory "${SPARK_WORKER_MEMORY:-2g}" \
    "${SPARK_MASTER_URL:-spark://spark-master:7077}"
fi

# ── Streaming / batch job mode ────────────────────────────────────────────────
JOB_SCRIPT="${JOB_SCRIPT:?JOB_SCRIPT must be set for job submission mode}"
SPARK_MASTER="${SPARK_MASTER_URL:-local[4]}"
MINIO_EP="${MINIO_ENDPOINT:-http://minio:9000}"

echo "==> [entrypoint] Job      : ${JOB_SCRIPT}"
echo "==> [entrypoint] Spark    : ${SPARK_MASTER}"
echo "==> [entrypoint] MinIO    : ${MINIO_EP}"

# ── Wait for Kafka ─────────────────────────────────────────────────────────────
if [[ -n "${KAFKA_BOOTSTRAP_SERVERS}" ]]; then
  KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS%%:*}"
  KAFKA_PORT="${KAFKA_BOOTSTRAP_SERVERS##*:}"
  echo "==> Waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}..."
  until nc -z "${KAFKA_HOST}" "${KAFKA_PORT}" 2>/dev/null; do sleep 3; done
  echo "==> Kafka ready."
fi

# ── Wait for Redis ────────────────────────────────────────────────────────────
if [[ -n "${REDIS_HOST}" ]]; then
  echo "==> Waiting for Redis at ${REDIS_HOST}:${REDIS_PORT:-6379}..."
  until nc -z "${REDIS_HOST}" "${REDIS_PORT:-6379}" 2>/dev/null; do sleep 3; done
  echo "==> Redis ready."
fi

# ── Wait for Spark master (if not local mode) ─────────────────────────────────
if [[ "${SPARK_MASTER}" != local* ]]; then
  echo "==> Waiting for Spark Master at spark-master:7077..."
  until nc -z spark-master 7077 2>/dev/null; do sleep 3; done
  echo "==> Spark Master ready."
fi

# ── Run spark-submit ───────────────────────────────────────────────────────────
exec "${SPARK_BIN}/spark-submit" \
  --master "${SPARK_MASTER}" \
  --conf "spark.cores.max=2" \
  --conf "spark.executor.cores=1" \
  --conf "spark.executor.memory=512m" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_EP}" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  "${JOB_SCRIPT}"
