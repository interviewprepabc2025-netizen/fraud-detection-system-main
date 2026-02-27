FROM apache/spark:3.5.3

USER root

# Install netcat (dep-wait) and curl (JAR downloads)
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd curl python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ── Pre-download JARs into Spark's jars dir ───────────────────────────────────
# Putting them here means workers automatically have them; no --packages needed.
ENV JARS_DIR=/opt/spark/jars

# Kafka Structured Streaming
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" \
      -o "${JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.3.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      -o "${JARS_DIR}/kafka-clients-3.4.1.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" \
      -o "${JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
      -o "${JARS_DIR}/commons-pool2-2.11.1.jar"

# Delta Lake 3.2 (compatible with Spark 3.5.x)
RUN curl -fSL "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
      -o "${JARS_DIR}/delta-spark_2.12-3.2.0.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
      -o "${JARS_DIR}/delta-storage-3.2.0.jar"

# Hadoop S3A + AWS SDK (for MinIO access)
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
      -o "${JARS_DIR}/hadoop-aws-3.3.4.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
      -o "${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar"

# ── Python packages ────────────────────────────────────────────────────────────
# Note: confluent-kafka is NOT installed here — all Kafka I/O in the Spark jobs
# goes through the spark-sql-kafka JAR (structured streaming connector).
# confluent-kafka (C extension) requires librdkafka-dev headers and is only
# needed in the agent containers (python:3.11-slim based).
RUN pip3 install --no-cache-dir \
    "pyspark==3.5.3" \
    "delta-spark==3.2.0" \
    "redis>=4.6,<6" \
    "python-dotenv>=1.0" \
    "structlog>=24" \
    "pandas>=1.5,<3" \
    "numpy>=1.23,<2"

# ── Application source ────────────────────────────────────────────────────────
WORKDIR /app

COPY spark/        ./spark/
COPY shared/       ./shared/

ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

COPY spark/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# apache/spark image uses uid 185 (spark user)
USER 185

ENTRYPOINT ["/entrypoint.sh"]
