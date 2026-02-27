"""Prometheus metrics helpers shared across agent services."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from shared.utils.settings import settings

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0)

# ── Counters ──────────────────────────────────────────────────────────────────
transactions_processed = Counter(
    "fraud_transactions_processed_total",
    "Total transactions processed by an agent",
    ["agent_type"],
)

transactions_flagged = Counter(
    "fraud_transactions_flagged_total",
    "Total transactions flagged as fraudulent",
    ["agent_type", "risk_level"],
)

# ── Histograms ────────────────────────────────────────────────────────────────
agent_latency = Histogram(
    "fraud_agent_latency_seconds",
    "Per-agent decision latency",
    ["agent_type"],
    buckets=LATENCY_BUCKETS,
)

e2e_latency = Histogram(
    "fraud_e2e_latency_seconds",
    "End-to-end pipeline latency from ingest to supervisor decision",
    buckets=LATENCY_BUCKETS,
)

# ── Gauges ────────────────────────────────────────────────────────────────────
kafka_consumer_lag = Gauge(
    "fraud_kafka_consumer_lag",
    "Kafka consumer group lag",
    ["topic", "partition"],
)


def start_metrics_server(port: int | None = None) -> None:
    """Start Prometheus HTTP metrics endpoint."""
    p = port or settings.prometheus_port
    start_http_server(p)
