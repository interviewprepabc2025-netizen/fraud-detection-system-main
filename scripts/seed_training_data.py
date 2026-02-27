"""
Seed the training_records PostgreSQL table with 2 000 synthetic labelled rows.
Fraudulent records (label=1) have clearly different feature distributions so
XGBoost can learn a separable boundary and achieve AUC > 0.90.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg2

ROWS = 2_000
FRAUD_RATE = 0.08   # 8 % fraud so there are ~160 positive examples
RNG = np.random.default_rng(42)

DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'fraud_detection')} "
    f"user={os.environ.get('POSTGRES_USER', 'fraud_user')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'changeme_in_production')}"
)


def _wait_for_postgres(max_tries: int = 30) -> psycopg2.extensions.connection:
    for attempt in range(1, max_tries + 1):
        try:
            conn = psycopg2.connect(DSN)
            print(f"Connected to PostgreSQL (attempt {attempt})")
            return conn
        except psycopg2.OperationalError as exc:
            print(f"Attempt {attempt}/{max_tries}: {exc}")
            time.sleep(3)
    raise RuntimeError("Could not connect to PostgreSQL after retries")


def _make_features(is_fraud: bool) -> dict:
    """Return a numeric feature dict with class-separated distributions."""
    if is_fraud:
        return {
            "amount":                float(np.clip(RNG.normal(650, 250), 50, 5000)),
            "hour_of_day":           int(RNG.choice([1, 2, 3, 23, 0])),
            "tx_count_1h":           int(RNG.integers(6, 20)),
            "tx_count_24h":          int(RNG.integers(15, 50)),
            "amt_sum_1h":            float(np.clip(RNG.normal(2000, 500), 100, 50000)),
            "amt_sum_24h":           float(np.clip(RNG.normal(6000, 1000), 200, 100000)),
            "unique_merchants_1h":   int(RNG.integers(3, 10)),
            "is_foreign":            int(RNG.choice([0, 1], p=[0.15, 0.85])),
            "geo_risk":              float(RNG.uniform(0.6, 1.0)),
            "behavioral_deviation":  float(RNG.uniform(0.5, 1.0)),
            "network_risk":          float(RNG.uniform(0.5, 1.0)),
            "velocity_risk":         float(RNG.uniform(0.6, 1.0)),
        }
    else:
        return {
            "amount":                float(np.clip(RNG.normal(150, 80), 1, 2000)),
            "hour_of_day":           int(RNG.integers(8, 22)),
            "tx_count_1h":           int(RNG.integers(0, 4)),
            "tx_count_24h":          int(RNG.integers(1, 10)),
            "amt_sum_1h":            float(np.clip(RNG.normal(300, 100), 0, 10000)),
            "amt_sum_24h":           float(np.clip(RNG.normal(900, 300), 0, 30000)),
            "unique_merchants_1h":   int(RNG.integers(0, 3)),
            "is_foreign":            int(RNG.choice([0, 1], p=[0.92, 0.08])),
            "geo_risk":              float(RNG.uniform(0.0, 0.3)),
            "behavioral_deviation":  float(RNG.uniform(0.0, 0.3)),
            "network_risk":          float(RNG.uniform(0.0, 0.2)),
            "velocity_risk":         float(RNG.uniform(0.0, 0.3)),
        }


def main() -> None:
    conn = _wait_for_postgres()
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    labels = RNG.choice([0, 1], size=ROWS, p=[1 - FRAUD_RATE, FRAUD_RATE])
    sources = ["rule_engine", "manual_review", "model_v1", "chargeback"]

    rows = []
    for i, label in enumerate(labels):
        is_fraud = bool(label)
        offset = timedelta(hours=RNG.integers(0, 24 * 29).item())
        rows.append((
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            int(label),
            RNG.choice(sources),
            json.dumps(_make_features(is_fraud)),
            (now - offset).isoformat(),
        ))

    cur.executemany(
        """
        INSERT INTO training_records
            (record_id, transaction_id, label, label_source, features, created_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (record_id) DO NOTHING
        """,
        rows,
    )
    conn.commit()
    fraud_count = int(labels.sum())
    print(
        f"Inserted {ROWS} training records "
        f"({fraud_count} fraud / {ROWS - fraud_count} legit) "
        f"into training_records."
    )
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
