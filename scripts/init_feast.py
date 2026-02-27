"""
Feast initialisation script.
Run once at startup to:
  1. Create sample offline Parquet data files for each feature view.
  2. Run `feast apply` so the registry.db is written.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_REPO = Path(os.environ.get("FEAST_REPO_PATH", "/opt/airflow/feature_repo"))
DATA_DIR = FEATURE_REPO / "data"
N_USERS = 500
N_DEVICES = 300
RNG = np.random.default_rng(42)


def _base_timestamps(n: int) -> pd.Series:
    """Generate event_timestamps spread over the last 30 days."""
    now = datetime.now(timezone.utc)
    offsets = RNG.integers(0, 30 * 24 * 3600, size=n)
    return pd.Series([now - timedelta(seconds=int(s)) for s in offsets])


def create_user_transaction_stats() -> None:
    user_ids = [f"user_{RNG.integers(1, 50_000)}" for _ in range(N_USERS)]
    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "event_timestamp": _base_timestamps(N_USERS),
            "txn_count_1h": RNG.integers(0, 20, size=N_USERS).astype("int64"),
            "txn_count_24h": RNG.integers(0, 80, size=N_USERS).astype("int64"),
            "amount_sum_1h": RNG.uniform(0, 2000, size=N_USERS).astype("float32"),
            "amount_sum_24h": RNG.uniform(0, 8000, size=N_USERS).astype("float32"),
            "unique_merchants_1h": RNG.integers(0, 10, size=N_USERS).astype("int64"),
        }
    )
    df.to_parquet(DATA_DIR / "user_transaction_stats.parquet", index=False)
    print(f"  wrote {len(df)} rows → user_transaction_stats.parquet")


def create_user_behavioral_stats() -> None:
    user_ids = [f"user_{RNG.integers(1, 50_000)}" for _ in range(N_USERS)]
    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "event_timestamp": _base_timestamps(N_USERS),
            "avg_txn_amount_30d": RNG.uniform(20, 500, size=N_USERS).astype("float32"),
            "stddev_txn_amount_30d": RNG.uniform(5, 200, size=N_USERS).astype("float32"),
            "amount_deviation_score": RNG.uniform(0, 1, size=N_USERS).astype("float32"),
            "hour_of_day_risk_score": RNG.uniform(0, 1, size=N_USERS).astype("float32"),
            "most_common_mcc": RNG.integers(1000, 9999, size=N_USERS).astype("float32"),
        }
    )
    df.to_parquet(DATA_DIR / "user_behavioral_stats.parquet", index=False)
    print(f"  wrote {len(df)} rows → user_behavioral_stats.parquet")


def create_geo_stats() -> None:
    user_ids = [f"user_{RNG.integers(1, 50_000)}" for _ in range(N_USERS)]
    country_codes = RNG.choice(["US", "GB", "DE", "FR", "IN", "CN", "BR", "AU"], size=N_USERS)
    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "event_timestamp": _base_timestamps(N_USERS),
            "last_txn_latitude": RNG.uniform(-90, 90, size=N_USERS).astype("float32"),
            "last_txn_longitude": RNG.uniform(-180, 180, size=N_USERS).astype("float32"),
            "distance_from_last_txn_km": RNG.uniform(0, 5000, size=N_USERS).astype("float32"),
            "is_new_country": RNG.choice([True, False], p=[0.15, 0.85], size=N_USERS),
            "is_new_city": RNG.choice([True, False], p=[0.25, 0.75], size=N_USERS),
            "last_country_code": country_codes,
        }
    )
    df.to_parquet(DATA_DIR / "geo_stats.parquet", index=False)
    print(f"  wrote {len(df)} rows → geo_stats.parquet")


def create_device_network_stats() -> None:
    device_fps = [f"device_{RNG.integers(1, 100_000):06d}" for _ in range(N_DEVICES)]
    asns = [f"AS{RNG.integers(100, 99999)}" for _ in range(N_DEVICES)]
    df = pd.DataFrame(
        {
            "device_fingerprint": device_fps,
            "event_timestamp": _base_timestamps(N_DEVICES),
            "ip_fraud_score": RNG.uniform(0, 1, size=N_DEVICES).astype("float32"),
            "device_seen_before": RNG.choice([True, False], p=[0.8, 0.2], size=N_DEVICES),
            "linked_fraud_accounts": RNG.integers(0, 5, size=N_DEVICES).astype("int64"),
            "is_vpn": RNG.choice([True, False], p=[0.05, 0.95], size=N_DEVICES),
            "is_tor": RNG.choice([True, False], p=[0.01, 0.99], size=N_DEVICES),
            "asn": asns,
        }
    )
    df.to_parquet(DATA_DIR / "device_network_stats.parquet", index=False)
    print(f"  wrote {len(df)} rows → device_network_stats.parquet")


def run_feast_apply() -> None:
    print("\nRunning feast apply...")
    result = subprocess.run(
        ["feast", "--chdir", str(FEATURE_REPO), "apply"],
        capture_output=True,
        text=True,
    )
    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        sys.exit(1)
    print("feast apply completed successfully.")


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("Creating offline Parquet data files for Feast feature views...")
    create_user_transaction_stats()
    create_user_behavioral_stats()
    create_geo_stats()
    create_device_network_stats()
    print("\nAll Parquet files created.")
    run_feast_apply()
    print("\nFeast initialisation complete.")
