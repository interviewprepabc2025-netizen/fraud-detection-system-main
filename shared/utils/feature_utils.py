"""Feast online feature store helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from feast import FeatureStore

from shared.utils.logging_utils import get_logger
from shared.utils.settings import settings

logger = get_logger(__name__)

_store: FeatureStore | None = None


def get_store() -> FeatureStore:
    """Return a singleton FeatureStore instance."""
    global _store
    if _store is None:
        _store = FeatureStore(repo_path=settings.feast_registry_path)
    return _store


def get_online_features(
    feature_refs: list[str],
    entity_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    """Fetch features from the online store for given entity rows.

    Args:
        feature_refs: e.g. ["user_stats:txn_count_1h", "user_stats:amount_sum_24h"]
        entity_rows:  e.g. [{"user_id": "u123", "merchant_id": "m456"}]

    Returns:
        DataFrame with one row per entity and one column per feature.
    """
    store = get_store()
    response = store.get_online_features(
        features=feature_refs,
        entity_rows=entity_rows,
    )
    return response.to_df()


def push_features(
    push_source_name: str,
    df: pd.DataFrame,
) -> None:
    """Push a DataFrame to a Feast push source (stream ingestion)."""
    store = get_store()
    store.push(push_source_name, df, to="online")
    logger.debug("features_pushed", source=push_source_name, rows=len(df))
