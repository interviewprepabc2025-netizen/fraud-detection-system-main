"""Feast feature view: per-user transaction velocity stats."""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float32, Int64

user = Entity(name="user_id", value_type=ValueType.STRING, description="Payment user ID")

_batch_source = FileSource(
    path="data/user_transaction_stats.parquet",
    timestamp_field="event_timestamp",
)

user_txn_push_source = PushSource(
    name="user_transaction_stats_push",
    batch_source=_batch_source,
)

user_transaction_stats = FeatureView(
    name="user_transaction_stats",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="txn_count_1h", dtype=Int64),
        Field(name="txn_count_24h", dtype=Int64),
        Field(name="amount_sum_1h", dtype=Float32),
        Field(name="amount_sum_24h", dtype=Float32),
        Field(name="unique_merchants_1h", dtype=Int64),
    ],
    source=user_txn_push_source,
    online=True,
    tags={"team": "fraud", "domain": "velocity"},
)
