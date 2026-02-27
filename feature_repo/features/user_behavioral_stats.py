"""Feast feature view: per-user behavioural profile."""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float32

user = Entity(name="user_id", value_type=ValueType.STRING, description="Payment user ID")

_batch_source = FileSource(
    path="data/user_behavioral_stats.parquet",
    timestamp_field="event_timestamp",
)

behavioral_push_source = PushSource(
    name="user_behavioral_stats_push",
    batch_source=_batch_source,
)

user_behavioral_stats = FeatureView(
    name="user_behavioral_stats",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="avg_txn_amount_30d", dtype=Float32),
        Field(name="stddev_txn_amount_30d", dtype=Float32),
        Field(name="amount_deviation_score", dtype=Float32),
        Field(name="hour_of_day_risk_score", dtype=Float32),
        Field(name="most_common_mcc", dtype=Float32),
    ],
    source=behavioral_push_source,
    online=True,
    tags={"team": "fraud", "domain": "behavioral"},
)
