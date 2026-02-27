"""Feast feature view: per-user geolocation history."""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Bool, Float32, String

user = Entity(name="user_id", value_type=ValueType.STRING, description="Payment user ID")

_batch_source = FileSource(
    path="data/geo_stats.parquet",
    timestamp_field="event_timestamp",
)

geo_push_source = PushSource(
    name="geo_stats_push",
    batch_source=_batch_source,
)

geo_stats = FeatureView(
    name="geo_stats",
    entities=[user],
    ttl=timedelta(days=7),
    schema=[
        Field(name="last_txn_latitude", dtype=Float32),
        Field(name="last_txn_longitude", dtype=Float32),
        Field(name="distance_from_last_txn_km", dtype=Float32),
        Field(name="is_new_country", dtype=Bool),
        Field(name="is_new_city", dtype=Bool),
        Field(name="last_country_code", dtype=String),
    ],
    source=geo_push_source,
    online=True,
    tags={"team": "fraud", "domain": "geolocation"},
)
