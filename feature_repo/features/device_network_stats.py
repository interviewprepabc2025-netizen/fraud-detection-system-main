"""Feast feature view: device and network reputation features."""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Bool, Float32, Int64, String

device = Entity(
    name="device_fingerprint",
    value_type=ValueType.STRING,
    description="Hashed device fingerprint",
)

_batch_source = FileSource(
    path="data/device_network_stats.parquet",
    timestamp_field="event_timestamp",
)

network_push_source = PushSource(
    name="device_network_stats_push",
    batch_source=_batch_source,
)

device_network_stats = FeatureView(
    name="device_network_stats",
    entities=[device],
    ttl=timedelta(days=7),
    schema=[
        Field(name="ip_fraud_score", dtype=Float32),
        Field(name="device_seen_before", dtype=Bool),
        Field(name="linked_fraud_accounts", dtype=Int64),
        Field(name="is_vpn", dtype=Bool),
        Field(name="is_tor", dtype=Bool),
        Field(name="asn", dtype=String),
    ],
    source=network_push_source,
    online=True,
    tags={"team": "fraud", "domain": "network"},
)
