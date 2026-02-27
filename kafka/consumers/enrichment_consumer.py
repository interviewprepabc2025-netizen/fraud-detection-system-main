"""Enrichment consumer — joins raw transactions with Feast online features."""

from __future__ import annotations

from typing import Any

from shared.utils.feature_utils import get_online_features
from shared.utils.kafka_utils import consume_loop, make_consumer, make_producer, produce
from shared.utils.logging_utils import configure_logging, get_logger
from shared.utils.models import EnrichedTransaction, Transaction
from shared.utils.settings import settings

configure_logging()
logger = get_logger(__name__)

FEATURE_REFS = [
    "user_transaction_stats:txn_count_1h",
    "user_transaction_stats:txn_count_24h",
    "user_transaction_stats:amount_sum_1h",
    "user_transaction_stats:amount_sum_24h",
    "user_behavioral_stats:avg_txn_amount_30d",
    "user_behavioral_stats:amount_deviation_score",
    "user_behavioral_stats:hour_of_day_risk_score",
    "device_network_stats:ip_fraud_score",
    "device_network_stats:device_seen_before",
    "device_network_stats:linked_fraud_accounts",
    "geo_stats:distance_from_last_txn_km",
    "geo_stats:is_new_country",
    "geo_stats:is_new_city",
]


def _enrich(payload: dict[str, Any]) -> None:
    producer = _enrich.__dict__.setdefault("_producer", make_producer())

    txn = Transaction.model_validate(payload)

    entity_rows = [
        {
            "user_id": txn.user_id,
            "device_fingerprint": txn.device_fingerprint,
            "ip_address": txn.ip_address,
        }
    ]

    try:
        features_df = get_online_features(FEATURE_REFS, entity_rows)
        row = features_df.iloc[0].to_dict()
    except Exception as exc:
        logger.warning("feature_fetch_failed", error=str(exc), transaction_id=str(txn.transaction_id))
        row = {}

    enriched = EnrichedTransaction(
        **txn.model_dump(),
        txn_count_1h=int(row.get("txn_count_1h") or 0),
        txn_count_24h=int(row.get("txn_count_24h") or 0),
        amount_sum_1h=row.get("amount_sum_1h") or 0,
        amount_sum_24h=row.get("amount_sum_24h") or 0,
        avg_txn_amount_30d=row.get("avg_txn_amount_30d"),
        amount_deviation_score=row.get("amount_deviation_score"),
        hour_of_day_risk_score=row.get("hour_of_day_risk_score"),
        ip_fraud_score=row.get("ip_fraud_score"),
        device_seen_before=bool(row.get("device_seen_before", False)),
        linked_fraud_accounts=int(row.get("linked_fraud_accounts") or 0),
        distance_from_last_txn_km=row.get("distance_from_last_txn_km"),
        is_new_country=bool(row.get("is_new_country", False)),
        is_new_city=bool(row.get("is_new_city", False)),
    )

    produce(
        producer,
        topic=settings.topic_enriched_transactions,
        value=enriched.model_dump(mode="json"),
        key=str(enriched.user_id),
    )


def main() -> None:
    consumer = make_consumer(group_id="enrichment-group")
    consume_loop(consumer, [settings.topic_raw_transactions], _enrich)


if __name__ == "__main__":
    main()
