"""Confluent-Kafka producer/consumer factory helpers."""

from __future__ import annotations

import json
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared.utils.logging_utils import get_logger
from shared.utils.settings import settings

logger = get_logger(__name__)


def _base_config() -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": "PLAINTEXT",
    }


def make_producer(extra: dict[str, Any] | None = None) -> Producer:
    """Return a configured Confluent Kafka Producer."""
    cfg = {
        **_base_config(),
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 200,
        "compression.type": "lz4",
        **(extra or {}),
    }
    return Producer(cfg)


def make_consumer(group_id: str, extra: dict[str, Any] | None = None) -> Consumer:
    """Return a configured Confluent Kafka Consumer.

    Consumer group naming convention: <service>-group
    """
    cfg = {
        **_base_config(),
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        **(extra or {}),
    }
    return Consumer(cfg)


def delivery_report(err: KafkaError | None, msg: Any) -> None:
    """Default delivery callback for producers."""
    if err:
        logger.error("message_delivery_failed", error=str(err), topic=msg.topic())
    else:
        logger.debug(
            "message_delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def produce(
    producer: Producer,
    topic: str,
    value: dict[str, Any],
    key: str | None = None,
    on_delivery: Callable | None = None,
) -> None:
    """Serialise value to JSON and produce to topic."""
    producer.produce(
        topic=topic,
        key=key.encode() if key else None,
        value=json.dumps(value, default=str).encode(),
        on_delivery=on_delivery or delivery_report,
    )
    producer.poll(0)


def consume_loop(
    consumer: Consumer,
    topics: list[str],
    handler: Callable[[dict[str, Any]], None],
    *,
    poll_timeout: float = 1.0,
) -> None:
    """Blocking consume loop — calls handler for each decoded message."""
    consumer.subscribe(topics)
    logger.info("consumer_subscribed", topics=topics)

    try:
        while True:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode())
                handler(payload)
                consumer.commit(asynchronous=False)
            except Exception:
                logger.exception("message_handling_failed", topic=msg.topic())
    finally:
        consumer.close()


def ensure_topics(topics: list[NewTopic]) -> None:
    """Idempotently create Kafka topics via AdminClient."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("topic_created", topic=topic)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.debug("topic_already_exists", topic=topic)
            else:
                logger.error("topic_creation_failed", topic=topic, error=str(exc))
