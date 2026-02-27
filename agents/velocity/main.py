"""Velocity Agent — service entrypoint."""

from __future__ import annotations

import os
import threading

import uvicorn
from fastapi import FastAPI

from shared.utils.kafka_utils import make_consumer, make_producer, produce, consume_loop
from shared.utils.logging_utils import configure_logging, get_logger
from shared.utils.metrics import start_metrics_server
from shared.utils.settings import settings

from agents.velocity.agent import evaluate

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Velocity Agent", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "agent": "velocity"}


def _kafka_loop() -> None:
    producer = make_producer()
    consumer = make_consumer(group_id="velocity-group")

    def handle(payload: dict) -> None:
        verdict = evaluate(payload)
        produce(
            producer,
            topic=settings.topic_agent_verdicts,
            value=verdict.model_dump(mode="json"),
            key=str(verdict.transaction_id),
        )

    consume_loop(consumer, [settings.topic_enriched_transactions], handle)


def main() -> None:
    start_metrics_server()
    t = threading.Thread(target=_kafka_loop, daemon=True)
    t.start()
    logger.info("velocity_agent_starting", port=int(os.getenv("AGENT_PORT", "8001")))
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("AGENT_PORT", "8001")))


if __name__ == "__main__":
    main()
