"""Supervisor Agent — collects verdicts and emits final decisions."""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from typing import Any
from uuid import UUID

import uvicorn
from fastapi import FastAPI

from shared.utils.kafka_utils import consume_loop, make_consumer, make_producer, produce
from shared.utils.logging_utils import configure_logging, get_logger
from shared.utils.metrics import e2e_latency, start_metrics_server, transactions_processed
from shared.utils.models import AgentType, AgentVerdict, TrainingRecord
from shared.utils.settings import settings

from agents.supervisor.aggregator import aggregate
from agents.supervisor.llm_explainer import explain

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Supervisor Agent", version="1.0.0")

# In-memory buffer: transaction_id → list of verdicts received so far
_verdict_buffer: dict[UUID, list[AgentVerdict]] = defaultdict(list)
_EXPECTED_AGENTS = 4  # velocity, geolocation, behavioral, network
_VERDICT_TIMEOUT_S = 0.15  # 150 ms to collect all verdicts


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "agent": "supervisor"}


def _handle_verdict(payload: dict[str, Any], producer: Any) -> None:
    # Skip supervisor's own decisions if they appear on this topic
    if payload.get("agent_type") not in {a.value for a in AgentType if a != AgentType.SUPERVISOR}:
        return

    verdict = AgentVerdict.model_validate(payload)
    txn_id = verdict.transaction_id
    _verdict_buffer[txn_id].append(verdict)

    if len(_verdict_buffer[txn_id]) < _EXPECTED_AGENTS:
        return

    # All verdicts received — aggregate
    verdicts = _verdict_buffer.pop(txn_id)
    weighted_score = sum(v.risk_score for v in verdicts) / len(verdicts)

    llm_text = ""
    if 0.40 <= weighted_score < 0.75:
        llm_text = explain(
            verdicts,
            weighted_score,
            {"transaction_id": str(txn_id)},
        )

    decision = aggregate(
        verdicts,
        fraud_score_threshold=settings.fraud_score_threshold,
        llm_reasoning=llm_text,
    )

    e2e_latency.observe(decision.total_latency_ms / 1000)
    transactions_processed.labels(agent_type="supervisor").inc()

    produce(
        producer,
        topic=settings.topic_agent_verdicts,
        value=decision.model_dump(mode="json"),
        key=str(decision.transaction_id),
    )

    # Write training record
    # Note: label will be updated later via chargeback / review pipeline
    training_rec = TrainingRecord(
        transaction_id=decision.transaction_id,  # type: ignore[call-arg]
        label=1 if decision.action == "decline" else 0,
        label_source="rule_engine",
    )
    produce(
        producer,
        topic=settings.topic_training_data,
        value=training_rec.model_dump(mode="json"),
        key=str(decision.transaction_id),
    )

    logger.info(
        "supervisor_decision",
        transaction_id=str(decision.transaction_id),
        action=decision.action,
        final_score=decision.final_risk_score,
        total_latency_ms=round(decision.total_latency_ms, 2),
    )


def _kafka_loop() -> None:
    producer = make_producer()
    consumer = make_consumer(group_id="supervisor-group")

    def handle(payload: dict) -> None:
        _handle_verdict(payload, producer)

    consume_loop(consumer, [settings.topic_agent_verdicts], handle)


def main() -> None:
    start_metrics_server()
    t = threading.Thread(target=_kafka_loop, daemon=True)
    t.start()
    logger.info("supervisor_agent_starting", port=int(os.getenv("AGENT_PORT", "8005")))
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("AGENT_PORT", "8005")))


if __name__ == "__main__":
    main()
