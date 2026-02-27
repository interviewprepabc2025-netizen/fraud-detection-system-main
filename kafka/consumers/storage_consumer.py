"""Storage consumer — persists supervisor decisions to PostgreSQL."""

from __future__ import annotations

import json
from typing import Any

import sqlalchemy as sa
from sqlalchemy import create_engine, text

from shared.utils.kafka_utils import consume_loop, make_consumer
from shared.utils.logging_utils import configure_logging, get_logger
from shared.utils.settings import settings

configure_logging()
logger = get_logger(__name__)

_engine: sa.Engine | None = None

DDL = """
CREATE TABLE IF NOT EXISTS fraud_decisions (
    decision_id     UUID PRIMARY KEY,
    transaction_id  UUID NOT NULL,
    final_risk_score FLOAT NOT NULL,
    risk_level      VARCHAR(16) NOT NULL,
    action          VARCHAR(16) NOT NULL,
    reasoning       TEXT,
    total_latency_ms FLOAT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fd_transaction_id ON fraud_decisions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_fd_action ON fraud_decisions(action);
"""


def _get_engine() -> sa.Engine:
    global _engine
    if _engine is None:
        dsn = (
            f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"  # type: ignore[attr-defined]
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"  # type: ignore[attr-defined]
        ) if hasattr(settings, "postgres_user") else "sqlite:///./fraud_decisions.db"
        _engine = create_engine(dsn)
        with _engine.connect() as conn:
            conn.execute(text(DDL))
            conn.commit()
    return _engine


def _persist(payload: dict[str, Any]) -> None:
    # Only handle supervisor decisions (they have a "final_risk_score" key)
    if "final_risk_score" not in payload:
        return

    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO fraud_decisions
                    (decision_id, transaction_id, final_risk_score, risk_level, action, reasoning, total_latency_ms)
                VALUES
                    (:decision_id, :transaction_id, :final_risk_score, :risk_level, :action, :reasoning, :total_latency_ms)
                ON CONFLICT (decision_id) DO NOTHING
                """
            ),
            {
                "decision_id": payload.get("decision_id"),
                "transaction_id": payload.get("transaction_id"),
                "final_risk_score": payload.get("final_risk_score"),
                "risk_level": payload.get("risk_level"),
                "action": payload.get("action"),
                "reasoning": payload.get("reasoning"),
                "total_latency_ms": payload.get("total_latency_ms"),
            },
        )
        conn.commit()

    logger.debug(
        "decision_persisted",
        transaction_id=payload.get("transaction_id"),
        action=payload.get("action"),
    )


def main() -> None:
    consumer = make_consumer(group_id="storage-group")
    consume_loop(consumer, [settings.topic_agent_verdicts], _persist)


if __name__ == "__main__":
    main()
