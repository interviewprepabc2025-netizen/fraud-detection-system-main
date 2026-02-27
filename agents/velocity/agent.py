"""Velocity Agent — detects transaction frequency anomalies."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from shared.utils.logging_utils import get_logger
from shared.utils.metrics import agent_latency, transactions_flagged, transactions_processed
from shared.utils.models import AgentType, AgentVerdict, EnrichedTransaction, RiskLevel

logger = get_logger(__name__)

AGENT_TYPE = AgentType.VELOCITY


def _compute_risk(txn: EnrichedTransaction) -> tuple[float, str, list[str]]:
    """Return (risk_score, reasoning, features_used)."""
    score = 0.0
    signals: list[str] = []
    features: list[str] = ["txn_count_1h", "txn_count_24h", "amount_sum_1h", "amount_sum_24h"]

    if txn.txn_count_1h > 10:
        score += 0.30
        signals.append(f"High hourly velocity: {txn.txn_count_1h} txns/hour")

    if txn.txn_count_24h > 50:
        score += 0.15
        signals.append(f"High daily velocity: {txn.txn_count_24h} txns/24h")

    if txn.avg_txn_amount_30d and txn.avg_txn_amount_30d > Decimal("0"):
        hourly_avg = txn.avg_txn_amount_30d * 24  # rough estimate
        if txn.amount_sum_1h > hourly_avg * 3:
            score += 0.25
            signals.append(
                f"Hourly spend {txn.amount_sum_1h} is >3x average ({hourly_avg:.2f})"
            )

    score = min(score, 1.0)

    if score >= 0.75:
        risk_level = RiskLevel.CRITICAL
    elif score >= 0.5:
        risk_level = RiskLevel.HIGH
    elif score >= 0.25:
        risk_level = RiskLevel.MEDIUM
    else:
        risk_level = RiskLevel.LOW

    reasoning = "; ".join(signals) if signals else "No velocity anomalies detected."
    return score, risk_level, reasoning, features


def evaluate(payload: dict[str, Any]) -> AgentVerdict:
    """Main entry point: accept a raw dict, return an AgentVerdict."""
    start = time.perf_counter()
    txn = EnrichedTransaction.model_validate(payload)

    score, risk_level, reasoning, features = _compute_risk(txn)
    latency_ms = (time.perf_counter() - start) * 1000

    transactions_processed.labels(agent_type=AGENT_TYPE.value).inc()
    if score >= 0.5:
        transactions_flagged.labels(
            agent_type=AGENT_TYPE.value, risk_level=risk_level.value
        ).inc()
    agent_latency.labels(agent_type=AGENT_TYPE.value).observe(latency_ms / 1000)

    verdict = AgentVerdict(
        transaction_id=txn.transaction_id,
        agent_type=AGENT_TYPE,
        risk_score=score,
        risk_level=risk_level,
        confidence=0.85,
        reasoning=reasoning,
        features_used=features,
        latency_ms=latency_ms,
    )

    logger.info(
        "velocity_verdict",
        transaction_id=str(txn.transaction_id),
        risk_score=score,
        latency_ms=round(latency_ms, 2),
    )
    return verdict
