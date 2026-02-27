"""Behavioral Agent — detects deviations from historical spending patterns."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from shared.utils.logging_utils import get_logger
from shared.utils.metrics import agent_latency, transactions_flagged, transactions_processed
from shared.utils.models import AgentType, AgentVerdict, EnrichedTransaction, RiskLevel

logger = get_logger(__name__)

AGENT_TYPE = AgentType.BEHAVIORAL

# MCCs that are commonly exploited in card-not-present fraud
HIGH_RISK_MCCS: frozenset[str] = frozenset(
    {"5912", "7995", "6051", "4829", "6211", "5999"}
)


def _compute_risk(txn: EnrichedTransaction) -> tuple[float, RiskLevel, str, list[str]]:
    score = 0.0
    signals: list[str] = []
    features = [
        "avg_txn_amount_30d",
        "amount_deviation_score",
        "hour_of_day_risk_score",
        "merchant_category_code",
    ]

    # Amount deviation
    if (
        txn.avg_txn_amount_30d is not None
        and txn.avg_txn_amount_30d > Decimal("0")
        and txn.amount_deviation_score is not None
    ):
        if txn.amount_deviation_score > 3.0:
            score += 0.35
            signals.append(
                f"Amount {txn.amount} is {txn.amount_deviation_score:.1f}σ above 30d average "
                f"({txn.avg_txn_amount_30d})"
            )
        elif txn.amount_deviation_score > 2.0:
            score += 0.15
            signals.append(
                f"Amount {txn.amount} is {txn.amount_deviation_score:.1f}σ above 30d average"
            )

    # Time-of-day risk
    if txn.hour_of_day_risk_score is not None and txn.hour_of_day_risk_score > 0.7:
        score += 0.20
        signals.append(
            f"Unusual transaction hour (risk score: {txn.hour_of_day_risk_score:.2f})"
        )

    # High-risk MCC
    if txn.merchant_category_code in HIGH_RISK_MCCS:
        score += 0.20
        signals.append(f"High-risk merchant category: {txn.merchant_category_code}")

    score = min(score, 1.0)

    if score >= 0.75:
        risk_level = RiskLevel.CRITICAL
    elif score >= 0.5:
        risk_level = RiskLevel.HIGH
    elif score >= 0.25:
        risk_level = RiskLevel.MEDIUM
    else:
        risk_level = RiskLevel.LOW

    reasoning = "; ".join(signals) if signals else "Behaviour consistent with history."
    return score, risk_level, reasoning, features


def evaluate(payload: dict[str, Any]) -> AgentVerdict:
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
        confidence=0.82,
        reasoning=reasoning,
        features_used=features,
        latency_ms=latency_ms,
    )

    logger.info(
        "behavioral_verdict",
        transaction_id=str(txn.transaction_id),
        risk_score=score,
        latency_ms=round(latency_ms, 2),
    )
    return verdict
