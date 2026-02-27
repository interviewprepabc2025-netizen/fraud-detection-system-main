"""Network Agent — IP reputation, device graph, and account linkage signals."""

from __future__ import annotations

import time
from typing import Any

from shared.utils.logging_utils import get_logger
from shared.utils.metrics import agent_latency, transactions_flagged, transactions_processed
from shared.utils.models import AgentType, AgentVerdict, EnrichedTransaction, RiskLevel

logger = get_logger(__name__)

AGENT_TYPE = AgentType.NETWORK


def _compute_risk(txn: EnrichedTransaction) -> tuple[float, RiskLevel, str, list[str]]:
    score = 0.0
    signals: list[str] = []
    features = [
        "ip_fraud_score",
        "device_seen_before",
        "linked_fraud_accounts",
    ]

    # IP fraud score
    if txn.ip_fraud_score is not None:
        if txn.ip_fraud_score > 0.8:
            score += 0.40
            signals.append(f"High-risk IP (score: {txn.ip_fraud_score:.2f})")
        elif txn.ip_fraud_score > 0.5:
            score += 0.20
            signals.append(f"Suspicious IP (score: {txn.ip_fraud_score:.2f})")

    # Device seen before
    if not txn.device_seen_before:
        score += 0.15
        signals.append("First-time device fingerprint for this user")

    # Linked fraud accounts
    if txn.linked_fraud_accounts > 3:
        score += 0.40
        signals.append(
            f"Device/IP linked to {txn.linked_fraud_accounts} known-fraud accounts"
        )
    elif txn.linked_fraud_accounts > 0:
        score += 0.20
        signals.append(
            f"Device/IP linked to {txn.linked_fraud_accounts} suspicious account(s)"
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

    reasoning = "; ".join(signals) if signals else "No network anomalies detected."
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
        confidence=0.88,
        reasoning=reasoning,
        features_used=features,
        latency_ms=latency_ms,
    )

    logger.info(
        "network_verdict",
        transaction_id=str(txn.transaction_id),
        risk_score=score,
        latency_ms=round(latency_ms, 2),
    )
    return verdict
