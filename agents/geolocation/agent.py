"""Geolocation Agent — detects location-based fraud signals."""

from __future__ import annotations

import time
from math import asin, cos, radians, sin, sqrt
from typing import Any

from shared.utils.logging_utils import get_logger
from shared.utils.metrics import agent_latency, transactions_flagged, transactions_processed
from shared.utils.models import AgentType, AgentVerdict, EnrichedTransaction, RiskLevel

logger = get_logger(__name__)

AGENT_TYPE = AgentType.GEOLOCATION

# Countries associated with elevated fraud rates (illustrative list)
HIGH_RISK_COUNTRIES: frozenset[str] = frozenset()

# Maximum plausible ground-speed (km/h) — supersonic travel is suspicious
MAX_PLAUSIBLE_SPEED_KMH = 900.0


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in km between two coordinates."""
    r = 6_371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return 2 * r * asin(sqrt(a))


def _compute_risk(txn: EnrichedTransaction) -> tuple[float, RiskLevel, str, list[str]]:
    score = 0.0
    signals: list[str] = []
    features = ["distance_from_last_txn_km", "is_new_country", "is_new_city", "country_code"]

    if txn.distance_from_last_txn_km is not None and txn.distance_from_last_txn_km > 500:
        score += 0.40
        signals.append(
            f"Large location jump: {txn.distance_from_last_txn_km:.1f} km from last transaction"
        )

    if txn.is_new_country:
        score += 0.25
        signals.append(f"First transaction in country: {txn.country_code}")

    if txn.is_new_city:
        score += 0.10
        signals.append(f"First transaction in city: {txn.city}")

    if txn.country_code in HIGH_RISK_COUNTRIES:
        score += 0.20
        signals.append(f"High-risk country code: {txn.country_code}")

    score = min(score, 1.0)

    if score >= 0.75:
        risk_level = RiskLevel.CRITICAL
    elif score >= 0.5:
        risk_level = RiskLevel.HIGH
    elif score >= 0.25:
        risk_level = RiskLevel.MEDIUM
    else:
        risk_level = RiskLevel.LOW

    reasoning = "; ".join(signals) if signals else "No geolocation anomalies detected."
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
        confidence=0.80,
        reasoning=reasoning,
        features_used=features,
        latency_ms=latency_ms,
    )

    logger.info(
        "geolocation_verdict",
        transaction_id=str(txn.transaction_id),
        risk_score=score,
        latency_ms=round(latency_ms, 2),
    )
    return verdict
