"""Unit tests for the supervisor aggregator."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from agents.supervisor.aggregator import aggregate
from shared.utils.models import AgentType, AgentVerdict, RiskLevel


def _make_verdict(agent: AgentType, score: float) -> AgentVerdict:
    txn_id = uuid4()
    return AgentVerdict(
        transaction_id=txn_id,
        agent_type=agent,
        risk_score=score,
        risk_level=RiskLevel.HIGH if score >= 0.5 else RiskLevel.LOW,
        confidence=0.9,
        reasoning="test",
        features_used=[],
        latency_ms=10.0,
    )


def _make_verdicts(score: float) -> list[AgentVerdict]:
    txn_id = uuid4()
    return [
        AgentVerdict(
            transaction_id=txn_id,
            agent_type=a,
            risk_score=score,
            risk_level=RiskLevel.HIGH if score >= 0.5 else RiskLevel.LOW,
            confidence=0.9,
            reasoning="test",
            features_used=[],
            latency_ms=10.0,
        )
        for a in [AgentType.VELOCITY, AgentType.GEOLOCATION, AgentType.BEHAVIORAL, AgentType.NETWORK]
    ]


def test_approve_low_risk():
    verdicts = _make_verdicts(0.1)
    decision = aggregate(verdicts)
    assert decision.action == "approve"


def test_decline_high_risk():
    verdicts = _make_verdicts(0.9)
    decision = aggregate(verdicts)
    assert decision.action == "decline"


def test_review_medium_risk():
    verdicts = _make_verdicts(0.55)
    decision = aggregate(verdicts)
    assert decision.action == "review"


def test_empty_verdicts_raises():
    with pytest.raises(ValueError):
        aggregate([])


def test_score_clamped_to_one():
    verdicts = _make_verdicts(1.0)
    decision = aggregate(verdicts)
    assert decision.final_risk_score <= 1.0
