"""Unit tests for the velocity agent."""

from __future__ import annotations

import pytest

from agents.velocity.agent import evaluate
from shared.utils.models import AgentType, RiskLevel


def test_low_risk_transaction(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.agent_type == AgentType.VELOCITY
    assert verdict.risk_score < 0.5
    assert verdict.risk_level in (RiskLevel.LOW, RiskLevel.MEDIUM)
    assert verdict.latency_ms > 0


def test_high_velocity_flags_risk(high_risk_transaction):
    verdict = evaluate(high_risk_transaction.model_dump(mode="json"))
    assert verdict.risk_score >= 0.25
    assert "velocity" in verdict.reasoning.lower() or verdict.risk_score > 0


def test_verdict_score_bounds(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert 0.0 <= verdict.risk_score <= 1.0
    assert 0.0 <= verdict.confidence <= 1.0
