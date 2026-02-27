"""Unit tests for the network agent."""

from __future__ import annotations

import pytest

from agents.network.agent import evaluate
from shared.utils.models import AgentType, RiskLevel


# ── Clean transaction — no signals ───────────────────────────────────────────

def test_clean_transaction_low_risk(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.agent_type == AgentType.NETWORK
    assert verdict.risk_score == pytest.approx(0.0)
    assert verdict.risk_level == RiskLevel.LOW
    assert verdict.reasoning == "No network anomalies detected."


def test_clean_transaction_latency(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.latency_ms > 0


def test_clean_transaction_confidence(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.confidence == pytest.approx(0.88)


def test_features_list(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert "ip_fraud_score" in verdict.features_used
    assert "device_seen_before" in verdict.features_used
    assert "linked_fraud_accounts" in verdict.features_used


# ── IP fraud score ────────────────────────────────────────────────────────────

def test_high_risk_ip_adds_large_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"ip_fraud_score": 0.95})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.40)
    assert verdict.risk_level == RiskLevel.MEDIUM
    assert "High-risk IP" in verdict.reasoning


def test_ip_exactly_at_high_risk_boundary(sample_transaction):
    # > 0.8 triggers high-risk; exactly 0.8 should fall to suspicious branch
    txn = sample_transaction.model_copy(update={"ip_fraud_score": 0.8})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)
    assert "Suspicious IP" in verdict.reasoning


def test_suspicious_ip_adds_lesser_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"ip_fraud_score": 0.65})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)
    assert "Suspicious IP" in verdict.reasoning


def test_ip_at_suspicious_boundary_no_signal(sample_transaction):
    # > 0.5 triggers; exactly 0.5 should not
    txn = sample_transaction.model_copy(update={"ip_fraud_score": 0.5})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_clean_ip_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"ip_fraud_score": 0.05})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_none_ip_score_ignored(sample_transaction):
    txn = sample_transaction.model_copy(update={"ip_fraud_score": None})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Device fingerprint ────────────────────────────────────────────────────────

def test_new_device_adds_minor_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"device_seen_before": False})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.15)
    assert "First-time device fingerprint" in verdict.reasoning


def test_known_device_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"device_seen_before": True})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Linked fraud accounts ─────────────────────────────────────────────────────

def test_many_linked_accounts_adds_large_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"linked_fraud_accounts": 4})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.40)
    assert "4 known-fraud accounts" in verdict.reasoning


def test_few_linked_accounts_adds_moderate_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"linked_fraud_accounts": 2})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)
    assert "2 suspicious account(s)" in verdict.reasoning


def test_single_linked_account_adds_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"linked_fraud_accounts": 1})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)


def test_three_linked_accounts_moderate_not_large(sample_transaction):
    # > 3 triggers the large branch; exactly 3 should use moderate
    txn = sample_transaction.model_copy(update={"linked_fraud_accounts": 3})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)


def test_zero_linked_accounts_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"linked_fraud_accounts": 0})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Combined signals ──────────────────────────────────────────────────────────

def test_high_ip_plus_new_device_is_high(sample_transaction):
    # 0.40 + 0.15 = 0.55 → HIGH
    txn = sample_transaction.model_copy(
        update={"ip_fraud_score": 0.95, "device_seen_before": False}
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.55)
    assert verdict.risk_level == RiskLevel.HIGH


def test_all_signals_critical(sample_transaction):
    # 0.40 (high IP) + 0.15 (new device) + 0.40 (>3 linked) = 0.95 → CRITICAL
    txn = sample_transaction.model_copy(
        update={
            "ip_fraud_score": 0.95,
            "device_seen_before": False,
            "linked_fraud_accounts": 5,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.95)
    assert verdict.risk_level == RiskLevel.CRITICAL
    assert len(verdict.reasoning.split(";")) == 3


def test_score_capped_at_one(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "ip_fraud_score": 1.0,
            "device_seen_before": False,
            "linked_fraud_accounts": 10,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score <= 1.0


# ── Output invariants ─────────────────────────────────────────────────────────

def test_score_bounds(high_risk_transaction):
    verdict = evaluate(high_risk_transaction.model_dump(mode="json"))
    assert 0.0 <= verdict.risk_score <= 1.0
    assert 0.0 <= verdict.confidence <= 1.0


def test_transaction_id_preserved(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.transaction_id == sample_transaction.transaction_id
