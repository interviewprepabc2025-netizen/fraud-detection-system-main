"""Unit tests for the behavioral agent."""

from __future__ import annotations

from decimal import Decimal

import pytest

from agents.behavioral.agent import HIGH_RISK_MCCS, evaluate
from shared.utils.models import AgentType, RiskLevel


# ── Clean transaction — no signals ───────────────────────────────────────────

def test_clean_transaction_low_risk(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.agent_type == AgentType.BEHAVIORAL
    assert verdict.risk_score == pytest.approx(0.0)
    assert verdict.risk_level == RiskLevel.LOW
    assert verdict.reasoning == "Behaviour consistent with history."


def test_clean_transaction_latency(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.latency_ms > 0


def test_clean_transaction_confidence(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.confidence == pytest.approx(0.82)


def test_features_list(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert "avg_txn_amount_30d" in verdict.features_used
    assert "amount_deviation_score" in verdict.features_used
    assert "hour_of_day_risk_score" in verdict.features_used
    assert "merchant_category_code" in verdict.features_used


# ── Amount deviation — severe (>3σ) ──────────────────────────────────────────

def test_severe_amount_deviation_adds_score(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": 3.5,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.35)
    assert verdict.risk_level == RiskLevel.MEDIUM
    assert "3.5σ" in verdict.reasoning


def test_severe_deviation_exact_boundary(sample_transaction):
    # deviation_score = 3.0 is NOT > 3.0, should fall to the elif branch
    txn = sample_transaction.model_copy(
        update={"avg_txn_amount_30d": Decimal("50.00"), "amount_deviation_score": 3.0}
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.15)


# ── Amount deviation — moderate (2σ–3σ) ──────────────────────────────────────

def test_moderate_amount_deviation_adds_lesser_score(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": 2.5,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.15)
    assert verdict.risk_level == RiskLevel.LOW  # 0.15 < 0.25


def test_deviation_below_threshold_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": 1.9,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Amount deviation — guard conditions ──────────────────────────────────────

def test_none_avg_amount_skips_deviation(sample_transaction):
    txn = sample_transaction.model_copy(
        update={"avg_txn_amount_30d": None, "amount_deviation_score": 5.0}
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_zero_avg_amount_skips_deviation(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("0.00"),
            "amount_deviation_score": 5.0,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_none_deviation_score_skips_check(sample_transaction):
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": None,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Time-of-day risk ──────────────────────────────────────────────────────────

def test_unusual_hour_adds_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"hour_of_day_risk_score": 0.9})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)
    assert verdict.risk_level == RiskLevel.LOW  # 0.20 < 0.25
    assert "Unusual transaction hour" in verdict.reasoning


def test_hour_at_threshold_no_signal(sample_transaction):
    # > 0.7, so exactly 0.7 should NOT trigger
    txn = sample_transaction.model_copy(update={"hour_of_day_risk_score": 0.7})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_none_hour_score_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"hour_of_day_risk_score": None})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── High-risk MCC ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("mcc", sorted(HIGH_RISK_MCCS))
def test_each_high_risk_mcc_adds_score(sample_transaction, mcc):
    txn = sample_transaction.model_copy(update={"merchant_category_code": mcc})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.20)
    assert mcc in verdict.reasoning


def test_safe_mcc_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"merchant_category_code": "5411"})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── Combined signals ──────────────────────────────────────────────────────────

def test_deviation_plus_unusual_hour_is_medium(sample_transaction):
    # 0.35 + 0.20 = 0.55 → HIGH
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": 4.0,
            "hour_of_day_risk_score": 0.9,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.55)
    assert verdict.risk_level == RiskLevel.HIGH


def test_all_signals_critical(sample_transaction):
    # 0.35 + 0.20 + 0.20 = 0.75 → CRITICAL
    txn = sample_transaction.model_copy(
        update={
            "avg_txn_amount_30d": Decimal("50.00"),
            "amount_deviation_score": 4.0,
            "hour_of_day_risk_score": 0.9,
            "merchant_category_code": "7995",
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.75)
    assert verdict.risk_level == RiskLevel.CRITICAL


# ── Output invariants ─────────────────────────────────────────────────────────

def test_score_bounds(high_risk_transaction):
    verdict = evaluate(high_risk_transaction.model_dump(mode="json"))
    assert 0.0 <= verdict.risk_score <= 1.0
    assert 0.0 <= verdict.confidence <= 1.0


def test_transaction_id_preserved(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.transaction_id == sample_transaction.transaction_id
