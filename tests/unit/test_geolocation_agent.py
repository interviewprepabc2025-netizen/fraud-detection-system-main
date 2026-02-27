"""Unit tests for the geolocation agent."""

from __future__ import annotations

import pytest

from agents.geolocation.agent import HIGH_RISK_COUNTRIES, _haversine, evaluate
from shared.utils.models import AgentType, RiskLevel


# ── Haversine helper ──────────────────────────────────────────────────────────

def test_haversine_same_point():
    assert _haversine(0.0, 0.0, 0.0, 0.0) == pytest.approx(0.0)


def test_haversine_known_distance():
    # London (51.5074, -0.1278) → Paris (48.8566, 2.3522) ≈ 340 km
    dist = _haversine(51.5074, -0.1278, 48.8566, 2.3522)
    assert 330 < dist < 350


def test_haversine_antipodal():
    # Opposite sides of the earth ≈ half circumference ≈ 20_015 km
    dist = _haversine(0.0, 0.0, 0.0, 180.0)
    assert dist == pytest.approx(20_015, rel=0.01)


# ── Clean transaction — no signals ───────────────────────────────────────────

def test_clean_transaction_low_risk(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.agent_type == AgentType.GEOLOCATION
    assert verdict.risk_score == pytest.approx(0.0)
    assert verdict.risk_level == RiskLevel.LOW
    assert verdict.reasoning == "No geolocation anomalies detected."


def test_clean_transaction_latency(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.latency_ms > 0


def test_clean_transaction_confidence(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.confidence == pytest.approx(0.80)


def test_features_list(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert "distance_from_last_txn_km" in verdict.features_used
    assert "is_new_country" in verdict.features_used
    assert "country_code" in verdict.features_used


# ── Distance signal ───────────────────────────────────────────────────────────

def test_large_distance_jump_adds_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"distance_from_last_txn_km": 8000.0})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.40)
    assert verdict.risk_level == RiskLevel.MEDIUM
    assert "8000.0 km" in verdict.reasoning


def test_distance_below_threshold_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"distance_from_last_txn_km": 499.9})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_distance_none_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"distance_from_last_txn_km": None})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


def test_distance_exactly_at_threshold_no_signal(sample_transaction):
    # Boundary: > 500, not >= 500
    txn = sample_transaction.model_copy(update={"distance_from_last_txn_km": 500.0})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── New-country signal ────────────────────────────────────────────────────────

def test_new_country_adds_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"is_new_country": True})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.25)
    assert verdict.risk_level == RiskLevel.MEDIUM
    assert sample_transaction.country_code in verdict.reasoning


def test_known_country_no_signal(sample_transaction):
    txn = sample_transaction.model_copy(update={"is_new_country": False})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.0)


# ── New-city signal ───────────────────────────────────────────────────────────

def test_new_city_adds_minor_score(sample_transaction):
    txn = sample_transaction.model_copy(update={"is_new_city": True})
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.10)
    assert verdict.risk_level == RiskLevel.LOW  # 0.10 < 0.25 threshold
    assert sample_transaction.city in verdict.reasoning


# ── Combined signals ──────────────────────────────────────────────────────────

def test_distance_plus_new_country_is_high(sample_transaction):
    # 0.40 + 0.25 = 0.65 → HIGH
    txn = sample_transaction.model_copy(
        update={"distance_from_last_txn_km": 8000.0, "is_new_country": True}
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.65)
    assert verdict.risk_level == RiskLevel.HIGH


def test_all_signals_critical(sample_transaction):
    # 0.40 + 0.25 + 0.10 = 0.75 → CRITICAL
    txn = sample_transaction.model_copy(
        update={
            "distance_from_last_txn_km": 8000.0,
            "is_new_country": True,
            "is_new_city": True,
        }
    )
    verdict = evaluate(txn.model_dump(mode="json"))
    assert verdict.risk_score == pytest.approx(0.75)
    assert verdict.risk_level == RiskLevel.CRITICAL


def test_score_capped_at_one(sample_transaction):
    # Monkeypatch HIGH_RISK_COUNTRIES to force all four signals
    import agents.geolocation.agent as geo_mod
    original = geo_mod.HIGH_RISK_COUNTRIES
    geo_mod.HIGH_RISK_COUNTRIES = frozenset({"US"})
    try:
        txn = sample_transaction.model_copy(
            update={
                "distance_from_last_txn_km": 8000.0,
                "is_new_country": True,
                "is_new_city": True,
                "country_code": "US",
            }
        )
        verdict = evaluate(txn.model_dump(mode="json"))
        assert verdict.risk_score <= 1.0
    finally:
        geo_mod.HIGH_RISK_COUNTRIES = original


# ── Score / output invariants ─────────────────────────────────────────────────

def test_score_bounds(high_risk_transaction):
    verdict = evaluate(high_risk_transaction.model_dump(mode="json"))
    assert 0.0 <= verdict.risk_score <= 1.0
    assert 0.0 <= verdict.confidence <= 1.0


def test_transaction_id_preserved(sample_transaction):
    verdict = evaluate(sample_transaction.model_dump(mode="json"))
    assert verdict.transaction_id == sample_transaction.transaction_id
