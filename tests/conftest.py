"""Shared pytest fixtures."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from shared.utils.models import AgentType, AgentVerdict, EnrichedTransaction, RiskLevel


@pytest.fixture()
def sample_transaction() -> EnrichedTransaction:
    return EnrichedTransaction(
        transaction_id=uuid4(),
        user_id="user_12345",
        merchant_id="merchant_999",
        amount=Decimal("49.99"),
        currency="USD",
        card_bin="411111",
        card_last4="1234",
        ip_address="1.2.3.4",
        device_fingerprint="abc123def456",
        merchant_category_code="5411",
        country_code="US",
        city="New York",
        latitude=40.7128,
        longitude=-74.0060,
        txn_count_1h=2,
        txn_count_24h=5,
        amount_sum_1h=Decimal("100.00"),
        amount_sum_24h=Decimal("250.00"),
        avg_txn_amount_30d=Decimal("55.00"),
        amount_deviation_score=0.5,
        hour_of_day_risk_score=0.1,
        ip_fraud_score=0.05,
        device_seen_before=True,
        linked_fraud_accounts=0,
        distance_from_last_txn_km=2.5,
        is_new_country=False,
        is_new_city=False,
    )


@pytest.fixture()
def high_risk_transaction(sample_transaction: EnrichedTransaction) -> EnrichedTransaction:
    """A transaction with many fraud signals."""
    return sample_transaction.model_copy(
        update={
            "txn_count_1h": 15,
            "amount_deviation_score": 4.5,
            "ip_fraud_score": 0.95,
            "linked_fraud_accounts": 5,
            "distance_from_last_txn_km": 8000.0,
            "is_new_country": True,
        }
    )


@pytest.fixture()
def sample_verdict(sample_transaction: EnrichedTransaction) -> AgentVerdict:
    return AgentVerdict(
        transaction_id=sample_transaction.transaction_id,
        agent_type=AgentType.VELOCITY,
        risk_score=0.3,
        risk_level=RiskLevel.MEDIUM,
        confidence=0.85,
        reasoning="Moderate velocity",
        features_used=["txn_count_1h"],
        latency_ms=5.0,
    )
