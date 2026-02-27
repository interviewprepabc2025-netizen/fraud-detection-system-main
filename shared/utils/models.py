"""Pydantic data models shared across all services."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

import json

from pydantic import BaseModel, Field, field_validator


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AgentType(str, Enum):
    VELOCITY = "velocity"
    GEOLOCATION = "geolocation"
    BEHAVIORAL = "behavioral"
    NETWORK = "network"
    SUPERVISOR = "supervisor"


class Transaction(BaseModel):
    """Raw inbound transaction event from the payment gateway."""

    transaction_id: UUID = Field(default_factory=uuid4)
    user_id: str
    merchant_id: str
    amount: Decimal
    currency: str = Field(default="USD", max_length=3)
    card_bin: str = Field(max_length=6)
    card_last4: str = Field(max_length=4)
    ip_address: str
    device_fingerprint: str
    merchant_category_code: str = Field(max_length=4)
    country_code: str = Field(max_length=2)
    city: str
    latitude: float | None = None
    longitude: float | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metadata", mode="before")
    @classmethod
    def parse_metadata_str(cls, v: Any) -> Any:
        """Accept JSON-string metadata (produced by Spark enricher)."""
        if isinstance(v, str):
            return json.loads(v)
        return v


class EnrichedTransaction(Transaction):
    """Transaction with computed features attached."""

    # Velocity features
    txn_count_1h: int = 0
    txn_count_24h: int = 0
    amount_sum_1h: Decimal = Decimal("0")
    amount_sum_24h: Decimal = Decimal("0")

    # Geolocation features
    distance_from_last_txn_km: float | None = None
    is_new_country: bool = False
    is_new_city: bool = False

    # Behavioral features
    avg_txn_amount_30d: Decimal | None = None
    amount_deviation_score: float | None = None
    hour_of_day_risk_score: float | None = None

    # Network features
    ip_fraud_score: float | None = None
    device_seen_before: bool = False
    linked_fraud_accounts: int = 0


class AgentVerdict(BaseModel):
    """Risk assessment produced by a single agent."""

    verdict_id: UUID = Field(default_factory=uuid4)
    transaction_id: UUID
    agent_type: AgentType
    risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: RiskLevel
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    features_used: list[str] = Field(default_factory=list)
    latency_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SupervisorDecision(BaseModel):
    """Final aggregated decision from the supervisor agent."""

    decision_id: UUID = Field(default_factory=uuid4)
    transaction_id: UUID
    final_risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: RiskLevel
    action: str  # "approve" | "decline" | "review"
    agent_verdicts: list[AgentVerdict]
    reasoning: str
    total_latency_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TrainingRecord(BaseModel):
    """Labelled record written to the training-data topic."""

    record_id: UUID = Field(default_factory=uuid4)
    transaction: Optional[EnrichedTransaction] = None  # enriched at join time
    transaction_id: Optional[UUID] = None  # set when transaction is not embedded
    label: int  # 0 = legitimate, 1 = fraud
    label_source: str  # "rule_engine" | "human_review" | "chargeback"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
