"""Verdict aggregation logic for the supervisor agent."""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

from shared.utils.models import AgentType, AgentVerdict, RiskLevel, SupervisorDecision

# Per-agent weights (sum to 1.0)
AGENT_WEIGHTS: dict[AgentType, float] = {
    AgentType.VELOCITY: float(os.getenv("WEIGHT_VELOCITY", "0.20")),
    AgentType.GEOLOCATION: float(os.getenv("WEIGHT_GEOLOCATION", "0.25")),
    AgentType.BEHAVIORAL: float(os.getenv("WEIGHT_BEHAVIORAL", "0.30")),
    AgentType.NETWORK: float(os.getenv("WEIGHT_NETWORK", "0.25")),
}


def aggregate(
    verdicts: list[AgentVerdict],
    fraud_score_threshold: float = 0.75,
    review_band: tuple[float, float] = (0.40, 0.75),
    llm_reasoning: str | None = None,
) -> SupervisorDecision:
    """Compute weighted risk score and derive a final action.

    Args:
        verdicts: List of verdicts from specialist agents.
        fraud_score_threshold: Score >= threshold → decline.
        review_band: (low, high) range that triggers manual review.
        llm_reasoning: Optional LLM-generated explanation.

    Returns:
        SupervisorDecision with action one of: approve / decline / review
    """
    if not verdicts:
        raise ValueError("Cannot aggregate an empty verdict list.")

    total_weight = 0.0
    weighted_score = 0.0
    total_latency = 0.0

    for v in verdicts:
        w = AGENT_WEIGHTS.get(v.agent_type, 0.25)
        weighted_score += v.risk_score * w
        total_weight += w
        total_latency += v.latency_ms

    final_score = weighted_score / total_weight if total_weight > 0 else 0.0
    final_score = round(min(final_score, 1.0), 4)

    if final_score >= fraud_score_threshold:
        action = "decline"
        risk_level = RiskLevel.CRITICAL if final_score >= 0.9 else RiskLevel.HIGH
    elif review_band[0] <= final_score < review_band[1]:
        action = "review"
        risk_level = RiskLevel.MEDIUM
    else:
        action = "approve"
        risk_level = RiskLevel.LOW

    reasoning_parts = [
        f"Agent scores — " + ", ".join(
            f"{v.agent_type.value}: {v.risk_score:.3f}" for v in verdicts
        ),
        f"Weighted score: {final_score:.4f} → action: {action}",
    ]
    if llm_reasoning:
        reasoning_parts.append(f"LLM analysis: {llm_reasoning}")

    return SupervisorDecision(
        transaction_id=verdicts[0].transaction_id,
        final_risk_score=final_score,
        risk_level=risk_level,
        action=action,
        agent_verdicts=verdicts,
        reasoning=" | ".join(reasoning_parts),
        total_latency_ms=total_latency,
    )
