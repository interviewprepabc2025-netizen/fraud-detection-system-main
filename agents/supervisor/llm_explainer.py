"""Optional LLM-based reasoning for ambiguous transactions."""

from __future__ import annotations

import json
from typing import Any

import anthropic

from shared.utils.logging_utils import get_logger
from shared.utils.models import AgentVerdict
from shared.utils.settings import settings

logger = get_logger(__name__)

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _client


SYSTEM_PROMPT = """\
You are a fraud analyst assistant. You receive signals from multiple specialist
fraud-detection agents and must provide a concise (2-3 sentence) explanation of
the overall risk assessment. Focus on the most significant signals and avoid
repeating raw numbers already in the input. Be direct and actionable.
"""


def explain(
    verdicts: list[AgentVerdict],
    weighted_score: float,
    transaction_summary: dict[str, Any],
) -> str:
    """Call the LLM to generate a human-readable fraud explanation.

    Returns an empty string if LLM is unavailable or call fails.
    """
    if not settings.anthropic_api_key:
        logger.debug("llm_explainer_skipped", reason="no_api_key")
        return ""

    agent_signals = [
        {
            "agent": v.agent_type.value,
            "score": round(v.risk_score, 3),
            "reasoning": v.reasoning,
        }
        for v in verdicts
    ]

    user_content = json.dumps(
        {
            "transaction": transaction_summary,
            "agent_signals": agent_signals,
            "weighted_score": round(weighted_score, 4),
        },
        default=str,
    )

    try:
        client = _get_client()
        message = client.messages.create(
            model=settings.llm_model,
            max_tokens=settings.llm_max_tokens,
            temperature=settings.llm_temperature,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )
        explanation = message.content[0].text
        logger.debug("llm_explanation_generated", tokens=message.usage.output_tokens)
        return explanation
    except Exception as exc:
        logger.warning("llm_explanation_failed", error=str(exc))
        return ""
