# agents/supervisor/

## Responsibility

Aggregates verdicts from all four specialist agents and produces the final
`SupervisorDecision`. Optionally calls an LLM (Claude) to generate a
human-readable explanation for edge cases.

## Decision logic

1. Collect all `AgentVerdict` events for a transaction (with timeout)
2. Compute weighted average risk score across agents
3. Apply business rules (hard blocks, force-approve list)
4. If the score is in the ambiguous range (0.40–0.75) → call LLM for reasoning
5. Publish `SupervisorDecision` to `agent-verdicts` topic
6. Write `TrainingRecord` to `training-data` topic

## Weights (configurable via env)

| Agent | Default weight |
|---|---|
| velocity | 0.20 |
| geolocation | 0.25 |
| behavioral | 0.30 |
| network | 0.25 |

## Consumer group

`supervisor-group`
