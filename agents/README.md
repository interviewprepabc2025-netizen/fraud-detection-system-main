# agents/

Five specialized AI agent microservices. Each agent:

1. Consumes from `enriched-transactions`
2. Computes a domain-specific risk score
3. Publishes an `AgentVerdict` to `agent-verdicts`
4. Exposes a FastAPI health endpoint

## Agents

| Agent | Port | Specialisation |
|---|---|---|
| `velocity` | 8001 | Transaction frequency / velocity anomalies |
| `geolocation` | 8002 | Location impossibility & travel speed |
| `behavioral` | 8003 | Deviation from historical spending patterns |
| `network` | 8004 | IP reputation, device graph, account linkage |
| `supervisor` | 8005 | Aggregates all verdicts → final decision |

## Shared Dockerfile pattern

Each agent folder contains its own `Dockerfile` that installs the shared package
and the agent-specific code.  The `shared/` package is bind-mounted during dev
and copied in during image build.

## Consumer group naming

Each agent registers as `<agent-name>-group` per project convention.
