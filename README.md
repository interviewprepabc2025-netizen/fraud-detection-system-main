# Fraud Detection System

Real-time fraud detection pipeline built on Apache Kafka, Feast feature store,
MLflow model registry, Apache Airflow, and five specialized AI agent microservices.

## Architecture Overview

```
                    ┌─────────────────────────────────────────────────────┐
                    │                   Kafka Cluster                     │
  Payment           │  raw-transactions ──► enriched-transactions         │
  Gateway ─────────►│                         │                           │
                    │                         ▼                           │
                    │              ┌──────────────────────┐               │
                    │              │   Agent Microservices │               │
                    │              │  ┌─────────────────┐  │               │
                    │              │  │  Velocity Agent  │  │               │
                    │              │  │ Geolocation Agent│  │               │
                    │              │  │ Behavioral Agent │  │──► agent-     │
                    │              │  │  Network Agent   │  │    verdicts   │
                    │              │  │ Supervisor Agent │  │               │
                    │              │  └─────────────────┘  │               │
                    │              └──────────────────────┘               │
                    └─────────────────────────────────────────────────────┘
                              │                    │
                    ┌─────────▼────────┐  ┌────────▼────────┐
                    │   Feast Feature  │  │  MLflow Model   │
                    │      Store       │  │    Registry     │
                    └──────────────────┘  └─────────────────┘
                              │
                    ┌─────────▼────────┐
                    │ Airflow Training │
                    │    Pipelines     │
                    └──────────────────┘
```

## Key SLAs
| Metric | Target |
|---|---|
| End-to-end decision latency | < 200 ms |
| Throughput | 50,000 TPS peak |

## Kafka Topics
| Topic | Purpose |
|---|---|
| `raw-transactions` | Inbound payment events from gateway |
| `enriched-transactions` | Transactions with computed features |
| `agent-verdicts` | Per-agent risk scores and reasoning |
| `training-data` | Labeled records for model retraining |

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env

# 2. Start all infrastructure
make up

# 3. Initialize feature store
make feast-apply

# 4. Create Kafka topics
make kafka-topics

# 5. Run all agents
make agents-up

# 6. Tear down
make down
```

## Project Layout

```
fraud-detection-system/
├── agents/                  # AI agent microservices
│   ├── velocity/
│   ├── geolocation/
│   ├── behavioral/
│   ├── network/
│   └── supervisor/
├── kafka/
│   ├── producers/           # Transaction producers / simulators
│   └── consumers/           # Downstream consumers (storage, alerting)
├── feature_repo/            # Feast feature definitions & registry
├── dags/                    # Airflow DAG definitions
├── mlflow/                  # MLflow helpers & model cards
├── shared/                  # Shared utilities used by all services
│   └── utils/
├── tests/                   # Integration & unit tests
├── docker-compose.yml
├── Makefile
├── .env.example
└── requirements.txt
```

## Development

```bash
make lint      # black + ruff
make test      # pytest with coverage
make mlflow-ui # Open MLflow at http://localhost:5000
make airflow-ui # Open Airflow at http://localhost:8080
```
