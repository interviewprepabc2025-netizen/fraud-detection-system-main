\# Fraud Detection System — Claude Code Context



\## Project Architecture

\- Kafka topics: raw-transactions, enriched-transactions, agent-verdicts, training-data

\- All agents are Python microservices in /agents/

\- Feature store: Feast, registry at ./feature\_repo/

\- Models stored in MLflow at http://localhost:5000



\## Coding Conventions

\- Python 3.11, Black formatter, type hints required

\- All Kafka consumers use consumer group naming: <service>-group

\- Environment variables via python-dotenv, never hardcode credentials



\## Key SLAs

\- End-to-end decision latency: < 200ms

\- Throughput target: 50,000 TPS peak



