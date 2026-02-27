# kafka/producers/

## transaction_producer.py

Synthetic payment-gateway simulator for local development and load testing.
Publishes `Transaction` events to the `raw-transactions` topic.

### Usage

```bash
# Produce 1000 transactions at 100 TPS
TPS=100 TOTAL=1000 python kafka/producers/transaction_producer.py
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `TPS` | `10` | Target transactions per second |
| `TOTAL` | `0` | Total events (0 = run forever) |
| `FRAUD_RATIO` | `0.02` | Fraction of events to make suspicious |
