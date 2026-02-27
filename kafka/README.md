# kafka/

Kafka producers and consumers outside the agent microservices.

## producers/

| File | Purpose |
|---|---|
| `transaction_producer.py` | Simulates a payment gateway; publishes synthetic `Transaction` events to `raw-transactions` |
| `Dockerfile` | Container image for the producer simulator |

## consumers/

| File | Purpose |
|---|---|
| `enrichment_consumer.py` | Reads `raw-transactions`, fetches Feast features, publishes to `enriched-transactions` |
| `storage_consumer.py` | Reads `agent-verdicts` and persists decisions to PostgreSQL |
| `Dockerfile` | Container image for consumer workers |
