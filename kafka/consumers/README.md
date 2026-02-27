# kafka/consumers/

## enrichment_consumer.py

Reads raw transactions, fetches online features from Feast, and publishes
enriched events to `enriched-transactions`.

Consumer group: `enrichment-group`

## storage_consumer.py

Reads `agent-verdicts` (supervisor decisions) and writes them to PostgreSQL
for auditing, reporting, and analyst review queues.

Consumer group: `storage-group`
