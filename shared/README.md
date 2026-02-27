# shared/

Shared utilities imported by all agent microservices and Kafka workers.

## Modules

| Module | Purpose |
|---|---|
| `utils/models.py` | Pydantic schemas for transactions, verdicts, features |
| `utils/kafka_utils.py` | Thin wrappers around `confluent-kafka` producer/consumer |
| `utils/logging_utils.py` | Structured JSON logging via `structlog` |
| `utils/feature_utils.py` | Feast online-store helpers |
| `utils/metrics.py` | Prometheus counter/histogram helpers |
| `utils/settings.py` | `pydantic-settings` config loaded from `.env` |

## Usage

```python
from shared.utils.models import Transaction, AgentVerdict
from shared.utils.kafka_utils import make_producer, make_consumer
from shared.utils.logging_utils import get_logger
```
