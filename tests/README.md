# tests/

Unit and integration tests for all system components.

## Structure

```
tests/
├── unit/
│   ├── test_velocity_agent.py
│   ├── test_geolocation_agent.py
│   ├── test_behavioral_agent.py
│   ├── test_network_agent.py
│   └── test_supervisor_aggregator.py
├── integration/
│   └── test_kafka_pipeline.py
└── conftest.py
```

## Running

```bash
make test           # all tests
make test-cov       # with HTML coverage report
pytest tests/unit/  # unit tests only
```
