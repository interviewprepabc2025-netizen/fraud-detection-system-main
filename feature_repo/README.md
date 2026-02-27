# feature_repo/

Feast feature store definitions for the fraud detection pipeline.

## Structure

```
feature_repo/
├── feature_store.yaml          # Feast configuration
├── features/
│   ├── user_transaction_stats.py   # Velocity features per user
│   ├── user_behavioral_stats.py    # Behavioural profile features
│   ├── device_network_stats.py     # Device / IP features
│   └── geo_stats.py                # Geolocation history features
└── registry.db                 # Auto-generated Feast registry (gitignored)
```

## Commands

```bash
# Apply feature definitions to the registry
feast apply

# Materialise features to the online store
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Start the Feast UI
feast ui
```

## Online store

Redis (configured via `feature_store.yaml`).
The enrichment consumer queries the online store on every transaction.
