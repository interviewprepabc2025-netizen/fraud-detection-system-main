# mlflow/

MLflow tracking server configuration and model management helpers.

## Contents

| File | Purpose |
|---|---|
| `Dockerfile` | MLflow server image (PostgreSQL backend + MinIO artifact store) |
| `model_utils.py` | Helper to load the latest Production model from the registry |

## Accessing the UI

```bash
make mlflow-ui   # → http://localhost:5000
```

## Model lifecycle

```
None → Staging → Production → Archived
```

The `fraud_model_retraining` Airflow DAG automatically promotes a new version
to **Production** when `roc_auc >= min_roc_auc_for_promotion` (Airflow Variable,
default `0.90`).

## Artifact storage

Artifacts are stored in **MinIO** (`s3://mlflow/`).
MinIO console → http://localhost:9001 (credentials in `.env`).
