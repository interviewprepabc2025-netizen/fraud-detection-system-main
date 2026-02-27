# dags/

Apache Airflow DAG definitions for scheduled fraud detection pipelines.

## DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `fraud_model_retraining` | `@daily` | Fetch training data, retrain XGBoost, register in MLflow, promote if AUC improves |
| `feature_materialization` | `@hourly` | Run `feast materialize-incremental` to refresh online store |
| `data_quality_check` | `@daily` | Validate training data schema and distribution drift |

## Adding a new DAG

1. Create a `.py` file in this directory.
2. Define a `dag` variable (TaskFlow API or classic operators).
3. The Airflow scheduler picks it up automatically on the next heartbeat.
