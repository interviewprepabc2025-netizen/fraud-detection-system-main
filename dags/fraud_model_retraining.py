"""Daily fraud model retraining DAG."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="fraud_model_retraining",
    description="Retrain the XGBoost fraud classifier daily using labelled Kafka data.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "fraud-ml-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["fraud", "ml", "retraining"],
)
def fraud_model_retraining() -> None:

    @task()
    def extract_training_data() -> dict:
        """Read labelled records from PostgreSQL and expand JSONB features."""
        import os
        import pandas as pd
        import sqlalchemy as sa

        engine = sa.create_engine(
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@"
            f"{os.environ.get('POSTGRES_HOST', 'postgres')}:"
            f"{os.environ.get('POSTGRES_PORT', '5432')}/"
            f"{os.environ.get('POSTGRES_DB', 'fraud_detection')}"
        )
        df = pd.read_sql(
            "SELECT * FROM training_records WHERE created_at > NOW() - INTERVAL '30 days'",
            engine,
        )
        # Expand the JSONB features column into individual numeric columns
        # so XGBoost receives a properly shaped numeric feature matrix.
        if "features" in df.columns and df["features"].notna().any():
            feat_df = pd.json_normalize(
                df["features"].where(df["features"].notna(), other={})
            )
            feat_df.index = df.index
            df = pd.concat(
                [df.drop(columns=["features", "transaction_id", "label_source"]), feat_df],
                axis=1,
            )
        else:
            df = df.drop(
                columns=["features", "transaction_id", "label_source"], errors="ignore"
            )
        # Coerce all feature columns to float (drops any remaining non-numeric)
        non_feat = {"label", "record_id", "created_at"}
        for col in [c for c in df.columns if c not in non_feat]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        # Convert UUID record_id to str — pyarrow cannot serialize Python UUID objects
        if "record_id" in df.columns:
            df["record_id"] = df["record_id"].astype(str)
        path = "/tmp/training_data.parquet"
        df.to_parquet(path, index=False)
        return {"path": path, "rows": len(df)}

    @task()
    def validate_data(extract_result: dict) -> dict:
        """Basic data quality checks before training."""
        import pandas as pd

        df = pd.read_parquet(extract_result["path"])
        assert len(df) > 1000, f"Not enough training data: {len(df)} rows"
        fraud_rate = df["label"].mean()
        assert 0.001 <= fraud_rate <= 0.5, f"Suspicious fraud rate: {fraud_rate:.4f}"
        return {**extract_result, "fraud_rate": round(float(fraud_rate), 4)}

    @task()
    def train_model(validated: dict) -> dict:
        """Train an XGBoost classifier and log to MLflow."""
        import os
        import pandas as pd
        import mlflow
        import mlflow.xgboost
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, average_precision_score

        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "fraud-detection"))

        df = pd.read_parquet(validated["path"])
        feature_cols = [c for c in df.columns if c not in ("label", "record_id", "created_at")]
        X = df[feature_cols].fillna(0)
        y = df["label"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        with mlflow.start_run() as run:
            params = {
                "n_estimators": 400,
                "max_depth": 6,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "scale_pos_weight": int((y == 0).sum() / (y == 1).sum()),
                "use_label_encoder": False,
                "eval_metric": "aucpr",
            }
            model = xgb.XGBClassifier(**params)
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

            preds = model.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, preds)
            aucpr = average_precision_score(y_test, preds)

            mlflow.log_params(params)
            mlflow.log_metrics({"roc_auc": auc, "avg_precision": aucpr})
            mlflow.xgboost.log_model(model, "model")

            return {"run_id": run.info.run_id, "roc_auc": auc, "avg_precision": aucpr}

    @task()
    def promote_model(train_result: dict) -> None:
        """Promote the new model to Production in MLflow if AUC improved."""
        import os
        import mlflow

        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        model_name = os.environ.get("MLFLOW_MODEL_NAME", "fraud-classifier")
        auc_threshold = float(Variable.get("min_roc_auc_for_promotion", default_var="0.90"))

        if train_result["roc_auc"] >= auc_threshold:
            mlflow.register_model(
                f"runs:/{train_result['run_id']}/model", model_name
            )

    raw = extract_training_data()
    validated = validate_data(raw)
    trained = train_model(validated)
    promote_model(trained)


fraud_model_retraining()
