"""Helpers for loading the production fraud model from MLflow registry."""

from __future__ import annotations

import mlflow
import mlflow.pyfunc

from shared.utils.logging_utils import get_logger
from shared.utils.settings import settings

logger = get_logger(__name__)

_model: mlflow.pyfunc.PyFuncModel | None = None


def load_production_model() -> mlflow.pyfunc.PyFuncModel:
    """Load (and cache) the latest Production stage model."""
    global _model
    if _model is not None:
        return _model

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    model_uri = f"models:/{settings.mlflow_model_name}/Production"

    try:
        _model = mlflow.pyfunc.load_model(model_uri)
        logger.info("mlflow_model_loaded", uri=model_uri)
    except Exception as exc:
        logger.error("mlflow_model_load_failed", uri=model_uri, error=str(exc))
        raise

    return _model


def predict(features: dict) -> float:
    """Return a fraud probability score [0, 1] for a feature dict."""
    import pandas as pd

    model = load_production_model()
    df = pd.DataFrame([features])
    result = model.predict(df)
    return float(result[0])
