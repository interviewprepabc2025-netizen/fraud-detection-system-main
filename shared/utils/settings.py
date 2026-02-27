"""Centralised settings loaded from environment / .env file."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = Field("localhost:9092")
    kafka_schema_registry_url: str = Field("http://localhost:8081")

    # Topics
    topic_raw_transactions: str = Field("raw-transactions")
    topic_enriched_transactions: str = Field("enriched-transactions")
    topic_agent_verdicts: str = Field("agent-verdicts")
    topic_training_data: str = Field("training-data")

    # Redis / Feast
    redis_host: str = Field("localhost")
    redis_port: int = Field(6379)
    redis_password: str = Field("")
    feast_registry_path: str = Field("./feature_repo/registry.db")

    # MLflow
    mlflow_tracking_uri: str = Field("http://localhost:5000")
    mlflow_experiment_name: str = Field("fraud-detection")
    mlflow_model_name: str = Field("fraud-classifier")

    # LLM
    anthropic_api_key: str = Field("")
    openai_api_key: str = Field("")
    llm_model: str = Field("claude-sonnet-4-6")
    llm_max_tokens: int = Field(1024)
    llm_temperature: float = Field(0.0)

    # Observability
    log_level: str = Field("INFO")
    prometheus_port: int = Field(8000)
    otel_exporter_otlp_endpoint: str = Field("http://localhost:4317")

    # SLA
    max_decision_latency_ms: int = Field(200)
    fraud_score_threshold: float = Field(0.75)


settings = Settings()
