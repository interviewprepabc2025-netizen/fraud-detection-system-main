-- Initialise additional databases for MLflow and Airflow
-- This script runs once when the Postgres container is first created.

CREATE DATABASE mlflow;
CREATE DATABASE airflow;

-- Training records table (populated by storage consumer from training-data topic)
\c fraud_detection;

CREATE TABLE IF NOT EXISTS training_records (
    record_id       UUID PRIMARY KEY,
    transaction_id  UUID NOT NULL,
    label           SMALLINT NOT NULL CHECK (label IN (0, 1)),
    label_source    VARCHAR(32) NOT NULL,
    features        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tr_label ON training_records(label);
CREATE INDEX IF NOT EXISTS idx_tr_created_at ON training_records(created_at);
