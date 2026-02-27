FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==2.13.0 psycopg2-binary boto3

EXPOSE 5000

CMD mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri "${MLFLOW_BACKEND_STORE_URI}" \
    --default-artifact-root "${MLFLOW_ARTIFACT_ROOT}"
