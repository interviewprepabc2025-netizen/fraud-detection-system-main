FROM python:3.11-slim

WORKDIR /app

COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../../shared ./shared
COPY . ./kafka/consumers

ENV PYTHONPATH=/app

CMD ["python", "-m", "kafka.consumers.enrichment_consumer"]
