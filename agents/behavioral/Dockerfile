FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY agents/behavioral ./agents/behavioral

ENV PYTHONPATH=/app

CMD ["python", "-m", "agents.behavioral.main"]
