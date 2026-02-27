.PHONY: help up down build restart logs \
        kafka-topics feast-apply agents-up agents-down \
        mlflow-ui airflow-ui spark-ui history-ui \
        spark-up spark-down spark-logs \
        lint fmt test test-cov \
        producer-start consumer-start \
        delta-tables lake-status \
        clean

# ── Colours ───────────────────────────────────────────────────────────────────
CYAN  := \033[0;36m
RESET := \033[0m

help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\n$(CYAN)Fraud Detection System$(RESET)\n\nUsage:\n  make $(CYAN)<target>$(RESET)\n\nTargets:\n"} \
	/^[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ── Environment ───────────────────────────────────────────────────────────────
.env:
	@echo "Copying .env.example → .env"
	cp .env.example .env

# ── Docker Compose ────────────────────────────────────────────────────────────
up: .env ## Start all services (infra + agents)
	docker compose up -d --build

infra-up: .env ## Start only infrastructure services (no agents, no Spark)
	docker compose up -d zookeeper kafka schema-registry kafka-ui \
	                     redis postgres minio minio-init mlflow \
	                     otel-collector prometheus grafana

agents-up: ## Start all agent microservices
	docker compose up -d velocity-agent geolocation-agent \
	                     behavioral-agent network-agent supervisor-agent

agents-down: ## Stop all agent microservices
	docker compose stop velocity-agent geolocation-agent \
	                    behavioral-agent network-agent supervisor-agent

down: ## Stop and remove all containers
	docker compose down

build: ## Rebuild all images
	docker compose build --no-cache

restart: ## Restart all containers
	docker compose restart

logs: ## Tail logs for all services
	docker compose logs -f --tail=100

logs-agents: ## Tail logs for agent services only
	docker compose logs -f --tail=100 \
	    velocity-agent geolocation-agent behavioral-agent \
	    network-agent supervisor-agent

# ── Kafka ─────────────────────────────────────────────────────────────────────
kafka-topics: ## Create all required Kafka topics
	docker compose exec kafka kafka-topics \
	    --bootstrap-server kafka:9092 --create --if-not-exists \
	    --topic raw-transactions --partitions 12 --replication-factor 1
	docker compose exec kafka kafka-topics \
	    --bootstrap-server kafka:9092 --create --if-not-exists \
	    --topic enriched-transactions --partitions 12 --replication-factor 1
	docker compose exec kafka kafka-topics \
	    --bootstrap-server kafka:9092 --create --if-not-exists \
	    --topic agent-verdicts --partitions 6 --replication-factor 1
	docker compose exec kafka kafka-topics \
	    --bootstrap-server kafka:9092 --create --if-not-exists \
	    --topic training-data --partitions 6 --replication-factor 1
	@echo "All Kafka topics created."

kafka-list: ## List all Kafka topics
	docker compose exec kafka kafka-topics \
	    --bootstrap-server kafka:9092 --list

# ── Feature Store ─────────────────────────────────────────────────────────────
feast-apply: ## Apply Feast feature definitions
	cd feature_repo && feast apply

feast-materialize: ## Materialize features to online store
	cd feature_repo && feast materialize-incremental $(shell date -u +"%Y-%m-%dT%H:%M:%S")

feast-ui: ## Open Feast UI
	cd feature_repo && feast ui

# ── MLflow ────────────────────────────────────────────────────────────────────
mlflow-ui: ## Open MLflow tracking UI (http://localhost:5000)
	@echo "MLflow UI → http://localhost:5000"
	@open http://localhost:5000 2>/dev/null || xdg-open http://localhost:5000 2>/dev/null || true

# ── Airflow ───────────────────────────────────────────────────────────────────
airflow-ui: ## Open Airflow webserver UI (http://localhost:8082)
	@echo "Airflow UI → http://localhost:8082"
	@open http://localhost:8082 2>/dev/null || xdg-open http://localhost:8082 2>/dev/null || true

# ── Spark ─────────────────────────────────────────────────────────────────────
spark-up: ## Start Spark cluster + streaming jobs
	docker compose up -d spark-master spark-worker-1 spark-worker-2 \
	                     spark-history-server \
	                     spark-streaming-enricher spark-streaming-sink

spark-down: ## Stop Spark cluster and streaming jobs
	docker compose stop spark-master spark-worker-1 spark-worker-2 \
	                    spark-history-server \
	                    spark-streaming-enricher spark-streaming-sink

spark-logs: ## Tail Spark streaming job logs
	docker compose logs -f --tail=100 \
	    spark-streaming-enricher spark-streaming-sink

spark-ui: ## Open Spark Master Web UI (http://localhost:8090)
	@echo "Spark Master UI → http://localhost:8090"
	@open http://localhost:8090 2>/dev/null || xdg-open http://localhost:8090 2>/dev/null || true

history-ui: ## Open Spark History Server (http://localhost:18080)
	@echo "Spark History Server → http://localhost:18080"
	@open http://localhost:18080 2>/dev/null || xdg-open http://localhost:18080 2>/dev/null || true

# ── Delta Lake ────────────────────────────────────────────────────────────────
lake-status: ## Print row counts for each Delta Lake layer
	docker compose exec spark-master /opt/bitnami/spark/bin/spark-sql \
	    --master local[1] \
	    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
	    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
	    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
	    --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
	    --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
	    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
	    -e "SELECT 'bronze' AS layer, COUNT(*) AS rows FROM delta.\`s3a://fraud-lake/bronze/transactions\`
	        UNION ALL
	        SELECT 'silver_enriched', COUNT(*) FROM delta.\`s3a://fraud-lake/silver/enriched\`
	        UNION ALL
	        SELECT 'gold_decisions', COUNT(*) FROM delta.\`s3a://fraud-lake/gold/fraud_decisions\`;"

delta-tables: ## List all Delta Lake tables and their versions
	@echo "Delta Lake tables in MinIO (fraud-lake bucket):"
	docker compose exec minio /usr/bin/mc ls --recursive myminio/fraud-lake/ 2>/dev/null | grep "_delta_log" | sed 's|/_delta_log.*||' | sort -u || \
	    echo "Run: docker compose exec minio /usr/bin/mc ls --recursive myminio/fraud-lake/"

# ── Producers / Consumers ─────────────────────────────────────────────────────
producer-start: ## Run transaction producer locally
	python kafka/producers/transaction_producer.py

consumer-start: ## Run storage consumer locally
	python kafka/consumers/storage_consumer.py

# ── Code Quality ──────────────────────────────────────────────────────────────
lint: ## Run ruff linter across the project
	ruff check .

fmt: ## Format all Python files with Black
	black .

fmt-check: ## Check formatting without modifying files
	black --check .

# ── Tests ─────────────────────────────────────────────────────────────────────
test: ## Run test suite
	pytest tests/ -v

test-cov: ## Run tests with HTML coverage report
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing
	@echo "Coverage report → htmlcov/index.html"

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove Docker volumes and generated artefacts
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov .coverage .pytest_cache
