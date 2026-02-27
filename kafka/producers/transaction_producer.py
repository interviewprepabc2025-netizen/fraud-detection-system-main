"""Synthetic transaction producer for development / load testing."""

from __future__ import annotations

import os
import random
import time
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from shared.utils.kafka_utils import delivery_report, make_producer, produce
from shared.utils.logging_utils import configure_logging, get_logger
from shared.utils.models import Transaction
from shared.utils.settings import settings

configure_logging()
logger = get_logger(__name__)
fake = Faker()

TPS: int = int(os.getenv("TPS", "10"))
TOTAL: int = int(os.getenv("TOTAL", "0"))
FRAUD_RATIO: float = float(os.getenv("FRAUD_RATIO", "0.02"))

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
COUNTRY_CODES = ["US", "GB", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "ZA"]
HIGH_RISK_COUNTRIES = ["NG", "RO", "UA", "VN"]


def _build_transaction(is_suspicious: bool = False) -> Transaction:
    country = random.choice(HIGH_RISK_COUNTRIES if is_suspicious else COUNTRY_CODES)
    amount = (
        Decimal(str(round(random.uniform(500, 5000), 2)))
        if is_suspicious
        else Decimal(str(round(random.uniform(5, 500), 2)))
    )
    return Transaction(
        transaction_id=uuid4(),
        user_id=f"user_{random.randint(1, 50_000)}",
        merchant_id=f"merchant_{random.randint(1, 10_000)}",
        amount=amount,
        currency=random.choice(CURRENCIES),
        card_bin=str(random.randint(400000, 499999)),
        card_last4=str(random.randint(1000, 9999)),
        ip_address=fake.ipv4_public(),
        device_fingerprint=fake.md5(),
        merchant_category_code=str(random.randint(1000, 9999)),
        country_code=country,
        city=fake.city(),
        latitude=float(fake.latitude()),
        longitude=float(fake.longitude()),
    )


def main() -> None:
    producer = make_producer()
    count = 0
    interval = 1.0 / TPS if TPS > 0 else 0

    logger.info("producer_starting", tps=TPS, total=TOTAL, fraud_ratio=FRAUD_RATIO)

    try:
        while TOTAL == 0 or count < TOTAL:
            is_fraud = random.random() < FRAUD_RATIO
            txn = _build_transaction(is_suspicious=is_fraud)
            produce(
                producer,
                topic=settings.topic_raw_transactions,
                value=txn.model_dump(mode="json"),
                key=str(txn.user_id),
            )
            count += 1
            if count % 1000 == 0:
                logger.info("producer_progress", sent=count)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        logger.info("producer_stopped", total_sent=count)


if __name__ == "__main__":
    main()
