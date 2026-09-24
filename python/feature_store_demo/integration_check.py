import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from feature_store_demo.config import (
    FEATURE_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    LATE_TOPIC,
    RAW_TOPIC,
)
from feature_store_demo.contracts import TransactionEvent


def wait_for_record(
    topic: str,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_seconds: int = 90,
) -> dict[str, Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"integration-check-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([topic])
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            record = json.loads(message.value())
            if predicate(record):
                return record
    finally:
        consumer.close()
    raise TimeoutError(f"No matching read_committed record found on {topic}")


def publish_late_probe() -> TransactionEvent:
    timestamp = datetime.now(UTC) - timedelta(days=32)
    event = TransactionEvent(
        event_id=f"late-probe-{uuid.uuid4()}",
        customer_id="late-probe-customer",
        amount=1.0,
        event_timestamp_ms=int(timestamp.timestamp() * 1000),
    )
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
        }
    )
    producer.produce(
        RAW_TOPIC,
        key=event.event_id.encode(),
        value=json.dumps(event.as_kafka_value()).encode(),
    )
    if producer.flush(20):
        raise RuntimeError("Unable to publish the late-event probe")
    return event


def main() -> None:
    feature = wait_for_record(
        FEATURE_TOPIC,
        lambda record: record.get("customer_id") == "customer-001"
        and float(record.get("total_amount_30d", -1)) == 60.0
        and int(record.get("tx_count_30d", -1)) == 3,
    )
    late_probe = publish_late_probe()
    late = wait_for_record(
        LATE_TOPIC,
        lambda record: record.get("event_id") == late_probe.event_id,
    )
    print(
        "Verified Kafka -> Flink output with read_committed and late side output: "
        f"feature_window_end={feature['event_timestamp']}, "
        f"late_event_id={late['event_id']}"
    )


if __name__ == "__main__":
    main()
