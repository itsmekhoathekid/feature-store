import json
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from feature_store_demo.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC
from feature_store_demo.contracts import TransactionEvent


def fixture_events(now: datetime | None = None) -> list[TransactionEvent]:
    current = now or datetime.now(UTC)
    current_hour = current.replace(minute=0, second=0, microsecond=0)
    base = current_hour - timedelta(hours=4)

    def millis(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    return [
        TransactionEvent("evt-001", "customer-001", 10.0, millis(base + timedelta(minutes=10))),
        TransactionEvent("evt-002", "customer-001", 20.0, millis(base + timedelta(minutes=40))),
        TransactionEvent(
            "evt-003",
            "customer-001",
            30.0,
            millis(base + timedelta(hours=1, minutes=20)),
        ),
        TransactionEvent("evt-002", "customer-001", 20.0, millis(base + timedelta(minutes=40))),
        TransactionEvent("evt-004", "customer-002", 7.5, millis(base + timedelta(hours=1))),
        TransactionEvent("watermark-001", "watermark-driver", 0.0, millis(current_hour)),
    ]


def main() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True})
    for event in fixture_events():
        producer.produce(
            RAW_TOPIC,
            key=event.event_id.encode(),
            value=json.dumps(event.as_kafka_value()).encode(),
        )
    remaining = producer.flush(20)
    if remaining:
        raise RuntimeError(f"Unable to publish {remaining} fixture events")
    print(f"Published {len(fixture_events())} deterministic events to {RAW_TOPIC}")
    time.sleep(1)


if __name__ == "__main__":
    main()
