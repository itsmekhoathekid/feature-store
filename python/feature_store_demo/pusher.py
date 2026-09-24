import json
import signal
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from feature_store_demo.config import (
    DATA_DIR,
    FEAST_SERVER_URL,
    FEATURE_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    PUSH_DLQ_TOPIC,
)
from feature_store_demo.contracts import feature_push_payload

RUNNING = True
BATCH_SIZE = 100
BATCH_WAIT_SECONDS = 1.0


def stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


class PushLedger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS pushed_windows ("
            "customer_id TEXT NOT NULL, window_end_ms INTEGER NOT NULL, "
            "PRIMARY KEY(customer_id, window_end_ms))"
        )

    def contains(self, record: dict[str, Any]) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM pushed_windows WHERE customer_id = ? AND window_end_ms = ?",
            (record["customer_id"], record["window_end_ms"]),
        ).fetchone()
        return row is not None

    def mark(self, records: Iterable[dict[str, Any]]) -> None:
        self.connection.executemany(
            "INSERT OR IGNORE INTO pushed_windows(customer_id, window_end_ms) VALUES (?, ?)",
            ((record["customer_id"], record["window_end_ms"]) for record in records),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def push_with_retry(records: list[dict[str, Any]], attempts: int = 5) -> requests.Response:
    payload = feature_push_payload(records)
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.post(f"{FEAST_SERVER_URL}/push", json=payload, timeout=20)
            if response.status_code < 500:
                return response
            last_error = RuntimeError(f"Feast returned {response.status_code}: {response.text}")
        except requests.RequestException as error:
            last_error = error
        time.sleep(min(2**attempt, 10))
    raise RuntimeError("Feast push failed after retries") from last_error


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    required = {
        "customer_id",
        "total_amount_30d",
        "tx_count_30d",
        "window_end_ms",
        "event_timestamp",
        "created_timestamp",
    }
    missing = required - record.keys()
    if missing:
        raise ValueError(f"Missing aggregate fields: {sorted(missing)}")
    return record


def publish_dlq(producer: Producer, message: Message, reason: str) -> None:
    envelope = {
        "reason": reason,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "value": message.value().decode(errors="replace"),
    }
    producer.produce(PUSH_DLQ_TOPIC, value=json.dumps(envelope).encode())
    producer.flush(10)


def commit_messages(consumer: Consumer, messages: list[Message]) -> None:
    offsets: dict[tuple[str, int], TopicPartition] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        current = offsets.get(key)
        next_offset = message.offset() + 1
        if current is None or next_offset > current.offset:
            offsets[key] = TopicPartition(*key, next_offset)
    if offsets:
        consumer.commit(offsets=list(offsets.values()), asynchronous=False)


def process_batch(
    consumer: Consumer,
    producer: Producer,
    ledger: PushLedger,
    messages: list[Message],
) -> None:
    committable: list[Message] = []
    pending: list[tuple[Message, dict[str, Any]]] = []
    transient_errors = {
        KafkaError._PARTITION_EOF,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
    }

    for message in messages:
        if message.error():
            if message.error().code() not in transient_errors:
                raise RuntimeError(message.error())
            continue
        try:
            record = normalize_record(json.loads(message.value()))
            if ledger.contains(record):
                committable.append(message)
            else:
                pending.append((message, record))
        except (UnicodeDecodeError, ValueError) as error:
            publish_dlq(producer, message, str(error))
            committable.append(message)

    if pending:
        records = [record for _, record in pending]
        response = push_with_retry(records)
        if response.status_code >= 400:
            reason = f"Feast HTTP {response.status_code}: {response.text}"
            for message, _ in pending:
                publish_dlq(producer, message, reason)
        else:
            ledger.mark(records)
        committable.extend(message for message, _ in pending)

    commit_messages(consumer, committable)


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    ledger = PushLedger(DATA_DIR / "ledger" / "feast-pusher.sqlite")
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "feast-feature-pusher",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True})
    consumer.subscribe([FEATURE_TOPIC])
    try:
        while RUNNING:
            messages = consumer.consume(
                num_messages=BATCH_SIZE,
                timeout=BATCH_WAIT_SECONDS,
            )
            if messages:
                process_batch(consumer, producer, ledger, messages)
    finally:
        consumer.close()
        ledger.close()


if __name__ == "__main__":
    main()
