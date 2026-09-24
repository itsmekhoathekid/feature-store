import json
import os
import signal
import tempfile
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError

from feature_store_demo.config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC
from feature_store_demo.contracts import TransactionEvent
from feature_store_demo.init_data import main as init_data

RUNNING = True


def stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


def append_event(path: Path, event: TransactionEvent) -> None:
    existing = pd.read_parquet(path)
    incoming = pd.DataFrame([event.as_batch_row()])
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(subset=["event_id"], keep="first")
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".parquet", delete=False) as handle:
        temporary_path = Path(handle.name)
    try:
        combined.to_parquet(temporary_path, index=False)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> None:
    init_data()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    path = DATA_DIR / "raw" / "transactions.parquet"
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "raw-parquet-mirror",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([RAW_TOPIC])
    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(message.error())
                continue
            payload = json.loads(message.value())
            event = TransactionEvent(**payload)
            append_event(path, event)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
