from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class TransactionEvent:
    event_id: str
    customer_id: str
    amount: float
    event_timestamp_ms: int

    def as_kafka_value(self) -> dict[str, Any]:
        return asdict(self)

    def as_batch_row(self) -> dict[str, Any]:
        event_time = datetime.fromtimestamp(self.event_timestamp_ms / 1000, tz=UTC)
        return {
            **asdict(self),
            "event_timestamp": event_time,
            "created_timestamp": event_time,
        }


def feature_push_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        raise ValueError("records must not be empty")
    columns = (
        "customer_id",
        "total_amount_30d",
        "tx_count_30d",
        "event_timestamp",
        "created_timestamp",
    )
    return {
        "push_source_name": "flink_customer_30d_push",
        "df": {column: [record[column] for record in records] for column in columns},
        "to": "online_and_offline",
    }
