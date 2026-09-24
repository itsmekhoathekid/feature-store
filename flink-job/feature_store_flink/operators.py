import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Iterable

from pyflink.common import Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from feature_store_flink.types import (
    AMOUNT,
    CUSTOMER_ID,
    EVENT_ID,
    EVENT_TIMESTAMP_MS,
    event_type,
)

INVALID_OUTPUT = OutputTag("invalid-transactions", Types.STRING())
# This must not share EVENT_TYPE: PyFlink caches a Java type object after using it on the main
# stream, and that Java object cannot be cloudpickled into the Python window operator.
LATE_OUTPUT = OutputTag("late-transactions", event_type())


def parse_transaction(payload: str) -> tuple[str, str, float, int]:
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON: {error.msg}") from error

    required = {"event_id", "customer_id", "amount", "event_timestamp_ms"}
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError(f"expected exactly these fields: {sorted(required)}")
    if not isinstance(raw["event_id"], str) or not raw["event_id"].strip():
        raise ValueError("event_id must be a non-empty string")
    if not isinstance(raw["customer_id"], str) or not raw["customer_id"].strip():
        raise ValueError("customer_id must be a non-empty string")
    if isinstance(raw["amount"], bool) or not isinstance(raw["amount"], (int, float)):
        raise ValueError("amount must be numeric")
    if isinstance(raw["event_timestamp_ms"], bool) or not isinstance(
        raw["event_timestamp_ms"], int
    ):
        raise ValueError("event_timestamp_ms must be an int64")

    return (
        raw["event_id"],
        raw["customer_id"],
        float(raw["amount"]),
        raw["event_timestamp_ms"],
    )


def event_fingerprint(event: tuple[str, str, float, int]) -> str:
    canonical = json.dumps(event, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


class ParseAndValidate(ProcessFunction):
    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            yield parse_transaction(value)
        except ValueError as error:
            yield INVALID_OUTPUT, f"{error}; payload={value}"


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: tuple[str, str, float, int], record_timestamp: int) -> int:
        return value[EVENT_TIMESTAMP_MS]


class DeduplicateByEventId(KeyedProcessFunction):
    TTL_DAYS = 32

    def open(self, runtime_context) -> None:
        ttl = (
            StateTtlConfig.new_builder(Time.days(self.TTL_DAYS))
            .update_ttl_on_create_and_write()
            .never_return_expired()
            .build()
        )
        descriptor = ValueStateDescriptor("event-fingerprint", Types.STRING())
        descriptor.enable_time_to_live(ttl)
        self.fingerprint_state = runtime_context.get_state(descriptor)

    def process_element(
        self,
        value: tuple[str, str, float, int],
        ctx: KeyedProcessFunction.Context,
    ):
        fingerprint = event_fingerprint(value)
        stored = self.fingerprint_state.value()
        if stored is None:
            self.fingerprint_state.update(fingerprint)
            yield value
        elif stored != fingerprint:
            yield INVALID_OUTPUT, f"conflicting duplicate event_id={value[EVENT_ID]}"


class TransactionAggregate(AggregateFunction):
    def create_accumulator(self) -> tuple[float, int]:
        return 0.0, 0

    def add(
        self,
        value: tuple[str, str, float, int],
        accumulator: tuple[float, int],
    ) -> tuple[float, int]:
        return accumulator[0] + value[AMOUNT], accumulator[1] + 1

    def get_result(self, accumulator: tuple[float, int]) -> tuple[float, int]:
        return accumulator

    def merge(
        self,
        accumulator_a: tuple[float, int],
        accumulator_b: tuple[float, int],
    ) -> tuple[float, int]:
        return accumulator_a[0] + accumulator_b[0], accumulator_a[1] + accumulator_b[1]


def iso_timestamp(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).isoformat().replace("+00:00", "Z")


class AddWindowMetadata(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[tuple[float, int]],
    ) -> Iterable[tuple[str, float, int, int, int, str, str]]:
        total_amount, tx_count = next(iter(elements))
        window = context.window()
        window_end_ms = window.end
        yield (
            key,
            total_amount,
            tx_count,
            window.start,
            window_end_ms,
            iso_timestamp(window_end_ms),
            datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )


def event_to_json(event: tuple[str, str, float, int]) -> str:
    return json.dumps(
        {
            "event_id": event[EVENT_ID],
            "customer_id": event[CUSTOMER_ID],
            "amount": event[AMOUNT],
            "event_timestamp_ms": event[EVENT_TIMESTAMP_MS],
        },
        separators=(",", ":"),
    )


def feature_to_json(feature: tuple[Any, ...]) -> str:
    return json.dumps(
        {
            "customer_id": feature[0],
            "total_amount_30d": feature[1],
            "tx_count_30d": feature[2],
            "window_start_ms": feature[3],
            "window_end_ms": feature[4],
            "event_timestamp": feature[5],
            "created_timestamp": feature[6],
        },
        separators=(",", ":"),
    )
