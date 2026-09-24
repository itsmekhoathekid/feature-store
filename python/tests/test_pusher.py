import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from feature_store_demo import pusher
from feature_store_demo.pusher import PushLedger, normalize_record, process_batch


def aggregate_record() -> dict[str, object]:
    return {
        "customer_id": "customer-1",
        "total_amount_30d": 20.0,
        "tx_count_30d": 2,
        "window_end_ms": 1_000,
        "event_timestamp": "1970-01-01T00:00:01Z",
        "created_timestamp": "1970-01-01T00:00:02Z",
    }


def test_ledger_is_idempotent_per_customer_and_window(tmp_path: Path) -> None:
    record = aggregate_record()
    ledger = PushLedger(tmp_path / "ledger.sqlite")
    assert not ledger.contains(record)
    ledger.mark([record])
    ledger.mark([record])
    assert ledger.contains(record)
    ledger.close()


def test_normalize_record_requires_window_identity() -> None:
    record = aggregate_record()
    assert normalize_record(record) == record


class FakeMessage:
    def __init__(self, record: dict[str, object], offset: int) -> None:
        self.record = record
        self._offset = offset

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "features.customer_30d.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return json.dumps(self.record).encode()


class FakeConsumer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.offsets = []

    def commit(self, *, offsets: list, asynchronous: bool) -> None:
        self.events.append("commit")
        self.offsets = offsets
        assert not asynchronous


def test_process_batch_pushes_before_committing_offsets(tmp_path: Path, monkeypatch) -> None:
    events: list[str] = []
    pushed: list[list[dict[str, object]]] = []

    def fake_push(records: list[dict[str, object]]):
        events.append("push")
        pushed.append(records)
        return SimpleNamespace(status_code=202, text="")

    monkeypatch.setattr(pusher, "push_with_retry", fake_push)
    ledger = PushLedger(tmp_path / "ledger.sqlite")
    records = [aggregate_record(), {**aggregate_record(), "window_end_ms": 2_000}]
    messages = [FakeMessage(record, index) for index, record in enumerate(records)]
    consumer = FakeConsumer(events)

    process_batch(consumer, object(), ledger, messages)

    assert events == ["push", "commit"]
    assert pushed == [records]
    assert consumer.offsets[0].offset == 2
    assert all(ledger.contains(record) for record in records)
    ledger.close()


def test_process_batch_does_not_commit_when_push_fails(tmp_path: Path, monkeypatch) -> None:
    def fail_push(_records):
        raise RuntimeError("Feast unavailable")

    monkeypatch.setattr(pusher, "push_with_retry", fail_push)
    ledger = PushLedger(tmp_path / "ledger.sqlite")
    consumer = FakeConsumer([])

    with pytest.raises(RuntimeError, match="Feast unavailable"):
        process_batch(consumer, object(), ledger, [FakeMessage(aggregate_record(), 0)])

    assert consumer.offsets == []
    assert not ledger.contains(aggregate_record())
    ledger.close()
