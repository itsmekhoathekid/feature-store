from pyflink.common import Time
from pyflink.datastream.state import StateTtlConfig

from feature_store_flink.operators import (
    INVALID_OUTPUT,
    DeduplicateByEventId,
    event_fingerprint,
)


class FakeValueState:
    def __init__(self) -> None:
        self.stored = None

    def value(self):
        return self.stored

    def update(self, value) -> None:
        self.stored = value


class FakeRuntimeContext:
    def __init__(self) -> None:
        self.descriptor = None
        self.state = FakeValueState()

    def get_state(self, descriptor):
        self.descriptor = descriptor
        return self.state


def test_value_state_suppresses_exact_duplicate_and_rejects_conflict() -> None:
    runtime = FakeRuntimeContext()
    deduplicate = DeduplicateByEventId()
    deduplicate.open(runtime)
    original = ("event-1", "customer-001", 10.0, 1_700_000_000_000)

    assert list(deduplicate.process_element(original, None)) == [original]
    assert list(deduplicate.process_element(original, None)) == []

    conflict = ("event-1", "customer-001", 99.0, 1_700_000_000_000)
    assert list(deduplicate.process_element(conflict, None)) == [
        (INVALID_OUTPUT, "conflicting duplicate event_id=event-1")
    ]
    assert runtime.state.stored == event_fingerprint(original)


def test_value_state_ttl_is_32_days_and_never_returns_expired_values() -> None:
    runtime = FakeRuntimeContext()
    DeduplicateByEventId().open(runtime)

    ttl = runtime.descriptor._ttl_config
    assert ttl.get_ttl() == Time.days(32)
    assert ttl.get_update_type() == StateTtlConfig.UpdateType.OnCreateAndWrite
    assert ttl.get_state_visibility() == StateTtlConfig.StateVisibility.NeverReturnExpired

