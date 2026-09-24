import json

import pytest

from feature_store_flink.operators import parse_transaction


def test_valid_transaction_contract() -> None:
    payload = json.dumps(
        {
            "event_id": "event-1",
            "customer_id": "customer-001",
            "amount": 10,
            "event_timestamp_ms": 1_700_000_000_000,
        }
    )
    assert parse_transaction(payload) == (
        "event-1",
        "customer-001",
        10.0,
        1_700_000_000_000,
    )


@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        "{}",
        '{"event_id":"event-1","customer_id":"customer-001","amount":true,'
        '"event_timestamp_ms":1700000000000}',
        '{"event_id":"event-1","customer_id":"customer-001","amount":1,'
        '"event_timestamp_ms":1.5}',
    ],
)
def test_invalid_transaction_contract(payload: str) -> None:
    with pytest.raises(ValueError):
        parse_transaction(payload)

