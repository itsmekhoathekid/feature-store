from datetime import UTC, datetime

from feature_store_demo.seed import fixture_events


def test_fixture_contains_an_exact_duplicate_and_expected_customer_total() -> None:
    events = fixture_events(datetime(2026, 9, 24, 12, tzinfo=UTC))
    customer_events = [event for event in events if event.customer_id == "customer-001"]
    unique = {event.event_id: event for event in customer_events}
    assert len(customer_events) == 4
    assert len(unique) == 3
    assert sum(event.amount for event in unique.values()) == 60.0
