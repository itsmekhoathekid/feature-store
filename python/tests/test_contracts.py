from feature_store_demo.contracts import feature_push_payload


def test_feature_push_payload_uses_column_oriented_feast_contract() -> None:
    payload = feature_push_payload(
        [
            {
                "customer_id": "customer-1",
                "total_amount_30d": 12.5,
                "tx_count_30d": 2,
                "event_timestamp": "2026-09-24T10:00:00Z",
                "created_timestamp": "2026-09-24T11:00:00Z",
            }
        ]
    )
    assert payload["push_source_name"] == "flink_customer_30d_push"
    assert payload["to"] == "online_and_offline"
    assert payload["df"]["customer_id"] == ["customer-1"]
    assert payload["df"]["tx_count_30d"] == [2]
