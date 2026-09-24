from feature_store_flink.operators import AddWindowMetadata, TransactionAggregate


class Window:
    start = 1_700_000_000_000
    end = start + 3_600_000


class Context:
    @staticmethod
    def window() -> Window:
        return Window()


def event(event_id: str, amount: float):
    return event_id, "customer-001", amount, 1_700_000_000_000


def test_create_accumulator() -> None:
    assert TransactionAggregate().create_accumulator() == (0.0, 0)


def test_add_is_incremental() -> None:
    aggregate = TransactionAggregate()
    accumulator = aggregate.add(event("event-1", 10.5), aggregate.create_accumulator())
    assert aggregate.add(event("event-2", 4.5), accumulator) == (15.0, 2)


def test_get_result() -> None:
    assert TransactionAggregate().get_result((15.0, 2)) == (15.0, 2)


def test_merge_partial_accumulators() -> None:
    assert TransactionAggregate().merge((10.5, 1), (4.5, 1)) == (15.0, 2)


def test_process_window_adds_key_and_window_metadata() -> None:
    result = list(AddWindowMetadata().process("customer-001", Context(), [(15.0, 2)]))
    assert result[0][:5] == (
        "customer-001",
        15.0,
        2,
        Window.start,
        Window.end,
    )
    assert result[0][5].endswith("Z")
    assert result[0][6].endswith("Z")

