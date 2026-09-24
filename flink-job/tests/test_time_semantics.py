import cloudpickle
from pyflink.common import Time
from pyflink.datastream.window import SlidingEventTimeWindows

from feature_store_flink.job import watermark_strategy
from feature_store_flink.operators import LATE_OUTPUT, EventTimestampAssigner
from feature_store_flink.types import EVENT_TYPE

HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS


def window_ending_at(assigner, timestamp_ms: int, expected_end_ms: int):
    return next(
        (window for window in assigner.assign_windows(None, timestamp_ms, None)
         if window.end == expected_end_ms),
        None,
    )


def test_window_boundary_is_left_inclusive_and_right_exclusive() -> None:
    assigner = SlidingEventTimeWindows.of(Time.days(30), Time.hours(1))
    window_end = 1_800_000_000_000 // HOUR_MS * HOUR_MS

    assert window_ending_at(assigner, window_end - 30 * DAY_MS, window_end) is not None
    assert window_ending_at(assigner, window_end - 1, window_end) is not None
    assert window_ending_at(assigner, window_end, window_end) is None


def test_sliding_windows_are_aligned_hourly() -> None:
    assigner = SlidingEventTimeWindows.of(Time.days(30), Time.hours(1))
    windows = assigner.assign_windows(None, 1_800_000_123_456, None)

    assert len(windows) == 30 * 24
    assert all(window.start % HOUR_MS == 0 for window in windows)
    assert all(window.end - window.start == 30 * DAY_MS for window in windows)


def test_timestamp_assigner_reads_event_time() -> None:
    event = ("event-1", "customer-001", 10.0, 1_700_000_000_123)
    assert EventTimestampAssigner().extract_timestamp(event, -1) == event[3]


def test_watermark_strategy_keeps_idleness_and_python_timestamp_assigner() -> None:
    strategy = watermark_strategy()
    assert isinstance(strategy._timestamp_assigner, EventTimestampAssigner)
    assert "WatermarkStrategyWithIdleness" in strategy._j_watermark_strategy.getClass().getName()


def test_late_output_type_remains_serializable_after_main_type_reaches_java() -> None:
    EVENT_TYPE.get_java_type_info()
    cloudpickle.dumps(LATE_OUTPUT)
