import argparse
import math
import time
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from feast import FeatureStore
from feature_store_demo.config import DATA_DIR, FEAST_REPO_PATH


def online_values(store: FeatureStore, view: str, customer_id: str) -> dict[str, Any]:
    response = store.get_online_features(
        features=[f"{view}:total_amount_30d", f"{view}:tx_count_30d"],
        entity_rows=[{"customer_id": customer_id}],
    )
    values = {key: rows[0] for key, rows in response.to_dict().items()}
    for name, result in zip(
        response.proto.metadata.feature_names.val,
        response.proto.results,
        strict=True,
    ):
        if name == "total_amount_30d" and result.event_timestamps:
            values["window_end"] = datetime.fromtimestamp(
                result.event_timestamps[0].seconds,
                tz=UTC,
            )
    return values


def wait_for_values(store: FeatureStore, view: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        values = online_values(store, view, "customer-001")
        if values.get("total_amount_30d") is not None:
            return values
        time.sleep(2)
    raise TimeoutError(f"No online values found for {view}")


def assert_expected(values: dict[str, Any]) -> None:
    if not math.isclose(float(values["total_amount_30d"]), 60.0):
        raise AssertionError(f"Expected total_amount_30d=60.0, got {values}")
    if int(values["tx_count_30d"]) != 3:
        raise AssertionError(f"Expected tx_count_30d=3, got {values}")


def assert_same_window(
    flink_values: dict[str, Any],
    sfv_values: dict[str, Any],
) -> dict[str, Any]:
    window_end = sfv_values.get("window_end")
    if window_end is None:
        raise AssertionError(f"SFV result has no feature timestamp: {sfv_values}")

    offline_path = DATA_DIR / "offline" / "customer_30d.parquet"
    frame = pd.read_parquet(offline_path)
    event_timestamps = pd.to_datetime(frame["event_timestamp"], utc=True)
    matching = frame[
        (frame["customer_id"] == "customer-001")
        & (event_timestamps == pd.Timestamp(window_end))
    ]
    if matching.empty:
        raise AssertionError(f"No Flink aggregate at SFV window_end={window_end.isoformat()}")
    latest = matching.iloc[-1]
    if not math.isclose(float(latest["total_amount_30d"]), 60.0) or int(
        latest["tx_count_30d"]
    ) != 3:
        raise AssertionError(f"Flink/SFV parity failed at {window_end.isoformat()}: {latest}")

    if float(sfv_values["total_amount_30d"]) != float(latest["total_amount_30d"]) or int(
        sfv_values["tx_count_30d"]
    ) != int(latest["tx_count_30d"]):
        raise AssertionError(
            f"Pipeline parity failed at {window_end.isoformat()}: "
            f"sfv={sfv_values}, flink_offline={latest.to_dict()}"
        )

    assert_expected(flink_values)
    return {
        "customer_id": latest["customer_id"],
        "total_amount_30d": float(latest["total_amount_30d"]),
        "tx_count_30d": int(latest["tx_count_30d"]),
        "window_end": window_end,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", choices=("sfv", "flink", "all"), default="all")
    parser.add_argument("--wait-seconds", type=int, default=30)
    args = parser.parse_args()
    store = FeatureStore(repo_path=FEAST_REPO_PATH)

    results: dict[str, dict[str, Any]] = {}
    if args.pipeline in {"sfv", "all"}:
        results["sfv"] = wait_for_values(store, "customer_30d_sfv", args.wait_seconds)
        assert_expected(results["sfv"])
    if args.pipeline in {"flink", "all"}:
        results["flink"] = wait_for_values(store, "customer_30d_flink", args.wait_seconds)
        assert_expected(results["flink"])
    if args.pipeline == "all":
        results["flink_at_sfv_window"] = assert_same_window(
            results["flink"],
            results["sfv"],
        )
    print(f"Verified pipeline results: {results}")


if __name__ == "__main__":
    main()
