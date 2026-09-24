import time
from datetime import UTC, datetime, timedelta

import pandas as pd

from feast import FeatureStore
from feature_store_demo.config import DATA_DIR, FEAST_REPO_PATH


def wait_for_raw_events(minimum_rows: int = 5, timeout_seconds: int = 60) -> None:
    path = DATA_DIR / "raw" / "transactions.parquet"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists() and len(pd.read_parquet(path)) >= minimum_rows:
            return
        time.sleep(1)
    raise TimeoutError(f"Raw Parquet mirror did not reach {minimum_rows} rows")


def main() -> None:
    wait_for_raw_events()
    store = FeatureStore(repo_path=FEAST_REPO_PATH)
    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = current_hour - timedelta(microseconds=1)
    start = end - timedelta(days=31)
    store.materialize(start, end, feature_views=["customer_30d_sfv"])
    print("Materialized customer_30d_sfv into the online store")


if __name__ == "__main__":
    main()
