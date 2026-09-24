from pathlib import Path

import pandas as pd

from feature_store_demo.config import DATA_DIR


def ensure_parquet(path: Path, columns: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        frame = pd.DataFrame({column: pd.Series(dtype=dtype) for column, dtype in columns.items()})
        frame.to_parquet(path, index=False)


def main() -> None:
    ensure_parquet(
        DATA_DIR / "raw" / "transactions.parquet",
        {
            "event_id": "string",
            "customer_id": "string",
            "amount": "float64",
            "event_timestamp_ms": "int64",
            "event_timestamp": "datetime64[ns, UTC]",
            "created_timestamp": "datetime64[ns, UTC]",
        },
    )
    ensure_parquet(
        DATA_DIR / "offline" / "customer_30d.parquet",
        {
            "customer_id": "string",
            "total_amount_30d": "float64",
            "tx_count_30d": "int64",
            "event_timestamp": "datetime64[ns, UTC]",
            "created_timestamp": "datetime64[ns, UTC]",
        },
    )
    (DATA_DIR / "ledger").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    main()
