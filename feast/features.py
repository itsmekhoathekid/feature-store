import os
from datetime import timedelta

from feast.aggregation import Aggregation
from feast.data_format import JsonFormat
from feast.types import Float64, Int64

from feast import Entity, Field, FileSource, KafkaSource, PushSource, StreamFeatureView, ValueType

DATA_DIR = os.getenv("DATA_DIR", "/data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

customer = Entity(name="customer_id", join_keys=["customer_id"], value_type=ValueType.STRING)

raw_transactions_batch = FileSource(
    name="raw_transactions_batch",
    path=f"{DATA_DIR}/raw/transactions.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

raw_transactions_stream = KafkaSource(
    name="raw_transactions_stream",
    kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic="raw.transactions.v1",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    message_format=JsonFormat(
        "event_id STRING, customer_id STRING, amount DOUBLE, event_timestamp_ms BIGINT"
    ),
    batch_source=raw_transactions_batch,
    watermark_delay_threshold=timedelta(hours=1),
)

customer_30d_sfv = StreamFeatureView(
    name="customer_30d_sfv",
    entities=[customer],
    ttl=timedelta(days=31),
    source=raw_transactions_stream,
    schema=[
        Field(name="total_amount_30d", dtype=Float64),
        Field(name="tx_count_30d", dtype=Int64),
    ],
    aggregations=[
        Aggregation(
            column="amount",
            function="sum",
            time_window=timedelta(days=30),
            slide_interval=timedelta(hours=1),
            name="total_amount_30d",
        ),
        Aggregation(
            column="amount",
            function="count",
            time_window=timedelta(days=30),
            slide_interval=timedelta(hours=1),
            name="tx_count_30d",
        ),
    ],
    timestamp_field="event_timestamp",
    enable_tiling=True,
    tiling_hop_size=timedelta(hours=1),
    online=True,
    offline=False,
)

flink_aggregates_batch = FileSource(
    name="flink_aggregates_batch",
    path=f"{DATA_DIR}/offline/customer_30d.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

flink_aggregates_push = PushSource(
    name="flink_customer_30d_push",
    batch_source=flink_aggregates_batch,
)

customer_30d_flink = StreamFeatureView(
    name="customer_30d_flink",
    entities=[customer],
    ttl=timedelta(days=31),
    source=flink_aggregates_push,
    schema=[
        Field(name="total_amount_30d", dtype=Float64),
        Field(name="tx_count_30d", dtype=Int64),
    ],
    online=True,
    offline=True,
)
