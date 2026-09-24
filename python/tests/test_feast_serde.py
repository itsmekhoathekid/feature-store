from datetime import timedelta

from feast.aggregation import Aggregation
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64

from docker.feast_serde import SerializedArtifacts
from feast import Entity, Field, FileSource, PushSource, StreamFeatureView, ValueType


def test_stream_feature_view_round_trip_uses_aggregate_output_schema() -> None:
    batch_source = FileSource(
        name="batch",
        path="/tmp/source.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
    stream_source = PushSource(name="push", batch_source=batch_source)
    entity = Entity(
        name="customer",
        join_keys=["customer_id"],
        value_type=ValueType.STRING,
    )
    view = StreamFeatureView(
        name="customer_features",
        entities=[entity],
        source=stream_source,
        schema=[
            Field(name="total_amount_30d", dtype=Float64),
            Field(name="tx_count_30d", dtype=Int64),
        ],
        aggregations=[
            Aggregation(
                column="amount",
                function="sum",
                time_window=timedelta(days=30),
                name="total_amount_30d",
            ),
            Aggregation(
                column="amount",
                function="count",
                time_window=timedelta(days=30),
                name="tx_count_30d",
            ),
        ],
        timestamp_field="event_timestamp",
    )
    config = RepoConfig(
        project="test",
        provider="local",
        registry="/tmp/registry.db",
        offline_store={"type": "file"},
        online_store={"type": "redis", "connection_string": "localhost:6379"},
        batch_engine={"type": "spark.engine"},
    )

    artifacts = SerializedArtifacts.serialize(view, config)
    restored, _, _, _ = artifacts.unserialize()

    assert artifacts.is_stream_feature_view
    assert isinstance(restored, StreamFeatureView)
    assert [field.name for field in restored.features] == [
        "total_amount_30d",
        "tx_count_30d",
    ]
    assert restored.batch_source.created_timestamp_column == ""
