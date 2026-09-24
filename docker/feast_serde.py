"""Compatibility fix for Feast 0.66.0 StreamFeatureView Spark writes.

Feast 0.66.0 serializes both FeatureView and StreamFeatureView into Spark workers,
but always decodes the payload as FeatureViewProto. Its Arrow-to-protobuf writer
also reads the pre-aggregation schema after Spark has produced aggregate columns.
Keep the upstream behavior while fixing those two StreamFeatureView cases.
"""

from dataclasses import dataclass

import dill
from feast.infra.passthrough_provider import PassthroughProvider
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.types import Int64

from feast import FeatureView, Field, StreamFeatureView


def _prepare_stream_aggregation_output(feature_view):
    """Describe the columns emitted by Spark after an SFV aggregation."""
    declared_types = {field.name: field.dtype for field in feature_view.features}
    result_fields = []
    for aggregation in feature_view.aggregations:
        result_name = aggregation.resolved_name(aggregation.time_window)
        dtype = declared_types.get(result_name)
        if dtype is None and aggregation.function in {"count", "count_distinct"}:
            dtype = Int64
        if dtype is None:
            dtype = declared_types.get(aggregation.column)
        if dtype is None:
            raise ValueError(f"No declared type for aggregate output {result_name}")
        result_fields.append(
            Field(
                name=result_name,
                dtype=dtype,
            )
        )

    feature_view.features = result_fields
    feature_view.projection.features = result_fields
    feature_view.batch_source.created_timestamp_column = ""
    return feature_view


@dataclass
class SerializedArtifacts:
    """Serialize objects that are otherwise unsafe to pass to compute workers."""

    feature_view_proto: bytes
    repo_config_byte: bytes
    is_stream_feature_view: bool = False

    @classmethod
    def serialize(cls, feature_view, repo_config):
        return cls(
            feature_view_proto=feature_view.to_proto().SerializeToString(),
            repo_config_byte=dill.dumps(repo_config),
            is_stream_feature_view=isinstance(feature_view, StreamFeatureView),
        )

    def unserialize(self):
        if self.is_stream_feature_view:
            proto = StreamFeatureViewProto()
            proto.ParseFromString(self.feature_view_proto)
            feature_view = StreamFeatureView.from_proto(sfv_proto=proto)
            if feature_view.aggregations:
                feature_view = _prepare_stream_aggregation_output(feature_view)
        else:
            proto = FeatureViewProto()
            proto.ParseFromString(self.feature_view_proto)
            feature_view = FeatureView.from_proto(proto, skip_udf=True)

        repo_config = dill.loads(self.repo_config_byte)
        provider = PassthroughProvider(repo_config)
        return feature_view, provider.online_store, provider.offline_store, repo_config
