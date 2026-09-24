import os

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.window import SlidingEventTimeWindows

from feature_store_flink.operators import (
    INVALID_OUTPUT,
    LATE_OUTPUT,
    AddWindowMetadata,
    DeduplicateByEventId,
    EventTimestampAssigner,
    ParseAndValidate,
    TransactionAggregate,
    event_to_json,
    feature_to_json,
)
from feature_store_flink.types import (
    ACCUMULATOR_TYPE,
    CUSTOMER_ID,
    EVENT_ID,
    EVENT_TYPE,
    FEATURE_TYPE,
)

RAW_TOPIC = "raw.transactions.v1"
FEATURE_TOPIC = "features.customer_30d.v1"
LATE_TOPIC = "transactions.late.v1"
INVALID_TOPIC = "transactions.invalid.v1"


def watermark_strategy() -> WatermarkStrategy:
    # with_idleness() returns a new PyFlink wrapper, so attach the Python timestamp assigner last.
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_hours(1))
        .with_idleness(Duration.of_minutes(1))
        .with_timestamp_assigner(EventTimestampAssigner())
    )


def kafka_sink(bootstrap_servers: str, topic: str, transactional_prefix: str) -> KafkaSink:
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(transactional_prefix)
        .set_property("transaction.timeout.ms", "900000")
        .build()
    )


def build_job(environment: StreamExecutionEnvironment, bootstrap_servers: str) -> None:
    environment.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = environment.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(5_000)
    checkpoint_config.set_checkpoint_timeout(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(RAW_TOPIC)
        .set_group_id("pyflink-customer-feature-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = environment.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "raw-transactions-kafka-source",
    )
    parsed = raw.process(ParseAndValidate(), output_type=EVENT_TYPE).name("validate-and-parse")

    timestamped = parsed.assign_timestamps_and_watermarks(watermark_strategy()).name(
        "event-time-watermarks"
    )
    deduplicated = (
        timestamped.key_by(lambda event: event[EVENT_ID], key_type=Types.STRING())
        .process(DeduplicateByEventId(), output_type=EVENT_TYPE)
        .name("deduplicate-by-event-id")
        .uid("deduplicate-by-event-id")
    )
    features = (
        deduplicated.key_by(lambda event: event[CUSTOMER_ID], key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.days(30), Time.hours(1)))
        .allowed_lateness(0)
        .side_output_late_data(LATE_OUTPUT)
        .aggregate(
            TransactionAggregate(),
            AddWindowMetadata(),
            accumulator_type=ACCUMULATOR_TYPE,
            output_type=FEATURE_TYPE,
        )
        .name("customer-30-day-aggregate")
        .uid("customer-30-day-aggregate")
    )

    feature_json = features.map(feature_to_json, output_type=Types.STRING())
    feature_json.sink_to(kafka_sink(bootstrap_servers, FEATURE_TOPIC, "customer-feature-")) \
        .name("aggregate-kafka-sink") \
        .uid("aggregate-kafka-sink")

    late_json = features.get_side_output(LATE_OUTPUT).map(
        event_to_json, output_type=Types.STRING()
    )
    late_json.sink_to(kafka_sink(bootstrap_servers, LATE_TOPIC, "late-event-")) \
        .name("late-event-kafka-sink") \
        .uid("late-event-kafka-sink")

    invalid = parsed.get_side_output(INVALID_OUTPUT).union(
        deduplicated.get_side_output(INVALID_OUTPUT)
    )
    invalid.sink_to(kafka_sink(bootstrap_servers, INVALID_TOPIC, "invalid-event-")) \
        .name("invalid-event-kafka-sink") \
        .uid("invalid-event-kafka-sink")


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    environment = StreamExecutionEnvironment.get_execution_environment()
    build_job(environment, bootstrap_servers)
    environment.execute("customer-30-day-streaming-features-pyflink")
