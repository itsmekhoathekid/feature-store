package io.github.itsmekhoathekid.featurestore;

import io.github.itsmekhoathekid.featurestore.model.CustomerFeatureRecord;
import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.OutputTag;

public final class CustomerFeatureJob {
    static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofHours(1);
    static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);
    private static final String RAW_TOPIC = "raw.transactions.v1";
    private static final String FEATURE_TOPIC = "features.customer_30d.v1";
    private static final String LATE_TOPIC = "transactions.late.v1";
    private static final String INVALID_TOPIC = "transactions.invalid.v1";

    private CustomerFeatureJob() {}

    public static void main(String[] args) throws Exception {
        String bootstrapServers = Optional.ofNullable(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                .orElse("kafka:9092");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        environment.getCheckpointConfig().setCheckpointTimeout(60_000L);

        OutputTag<String> invalidOutput =
                new OutputTag<>("invalid-transactions", TypeInformation.of(String.class));
        OutputTag<TransactionEvent> lateOutput =
                new OutputTag<>("late-transactions", TypeInformation.of(TransactionEvent.class));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(RAW_TOPIC)
                .setGroupId("flink-customer-feature-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = environment.fromSource(
                source, WatermarkStrategy.noWatermarks(), "raw-transactions-kafka-source");

        SingleOutputStreamOperator<TransactionEvent> parsed = raw
                .process(new TransactionParser(invalidOutput))
                .name("validate-and-parse");

        WatermarkStrategy<TransactionEvent> watermarks = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, previousTimestamp) -> event.getEventTimestampMs())
                .withIdleness(IDLE_TIMEOUT);

        SingleOutputStreamOperator<TransactionEvent> timestamped = parsed
                .assignTimestampsAndWatermarks(watermarks)
                .name("event-time-watermarks");

        SingleOutputStreamOperator<TransactionEvent> deduplicated = timestamped
                .keyBy(TransactionEvent::getEventId)
                .process(new DeduplicateByEventId(invalidOutput))
                .name("deduplicate-by-event-id");

        SingleOutputStreamOperator<CustomerFeatureRecord> features = deduplicated
                .keyBy(TransactionEvent::getCustomerId)
                .window(SlidingEventTimeWindows.of(Duration.ofDays(30), Duration.ofHours(1)))
                .allowedLateness(Duration.ZERO)
                .sideOutputLateData(lateOutput)
                .aggregate(new TransactionAggregate(), new WindowResultFunction())
                .name("customer-30-day-aggregate");

        features.sinkTo(featureSink(bootstrapServers, FEATURE_TOPIC, "customer-feature-"))
                .name("aggregate-kafka-sink");
        features.getSideOutput(lateOutput)
                .sinkTo(transactionSink(bootstrapServers, LATE_TOPIC, "late-event-"))
                .name("late-event-kafka-sink");
        parsed.getSideOutput(invalidOutput)
                .union(deduplicated.getSideOutput(invalidOutput))
                .sinkTo(stringSink(bootstrapServers, INVALID_TOPIC, "invalid-event-"))
                .name("invalid-event-kafka-sink");

        environment.execute("customer-30-day-streaming-features");
    }

    private static KafkaSink<CustomerFeatureRecord> featureSink(
            String bootstrapServers, String topic, String transactionalPrefix) {
        return kafkaSink(
                bootstrapServers,
                topic,
                transactionalPrefix,
                new CustomerFeatureJsonSerializationSchema());
    }

    private static KafkaSink<TransactionEvent> transactionSink(
            String bootstrapServers, String topic, String transactionalPrefix) {
        return kafkaSink(
                bootstrapServers,
                topic,
                transactionalPrefix,
                new TransactionEventJsonSerializationSchema());
    }

    private static KafkaSink<String> stringSink(
            String bootstrapServers, String topic, String transactionalPrefix) {
        return kafkaSink(
                bootstrapServers,
                topic,
                transactionalPrefix,
                new SimpleStringSchema());
    }

    private static <T> KafkaSink<T> kafkaSink(
            String bootstrapServers,
            String topic,
            String transactionalPrefix,
            org.apache.flink.api.common.serialization.SerializationSchema<T> serializer) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "900000");
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(serializer)
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalPrefix)
                .build();
    }
}
