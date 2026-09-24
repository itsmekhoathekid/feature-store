# Demo feature store với Feast và Flink

Repo này triển khai đúng hai cách tính feature giao dịch 30 ngày:

1. Feast `StreamFeatureView` chạy aggregation bằng Spark compute engine.
2. Flink DataStream API dùng `AggregateFunction`, ghi aggregate sang Kafka rồi push vào Feast bằng `PushSource`.

Không có pipeline thứ ba. Toàn bộ runtime chạy bằng Docker Compose; dependency Python, Maven và Docker image đều được pin phiên bản, các image nền còn được pin theo digest.

## Kiến trúc

Pipeline Feast đi theo luồng: raw event → Kafka → raw mirror → Parquet → Feast `StreamFeatureView`/Spark → Redis.

Pipeline Flink đi theo luồng: raw event → Kafka → validate → watermark → deduplicate → sliding window → aggregate topic → Python pusher → Feast `/push` → Redis và Parquet.

Các topic chính:

| Topic | Vai trò |
|---|---|
| `raw.transactions.v1` | Raw transaction đầu vào |
| `features.customer_30d.v1` | Kết quả cửa sổ do Flink phát ra |
| `transactions.late.v1` | Event đến sau khi cửa sổ đã đóng |
| `transactions.invalid.v1` | Payload sai contract hoặc trùng `event_id` nhưng khác nội dung |
| `features.customer_30d.push-dlq.v1` | Aggregate Feast từ chối vĩnh viễn |

## Chạy demo

Yêu cầu Docker Engine/Desktop có Compose v2 và nên cấp tối thiểu 8 GB RAM. Từ clone sạch, chạy `make demo-flink`, sau đó `make demo-sfv`, rồi `make verify`. Lệnh cuối kiểm tra cả online store và parity tại cùng `customer_id`/`window_end`. Dùng `make down` để dừng stack.

Các lệnh công khai khác: `make up-sfv`, `make up-flink`, `make build-flink`, `make submit-flink`, `make seed`, `make verify-flink-topics`, `make test`, và `make test-restart`.

`make demo-flink` đã bao gồm build, submit job, seed, kiểm tra Kafka bằng consumer `read_committed`, kiểm tra late side output và xác minh Feast online. `make demo-sfv` đã bao gồm dựng stack, seed và materialize `StreamFeatureView`.

## Data contract và ngữ nghĩa thời gian

Raw transaction:

| Field | Type |
|---|---|
| `event_id` | string |
| `customer_id` | string |
| `amount` | double |
| `event_timestamp_ms` | int64 |

Feature output:

| Field | Type |
|---|---|
| `customer_id` | string |
| `total_amount_30d` | float64 |
| `tx_count_30d` | int64 |
| `event_timestamp` | timestamp, bằng `window_end` |
| `created_timestamp` | timestamp lúc Flink phát record |

Cửa sổ là `[T-30 days, T)`, trượt mỗi giờ. Mỗi event thuộc 720 cửa sổ. Watermark cho phép out-of-order tối đa một giờ; partition im lặng một phút được đánh dấu idle. `allowedLateness=0`, vì vậy event tới sau watermark được đưa sang late topic thay vì cập nhật lại kết quả đã phát.

## Cách 1 — Feast StreamFeatureView

### Kafka source, aggregation 30 ngày và tiling

Source: [`feast/features.py`, lines 22–65](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/feast/features.py#L22-L65)

```python
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
```

> **Note**
> Kafka khai báo JSON contract, event timestamp, watermark một giờ và batch source Parquet. Hai aggregation dùng cùng cửa sổ 30 ngày/slide một giờ; tiling một giờ tái sử dụng các tile trung gian. Raw mirror loại duplicate theo `event_id` trước khi Spark materialize vào Redis. `offline=False` tránh ghi aggregate ngược vào file raw.

Feast 0.66.0 đánh dấu `StreamFeatureView` là alpha. Phiên bản này có hai lỗi ở Spark worker khi giải mã proto và khi ánh xạ schema sau aggregation; image áp dụng một compatibility shim nhỏ, có test riêng, tại [`docker/feast_serde.py`](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/docker/feast_serde.py).

## Cách 2 — Flink DataStream API

### Watermark theo event time

Source: [`CustomerFeatureJob.java`, lines 62–69](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/CustomerFeatureJob.java#L62-L69)

```java
        WatermarkStrategy<TransactionEvent> watermarks = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, previousTimestamp) -> event.getEventTimestampMs())
                .withIdleness(IDLE_TIMEOUT);

        SingleOutputStreamOperator<TransactionEvent> timestamped = parsed
                .assignTimestampsAndWatermarks(watermarks)
                .name("event-time-watermarks");
```

> **Note**
> `MAX_OUT_OF_ORDERNESS` là một giờ và `IDLE_TIMEOUT` là một phút. Watermark quyết định lúc cửa sổ đóng; idleness ngăn partition không có dữ liệu giữ watermark chung đứng yên. Bước tiếp theo key stream theo `event_id` để deduplicate.

### Sliding window và operator chain

Source: [`CustomerFeatureJob.java`, lines 76–82](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/CustomerFeatureJob.java#L76-L82)

```java
        SingleOutputStreamOperator<CustomerFeatureRecord> features = deduplicated
                .keyBy(TransactionEvent::getCustomerId)
                .window(SlidingEventTimeWindows.of(Duration.ofDays(30), Duration.ofHours(1)))
                .allowedLateness(Duration.ZERO)
                .sideOutputLateData(lateOutput)
                .aggregate(new TransactionAggregate(), new WindowResultFunction())
                .name("customer-30-day-aggregate");
```

> **Note**
> `SlidingEventTimeWindows` tạo window 30 ngày theo từng giờ và giữ biên phải loại trừ. `AggregateFunction` cập nhật state tăng dần; `ProcessWindowFunction` chỉ thêm key và metadata. Event đã trễ được tách ra, không làm thay đổi feature đã phát.

### Incremental AggregateFunction

Source: [`TransactionAggregate.java`, lines 8–34](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/TransactionAggregate.java#L8-L34)

```java
public final class TransactionAggregate
        implements AggregateFunction<TransactionEvent, CustomerAccumulator, AggregateResult> {

    @Override
    public CustomerAccumulator createAccumulator() {
        return new CustomerAccumulator(0.0, 0L);
    }

    @Override
    public CustomerAccumulator add(TransactionEvent event, CustomerAccumulator accumulator) {
        accumulator.setTotalAmount(accumulator.getTotalAmount() + event.getAmount());
        accumulator.setTxCount(accumulator.getTxCount() + 1L);
        return accumulator;
    }

    @Override
    public AggregateResult getResult(CustomerAccumulator accumulator) {
        return new AggregateResult(accumulator.getTotalAmount(), accumulator.getTxCount());
    }

    @Override
    public CustomerAccumulator merge(CustomerAccumulator left, CustomerAccumulator right) {
        return new CustomerAccumulator(
                left.getTotalAmount() + right.getTotalAmount(),
                left.getTxCount() + right.getTxCount());
    }
}
```

> **Note**
> Accumulator chỉ giữ tổng tiền và số giao dịch, không giữ toàn bộ raw event. `add()` chạy cho từng event; `getResult()` tạo output; `merge()` cộng hai partial accumulator để vẫn đúng khi Flink hợp nhất state. Cả bốn method đều có unit test.

### Bổ sung key và metadata cửa sổ

Source: [`WindowResultFunction.java`, lines 10–31](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/WindowResultFunction.java#L10-L31)

```java
public final class WindowResultFunction
        extends ProcessWindowFunction<AggregateResult, CustomerFeatureRecord, String, TimeWindow> {

    @Override
    public void process(
            String customerId,
            Context context,
            Iterable<AggregateResult> aggregateResults,
            Collector<CustomerFeatureRecord> output) {
        AggregateResult aggregate = aggregateResults.iterator().next();
        long windowEnd = context.window().getEnd();
        output.collect(
                new CustomerFeatureRecord(
                        customerId,
                        aggregate.getTotalAmount(),
                        aggregate.getTxCount(),
                        context.window().getStart(),
                        windowEnd,
                        Instant.ofEpochMilli(windowEnd).toString(),
                        Instant.now().toString()));
    }
}
```

> **Note**
> Hàm này nhận đúng một kết quả đã aggregate, nên không buffer raw events. `event_timestamp` chính là `window_end`; điều đó cho phép Feast chọn bản ghi mới nhất theo entity và cho phép parity test ghép hai pipeline tại cùng cửa sổ.

### Deduplicate bằng ValueState và TTL

Source: [`DeduplicateByEventId.java`, lines 28–68](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/DeduplicateByEventId.java#L28-L68)

```java
    @Override
    public void open(OpenContext context) throws Exception {
        StateTtlConfig ttl = stateTtlConfig();
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("event-fingerprint", String.class);
        descriptor.enableTimeToLive(ttl);
        fingerprintState = getRuntimeContext().getState(descriptor);
    }

    static StateTtlConfig stateTtlConfig() {
        return StateTtlConfig.newBuilder(Duration.ofDays(32))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }

    static Decision classify(String storedFingerprint, TransactionEvent event) {
        if (storedFingerprint == null) {
            return Decision.ACCEPT;
        }
        if (storedFingerprint.equals(event.fingerprint())) {
            return Decision.IGNORE_EXACT_DUPLICATE;
        }
        return Decision.REJECT_CONFLICT;
    }

    @Override
    public void processElement(
            TransactionEvent event,
            Context context,
            Collector<TransactionEvent> output) throws Exception {
        String storedFingerprint = fingerprintState.value();
        Decision decision = classify(storedFingerprint, event);
        if (decision == Decision.ACCEPT) {
            fingerprintState.update(event.fingerprint());
            output.collect(event);
        } else if (decision == Decision.REJECT_CONFLICT) {
            context.output(
                    invalidOutput,
                    "conflicting duplicate event_id=" + event.getEventId());
        }
```

> **Note**
> Stream được key theo `event_id`, vì vậy mỗi key có một fingerprint trong `ValueState`. Duplicate giống hệt bị bỏ qua; cùng ID nhưng khác nội dung đi sang invalid topic. TTL 32 ngày dài hơn feature window hai ngày và dùng processing time; test harness xác minh cả suppression lẫn expiry.

### Late-event side output

Source: [`CustomerFeatureJob.java`, lines 76–88](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/flink-job/src/main/java/io/github/itsmekhoathekid/featurestore/CustomerFeatureJob.java#L76-L88)

```java
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
```

> **Note**
> Khi watermark đã vượt `window_end`, `allowedLateness=0` làm event đi thẳng sang side output và Kafka late topic. Integration test phát một probe cũ 32 ngày sau khi watermark tiến lên và chỉ pass khi consumer `read_committed` đọc được probe ở topic này.

## Đưa aggregate từ Flink vào Feast

### PushSource và online/offline stores

Source: [`feast/features.py`, lines 67–90](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/feast/features.py#L67-L90)

```python
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
```

> **Note**
> Pusher không aggregate lại. `PushSource` nhận đúng schema do Flink tạo; Feast ghi online vào Redis và offline vào Parquet. TTL 31 ngày bao phủ feature window 30 ngày và thêm một ngày vận hành.

### HTTP push có retry

Source: [`pusher.py`, lines 59–71](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/python/feature_store_demo/pusher.py#L59-L71)

```python
def push_with_retry(records: list[dict[str, Any]], attempts: int = 5) -> requests.Response:
    payload = feature_push_payload(records)
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.post(f"{FEAST_SERVER_URL}/push", json=payload, timeout=20)
            if response.status_code < 500:
                return response
            last_error = RuntimeError(f"Feast returned {response.status_code}: {response.text}")
        except requests.RequestException as error:
            last_error = error
        time.sleep(min(2**attempt, 10))
    raise RuntimeError("Feast push failed after retries") from last_error
```

> **Note**
> Batch được chuyển thành payload cột của Feast và gửi tới `/push`. Lỗi mạng hoặc HTTP 5xx được retry theo exponential backoff; 4xx là lỗi vĩnh viễn và được đưa sang push DLQ ở bước gọi bên dưới.

### Chỉ commit Kafka sau khi push thành công

Source: [`pusher.py`, lines 141–152](https://github.com/itsmekhoathekid/feature-store/blob/6b4944bc6b73319b3ad291794d26ff0ea2db671c/python/feature_store_demo/pusher.py#L141-L152)

```python
    if pending:
        records = [record for _, record in pending]
        response = push_with_retry(records)
        if response.status_code >= 400:
            reason = f"Feast HTTP {response.status_code}: {response.text}"
            for message, _ in pending:
                publish_dlq(producer, message, reason)
        else:
            ledger.mark(records)
        committable.extend(message for message, _ in pending)

    commit_messages(consumer, committable)
```

> **Note**
> Consumer dùng `read_committed` và tắt auto commit. Sau HTTP 2xx, pusher ghi ledger theo `(customer_id, window_end_ms)` rồi mới commit offset; poison record chỉ được commit sau khi đã vào DLQ. Flink → Kafka là exactly-once nhờ checkpoint và transactional sink. Ranh giới Kafka → HTTP vẫn là at-least-once; ledger giảm duplicate khi restart nhưng không tạo distributed transaction với Feast.

## Kiểm thử

`make test` chạy Java unit/operator tests và Python tests/lint. Java test bao phủ bốn method của `AggregateFunction`, `ValueState`/TTL, biên `[T-30d,T)`, hourly slide, watermark và idle partition. `make demo-flink` kiểm tra Kafka → Flink → aggregate topic, `read_committed`, late side output, PushSource → Redis/Parquet. `make demo-sfv` kiểm tra riêng Spark `StreamFeatureView`. `make verify` so parity tại đúng timestamp cửa sổ của SFV. `make test-restart` stop job bằng canonical savepoint, restore state, seed duplicate rồi xác minh kết quả không đổi.

CI chạy Java, Python và Docker Compose smoke test. `scripts/check_readme_references.py` yêu cầu mọi code block khớp byte-for-byte với line range tại commit permalink và với source hiện tại; thay code hoặc làm lệch dòng sẽ buộc cập nhật README trước khi merge.

## Tài liệu nền

- [Feast Stream feature view](https://docs.feast.dev/getting-started/concepts/stream-feature-view)
- [Feast tiling](https://docs.feast.dev/getting-started/concepts/tiling)
- [Flink 2.2 DataStream windows và AggregateFunction](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/)
- [Flink Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)

## Giới hạn có chủ đích

Demo dùng một Kafka broker, Redis không persistence, Spark `local[*]` bên trong container Python và một Flink TaskManager. Cửa sổ 30 ngày/slide một giờ tạo 720 window assignment cho mỗi event, phù hợp để học semantics nhưng cần đánh giá state/storage kỹ trước production. Credentials, TLS, schema registry, observability và distributed transaction Kafka–Feast nằm ngoài phạm vi demo.

License: MIT.
