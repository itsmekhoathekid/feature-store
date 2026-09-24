# Demo feature store với Feast và Flink

Repo này triển khai đúng hai cách tính feature giao dịch 30 ngày:

1. Feast `StreamFeatureView` chạy aggregation bằng Spark compute engine.
2. PyFlink DataStream API dùng `AggregateFunction`, ghi aggregate sang Kafka rồi push vào Feast bằng `PushSource`.

Không có pipeline thứ ba. Toàn bộ runtime chạy bằng Docker Compose; dependency Python, Kafka connector và Docker image đều được pin phiên bản, các image nền còn được pin theo digest.

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

Source: [`feast/features.py`, lines 22–65](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/feast/features.py#L22-L65)

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

Feast 0.66.0 đánh dấu `StreamFeatureView` là alpha. Phiên bản này có hai lỗi ở Spark worker khi giải mã proto và khi ánh xạ schema sau aggregation; image áp dụng một compatibility shim nhỏ, có test riêng, tại [`docker/feast_serde.py`](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/docker/feast_serde.py).

## Cách 2 — PyFlink DataStream API

### Watermark theo event time

Source: [`job.py`, lines 40–46](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/job.py#L40-L46)

```python
def watermark_strategy() -> WatermarkStrategy:
    # with_idleness() returns a new PyFlink wrapper, so attach the Python timestamp assigner last.
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_hours(1))
        .with_idleness(Duration.of_minutes(1))
        .with_timestamp_assigner(EventTimestampAssigner())
    )
```

> **Note**
> Bounded out-of-orderness là một giờ và idle timeout là một phút. Watermark quyết định lúc cửa sổ đóng; idleness ngăn partition không có dữ liệu giữ watermark chung đứng yên. Với PyFlink, `with_idleness()` tạo wrapper mới nên timestamp assigner phải gắn sau cùng để không bị mất.

### Sliding window và operator chain

Source: [`job.py`, lines 92–111](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/job.py#L92-L111)

```python
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
```

> **Note**
> Stream được key theo `event_id` trước khi deduplicate, rồi key lại theo `customer_id`. `SlidingEventTimeWindows` tạo window 30 ngày theo từng giờ và giữ biên phải loại trừ. UID ổn định giúp stateful operator khôi phục từ savepoint; event đã trễ được tách ra thay vì sửa feature đã phát.

### Incremental AggregateFunction

Source: [`operators.py`, lines 105–124](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/operators.py#L105-L124)

```python
class TransactionAggregate(AggregateFunction):
    def create_accumulator(self) -> tuple[float, int]:
        return 0.0, 0

    def add(
        self,
        value: tuple[str, str, float, int],
        accumulator: tuple[float, int],
    ) -> tuple[float, int]:
        return accumulator[0] + value[AMOUNT], accumulator[1] + 1

    def get_result(self, accumulator: tuple[float, int]) -> tuple[float, int]:
        return accumulator

    def merge(
        self,
        accumulator_a: tuple[float, int],
        accumulator_b: tuple[float, int],
    ) -> tuple[float, int]:
        return accumulator_a[0] + accumulator_b[0], accumulator_a[1] + accumulator_b[1]
```

> **Note**
> Đây là `pyflink.datastream.functions.AggregateFunction` native. Accumulator chỉ giữ tổng tiền và số giao dịch, không giữ toàn bộ raw event. `add()` chạy cho từng event; `get_result()` tạo output; `merge()` cộng hai partial accumulator. Cả bốn method đều có unit test.

### Bổ sung key và metadata cửa sổ

Source: [`operators.py`, lines 131–149](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/operators.py#L131-L149)

```python
class AddWindowMetadata(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[tuple[float, int]],
    ) -> Iterable[tuple[str, float, int, int, int, str, str]]:
        total_amount, tx_count = next(iter(elements))
        window = context.window()
        window_end_ms = window.end
        yield (
            key,
            total_amount,
            tx_count,
            window.start,
            window_end_ms,
            iso_timestamp(window_end_ms),
            datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
```

> **Note**
> `ProcessWindowFunction` nhận đúng một kết quả incremental aggregate, nên không buffer raw events lần thứ hai. Hàm bổ sung customer, `window_start` và `window_end`; `event_timestamp` bằng `window_end` để Feast chọn bản mới nhất và parity test ghép hai pipeline tại cùng cửa sổ.

### Deduplicate bằng ValueState và TTL

Source: [`operators.py`, lines 77–102](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/operators.py#L77-L102)

```python
class DeduplicateByEventId(KeyedProcessFunction):
    TTL_DAYS = 32

    def open(self, runtime_context) -> None:
        ttl = (
            StateTtlConfig.new_builder(Time.days(self.TTL_DAYS))
            .update_ttl_on_create_and_write()
            .never_return_expired()
            .build()
        )
        descriptor = ValueStateDescriptor("event-fingerprint", Types.STRING())
        descriptor.enable_time_to_live(ttl)
        self.fingerprint_state = runtime_context.get_state(descriptor)

    def process_element(
        self,
        value: tuple[str, str, float, int],
        ctx: KeyedProcessFunction.Context,
    ):
        fingerprint = event_fingerprint(value)
        stored = self.fingerprint_state.value()
        if stored is None:
            self.fingerprint_state.update(fingerprint)
            yield value
        elif stored != fingerprint:
            yield INVALID_OUTPUT, f"conflicting duplicate event_id={value[EVENT_ID]}"
```

> **Note**
> Stream đã key theo `event_id`, vì vậy mỗi key có một fingerprint trong PyFlink `ValueState`. Duplicate giống hệt không yield output; cùng ID nhưng khác nội dung đi sang invalid topic. TTL processing-time 32 ngày dài hơn feature window hai ngày; test xác minh suppression, conflict và cấu hình TTL/visibility.

### Late-event side output

Source: [`job.py`, lines 98–123](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/flink-job/feature_store_flink/job.py#L98-L123)

```python
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
```

> **Note**
> Khi watermark đã vượt `window_end`, `allowed_lateness(0)` làm event đi sang side output và Kafka late topic. Cả feature sink và late sink dùng Kafka transaction/exactly-once helper. Integration test chỉ pass khi consumer `read_committed` đọc được aggregate và late probe đúng topic.

## Đưa aggregate từ Flink vào Feast

### PushSource và online/offline stores

Source: [`feast/features.py`, lines 67–90](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/feast/features.py#L67-L90)

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

Source: [`pusher.py`, lines 59–71](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/python/feature_store_demo/pusher.py#L59-L71)

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

Source: [`pusher.py`, lines 141–152](https://github.com/itsmekhoathekid/feature-store/blob/eae32219a8871d01245f172f43c008d687486243/python/feature_store_demo/pusher.py#L141-L152)

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

`make test` chạy PyFlink tests và Feast/Pusher tests/lint. PyFlink test bao phủ bốn method của `AggregateFunction`, `ValueState`/TTL, biên `[T-30d,T)`, hourly slide, timestamp assigner, watermark/idleness và validation. `make demo-flink` kiểm tra Kafka → Flink → aggregate topic, `read_committed`, late side output, PushSource → Redis/Parquet. `make demo-sfv` kiểm tra riêng Spark `StreamFeatureView`. `make verify` so parity tại đúng timestamp cửa sổ của SFV. `make test-restart` stop job bằng canonical savepoint, restore state, seed duplicate rồi xác minh kết quả không đổi.

CI chạy PyFlink, Feast/Pusher Python và Docker Compose smoke test. `scripts/check_readme_references.py` yêu cầu mọi code block khớp byte-for-byte với line range tại commit permalink và với source hiện tại; thay code hoặc làm lệch dòng sẽ buộc cập nhật README trước khi merge.

## Tài liệu nền

- [Feast Stream feature view](https://docs.feast.dev/getting-started/concepts/stream-feature-view)
- [Feast tiling](https://docs.feast.dev/getting-started/concepts/tiling)
- [Flink 2.2 DataStream windows và AggregateFunction](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/)
- [Flink Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)

## Giới hạn có chủ đích

Demo dùng một Kafka broker, Redis không persistence, Spark `local[*]` bên trong container Python và một Flink TaskManager. Cửa sổ 30 ngày/slide một giờ tạo 720 window assignment cho mỗi event, phù hợp để học semantics nhưng cần đánh giá state/storage kỹ trước production. Credentials, TLS, schema registry, observability và distributed transaction Kafka–Feast nằm ngoài phạm vi demo.

License: MIT.
