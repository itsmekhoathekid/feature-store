package io.github.itsmekhoathekid.featurestore;

import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class DeduplicateByEventId
        extends KeyedProcessFunction<String, TransactionEvent, TransactionEvent> {
    enum Decision {
        ACCEPT,
        IGNORE_EXACT_DUPLICATE,
        REJECT_CONFLICT
    }

    private final OutputTag<String> invalidOutput;
    private transient ValueState<String> fingerprintState;

    public DeduplicateByEventId(OutputTag<String> invalidOutput) {
        this.invalidOutput = invalidOutput;
    }

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
    }
}
