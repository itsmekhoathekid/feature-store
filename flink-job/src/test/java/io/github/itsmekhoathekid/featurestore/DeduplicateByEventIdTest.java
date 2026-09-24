package io.github.itsmekhoathekid.featurestore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

class DeduplicateByEventIdTest {
    private final TransactionEvent event =
            new TransactionEvent("event-1", "customer-1", 10.0, 100L);

    @Test
    void acceptsNewEventsAndIgnoresExactDuplicates() {
        assertEquals(
                DeduplicateByEventId.Decision.ACCEPT,
                DeduplicateByEventId.classify(null, event));
        assertEquals(
                DeduplicateByEventId.Decision.IGNORE_EXACT_DUPLICATE,
                DeduplicateByEventId.classify(event.fingerprint(), event));
    }

    @Test
    void rejectsAnEventIdReusedWithDifferentData() {
        TransactionEvent conflicting =
                new TransactionEvent("event-1", "customer-1", 999.0, 100L);
        assertEquals(
                DeduplicateByEventId.Decision.REJECT_CONFLICT,
                DeduplicateByEventId.classify(event.fingerprint(), conflicting));
    }

    @Test
    void retainsDeduplicationStateForLongerThanTheFeatureWindow() {
        assertEquals(Duration.ofDays(32), DeduplicateByEventId.stateTtlConfig().getTimeToLive());
    }

    @Test
    void keyedStateSuppressesDuplicatesAndExpiresAfterTtl() throws Exception {
        OutputTag<String> invalidOutput = new OutputTag<>("invalid", Types.STRING);
        DeduplicateByEventId function = new DeduplicateByEventId(invalidOutput);
        try (KeyedOneInputStreamOperatorTestHarness<String, TransactionEvent, TransactionEvent>
                harness = new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(function),
                        TransactionEvent::getEventId,
                        Types.STRING)) {
            harness.open();
            harness.processElement(new StreamRecord<>(event));
            harness.processElement(new StreamRecord<>(event));
            harness.processElement(new StreamRecord<>(
                    new TransactionEvent("event-1", "customer-1", 999.0, 100L)));

            assertEquals(1, harness.getOutput().size());
            assertEquals(1, harness.getSideOutput(invalidOutput).size());

            harness.setStateTtlProcessingTime(
                    Duration.ofDays(32).plusSeconds(1).toMillis());
            harness.processElement(new StreamRecord<>(event));
            assertEquals(2, harness.getOutput().size());
        }
    }
}
