package io.github.itsmekhoathekid.featurestore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarksWithIdleness;
import org.apache.flink.util.clock.ManualClock;
import org.junit.jupiter.api.Test;

class WatermarkBehaviorTest {
    @Test
    void emitsOneHourBoundedOutOfOrdernessWatermark() {
        long eventTimestamp = Duration.ofDays(100).toMillis();
        BoundedOutOfOrdernessWatermarks<TransactionEvent> generator =
                new BoundedOutOfOrdernessWatermarks<>(CustomerFeatureJob.MAX_OUT_OF_ORDERNESS);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(
                new TransactionEvent("event-1", "customer-1", 10.0, eventTimestamp),
                eventTimestamp,
                output);
        generator.onPeriodicEmit(output);

        assertEquals(
                eventTimestamp - Duration.ofHours(1).toMillis() - 1,
                output.watermarks.get(0).getTimestamp());
    }

    @Test
    void marksAQuietPartitionIdleAfterOneMinute() {
        ManualClock clock = new ManualClock();
        WatermarksWithIdleness<TransactionEvent> generator = new WatermarksWithIdleness<>(
                new BoundedOutOfOrdernessWatermarks<>(CustomerFeatureJob.MAX_OUT_OF_ORDERNESS),
                CustomerFeatureJob.IDLE_TIMEOUT,
                clock);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(
                new TransactionEvent("event-1", "customer-1", 10.0, 1L),
                1L,
                output);
        generator.onPeriodicEmit(output);
        clock.advanceTime(Duration.ofMillis(1));
        generator.onPeriodicEmit(output);
        clock.advanceTime(Duration.ofSeconds(59));
        generator.onPeriodicEmit(output);
        assertEquals(0, output.idleCount);

        clock.advanceTime(Duration.ofSeconds(2));
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCount);
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final List<Watermark> watermarks = new ArrayList<>();
        private int idleCount;

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark);
        }

        @Override
        public void markIdle() {
            idleCount++;
        }

        @Override
        public void markActive() {}
    }
}
