package io.github.itsmekhoathekid.featurestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

class WindowAssignmentTest {
    private static final SlidingEventTimeWindows ASSIGNER =
            SlidingEventTimeWindows.of(Duration.ofDays(30), Duration.ofHours(1));
    private static final WindowAssigner.WindowAssignerContext CONTEXT =
            new WindowAssigner.WindowAssignerContext() {
                @Override
                public long getCurrentProcessingTime() {
                    return 0L;
                }
            };

    @Test
    void assignsEachEventToSevenHundredTwentyHourlyWindows() {
        long eventTimestamp = Duration.ofDays(100).toMillis() + Duration.ofMinutes(15).toMillis();
        Collection<TimeWindow> windows = ASSIGNER.assignWindows(
                new TransactionEvent("event-1", "customer-1", 10.0, eventTimestamp),
                eventTimestamp,
                CONTEXT);

        assertEquals(30 * 24, windows.size());
        assertTrue(windows.stream().allMatch(window -> window.getStart() <= eventTimestamp));
        assertTrue(windows.stream().allMatch(window -> eventTimestamp < window.getEnd()));
        assertTrue(windows.stream().allMatch(
                window -> window.getEnd() - window.getStart() == Duration.ofDays(30).toMillis()));
    }

    @Test
    void usesInclusiveStartAndExclusiveEndBoundary() {
        long windowEnd = Duration.ofDays(100).toMillis();
        long windowStart = windowEnd - Duration.ofDays(30).toMillis();

        assertTrue(assignedWindowEnds(windowStart).contains(windowEnd));
        assertTrue(!assignedWindowEnds(windowEnd).contains(windowEnd));
    }

    @Test
    void alignsEveryWindowEndToAnHourlySlide() {
        long eventTimestamp = Duration.ofDays(100).toMillis() + Duration.ofMinutes(15).toMillis();
        Set<Long> ends = assignedWindowEnds(eventTimestamp);

        assertTrue(ends.stream().allMatch(end -> end % Duration.ofHours(1).toMillis() == 0));
        assertEquals(30 * 24, ends.size());
    }

    private static Set<Long> assignedWindowEnds(long eventTimestamp) {
        return ASSIGNER.assignWindows(
                        new TransactionEvent("event-1", "customer-1", 10.0, eventTimestamp),
                        eventTimestamp,
                        CONTEXT)
                .stream()
                .map(TimeWindow::getEnd)
                .collect(Collectors.toSet());
    }
}
