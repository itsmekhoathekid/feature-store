package io.github.itsmekhoathekid.featurestore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.itsmekhoathekid.featurestore.model.AggregateResult;
import io.github.itsmekhoathekid.featurestore.model.CustomerAccumulator;
import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import org.junit.jupiter.api.Test;

class TransactionAggregateTest {
    private final TransactionAggregate aggregate = new TransactionAggregate();

    @Test
    void createsAnEmptyAccumulator() {
        CustomerAccumulator accumulator = aggregate.createAccumulator();
        assertEquals(0.0, accumulator.getTotalAmount());
        assertEquals(0L, accumulator.getTxCount());
    }

    @Test
    void addsEventsIncrementallyAndReturnsTheResult() {
        CustomerAccumulator accumulator = aggregate.createAccumulator();
        aggregate.add(new TransactionEvent("evt-1", "customer-1", 12.5, 1L), accumulator);
        aggregate.add(new TransactionEvent("evt-2", "customer-1", 7.5, 2L), accumulator);

        AggregateResult result = aggregate.getResult(accumulator);
        assertEquals(20.0, result.getTotalAmount());
        assertEquals(2L, result.getTxCount());
    }

    @Test
    void mergesPartialAccumulators() {
        CustomerAccumulator merged = aggregate.merge(
                new CustomerAccumulator(10.0, 2L),
                new CustomerAccumulator(25.5, 3L));
        assertEquals(35.5, merged.getTotalAmount());
        assertEquals(5L, merged.getTxCount());
    }
}
