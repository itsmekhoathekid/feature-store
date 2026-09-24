package io.github.itsmekhoathekid.featurestore;

import io.github.itsmekhoathekid.featurestore.model.AggregateResult;
import io.github.itsmekhoathekid.featurestore.model.CustomerAccumulator;
import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

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
