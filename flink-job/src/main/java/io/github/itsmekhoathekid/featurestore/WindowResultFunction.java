package io.github.itsmekhoathekid.featurestore;

import io.github.itsmekhoathekid.featurestore.model.AggregateResult;
import io.github.itsmekhoathekid.featurestore.model.CustomerFeatureRecord;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
