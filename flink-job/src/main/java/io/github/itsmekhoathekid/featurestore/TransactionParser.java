package io.github.itsmekhoathekid.featurestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class TransactionParser extends ProcessFunction<String, TransactionEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final OutputTag<String> invalidOutput;

    public TransactionParser(OutputTag<String> invalidOutput) {
        this.invalidOutput = invalidOutput;
    }

    @Override
    public void processElement(String json, Context context, Collector<TransactionEvent> output) {
        try {
            TransactionEvent event = MAPPER.readValue(json, TransactionEvent.class);
            event.validate();
            output.collect(event);
        } catch (Exception error) {
            context.output(invalidOutput, json + "\terror=" + error.getMessage());
        }
    }
}
