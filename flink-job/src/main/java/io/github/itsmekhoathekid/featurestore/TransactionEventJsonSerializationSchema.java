package io.github.itsmekhoathekid.featurestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;

public final class TransactionEventJsonSerializationSchema
        implements SerializationSchema<TransactionEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(TransactionEvent value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (Exception error) {
            throw new IllegalArgumentException("Unable to serialize transaction event", error);
        }
    }
}
