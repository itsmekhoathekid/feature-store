package io.github.itsmekhoathekid.featurestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.itsmekhoathekid.featurestore.model.CustomerFeatureRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

public final class CustomerFeatureJsonSerializationSchema
        implements SerializationSchema<CustomerFeatureRecord> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(CustomerFeatureRecord value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (Exception error) {
            throw new IllegalArgumentException("Unable to serialize customer feature", error);
        }
    }
}
