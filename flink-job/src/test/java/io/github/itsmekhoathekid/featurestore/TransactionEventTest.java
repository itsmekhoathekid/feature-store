package io.github.itsmekhoathekid.featurestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.itsmekhoathekid.featurestore.model.TransactionEvent;
import org.junit.jupiter.api.Test;

class TransactionEventTest {
    @Test
    void fingerprintIsStableForTheSameLogicalEvent() {
        TransactionEvent left = new TransactionEvent("event-1", "customer-1", 10.0, 100L);
        TransactionEvent right = new TransactionEvent("event-1", "customer-1", 10.0, 100L);
        assertEquals(left.fingerprint(), right.fingerprint());
    }

    @Test
    void rejectsInvalidContracts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TransactionEvent("", "customer-1", 10.0, 100L).validate());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TransactionEvent("event-1", "customer-1", -1.0, 100L).validate());
    }
}
