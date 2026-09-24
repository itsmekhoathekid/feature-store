package io.github.itsmekhoathekid.featurestore.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

public class TransactionEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("customer_id")
    private String customerId;

    private double amount;

    @JsonProperty("event_timestamp_ms")
    private long eventTimestampMs;

    public TransactionEvent() {}

    public TransactionEvent(String eventId, String customerId, double amount, long eventTimestampMs) {
        this.eventId = eventId;
        this.customerId = customerId;
        this.amount = amount;
        this.eventTimestampMs = eventTimestampMs;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public long getEventTimestampMs() { return eventTimestampMs; }
    public void setEventTimestampMs(long eventTimestampMs) { this.eventTimestampMs = eventTimestampMs; }

    public void validate() {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("event_id must not be blank");
        }
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("customer_id must not be blank");
        }
        if (!Double.isFinite(amount) || amount < 0) {
            throw new IllegalArgumentException("amount must be a finite non-negative number");
        }
        if (eventTimestampMs <= 0) {
            throw new IllegalArgumentException("event_timestamp_ms must be positive");
        }
    }

    public String fingerprint() {
        return customerId + "|" + Double.toHexString(amount) + "|" + eventTimestampMs;
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (!(value instanceof TransactionEvent that)) return false;
        return Double.compare(amount, that.amount) == 0
                && eventTimestampMs == that.eventTimestampMs
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(customerId, that.customerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, customerId, amount, eventTimestampMs);
    }
}
