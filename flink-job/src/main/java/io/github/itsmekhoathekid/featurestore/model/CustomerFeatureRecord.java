package io.github.itsmekhoathekid.featurestore.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class CustomerFeatureRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("total_amount_30d")
    private double totalAmount30d;

    @JsonProperty("tx_count_30d")
    private long txCount30d;

    @JsonProperty("window_start_ms")
    private long windowStartMs;

    @JsonProperty("window_end_ms")
    private long windowEndMs;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("created_timestamp")
    private String createdTimestamp;

    public CustomerFeatureRecord() {}

    public CustomerFeatureRecord(
            String customerId,
            double totalAmount30d,
            long txCount30d,
            long windowStartMs,
            long windowEndMs,
            String eventTimestamp,
            String createdTimestamp) {
        this.customerId = customerId;
        this.totalAmount30d = totalAmount30d;
        this.txCount30d = txCount30d;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.eventTimestamp = eventTimestamp;
        this.createdTimestamp = createdTimestamp;
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public double getTotalAmount30d() { return totalAmount30d; }
    public void setTotalAmount30d(double totalAmount30d) { this.totalAmount30d = totalAmount30d; }
    public long getTxCount30d() { return txCount30d; }
    public void setTxCount30d(long txCount30d) { this.txCount30d = txCount30d; }
    public long getWindowStartMs() { return windowStartMs; }
    public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }
    public long getWindowEndMs() { return windowEndMs; }
    public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }
    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }
    public String getCreatedTimestamp() { return createdTimestamp; }
    public void setCreatedTimestamp(String createdTimestamp) { this.createdTimestamp = createdTimestamp; }
}
