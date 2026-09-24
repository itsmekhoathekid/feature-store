package io.github.itsmekhoathekid.featurestore.model;

import java.io.Serializable;

public class CustomerAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private double totalAmount;
    private long txCount;

    public CustomerAccumulator() {}

    public CustomerAccumulator(double totalAmount, long txCount) {
        this.totalAmount = totalAmount;
        this.txCount = txCount;
    }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
    public long getTxCount() { return txCount; }
    public void setTxCount(long txCount) { this.txCount = txCount; }
}
