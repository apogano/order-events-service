package com.example.orderevents.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

/**
 * Denormalized read model. Populated by three independent consumers
 * (order-history, inventory, notification) via partial upserts -- see
 * OrderHistoryUpdateService for why plain object-mutation-then-save()
 * would be unsafe here. This class is a read-only view of whatever's
 * currently in MongoDB; it has no setters, since nothing in the
 * application ever mutates an existing instance of it -- every write
 * goes through OrderHistoryUpdateService's targeted field updates instead.
 */
@Document(collection = "order_history")
public class OrderHistoryDocument {

    @Id
    private String orderId;

    private String customerId;
    private List<OrderItem> items;
    private double totalAmount;
    private Instant orderCreatedAt;
    private Instant lastUpdatedAt;
    private String inventoryStatus;
    private String notificationStatus;

    protected OrderHistoryDocument() {
        // required by Spring Data
    }

    public String getOrderId() {
        return orderId; 
    }

    public String getCustomerId() {
        return customerId;
    }

    public List<OrderItem> getItems() {
        return items;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public Instant getOrderCreatedAt() {
        return orderCreatedAt;
    }

    public Instant getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public String getInventoryStatus() {
        return inventoryStatus;
    }

    public String getNotificationStatus() {
        return notificationStatus;
    }
}
