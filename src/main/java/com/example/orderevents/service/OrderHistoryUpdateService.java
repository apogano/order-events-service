package com.example.orderevents.service;

import com.example.orderevents.model.OrderCreatedEvent;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Writes to the order_history collection via partial upserts (MongoTemplate),
 * not MongoRepository.save() with a full object.
 *
 * Why this matters: three independent consumers (order-history, inventory,
 * notification) all eventually write to the SAME document, but they run in
 * three separate consumer groups with no ordering guarantee relative to
 * each other -- there's no rule saying OrderHistoryConsumer's event is
 * processed before InventoryConsumer's. If OrderHistoryConsumer used a
 * blind save() of a full object, and it happened to run AFTER
 * InventoryConsumer had already written the inventory status, that save()
 * would silently overwrite (wipe out) the inventory status field with a
 * fresh object that doesn't have it set.
 *
 * Partial upserts avoid this entirely: each consumer only $sets the
 * field(s) it's actually responsible for, regardless of whether the
 * document already exists or which consumer happens to run first. MongoDB
 * creates the document on the first write (from whichever consumer wins
 * the race) and every subsequent write merges into it.
 */
@Service
public class OrderHistoryUpdateService {

    private final MongoTemplate mongoTemplate;

    public OrderHistoryUpdateService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void recordOrderCreated(OrderCreatedEvent event) {
        Update update = new Update()
                .set("customerId", event.customerId())
                .set("items", event.items())
                .set("totalAmount", event.totalAmount())
                .set("orderCreatedAt", event.createdAt())
                .set("lastUpdatedAt", Instant.now());
        upsert(event.orderId(), update);
    }

    public void recordInventoryStatus(String orderId, String status) {
        Update update = new Update()
                .set("inventoryStatus", status)
                .set("lastUpdatedAt", Instant.now());
        upsert(orderId, update);
    }

    public void recordNotificationStatus(String orderId, String status) {
        Update update = new Update()
                .set("notificationStatus", status)
                .set("lastUpdatedAt", Instant.now());
        upsert(orderId, update);
    }

    private void upsert(String orderId, Update update) {
        Query query = Query.query(where("_id").is(orderId));
        mongoTemplate.upsert(query, update, "order_history");
    }
}
