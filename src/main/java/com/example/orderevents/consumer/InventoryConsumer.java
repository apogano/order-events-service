package com.example.orderevents.consumer;

import com.example.orderevents.model.OrderCreatedEvent;
import com.example.orderevents.service.OrderHistoryUpdateService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Simulates an inventory check/reservation for a newly created order.
 *
 * The actual "check" is stubbed (always succeeds) -- the point of this
 * consumer is to demonstrate the fan-out pattern itself (an independent
 * consumer group reacting to the same event as the other two consumers,
 * without any of them coordinating with each other), not to build a real
 * inventory management system. It does write a real status into the
 * shared MongoDB read model, via a targeted partial upsert -- see
 * OrderHistoryUpdateService for why that matters here.
 */
@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final OrderHistoryUpdateService orderHistoryUpdateService;

    public InventoryConsumer(OrderHistoryUpdateService orderHistoryUpdateService) {
        this.orderHistoryUpdateService = orderHistoryUpdateService;
    }

    @KafkaListener(
            topics = "order-events",
           groupId = "inventory-service-group"
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[inventory-service] Checking stock for order {} ({} item(s))",
                event.orderId(), event.items().size());
        // Real logic would check/decrement actual stock levels here.
        orderHistoryUpdateService.recordInventoryStatus(event.orderId(), "RESERVED");
    }
}