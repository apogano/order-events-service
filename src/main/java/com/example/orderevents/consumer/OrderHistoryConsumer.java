package com.example.orderevents.consumer;

import com.example.orderevents.model.OrderCreatedEvent;
import com.example.orderevents.service.OrderHistoryUpdateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Builds the denormalized order_history read model in MongoDB -- the
 * "read model" side of this pipeline's CQRS-like shape: the Order Service
 * writes an event, and this consumer (independently, asynchronously)
 * builds the fast-to-query view of it.
 */
@Component
public class OrderHistoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderHistoryConsumer.class);

    private final OrderHistoryUpdateService orderHistoryUpdateService;

    public OrderHistoryConsumer(OrderHistoryUpdateService orderHistoryUpdateService) {
        this.orderHistoryUpdateService = orderHistoryUpdateService;
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "order-history-service-group"
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[order-history-service] Recording order {} for customer {}, total {}",
                event.orderId(), event.customerId(), event.totalAmount());
        orderHistoryUpdateService.recordOrderCreated(event);
    }
}
