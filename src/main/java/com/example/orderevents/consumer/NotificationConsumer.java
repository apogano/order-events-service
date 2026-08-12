package com.example.orderevents.consumer;

import com.example.orderevents.model.OrderCreatedEvent;
import com.example.orderevents.service.OrderHistoryUpdateService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Simulates sending an order-confirmation notification to the customer.
 * Same stubbed-logic principle as InventoryConsumer -- see its Javadoc.
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final OrderHistoryUpdateService orderHistoryUpdateService;

    public NotificationConsumer(OrderHistoryUpdateService orderHistoryUpdateService) {
        this.orderHistoryUpdateService = orderHistoryUpdateService;
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "notification-service-group"
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[notification-service] Sending confirmation to customer {} for order {}",
                event.customerId(), event.orderId());
        // Real logic would call an email/SMS provider here.
        orderHistoryUpdateService.recordNotificationStatus(event.orderId(), "SENT");
    }
}