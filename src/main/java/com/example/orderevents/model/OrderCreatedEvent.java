package com.example.orderevents.model;

import java.time.Instant;
import java.util.List;

/** 
 * The event to be published to Kafka. 
 * Deliberately a flat, self-contained snapshot of everything 
 * a consumer needs to process the order (customer,items, total, timestamp) 
 */
public record OrderCreatedEvent(
		String orderId,
		String customerId,
		List<OrderItem> items,
		double totalAmount,
		Instant createdAt
	) {
	public static OrderCreatedEvent from(String orderId, OrderCreateRequest request) {
		double total = request.items().stream()
				.mapToDouble(item -> item.quantity() * item.unitPrice())
				.sum();
		return new OrderCreatedEvent(orderId,request.customerId(),request.items(),total,Instant.now());
	}
}