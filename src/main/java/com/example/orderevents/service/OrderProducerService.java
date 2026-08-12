package com.example.orderevents.service;

import com.example.orderevents.config.KafkaTopicConfig;
import com.example.orderevents.model.OrderCreatedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducerService{
	
	private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
	
	private final KafkaTemplate<String, Object> kafkaTemplate;
	
	public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
	public void publishOrderCreated(OrderCreatedEvent event) {
		kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENT_TOPIC, event.orderId(), event);
		log.info("Published OrderCreated event for order {}", event.orderId());
	}
}