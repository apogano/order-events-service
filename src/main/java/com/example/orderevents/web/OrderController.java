package com.example.orderevents.web;

import com.example.orderevents.model.OrderCreateRequest;
import com.example.orderevents.model.OrderCreatedEvent;
import com.example.orderevents.service.OrderProducerService;

import jakarta.validation.Valid;

import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController{
	private final OrderProducerService producerService;
	
	public OrderController(OrderProducerService producerService) {
		this.producerService = producerService;
	}
	
	@PostMapping
	public ResponseEntity<Map<String,String>> createOrder(@Valid @RequestBody OrderCreateRequest request){
		String orderId = UUID.randomUUID().toString();
		OrderCreatedEvent event = OrderCreatedEvent.from(orderId, request);
		producerService.publishOrderCreated(event);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("orderId",orderId));
	}
}