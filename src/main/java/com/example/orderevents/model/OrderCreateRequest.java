package com.example.orderevents.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record OrderCreateRequest(
		@NotBlank String customerId,
		@NotEmpty @Valid List<OrderItem> items
	) {}
