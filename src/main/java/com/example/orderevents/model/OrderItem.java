package com.example.orderevents.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record OrderItem(
		@NotBlank String productId,
		@Positive int quantity,
		@Positive double unitPrice
	) {}