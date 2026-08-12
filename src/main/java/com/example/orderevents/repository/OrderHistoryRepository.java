package com.example.orderevents.repository;

import com.example.orderevents.model.OrderHistoryDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface OrderHistoryRepository extends MongoRepository<OrderHistoryDocument, String> {
}
