package com.example.orderevents.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig{
	
	public static final String ORDER_EVENT_TOPIC = "order-events";

	public static final String ORDER_EVENT_DLT_TOPIC = "order-events.DLT";
	
	@Bean 
	public NewTopic orderEventsTopic() {
		return TopicBuilder.name(ORDER_EVENT_TOPIC)
				.partitions(3)
				.replicas(1)
				.build();
	}
	
	@Bean 
	public NewTopic orderEventsDltTopic() {
		return TopicBuilder.name(ORDER_EVENT_DLT_TOPIC)
				.partitions(1)
				.replicas(1)
				.build();
	}
	
	
}