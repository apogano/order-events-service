package com.example.orderevents;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


import com.example.orderevents.model.OrderCreateRequest;
import com.example.orderevents.model.OrderHistoryDocument;
import com.example.orderevents.model.OrderItem;
import com.example.orderevents.repository.OrderHistoryRepository;

/**
 * Full end-to-end test: real Kafka and MongoDB (via Testcontainers, not
 * mocks), exercising the entire pipeline -- POST /orders publishes an
 * event, all three consumers independently process it, and the
 * denormalized read model ends up fully populated in MongoDB.
 *
 * Requires Docker to be running locally. This is deliberately a real
 * integration test, not a unit test with mocked Kafka/Mongo clients --
 * the whole point of this project is the interaction between the
 * components, which mocks would hide rather than verify.
 */


@AutoConfigureTestRestTemplate   // <- add this
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderEventsServiceApplicationTests {
	@Autowired
    private TestRestTemplate restTemplate;
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @Container
    static MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
    }

    @LocalServerPort
    int port;

    @Autowired
    OrderHistoryRepository orderHistoryRepository;

    @Test
    void submittingAnOrderIsProcessedByAllThreeConsumers() {
        OrderCreateRequest request = new OrderCreateRequest(
                "customer-123",
                List.of(new OrderItem("sku-001", 2, 19.99))
        );

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders", request, Map.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String orderId = (String) response.getBody().get("orderId");
        assertThat(orderId).isNotBlank();

        // Processing is asynchronous across three independent consumer
        // groups -- await polls until the read model reflects all
        // three, rather than asserting immediately 
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<OrderHistoryDocument> doc = orderHistoryRepository.findById(orderId);
            assertThat(doc).isPresent();
            assertThat(doc.get().getCustomerId()).isEqualTo("customer-123");
            assertThat(doc.get().getInventoryStatus()).isEqualTo("RESERVED");
            assertThat(doc.get().getNotificationStatus()).isEqualTo("SENT");
        });
    }
}
