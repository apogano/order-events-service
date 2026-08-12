# Order Events Service

An event-driven order processing pipeline: a REST API publishes `OrderCreated` events to Kafka, and three independent consumer groups react to the same event in parallel, converging on a denormalized read model in MongoDB.

## Why this project

Demonstrates event-driven architecture with a message broker: fan-out to independent consumers, dead-letter handling, and idempotent writes under no ordering guarantee between consumers. The scenario (order processing) is intentionally generic — the point is demonstrating these Kafka and MongoDB patterns clearly, not building a real e-commerce system.

## Architecture

```
                    POST /orders
                         |
                         v
                +------------------+
                |  OrderController |
                +--------+---------+
                         | publish (key = orderId)
                         v
                +------------------+
                |  order-events    |  (Kafka topic, 3 partitions)
                |     topic        |
                +--------+---------+
                         |
       +-----------------+-----------------+
       v                 v                 v
+--------------+  +---------------+  +--------------------+
|  inventory-  |  | notification- |  |  order-history-    |
|service-group |  | service-group |  |  service-group     |
| (Inventory   |  |(Notification  |  |  (OrderHistory     |
|  Consumer)   |  |  Consumer)    |  |   Consumer)        |
+------+-------+  +------+--------+  +---------+----------+
       |                 |                     |
       +-----------------+---------------------+
                         v
              +--------------------------+
              |  MongoDB: order_history  |  (denormalized read model)
              +--------------------------+

Any consumer's processing failure -> retried with backoff -> still failing
after max attempts -> published to order-events.DLT (dead-letter topic)
```

## Why three separate consumer *groups*, not one

Kafka consumers in the **same** group split partitions between them (load balancing — each message goes to exactly one consumer). Consumers in **different** groups each get their own full, independent copy of every message. `inventory-service-group`, `notification-service-group`, and `order-history-service-group` are three separate groups specifically so all three see every event, without coordinating with each other or knowing the others exist.

## Running it

Requires Docker (for Kafka + MongoDB during tests), Java 21+, and Maven (or the bundled `./mvnw`).

**Run the tests** — this is the most complete way to see the whole pipeline work, since the test spins up real Kafka and MongoDB via Testcontainers and exercises the full flow end-to-end:
```bash
./mvnw test
```

**Run the app itself locally**, against Kafka + MongoDB via `docker-compose.yml`:
```bash
docker compose up -d
./mvnw spring-boot:run
```

The API starts on `http://localhost:8082`.

### Submit an order

```bash
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-123",
    "items": [{"productId": "sku-001", "quantity": 2, "unitPrice": 19.99}]
  }'
# => {"orderId": "..."}  (202 Accepted)
```

### Check the read model

```bash
curl http://localhost:8082/orders/<orderId>
curl http://localhost:8082/orders          # list all
```

Poll the single-order endpoint a few times right after submitting — you'll see `inventoryStatus` and `notificationStatus` populate asynchronously as each consumer processes the event, independently of the others and of how fast you poll.

## Design decisions

- **`orderId` as the Kafka message key**, not just a field in the payload. Kafka guarantees ordering only *within* a partition, and messages with the same key always land on the same partition — so if this order ever produces a second event in the future, it's guaranteed to be processed in order relative to the first, without any extra coordination.
- **`POST /orders` returns `202 Accepted`, not `200`/`201`.** The order is durably queued the moment the event is published, not fully processed. `GET /orders` and `GET /orders/{id}`, by contrast, return `200 OK` — they're synchronous reads that already have their complete answer, not requests accepted for later processing; using `202` there would misuse the same signal for something it doesn't mean.
- **Idempotent writes via targeted partial upserts** (`MongoTemplate` + `$set`), not `MongoRepository.save()` with a full object. Three independent consumers write to the *same* MongoDB document with no ordering guarantee relative to each other — a blind full-object `save()` from whichever consumer runs last would silently overwrite fields the other consumers had already written. Partial upserts avoid this: each consumer only sets the field(s) it owns, regardless of which one happens to run first, and MongoDB creates the document on whichever write wins the race.
- **Dead-letter queue via Spring Kafka's built-in `DeadLetterPublishingRecoverer`**, not hand-rolled retry logic. A failing listener retries with a fixed backoff (`app.kafka.retry.*`), and after exhausting attempts, the original message is published to `order-events.DLT` instead of retrying forever or silently dropping it.
- **A real integration test using Testcontainers** (actual Kafka and MongoDB in Docker, not mocks) that submits an order over HTTP and polls (via Awaitility) until all three consumers have independently written their piece of the read model. Mocking the broker/database here would hide the exact interactions this project exists to demonstrate.

## Known limitations

- **No permanent-vs-transient failure distinction in the consumers.** Every listener failure is retried uniformly up to the configured max attempts before going to the DLT — a genuinely permanent failure (e.g. malformed event data) still burns through the full retry budget instead of failing fast.
- **Inventory and notification logic is stubbed** (always "succeeds" and logs) — the point of this project is the event-driven plumbing, not building real inventory/notification systems.
- **`GET /orders` has no pagination** — fine at demo scale, would need it before this collection grows large in any real deployment.
- **Single-broker Kafka, single-node MongoDB** — fine for a demo; a real deployment would need a proper multi-broker cluster and MongoDB replica set for actual durability guarantees.
- **No schema registry / Avro** — events are plain JSON.

## What I'd change at scale

- Add explicit handling to skip retries for genuinely permanent failures rather than retrying everything uniformly.
- Add pagination to `GET /orders`.
- Add a schema registry (e.g. Confluent Schema Registry with Avro) once more than one service starts producing or consuming these events.
- Add consumer lag monitoring — critical in production for noticing a consumer group falling behind before it becomes a real problem.

## Running tests locally

```bash
./mvnw test
```

Requires Docker running locally — the integration test spins up real Kafka and MongoDB containers via Testcontainers for the duration of the test run, and tears them down automatically afterward.
