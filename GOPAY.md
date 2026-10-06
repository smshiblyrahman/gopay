# GoPay --- Payment Processing Service

## Project Overview

Build a production-grade payment processing backend called **GoPay**.

The system supports:

-   Payment initiation
-   Payment confirmation
-   Refund processing
-   Transaction status tracking
-   Webhook delivery
-   Reliable asynchronous event processing
-   Duplicate payment prevention
-   Concurrency-safe payment workflows

## Technology Stack

-   Language: Go 1.22+
-   Database: PostgreSQL 16
-   Cache: Redis
-   Message Queue: RabbitMQ
-   Containerization: Docker / Docker Compose
-   API Style: REST JSON APIs

## Architecture

    Client
      |
    REST API
      |
    GoPay Backend
      |
    --------------------------------
    |              |               |
    PostgreSQL    Redis        RabbitMQ
    |              |               |
    Data       Cache/Lock     Workers
                                  |
                             Webhooks

## Core Features

### Payment Creation

Endpoint:

POST /api/v1/payments

Features:

-   Generate unique payment ID
-   Store transaction data
-   Support idempotency keys
-   Publish payment events

### Idempotency

Every payment request must support:

Header:

Idempotency-Key: unique-key

Rules:

-   Same key + same request returns previous response
-   Same key + different request is rejected
-   Duplicate charges are prevented

Database constraint:

``` sql
UNIQUE(idempotency_key)
```

## Payment Confirmation

Endpoint:

POST /api/v1/payments/{id}/confirm

Flow:

Request → Validate → Database Transaction → Update Status → Publish
Event

Payment states:

-   CREATED
-   PENDING
-   CONFIRMED
-   FAILED
-   REFUNDED

## Refund Processing

Endpoint:

POST /api/v1/payments/{id}/refund

Requirements:

-   Validate original payment
-   Create refund transaction
-   Prevent duplicate refunds
-   Publish refund event

## Database Design

### payments table

``` sql
CREATE TABLE payments(
id UUID PRIMARY KEY,
customer_id VARCHAR(100),
amount NUMERIC(12,2),
currency VARCHAR(10),
status VARCHAR(20),
idempotency_key VARCHAR(255),
created_at TIMESTAMP DEFAULT NOW(),
updated_at TIMESTAMP DEFAULT NOW()
);
```

### transactions table

``` sql
CREATE TABLE transactions(
id UUID PRIMARY KEY,
payment_id UUID,
type VARCHAR(20),
amount NUMERIC(12,2),
status VARCHAR(20),
created_at TIMESTAMP DEFAULT NOW()
);
```

### outbox_events table

``` sql
CREATE TABLE outbox_events(
id UUID PRIMARY KEY,
event_type VARCHAR(100),
payload JSONB,
published BOOLEAN DEFAULT FALSE,
created_at TIMESTAMP DEFAULT NOW()
);
```

## Transactional Outbox Pattern

Payment transaction:

1.  Insert payment
2.  Insert event into outbox table
3.  Commit transaction

Worker:

1.  Read unpublished events
2.  Publish to RabbitMQ
3.  Mark event as completed

Benefits:

-   No lost events
-   Reliable messaging
-   Database consistency

## Redis Usage

Used for:

-   Payment status caching
-   Distributed locking
-   Temporary data storage

Example cache key:

    payment:{id}:status

Distributed lock:

    lock:payment:{id}

## RabbitMQ Event System

Events:

-   payment.created
-   payment.confirmed
-   payment.refunded
-   webhook.delivery

Example:

``` json
{
"type":"payment.confirmed",
"payment_id":"123",
"amount":100
}
```

## Webhook Processing

Features:

-   Async delivery
-   Retry mechanism
-   Exponential backoff
-   Dead letter queue

Retry schedule:

-   5 seconds
-   30 seconds
-   5 minutes
-   30 minutes

Failed messages move to DLQ.

## API Endpoints

    POST /payments

    GET /payments/{id}

    POST /payments/{id}/confirm

    POST /payments/{id}/refund

    GET /health

## Testing Strategy

### Unit Tests

Test:

-   Payment service
-   Refund logic
-   Validation
-   Idempotency

### Integration Tests

Test:

-   PostgreSQL transactions
-   Redis locks
-   RabbitMQ events

### Concurrency Tests

Simulate:

100 simultaneous payment requests

Expected:

Only one successful charge.

Command:

    go test -race ./...

## Docker Services

Required containers:

-   PostgreSQL
-   Redis
-   RabbitMQ
-   Go API
-   Background Workers

## Security

Implement:

-   JWT authentication
-   Request validation
-   Rate limiting
-   SQL injection protection
-   Secure webhook signatures using HMAC-SHA256

## Monitoring

Add:

-   Prometheus metrics
-   Health checks
-   Request latency tracking
-   Payment success metrics

## Development Roadmap

Phase 1: - Setup Go project - Configure Docker - Setup PostgreSQL

Phase 2: - Build payment APIs

Phase 3: - Add idempotency - Add transactions - Add outbox pattern

Phase 4: - Add RabbitMQ workers - Add webhook delivery

Phase 5: - Add Redis optimization - Add testing

Phase 6: - Production deployment

## Final Deliverable

A production-ready fintech payment backend with:

-   Go REST API
-   PostgreSQL persistence
-   Redis caching
-   RabbitMQ messaging
-   Idempotent payments
-   Transactional outbox
-   Webhook system
-   Retry handling
-   Dead-letter queues
-   Distributed locking
-   Automated testing
-   Docker deployment
