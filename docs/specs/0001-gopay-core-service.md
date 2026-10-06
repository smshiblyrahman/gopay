# 0001 · GoPay Payment Processing Backend

**Status**: Accepted
**Date**: 2026-10-06
**Mode**: ARCHITECTURE

## Summary
Build production grade payment processing system in Go based on `GOPAY.md`. Includes REST APIs, PostgreSQL persistence, Redis distributed locks and caching, RabbitMQ transactional outbox and worker pipelines, webhook dispatching with exponential backoff and DLQ, JWT authentication, and Prometheus metrics.

## Requirements
- **AC-1**: `POST /api/v1/payments` accepts payment creation with `Idempotency-Key` header. Duplicate request with identical payload returns saved result. Duplicate key with different payload returns 409 Conflict.
- **AC-2**: Concurrency safe payments. Simultaneous requests for same payment ID or idempotency key serialize cleanly via Redis distributed locking (`lock:payment:{id}`).
- **AC-3**: `POST /api/v1/payments/{id}/confirm` transitions payment (`CREATED` -> `CONFIRMED` or `FAILED`). Atomic database transaction writes payment record, transaction history, and outbox event.
- **AC-4**: `POST /api/v1/payments/{id}/refund` verifies payment confirmed state, creates refund transaction, avoids double refunds, updates payment state to `REFUNDED`, writes outbox event.
- **AC-5**: Transactional Outbox worker polls unpublished `outbox_events` and publishes to RabbitMQ exchange (`gopay.events`). Marks events as published upon ACK.
- **AC-6**: Webhook delivery worker consumes `payment.*` events and dispatches HTTP requests signed with HMAC-SHA256 signature. Failed deliveries retry on schedule (5s, 30s, 5m, 30m) before dead letter queue routing.
- **AC-7**: Health check `GET /health` and Prometheus metrics `GET /metrics`.
- **AC-8**: Docker compose deployment containerizes PostgreSQL, Redis, RabbitMQ, API server, and Background Worker.

## Architecture & Tech Stack
- **Language**: Go 1.22+ (`go1.27.1`)
- **Web Framework / Router**: `chi` (clean, idiomatic, zero external bloat)
- **Database**: PostgreSQL 16 (`pgx/v5` driver, connection pool, transactional safety)
- **Cache & Locking**: Redis (`redis/go-redis/v9`)
- **Message Broker**: RabbitMQ (`rabbitmq/amqp091-go`)
- **Metrics**: `prometheus/client_golang`
- **Security**: JWT authentication middleware, HMAC-SHA256 webhook signatures, rate limiting
- **Testing**: Unit tests, concurrency tests (`go test -race ./...`), integration tests

## Database Schema
- `payments`: `id UUID`, `customer_id VARCHAR`, `amount NUMERIC(12,2)`, `currency VARCHAR`, `status VARCHAR`, `idempotency_key VARCHAR UNIQUE`, `created_at TIMESTAMP`, `updated_at TIMESTAMP`
- `transactions`: `id UUID`, `payment_id UUID REFERENCES payments(id)`, `type VARCHAR`, `amount NUMERIC(12,2)`, `status VARCHAR`, `created_at TIMESTAMP`
- `outbox_events`: `id UUID`, `event_type VARCHAR`, `payload JSONB`, `published BOOLEAN DEFAULT FALSE`, `created_at TIMESTAMP`
- `idempotency_records`: `key VARCHAR PRIMARY KEY`, `request_hash VARCHAR`, `response_status INT`, `response_body TEXT`, `created_at TIMESTAMP`
- `webhook_deliveries`: `id UUID`, `event_id UUID`, `url TEXT`, `attempt INT`, `status VARCHAR`, `next_retry_at TIMESTAMP`, `last_error TEXT`, `created_at TIMESTAMP`

## Build Plan
1. Project layout (`cmd/api`, `cmd/worker`, `internal/domain`, `internal/repository`, `internal/service`, `internal/handler`, `internal/outbox`, `internal/webhook`, `internal/queue`, `internal/lock`).
2. Migrations and SQL schemas.
3. Domain models and repository implementation with PostgreSQL.
4. Redis client, distributed lock manager, and status caching.
5. Service layer with Idempotency, Payment workflow, and Refund workflow.
6. RabbitMQ exchange, queues, DLQ topology, and transactional outbox publisher.
7. Webhook dispatcher with HMAC-SHA256 and exponential backoff retry worker.
8. REST HTTP handlers with Chi router, JWT auth, rate limiter, Prometheus metrics, and health check.
9. Dockerfile and Docker Compose setup for full multi-container stack.
10. Test suite: unit tests, race condition / concurrency test simulating 100 simultaneous requests.
