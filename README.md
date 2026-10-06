# GoPay — Production Grade Payment Backend

GoPay is a concurrency-safe, idempotent payment processing backend written in Go.

## Architecture

```
Client
  │
REST API (Chi, JWT, Rate Limiting, Prometheus)
  │
GoPay Core
  ├── PostgreSQL (Payments, Transactions, Idempotency, Outbox)
  ├── Redis (Distributed Locks, Status Cache)
  └── RabbitMQ (Transactional Outbox Publisher, Consumers, DLQ)
        └── Webhook Worker (HMAC-SHA256, Exponential Backoff, DLQ)
```

## Features

- **Idempotency Guarantee**: Requests with `Idempotency-Key` are cached and fingerprinted (SHA-256). Identical requests return original responses, differing payloads return 409 Conflict.
- **Transactional Outbox Pattern**: Atomic database transactions commit payment updates and outbox events simultaneously. Outbox worker guarantees at-least-once message delivery to RabbitMQ.
- **Distributed Locking**: Concurrency safety with Redis-based distributed mutexes on payment operations (`lock:payment:{id}`).
- **Webhook Delivery Engine**: Asynchronous delivery signed with HMAC-SHA256, exponential backoff retries (5s, 30s, 5m, 30m), and Dead Letter Queue (DLQ).
- **Monitoring & Security**: Prometheus metrics at `/metrics`, health checks at `/health`, token bucket rate limiting, and JWT auth support.

## API Endpoints

- `POST /payments` or `POST /api/v1/payments` (header `Idempotency-Key`)
- `GET /payments/{id}` or `GET /api/v1/payments/{id}`
- `POST /payments/{id}/confirm` or `POST /api/v1/payments/{id}/confirm`
- `POST /payments/{id}/refund` or `POST /api/v1/payments/{id}/refund`
- `GET /health`
- `GET /metrics`

## Quick Start (Docker)

```bash
docker compose up --build -d
```

Check health:
```bash
curl http://localhost:8080/health
```

## Running Tests

Unit, concurrency & race condition test (simulates 100 simultaneous requests):
```bash
go test -v -race ./...
```
