package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/smshiblyrahman/gopay/internal/domain"
)

type Repository interface {
	// Payments
	CreatePaymentWithTx(ctx context.Context, p *domain.Payment, outboxEvent *domain.OutboxEvent, reqHash string, respBody string) error
	GetPaymentByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
	ConfirmPaymentWithTx(ctx context.Context, id uuid.UUID, txRecord *domain.Transaction, outboxEvent *domain.OutboxEvent) (*domain.Payment, error)
	RefundPaymentWithTx(ctx context.Context, id uuid.UUID, refundTx *domain.Transaction, outboxEvent *domain.OutboxEvent) (*domain.Payment, error)

	// Idempotency
	GetIdempotencyRecord(ctx context.Context, key string) (*domain.IdempotencyRecord, error)

	// Outbox
	FetchUnpublishedEvents(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkEventPublished(ctx context.Context, id uuid.UUID) error

	// Webhook Deliveries
	CreateWebhookDelivery(ctx context.Context, delivery *domain.WebhookDelivery) error
	UpdateWebhookDelivery(ctx context.Context, delivery *domain.WebhookDelivery) error
	GetPendingWebhookDeliveries(ctx context.Context, limit int) ([]domain.WebhookDelivery, error)

	// Ping
	Ping(ctx context.Context) error
	Close()
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, connString string) (*PostgresRepository, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

func HashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (r *PostgresRepository) CreatePaymentWithTx(ctx context.Context, p *domain.Payment, outboxEvent *domain.OutboxEvent, reqHash string, respBody string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert Payment
	queryPayment := `
		INSERT INTO payments (id, customer_id, amount, currency, status, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = tx.Exec(ctx, queryPayment, p.ID, p.CustomerID, p.Amount, p.Currency, p.Status, p.IdempotencyKey, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	// Insert initial transaction record
	initTxID := uuid.New()
	queryTx := `
		INSERT INTO transactions (id, payment_id, type, amount, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(ctx, queryTx, initTxID, p.ID, domain.TxTypeCharge, p.Amount, domain.TxStatusSuccess, p.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}

	// Insert Outbox Event
	if outboxEvent != nil {
		queryOutbox := `
			INSERT INTO outbox_events (id, event_type, payload, published, created_at)
			VALUES ($1, $2, $3, $4, $5)
		`
		_, err = tx.Exec(ctx, queryOutbox, outboxEvent.ID, outboxEvent.EventType, outboxEvent.Payload, outboxEvent.Published, outboxEvent.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}

	// Insert Idempotency Record if key present
	if p.IdempotencyKey != nil && *p.IdempotencyKey != "" {
		queryIdemp := `
			INSERT INTO idempotency_records (key, request_hash, response_status, response_body, created_at)
			VALUES ($1, $2, $3, $4, $5)
		`
		_, err = tx.Exec(ctx, queryIdemp, *p.IdempotencyKey, reqHash, 201, respBody, p.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert idempotency: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func (r *PostgresRepository) GetPaymentByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	query := `
		SELECT id, customer_id, amount, currency, status, idempotency_key, created_at, updated_at
		FROM payments
		WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)

	var p domain.Payment
	err := row.Scan(&p.ID, &p.CustomerID, &p.Amount, &p.Currency, &p.Status, &p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPaymentNotFound
		}
		return nil, fmt.Errorf("scan payment: %w", err)
	}

	return &p, nil
}

func (r *PostgresRepository) ConfirmPaymentWithTx(ctx context.Context, id uuid.UUID, txRecord *domain.Transaction, outboxEvent *domain.OutboxEvent) (*domain.Payment, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch current payment for update
	queryLock := `
		SELECT id, customer_id, amount, currency, status, idempotency_key, created_at, updated_at
		FROM payments
		WHERE id = $1
		FOR UPDATE
	`
	var p domain.Payment
	err = tx.QueryRow(ctx, queryLock, id).Scan(&p.ID, &p.CustomerID, &p.Amount, &p.Currency, &p.Status, &p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPaymentNotFound
		}
		return nil, fmt.Errorf("lock payment: %w", err)
	}

	if p.Status == domain.StatusConfirmed {
		// Already confirmed, idempotent return
		return &p, nil
	}
	if p.Status != domain.StatusCreated && p.Status != domain.StatusPending {
		return nil, fmt.Errorf("%w: cannot confirm status %s", domain.ErrInvalidStatus, p.Status)
	}

	now := time.Now().UTC()
	p.Status = domain.StatusConfirmed
	p.UpdatedAt = now

	// Update Payment
	queryUpdate := `
		UPDATE payments
		SET status = $1, updated_at = $2
		WHERE id = $3
	`
	_, err = tx.Exec(ctx, queryUpdate, p.Status, p.UpdatedAt, p.ID)
	if err != nil {
		return nil, fmt.Errorf("update payment status: %w", err)
	}

	// Insert Transaction History
	if txRecord != nil {
		queryTx := `
			INSERT INTO transactions (id, payment_id, type, amount, status, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
		`
		_, err = tx.Exec(ctx, queryTx, txRecord.ID, txRecord.PaymentID, txRecord.Type, txRecord.Amount, txRecord.Status, txRecord.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert tx record: %w", err)
		}
	}

	// Insert Outbox
	if outboxEvent != nil {
		queryOutbox := `
			INSERT INTO outbox_events (id, event_type, payload, published, created_at)
			VALUES ($1, $2, $3, $4, $5)
		`
		_, err = tx.Exec(ctx, queryOutbox, outboxEvent.ID, outboxEvent.EventType, outboxEvent.Payload, outboxEvent.Published, outboxEvent.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &p, nil
}

func (r *PostgresRepository) RefundPaymentWithTx(ctx context.Context, id uuid.UUID, refundTx *domain.Transaction, outboxEvent *domain.OutboxEvent) (*domain.Payment, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Select for update
	queryLock := `
		SELECT id, customer_id, amount, currency, status, idempotency_key, created_at, updated_at
		FROM payments
		WHERE id = $1
		FOR UPDATE
	`
	var p domain.Payment
	err = tx.QueryRow(ctx, queryLock, id).Scan(&p.ID, &p.CustomerID, &p.Amount, &p.Currency, &p.Status, &p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPaymentNotFound
		}
		return nil, fmt.Errorf("lock payment: %w", err)
	}

	if p.Status == domain.StatusRefunded {
		return nil, domain.ErrAlreadyRefunded
	}
	if p.Status != domain.StatusConfirmed {
		return nil, domain.ErrRefundInvalidState
	}

	now := time.Now().UTC()
	p.Status = domain.StatusRefunded
	p.UpdatedAt = now

	// Update status
	queryUpdate := `
		UPDATE payments
		SET status = $1, updated_at = $2
		WHERE id = $3
	`
	_, err = tx.Exec(ctx, queryUpdate, p.Status, p.UpdatedAt, p.ID)
	if err != nil {
		return nil, fmt.Errorf("update payment status: %w", err)
	}

	// Insert Refund Transaction Record
	queryTx := `
		INSERT INTO transactions (id, payment_id, type, amount, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(ctx, queryTx, refundTx.ID, refundTx.PaymentID, refundTx.Type, refundTx.Amount, refundTx.Status, refundTx.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert refund tx: %w", err)
	}

	// Outbox event
	if outboxEvent != nil {
		queryOutbox := `
			INSERT INTO outbox_events (id, event_type, payload, published, created_at)
			VALUES ($1, $2, $3, $4, $5)
		`
		_, err = tx.Exec(ctx, queryOutbox, outboxEvent.ID, outboxEvent.EventType, outboxEvent.Payload, outboxEvent.Published, outboxEvent.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &p, nil
}

func (r *PostgresRepository) GetIdempotencyRecord(ctx context.Context, key string) (*domain.IdempotencyRecord, error) {
	query := `
		SELECT key, request_hash, response_status, response_body, created_at
		FROM idempotency_records
		WHERE key = $1
	`
	row := r.pool.QueryRow(ctx, query, key)

	var rec domain.IdempotencyRecord
	err := row.Scan(&rec.Key, &rec.RequestHash, &rec.ResponseStatus, &rec.ResponseBody, &rec.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get idempotency record: %w", err)
	}

	return &rec, nil
}

func (r *PostgresRepository) FetchUnpublishedEvents(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	query := `
		SELECT id, event_type, payload, published, created_at
		FROM outbox_events
		WHERE published = FALSE
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.Published, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		events = append(events, e)
	}

	return events, nil
}

func (r *PostgresRepository) MarkEventPublished(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE outbox_events SET published = TRUE WHERE id = $1`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

func (r *PostgresRepository) CreateWebhookDelivery(ctx context.Context, delivery *domain.WebhookDelivery) error {
	query := `
		INSERT INTO webhook_deliveries (id, event_id, target_url, attempt, status, next_retry_at, last_error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query, delivery.ID, delivery.EventID, delivery.TargetURL, delivery.Attempt, delivery.Status, delivery.NextRetryAt, delivery.LastError, delivery.CreatedAt, delivery.UpdatedAt)
	return err
}

func (r *PostgresRepository) UpdateWebhookDelivery(ctx context.Context, delivery *domain.WebhookDelivery) error {
	query := `
		UPDATE webhook_deliveries
		SET attempt = $1, status = $2, next_retry_at = $3, last_error = $4, updated_at = $5
		WHERE id = $6
	`
	_, err := r.pool.Exec(ctx, query, delivery.Attempt, delivery.Status, delivery.NextRetryAt, delivery.LastError, delivery.UpdatedAt, delivery.ID)
	return err
}

func (r *PostgresRepository) GetPendingWebhookDeliveries(ctx context.Context, limit int) ([]domain.WebhookDelivery, error) {
	query := `
		SELECT id, event_id, target_url, attempt, status, next_retry_at, last_error, created_at, updated_at
		FROM webhook_deliveries
		WHERE status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending webhooks: %w", err)
	}
	defer rows.Close()

	var deliveries []domain.WebhookDelivery
	for rows.Next() {
		var d domain.WebhookDelivery
		if err := rows.Scan(&d.ID, &d.EventID, &d.TargetURL, &d.Attempt, &d.Status, &d.NextRetryAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		deliveries = append(deliveries, d)
	}

	return deliveries, nil
}
