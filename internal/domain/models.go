package domain

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrPaymentNotFound      = errors.New("payment not found")
	ErrIdempotencyConflict  = errors.New("idempotency key conflict: request payload mismatch")
	ErrInvalidStatus        = errors.New("invalid payment state transition")
	ErrAlreadyRefunded      = errors.New("payment already refunded")
	ErrRefundInvalidState   = errors.New("only confirmed payments can be refunded")
	ErrLockAcquisitionFailed = errors.New("failed to acquire distributed lock")
)

type PaymentStatus string

const (
	StatusCreated   PaymentStatus = "CREATED"
	StatusPending   PaymentStatus = "PENDING"
	StatusConfirmed PaymentStatus = "CONFIRMED"
	StatusFailed    PaymentStatus = "FAILED"
	StatusRefunded  PaymentStatus = "REFUNDED"
)

type TransactionType string

const (
	TxTypeCharge TransactionType = "CHARGE"
	TxTypeRefund TransactionType = "REFUND"
)

type TransactionStatus string

const (
	TxStatusSuccess TransactionStatus = "SUCCESS"
	TxStatusFailed  TransactionStatus = "FAILED"
)

type Payment struct {
	ID             uuid.UUID     `json:"id"`
	CustomerID     string        `json:"customer_id"`
	Amount         float64       `json:"amount"`
	Currency       string        `json:"currency"`
	Status         PaymentStatus `json:"status"`
	IdempotencyKey *string       `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type Transaction struct {
	ID        uuid.UUID         `json:"id"`
	PaymentID uuid.UUID         `json:"payment_id"`
	Type      TransactionType   `json:"type"`
	Amount    float64           `json:"amount"`
	Status    TransactionStatus `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}

type OutboxEvent struct {
	ID        uuid.UUID       `json:"id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	Published bool            `json:"published"`
	CreatedAt time.Time       `json:"created_at"`
}

type IdempotencyRecord struct {
	Key            string    `json:"key"`
	RequestHash    string    `json:"request_hash"`
	ResponseStatus int       `json:"response_status"`
	ResponseBody   string    `json:"response_body"`
	CreatedAt      time.Time `json:"created_at"`
}

type WebhookDelivery struct {
	ID          uuid.UUID  `json:"id"`
	EventID     uuid.UUID  `json:"event_id"`
	TargetURL   string     `json:"target_url"`
	Attempt     int        `json:"attempt"`
	Status      string     `json:"status"` // PENDING, DELIVERED, FAILED
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
