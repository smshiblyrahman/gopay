package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/smshiblyrahman/gopay/internal/domain"
	"github.com/smshiblyrahman/gopay/internal/lock"
	"github.com/smshiblyrahman/gopay/internal/repository"
)

type CreatePaymentInput struct {
	CustomerID     string  `json:"customer_id"`
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	IdempotencyKey string  `json:"-"`
	RawPayload     []byte  `json:"-"`
}

type PaymentService interface {
	CreatePayment(ctx context.Context, input CreatePaymentInput) (*domain.Payment, bool, error)
	GetPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
	ConfirmPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
	RefundPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
}

type paymentService struct {
	repo   repository.Repository
	locker lock.Locker
}

func NewPaymentService(repo repository.Repository, locker lock.Locker) PaymentService {
	return &paymentService{
		repo:   repo,
		locker: locker,
	}
}

// CreatePayment handles idempotent payment creation with distributed locking
func (s *paymentService) CreatePayment(ctx context.Context, input CreatePaymentInput) (*domain.Payment, bool, error) {
	if input.Amount <= 0 {
		return nil, false, fmt.Errorf("amount must be greater than zero")
	}
	if input.CustomerID == "" {
		return nil, false, fmt.Errorf("customer_id is required")
	}
	if input.Currency == "" {
		return nil, false, fmt.Errorf("currency is required")
	}

	reqHash := repository.HashPayload(input.RawPayload)

	// Check idempotency cache/db if key provided
	if input.IdempotencyKey != "" {
		// Acquire lock for idempotency key to prevent concurrent duplicates
		lockVal, err := s.locker.Acquire(ctx, "idemp:"+input.IdempotencyKey, 10*time.Second)
		if err != nil {
			return nil, false, fmt.Errorf("idempotency lock: %w", err)
		}
		defer s.locker.Release(ctx, "idemp:"+input.IdempotencyKey, lockVal)

		existing, err := s.repo.GetIdempotencyRecord(ctx, input.IdempotencyKey)
		if err != nil {
			return nil, false, fmt.Errorf("check idempotency: %w", err)
		}
		if existing != nil {
			if existing.RequestHash != reqHash {
				return nil, false, domain.ErrIdempotencyConflict
			}
			var cachedPayment domain.Payment
			if err := json.Unmarshal([]byte(existing.ResponseBody), &cachedPayment); err == nil {
				return &cachedPayment, true, nil
			}
		}
	}

	paymentID := uuid.New()
	now := time.Now().UTC()

	var idempKey *string
	if input.IdempotencyKey != "" {
		idempKey = &input.IdempotencyKey
	}

	payment := &domain.Payment{
		ID:             paymentID,
		CustomerID:     input.CustomerID,
		Amount:         input.Amount,
		Currency:       input.Currency,
		Status:         domain.StatusCreated,
		IdempotencyKey: idempKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"type":        "payment.created",
		"payment_id":  payment.ID.String(),
		"customer_id": payment.CustomerID,
		"amount":      payment.Amount,
		"currency":    payment.Currency,
		"status":      payment.Status,
		"timestamp":   now,
	})

	outboxEvent := &domain.OutboxEvent{
		ID:        uuid.New(),
		EventType: "payment.created",
		Payload:   eventPayload,
		Published: false,
		CreatedAt: now,
	}

	respBody, _ := json.Marshal(payment)

	if err := s.repo.CreatePaymentWithTx(ctx, payment, outboxEvent, reqHash, string(respBody)); err != nil {
		return nil, false, fmt.Errorf("create payment tx: %w", err)
	}

	// Update status cache
	_ = s.locker.SetStatus(ctx, payment.ID.String(), string(payment.Status), 30*time.Minute)

	return payment, false, nil
}

func (s *paymentService) GetPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	return s.repo.GetPaymentByID(ctx, id)
}

func (s *paymentService) ConfirmPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	// Distributed lock per payment
	lockVal, err := s.locker.Acquire(ctx, "payment:"+id.String(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("acquire payment lock: %w", err)
	}
	defer s.locker.Release(ctx, "payment:"+id.String(), lockVal)

	now := time.Now().UTC()
	txRecord := &domain.Transaction{
		ID:        uuid.New(),
		PaymentID: id,
		Type:      domain.TxTypeCharge,
		Amount:    0, // Will be updated or verified inside repo
		Status:    domain.TxStatusSuccess,
		CreatedAt: now,
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"type":       "payment.confirmed",
		"payment_id": id.String(),
		"status":     domain.StatusConfirmed,
		"timestamp":  now,
	})

	outboxEvent := &domain.OutboxEvent{
		ID:        uuid.New(),
		EventType: "payment.confirmed",
		Payload:   eventPayload,
		Published: false,
		CreatedAt: now,
	}

	payment, err := s.repo.ConfirmPaymentWithTx(ctx, id, txRecord, outboxEvent)
	if err != nil {
		return nil, err
	}

	_ = s.locker.SetStatus(ctx, payment.ID.String(), string(payment.Status), 30*time.Minute)
	return payment, nil
}

func (s *paymentService) RefundPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	// Distributed lock per payment
	lockVal, err := s.locker.Acquire(ctx, "payment:"+id.String(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("acquire payment lock: %w", err)
	}
	defer s.locker.Release(ctx, "payment:"+id.String(), lockVal)

	now := time.Now().UTC()
	refundTx := &domain.Transaction{
		ID:        uuid.New(),
		PaymentID: id,
		Type:      domain.TxTypeRefund,
		Amount:    0,
		Status:    domain.TxStatusSuccess,
		CreatedAt: now,
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"type":       "payment.refunded",
		"payment_id": id.String(),
		"status":     domain.StatusRefunded,
		"timestamp":  now,
	})

	outboxEvent := &domain.OutboxEvent{
		ID:        uuid.New(),
		EventType: "payment.refunded",
		Payload:   eventPayload,
		Published: false,
		CreatedAt: now,
	}

	payment, err := s.repo.RefundPaymentWithTx(ctx, id, refundTx, outboxEvent)
	if err != nil {
		return nil, err
	}

	_ = s.locker.SetStatus(ctx, payment.ID.String(), string(payment.Status), 30*time.Minute)
	return payment, nil
}
