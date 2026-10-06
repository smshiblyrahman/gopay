package service_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/smshiblyrahman/gopay/internal/domain"
	"github.com/smshiblyrahman/gopay/internal/service"
)

// Mock Locker implementation using standard mutex
type MockLocker struct {
	mu     sync.Mutex
	locks  map[string]string
	status map[string]string
}

func NewMockLocker() *MockLocker {
	return &MockLocker{
		locks:  make(map[string]string),
		status: make(map[string]string),
	}
}

func (m *MockLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (string, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		if _, exists := m.locks[key]; !exists {
			val := uuid.New().String()
			m.locks[key] = val
			m.mu.Unlock()
			return val, nil
		}
		m.mu.Unlock()

		if time.Now().After(deadline) {
			return "", domain.ErrLockAcquisitionFailed
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (m *MockLocker) Release(ctx context.Context, key string, lockVal string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, exists := m.locks[key]; exists && v == lockVal {
		delete(m.locks, key)
	}
	return nil
}

func (m *MockLocker) GetStatus(ctx context.Context, paymentID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status[paymentID], nil
}

func (m *MockLocker) SetStatus(ctx context.Context, paymentID string, status string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status[paymentID] = status
	return nil
}

func (m *MockLocker) Ping(ctx context.Context) error { return nil }
func (m *MockLocker) Close() error                   { return nil }

// Mock Repository
type MockRepository struct {
	mu          sync.Mutex
	payments    map[uuid.UUID]*domain.Payment
	idempotency map[string]*domain.IdempotencyRecord
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		payments:    make(map[uuid.UUID]*domain.Payment),
		idempotency: make(map[string]*domain.IdempotencyRecord),
	}
}

func (r *MockRepository) CreatePaymentWithTx(ctx context.Context, p *domain.Payment, outbox *domain.OutboxEvent, reqHash string, respBody string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.payments[p.ID] = p
	if p.IdempotencyKey != nil && *p.IdempotencyKey != "" {
		r.idempotency[*p.IdempotencyKey] = &domain.IdempotencyRecord{
			Key:            *p.IdempotencyKey,
			RequestHash:    reqHash,
			ResponseStatus: 201,
			ResponseBody:   respBody,
			CreatedAt:      p.CreatedAt,
		}
	}
	return nil
}

func (r *MockRepository) GetPaymentByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok {
		return nil, domain.ErrPaymentNotFound
	}
	return p, nil
}

func (r *MockRepository) ConfirmPaymentWithTx(ctx context.Context, id uuid.UUID, txRecord *domain.Transaction, outbox *domain.OutboxEvent) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok {
		return nil, domain.ErrPaymentNotFound
	}
	if p.Status == domain.StatusConfirmed {
		return p, nil
	}
	if p.Status != domain.StatusCreated && p.Status != domain.StatusPending {
		return nil, fmt.Errorf("%w: invalid state", domain.ErrInvalidStatus)
	}
	p.Status = domain.StatusConfirmed
	p.UpdatedAt = time.Now().UTC()
	return p, nil
}

func (r *MockRepository) RefundPaymentWithTx(ctx context.Context, id uuid.UUID, refundTx *domain.Transaction, outbox *domain.OutboxEvent) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok {
		return nil, domain.ErrPaymentNotFound
	}
	if p.Status == domain.StatusRefunded {
		return nil, domain.ErrAlreadyRefunded
	}
	if p.Status != domain.StatusConfirmed {
		return nil, domain.ErrRefundInvalidState
	}
	p.Status = domain.StatusRefunded
	p.UpdatedAt = time.Now().UTC()
	return p, nil
}

func (r *MockRepository) GetIdempotencyRecord(ctx context.Context, key string) (*domain.IdempotencyRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.idempotency[key], nil
}

func (r *MockRepository) FetchUnpublishedEvents(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	return nil, nil
}

func (r *MockRepository) MarkEventPublished(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (r *MockRepository) CreateWebhookDelivery(ctx context.Context, d *domain.WebhookDelivery) error {
	return nil
}

func (r *MockRepository) UpdateWebhookDelivery(ctx context.Context, d *domain.WebhookDelivery) error {
	return nil
}

func (r *MockRepository) GetPendingWebhookDeliveries(ctx context.Context, limit int) ([]domain.WebhookDelivery, error) {
	return nil, nil
}

func (r *MockRepository) Ping(ctx context.Context) error { return nil }
func (r *MockRepository) Close()                         {}

// Concurrency Test: Simulate 100 simultaneous requests with the SAME Idempotency Key
func TestConcurrentPaymentIdempotency(t *testing.T) {
	repo := NewMockRepository()
	locker := NewMockLocker()
	svc := service.NewPaymentService(repo, locker)

	const totalRequests = 100
	idempKey := "order-test-idemp-100"
	payload := []byte(`{"customer_id":"cust_123","amount":99.99,"currency":"USD"}`)

	var (
		wg          sync.WaitGroup
		successNew  int32
		successReplay int32
		errorsCount int32
	)

	wg.Add(totalRequests)
	for i := 0; i < totalRequests; i++ {
		go func() {
			defer wg.Done()
			input := service.CreatePaymentInput{
				CustomerID:     "cust_123",
				Amount:         99.99,
				Currency:       "USD",
				IdempotencyKey: idempKey,
				RawPayload:     payload,
			}
			p, isReplay, err := svc.CreatePayment(context.Background(), input)
			if err != nil {
				atomic.AddInt32(&errorsCount, 1)
				return
			}
			if p != nil {
				if isReplay {
					atomic.AddInt32(&successReplay, 1)
				} else {
					atomic.AddInt32(&successNew, 1)
				}
			}
		}()
	}

	wg.Wait()

	if successNew != 1 {
		t.Fatalf("expected exactly 1 new payment created, got %d", successNew)
	}

	if successReplay != totalRequests-1 {
		t.Fatalf("expected %d replays, got %d (errors: %d)", totalRequests-1, successReplay, errorsCount)
	}
}

// Test Confirm and Double Refund prevention
func TestConfirmAndRefund(t *testing.T) {
	repo := NewMockRepository()
	locker := NewMockLocker()
	svc := service.NewPaymentService(repo, locker)

	// Create
	p, _, err := svc.CreatePayment(context.Background(), service.CreatePaymentInput{
		CustomerID:     "cust_test",
		Amount:         50.00,
		Currency:       "EUR",
		IdempotencyKey: "key-1",
		RawPayload:     []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("failed create: %v", err)
	}

	// Cannot refund unconfirmed payment
	_, err = svc.RefundPayment(context.Background(), p.ID)
	if err == nil {
		t.Fatalf("expected error refunding unconfirmed payment")
	}

	// Confirm payment
	confirmed, err := svc.ConfirmPayment(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("failed confirm: %v", err)
	}
	if confirmed.Status != domain.StatusConfirmed {
		t.Fatalf("expected CONFIRMED, got %s", confirmed.Status)
	}

	// Refund payment
	refunded, err := svc.RefundPayment(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("failed refund: %v", err)
	}
	if refunded.Status != domain.StatusRefunded {
		t.Fatalf("expected REFUNDED, got %s", refunded.Status)
	}

	// Attempt second refund -> must fail
	_, err = svc.RefundPayment(context.Background(), p.ID)
	if err != domain.ErrAlreadyRefunded {
		t.Fatalf("expected ErrAlreadyRefunded, got %v", err)
	}
}
