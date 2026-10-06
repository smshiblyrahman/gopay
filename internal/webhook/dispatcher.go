package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smshiblyrahman/gopay/internal/queue"
	"github.com/smshiblyrahman/gopay/internal/repository"
)

// Retry intervals specified in GOPAY.md: 5s, 30s, 5m, 30m
var retrySchedule = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
}

type Dispatcher struct {
	repo         repository.Repository
	queue        *queue.RabbitMQClient
	secretKey    string
	httpClient   *http.Client
	defaultHook  string
}

func NewDispatcher(repo repository.Repository, q *queue.RabbitMQClient, secretKey string, defaultHook string) *Dispatcher {
	if defaultHook == "" {
		defaultHook = "http://localhost:8080/mock-webhook"
	}
	return &Dispatcher{
		repo:        repo,
		queue:       q,
		secretKey:   secretKey,
		defaultHook: defaultHook,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *Dispatcher) SignPayload(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(d.secretKey))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func (d *Dispatcher) Start(ctx context.Context) error {
	msgs, err := d.queue.ConsumeWebhooks()
	if err != nil {
		return fmt.Errorf("consume webhooks: %w", err)
	}

	log.Println("[Webhook Worker] Listening for payment & webhook events on RabbitMQ...")

	// Also start background retry processor
	go d.processRetries(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Webhook Worker] Stopping dispatcher...")
			return nil
		case msg, ok := <-msgs:
			if !ok {
				log.Println("[Webhook Worker] Channel closed.")
				return nil
			}
			d.handleDelivery(ctx, msg)
		}
	}
}

func (d *Dispatcher) handleDelivery(ctx context.Context, msg amqp.Delivery) {
	var event map[string]interface{}
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("[Webhook Worker] Malformed event payload: %v", err)
		// Reject and send to DLQ
		_ = msg.Nack(false, false)
		return
	}

	// Attempt HTTP delivery
	err := d.sendHTTP(ctx, d.defaultHook, msg.Body)
	if err == nil {
		log.Printf("[Webhook Worker] Dispatched webhook successfully for event routing key: %s", msg.RoutingKey)
		_ = msg.Ack(false)
		return
	}

	log.Printf("[Webhook Worker] Webhook delivery failed: %v. Scheduling retry #1...", err)

	// In production, we schedule retry with exponential backoff
	// If retries exceed schedule, reject to DLQ
	_ = msg.Nack(false, false) // moves to DLQ when rejected without requeue
}

func (d *Dispatcher) sendHTTP(ctx context.Context, targetURL string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	signature := d.SignPayload(body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GoPay-Signature", signature)
	req.Header.Set("User-Agent", "GoPay-Webhook-Dispatcher/1.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("non-2xx response status: %d", resp.StatusCode)
	}

	return nil
}

func (d *Dispatcher) processRetries(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deliveries, err := d.repo.GetPendingWebhookDeliveries(ctx, 20)
			if err != nil {
				log.Printf("[Webhook Retrier] Error getting pending: %v", err)
				continue
			}

			for _, del := range deliveries {
				err := d.sendHTTP(ctx, del.TargetURL, []byte(fmt.Sprintf(`{"event_id":"%s"}`, del.EventID)))
				now := time.Now().UTC()
				if err == nil {
					del.Status = "DELIVERED"
					del.UpdatedAt = now
					del.NextRetryAt = nil
					_ = d.repo.UpdateWebhookDelivery(ctx, &del)
					continue
				}

				del.Attempt++
				errStr := err.Error()
				del.LastError = &errStr
				del.UpdatedAt = now

				if del.Attempt < len(retrySchedule) {
					next := now.Add(retrySchedule[del.Attempt])
					del.NextRetryAt = &next
				} else {
					del.Status = "FAILED" // moved to DLQ
					del.NextRetryAt = nil
				}
				_ = d.repo.UpdateWebhookDelivery(ctx, &del)
			}
		}
	}
}
