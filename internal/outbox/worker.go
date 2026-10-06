package outbox

import (
	"context"
	"log"
	"time"

	"github.com/smshiblyrahman/gopay/internal/queue"
	"github.com/smshiblyrahman/gopay/internal/repository"
)

type OutboxWorker struct {
	repo     repository.Repository
	queue    *queue.RabbitMQClient
	pollFreq time.Duration
	batchLen int
}

func NewOutboxWorker(repo repository.Repository, q *queue.RabbitMQClient, freq time.Duration, batch int) *OutboxWorker {
	if freq == 0 {
		freq = 1 * time.Second
	}
	if batch == 0 {
		batch = 50
	}
	return &OutboxWorker{
		repo:     repo,
		queue:    q,
		pollFreq: freq,
		batchLen: batch,
	}
}

func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.pollFreq)
	defer ticker.Stop()

	log.Println("[Outbox Worker] Started processing unpublished events...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[Outbox Worker] Shutting down...")
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) {
	events, err := w.repo.FetchUnpublishedEvents(ctx, w.batchLen)
	if err != nil {
		log.Printf("[Outbox Worker] Error fetching events: %v", err)
		return
	}

	for _, event := range events {
		// Publish to RabbitMQ using event_type as routing key
		err := w.queue.Publish(ctx, event.EventType, event.Payload)
		if err != nil {
			log.Printf("[Outbox Worker] Failed to publish event %s (%s): %v", event.ID, event.EventType, err)
			continue
		}

		// Mark as published
		if err := w.repo.MarkEventPublished(ctx, event.ID); err != nil {
			log.Printf("[Outbox Worker] Failed to mark event %s as published: %v", event.ID, err)
		} else {
			log.Printf("[Outbox Worker] Successfully published event %s [%s]", event.ID, event.EventType)
		}
	}
}
