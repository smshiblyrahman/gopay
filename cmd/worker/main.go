package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/smshiblyrahman/gopay/internal/outbox"
	"github.com/smshiblyrahman/gopay/internal/queue"
	"github.com/smshiblyrahman/gopay/internal/repository"
	"github.com/smshiblyrahman/gopay/internal/webhook"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gopay?sslmode=disable")
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	webhookSecret := getEnv("WEBHOOK_SECRET", "super-secret-webhook-hmac-key")
	webhookTarget := getEnv("WEBHOOK_TARGET_URL", "http://localhost:8080/mock-webhook")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("[Worker Process] Starting GoPay Background Workers (Outbox & Webhooks)...")

	// 1. Connect Postgres
	repo, err := repository.NewPostgresRepository(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer repo.Close()

	// 2. Connect RabbitMQ
	rabbitClient, err := queue.NewRabbitMQClient(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to rabbitmq: %v", err)
	}
	defer rabbitClient.Close()

	// 3. Start Transactional Outbox Worker
	outboxWorker := outbox.NewOutboxWorker(repo, rabbitClient, 1*time.Second, 50)
	go outboxWorker.Start(ctx)

	// 4. Start Webhook Delivery Worker
	webhookDispatcher := webhook.NewDispatcher(repo, rabbitClient, webhookSecret, webhookTarget)
	go func() {
		if err := webhookDispatcher.Start(ctx); err != nil {
			log.Printf("[Worker Process] Webhook dispatcher stopped: %v", err)
		}
	}()

	log.Println("[Worker Process] All background workers operational.")

	// Wait for OS termination signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[Worker Process] Graceful shutdown initiated...")
	cancel()
	time.Sleep(1 * time.Second)
	log.Println("[Worker Process] Background workers stopped.")
}
