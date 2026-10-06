package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/smshiblyrahman/gopay/internal/handler"
	"github.com/smshiblyrahman/gopay/internal/lock"
	"github.com/smshiblyrahman/gopay/internal/metrics"
	"github.com/smshiblyrahman/gopay/internal/middleware"
	"github.com/smshiblyrahman/gopay/internal/repository"
	"github.com/smshiblyrahman/gopay/internal/service"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8080")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gopay?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	jwtSecret := getEnv("JWT_SECRET", "super-secret-gopay-key")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("[API Server] Starting GoPay REST Service on port %s...", port)

	// Initialize Database Repository
	repo, err := repository.NewPostgresRepository(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer repo.Close()

	// Initialize Redis Locker
	locker := lock.NewRedisLocker(redisAddr, "", 0)
	defer locker.Close()

	// Initialize Service & Handler
	paymentSvc := service.NewPaymentService(repo, locker)
	paymentHandler := handler.NewPaymentHandler(paymentSvc)

	// Router setup
	r := chi.NewRouter()

	// Middlewares
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.Logger)
	r.Use(chimiddleware.Recoverer)
	r.Use(metrics.MetricsMiddleware)

	// Rate limiter: 100 reqs / second
	rateLimiter := middleware.NewRateLimiter(100, 1*time.Second)
	r.Use(rateLimiter.Middleware())

	// Public Health and Metrics
	r.Get("/health", paymentHandler.HealthCheck)
	r.Handle("/metrics", promhttp.Handler())

	// Optional JWT Auth wrapper (enabled if REQUIRE_AUTH=true)
	requireAuth := getEnv("REQUIRE_AUTH", "false")

	mountRoutes := func(router chi.Router) {
		router.Post("/payments", paymentHandler.CreatePayment)
		router.Get("/payments/{id}", paymentHandler.GetPayment)
		router.Post("/payments/{id}/confirm", paymentHandler.ConfirmPayment)
		router.Post("/payments/{id}/refund", paymentHandler.RefundPayment)
	}

	// Route bindings (both root & /api/v1 prefixes as requested in GOPAY.md)
	r.Group(func(sub chi.Router) {
		if requireAuth == "true" {
			sub.Use(middleware.JWTAuth(jwtSecret))
		}
		mountRoutes(sub)
	})

	r.Route("/api/v1", func(api chi.Router) {
		if requireAuth == "true" {
			api.Use(middleware.JWTAuth(jwtSecret))
		}
		mountRoutes(api)
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	log.Printf("[API Server] Server is ready to receive requests on http://localhost:%s", port)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[API Server] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("forced server shutdown: %v", err)
	}
	log.Println("[API Server] Stopped.")
}
