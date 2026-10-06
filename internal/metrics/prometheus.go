package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gopay_http_requests_total",
			Help: "Total HTTP requests processed by GoPay",
		},
		[]string{"method", "path", "status"},
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gopay_http_request_duration_seconds",
			Help:    "HTTP request latency distributions",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	PaymentSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gopay_payments_success_total",
			Help: "Total successful confirmed payments",
		},
	)

	PaymentFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gopay_payments_failed_total",
			Help: "Total failed payments",
		},
	)

	RefundSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gopay_refunds_success_total",
			Help: "Total successful refunds processed",
		},
	)
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		interceptor := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(interceptor, r)

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(interceptor.statusCode)

		HttpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, statusStr).Inc()
		HttpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
	})
}
