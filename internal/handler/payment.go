package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/smshiblyrahman/gopay/internal/domain"
	"github.com/smshiblyrahman/gopay/internal/metrics"
	"github.com/smshiblyrahman/gopay/internal/service"
)

type PaymentHandler struct {
	svc service.PaymentService
}

func NewPaymentHandler(svc service.PaymentService) *PaymentHandler {
	return &PaymentHandler{svc: svc}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// POST /api/v1/payments & POST /payments
func (h *PaymentHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read request body")
		return
	}

	var req struct {
		CustomerID string  `json:"customer_id"`
		Amount     float64 `json:"amount"`
		Currency   string  `json:"currency"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json payload")
		return
	}

	idempKey := r.Header.Get("Idempotency-Key")

	input := service.CreatePaymentInput{
		CustomerID:     req.CustomerID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		IdempotencyKey: idempKey,
		RawPayload:     body,
	}

	payment, isReplay, err := h.svc.CreatePayment(r.Context(), input)
	if err != nil {
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if isReplay {
		w.Header().Set("X-Idempotent-Replay", "true")
	}

	writeJSON(w, http.StatusCreated, payment)
}

// GET /api/v1/payments/{id} & GET /payments/{id}
func (h *PaymentHandler) GetPayment(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	paymentID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid payment id uuid format")
		return
	}

	payment, err := h.svc.GetPayment(r.Context(), paymentID)
	if err != nil {
		if errors.Is(err, domain.ErrPaymentNotFound) {
			writeError(w, http.StatusNotFound, "payment not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, payment)
}

// POST /api/v1/payments/{id}/confirm & POST /payments/{id}/confirm
func (h *PaymentHandler) ConfirmPayment(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	paymentID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid payment id format")
		return
	}

	payment, err := h.svc.ConfirmPayment(r.Context(), paymentID)
	if err != nil {
		if errors.Is(err, domain.ErrPaymentNotFound) {
			writeError(w, http.StatusNotFound, "payment not found")
			return
		}
		if errors.Is(err, domain.ErrInvalidStatus) {
			metrics.PaymentFailedTotal.Inc()
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		metrics.PaymentFailedTotal.Inc()
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	metrics.PaymentSuccessTotal.Inc()
	writeJSON(w, http.StatusOK, payment)
}

// POST /api/v1/payments/{id}/refund & POST /payments/{id}/refund
func (h *PaymentHandler) RefundPayment(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	paymentID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid payment id format")
		return
	}

	payment, err := h.svc.RefundPayment(r.Context(), paymentID)
	if err != nil {
		if errors.Is(err, domain.ErrPaymentNotFound) {
			writeError(w, http.StatusNotFound, "payment not found")
			return
		}
		if errors.Is(err, domain.ErrAlreadyRefunded) {
			writeError(w, http.StatusConflict, "payment already refunded")
			return
		}
		if errors.Is(err, domain.ErrRefundInvalidState) {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	metrics.RefundSuccessTotal.Inc()
	writeJSON(w, http.StatusOK, payment)
}

// GET /health
func (h *PaymentHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "gopay",
	})
}
