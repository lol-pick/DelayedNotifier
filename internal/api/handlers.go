package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"l3.1/internal/models"
	"l3.1/internal/queue"
	"l3.1/internal/storage"
)

type Handler struct {
	repo      storage.NotificationRepository
	publisher queue.Publisher
}

func New(repo storage.NotificationRepository, pub queue.Publisher) *Handler {
	return &Handler{repo: repo, publisher: pub}
}

func (h *Handler) Routes(uiDir string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /notify", h.create)
	mux.HandleFunc("GET /notify/{id}", h.get)
	mux.HandleFunc("DELETE /notify/{id}", h.cancel)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	if uiDir != "" {
		mux.Handle("GET /", http.FileServer(http.Dir(uiDir)))
	}

	return logMiddleware(mux)
}

type createRequest struct {
	Channel   models.Channel `json:"channel"`
	Recipient string         `json:"recipient"`
	Subject   string         `json:"subject"`
	Message   string         `json:"message"`
	SendAt    time.Time      `json:"send_at"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
	}

	if req.Channel != models.ChannelEmail && req.Channel != models.ChannelTelegram {
		writeError(w, http.StatusBadRequest, "channel must be 'email' or 'telegram'")
		return
	}
	if req.Recipient == "" || req.Message == "" {
		writeError(w, http.StatusBadRequest, "recipient and message are required")
	}
	if req.SendAt.IsZero() {
		writeError(w, http.StatusBadRequest, "send_at is required (RFC3339)")
		return
	}

	n := &models.Notification{
		ID:        uuid.NewString(),
		Channel:   req.Channel,
		Recipient: req.Recipient,
		Subject:   req.Subject,
		Message:   req.Message,
		SendAt:    req.SendAt.UTC(),
		Status:    models.StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	if err := h.repo.Save(r.Context(), n); err != nil {
		writeError(w, http.StatusInternalServerError, "save notififcation: "+err.Error())
		return
	}

	delay := time.Until(n.SendAt)
	if delay < 0 {
		delay = 0
	}

	if err := h.publisher.Publish(r.Context(), queue.Message{ID: n.ID, Attempts: 0}, delay); err != nil {
		writeError(w, http.StatusInternalServerError, "publish: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, n)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, err := h.repo.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, err := h.repo.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n.IsTerminal() {
		writeError(w, http.StatusConflict, "notification is already in terminal state:"+string(n.Status))
		return
	}
	if err := h.repo.UpdateStatus(r.Context(), id, models.StatusCanceled, "", n.Attempts); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[api] write json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(lrw, r)
		log.Printf("[http] %s %s -> %d (%s)", r.Method, r.URL.Path, lrw.status, time.Since(start))
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (l *loggingResponseWriter) WriteHeader(code int) {
	l.status = code
	l.ResponseWriter.WriteHeader(code)
}
