package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// maxBodyBytes — предел тела запроса (1 MiB); больше — 413.
const maxBodyBytes = 1 << 20

// BatchWriter — точка записи ingest-контура; реализация на ClickHouse — в clickhouse.go.
type BatchWriter interface {
	EnsureTables(ctx context.Context) error
	InsertModelRun(ctx context.Context, r ModelRun) error
	InsertManualSeries(ctx context.Context, points []ManualSeriesPoint) error
}

// Server — HTTP-сервер ingest-контура: единственный путь записи в model_runs/forecast_log/macro_series.
type Server struct {
	writer  BatchWriter
	token   string
	mux     *http.ServeMux
	limiter *tokenBucket
}

// NewServer строит сервер с общим лимитом ratePerMin запросов в минуту (burst 10).
func NewServer(writer BatchWriter, token string, ratePerMin int) *Server {
	return newServer(writer, token, newTokenBucket(ratePerMin, rateBurst, time.Now))
}

func newServer(writer BatchWriter, token string, limiter *tokenBucket) *Server {
	s := &Server{writer: writer, token: token, mux: http.NewServeMux(), limiter: limiter}
	s.mux.HandleFunc("POST /v1/model_run", s.handleModelRun)
	s.mux.HandleFunc("POST /v1/manual_series", s.handleManualSeries)
	return s
}

// ServeHTTP: сначала авторизация (401), затем лимит частоты (429), затем маршрут.
// Неавторизованные запросы лимит не расходуют.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.limiter.allow() {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	s.mux.ServeHTTP(w, r)
}

// decodeBody читает тело ровно одного JSON-объекта: лимит размера, неизвестные поля — ошибка.
// Возвращает статус HTTP для ошибки: 413 при превышении лимита, иначе 400.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) (int, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return http.StatusRequestEntityTooLarge, err
		}
		return http.StatusBadRequest, err
	}
	return http.StatusOK, nil
}

func (s *Server) handleModelRun(w http.ResponseWriter, r *http.Request) {
	var run ModelRun
	if status, err := decodeBody(w, r, &run); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), status)
		return
	}
	if err := ValidateModelRun(run); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.writer.InsertModelRun(r.Context(), run); err != nil {
		log.Errorf("ingest: insert model_run %s: %v", run.RunID, err)
		http.Error(w, "insert failed", http.StatusInternalServerError)
		return
	}
	writeInserted(w, 1)
}

func (s *Server) handleManualSeries(w http.ResponseWriter, r *http.Request) {
	var points []ManualSeriesPoint
	if status, err := decodeBody(w, r, &points); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), status)
		return
	}
	for _, point := range points {
		if err := ValidateManualSeriesPoint(point); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if err := s.writer.InsertManualSeries(r.Context(), points); err != nil {
		log.Errorf("ingest: insert manual_series: %v", err)
		http.Error(w, "insert failed", http.StatusInternalServerError)
		return
	}
	writeInserted(w, len(points))
}

func writeInserted(w http.ResponseWriter, inserted int) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]int{"inserted": inserted}); err != nil {
		log.Errorf("ingest: encode response: %v", err)
	}
}
