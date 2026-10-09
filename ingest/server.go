package ingest

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"

	"context"

	log "github.com/sirupsen/logrus"
)

// BatchWriter — точка записи ingest-контура; реализация на ClickHouse — в clickhouse.go.
type BatchWriter interface {
	EnsureTables(ctx context.Context) error
	InsertModelRun(ctx context.Context, r ModelRun) error
	InsertManualSeries(ctx context.Context, points []ManualSeriesPoint) error
}

// Server — HTTP-сервер ingest-контура: единственный путь записи в model_runs/forecast_log/macro_series.
type Server struct {
	writer BatchWriter
	token  string
	mux    *http.ServeMux
}

func NewServer(writer BatchWriter, token string) *Server {
	s := &Server{writer: writer, token: token, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/model_run", s.handleModelRun)
	s.mux.HandleFunc("POST /v1/manual_series", s.handleManualSeries)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleModelRun(w http.ResponseWriter, r *http.Request) {
	var run ModelRun
	if err := json.NewDecoder(r.Body).Decode(&run); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
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
	if err := json.NewDecoder(r.Body).Decode(&points); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
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
