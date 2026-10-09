package ingest

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// runExistsDelay расширяет окно между проверкой существования и вставкой:
// без мьютекса конкурентные повторы успевают пройти проверку до первой записи.
const runExistsDelay = 20 * time.Millisecond

// sentBatch — батч, успешно отправленный фейковым ClickHouse.
type sentBatch struct {
	query string
	rows  [][]any
}

// fakeStore — фейк chStore: помнит закоммиченные run_id и отправленные батчи.
type fakeStore struct {
	mu        sync.Mutex
	committed map[uuid.UUID]bool
	sent      []sentBatch
	failSend  map[string]error // ошибка Send по тексту запроса
}

func newFakeStore() *fakeStore {
	return &fakeStore{committed: map[uuid.UUID]bool{}, failSend: map[string]error{}}
}

func (f *fakeStore) runExists(_ context.Context, runID uuid.UUID) (bool, error) {
	f.mu.Lock()
	exists := f.committed[runID]
	f.mu.Unlock()
	time.Sleep(runExistsDelay)
	return exists, nil
}

func (f *fakeStore) prepareBatch(_ context.Context, query string) (chBatch, error) {
	return &fakeBatch{store: f, query: query}, nil
}

// batchesFor — сколько батчей с этим запросом успешно отправлено.
func (f *fakeStore) batchesFor(query string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.sent {
		if b.query == query {
			n++
		}
	}
	return n
}

// fakeBatch — батч фейка. Send для model_runs помечает run_id (первый столбец) как закоммиченный.
type fakeBatch struct {
	store *fakeStore
	query string
	rows  [][]any
}

func (b *fakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, v)
	return nil
}

func (b *fakeBatch) Send() error {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	if err := b.store.failSend[b.query]; err != nil {
		return err
	}
	if b.query == modelRunsInsert {
		runID, ok := b.rows[0][0].(uuid.UUID)
		if !ok {
			return errors.New("model_runs row must start with a uuid.UUID run_id")
		}
		b.store.committed[runID] = true
	}
	b.store.sent = append(b.store.sent, sentBatch{query: b.query, rows: b.rows})
	return nil
}

func newTestWriter(store *fakeStore) *ClickHouseWriter {
	return &ClickHouseWriter{store: store}
}

func TestScenarioTargetDate(t *testing.T) {
	tests := []struct {
		name    string
		horizon string
		want    string
	}{
		{"quarter Q1", "Q1-2026", "2026-03-31"},
		{"quarter Q4", "Q4-2026", "2026-12-31"},
		{"year end YE", "YE-2026", "2026-12-31"},
		{"bare year", "2026", "2026-12-31"},
		{"ISO date", "2026-12-31", "2026-12-31"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := scenarioTargetDate(map[string]any{"horizon": tt.horizon})
			if err != nil {
				t.Fatalf("horizon %q: unexpected error: %v", tt.horizon, err)
			}
			if s := got.Format(manualSeriesDateLayout); s != tt.want {
				t.Fatalf("horizon %q: target_date %s, want %s", tt.horizon, s, tt.want)
			}
		})
	}
}

func TestScenarioTargetDateErrors(t *testing.T) {
	tests := []struct {
		name     string
		scenario map[string]any
	}{
		{"horizon missing", map[string]any{}},
		{"horizon not a string", map[string]any{"horizon": 2026.0}},
		{"quarter out of range", map[string]any{"horizon": "Q5-2026"}},
		{"free text", map[string]any{"horizon": "next year"}},
		{"short year", map[string]any{"horizon": "YE-26"}},
		{"negative year", map[string]any{"horizon": "-123"}},
		{"impossible date", map[string]any{"horizon": "2026-02-30"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := scenarioTargetDate(tt.scenario); err == nil {
				t.Fatalf("expected error for %v", tt.scenario)
			}
		})
	}
}

func TestWeightedGoldPoint(t *testing.T) {
	tests := []struct {
		name          string
		scenario      map[string]any
		probabilities map[string]float64
		want          float64
		wantErr       bool
	}{
		{
			name:          "point wins over midpoint",
			scenario:      map[string]any{"bull": map[string]any{"low": 4600.0, "high": 5000.0, "point": 4900.0}},
			probabilities: map[string]float64{"bull": 100},
			want:          4900,
		},
		{
			name:          "midpoint when no point",
			scenario:      map[string]any{"base": map[string]any{"low": 4000.0, "high": 4600.0}},
			probabilities: map[string]float64{"base": 100},
			want:          4300,
		},
		{
			name: "midpoint mixed with point",
			scenario: map[string]any{
				"bull": map[string]any{"low": 4600.0, "high": 5000.0},
				"bear": map[string]any{"point": 3900.0},
			},
			probabilities: map[string]float64{"bull": 50, "bear": 50},
			want:          4350,
		},
		{
			name: "weights divided by their sum",
			scenario: map[string]any{
				"bull": map[string]any{"point": 4800.0},
				"bear": map[string]any{"point": 3900.0},
			},
			probabilities: map[string]float64{"bull": 2, "bear": 2},
			want:          4350,
		},
		{
			name:          "probability key without block",
			scenario:      map[string]any{"bull": map[string]any{"point": 4800.0}},
			probabilities: map[string]float64{"bull": 25, "base": 75},
			wantErr:       true,
		},
		{
			name:          "block without point and range",
			scenario:      map[string]any{"base": map[string]any{"low": 4000.0}},
			probabilities: map[string]float64{"base": 100},
			wantErr:       true,
		},
		{
			name:          "block is not an object",
			scenario:      map[string]any{"base": 4300.0},
			probabilities: map[string]float64{"base": 100},
			wantErr:       true,
		},
		{
			name:          "probabilities empty",
			scenario:      map[string]any{"base": map[string]any{"point": 4300.0}},
			probabilities: nil,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := weightedGoldPoint(tt.scenario, tt.probabilities)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("weighted point %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInsertModelRunExistingRunSkipsAllBatches(t *testing.T) {
	store := newFakeStore()
	store.committed[uuid.MustParse(testRunID)] = true
	writer := newTestWriter(store)

	if err := writer.InsertModelRun(context.Background(), validModelRun()); err != nil {
		t.Fatalf("repeat POST must succeed: %v", err)
	}
	if len(store.sent) != 0 {
		t.Fatalf("existing run_id must not trigger any batch, got %d", len(store.sent))
	}
}

func TestInsertModelRunWritesForecastLogBeforeModelRuns(t *testing.T) {
	store := newFakeStore()
	run := validModelRun()
	run.NavPerShare = 12500

	if err := newTestWriter(store).InsertModelRun(context.Background(), run); err != nil {
		t.Fatalf("InsertModelRun: %v", err)
	}
	if len(store.sent) != 2 {
		t.Fatalf("batches sent: %d, want 2", len(store.sent))
	}
	if store.sent[0].query != forecastLogInsert || store.sent[1].query != modelRunsInsert {
		t.Fatalf("order: %q then %q, want forecast_log then model_runs", store.sent[0].query, store.sent[1].query)
	}
	if !store.committed[uuid.MustParse(testRunID)] {
		t.Fatal("model_runs row must be committed")
	}

	forecast := store.sent[0].rows
	if len(forecast) != 2 {
		t.Fatalf("forecast_log rows: %d, want 2", len(forecast))
	}
	wantRows := []struct {
		metric    string
		predicted float64
	}{
		{"nav", 12500},
		{"xau_q_avg", 4285},
	}
	for i, want := range wantRows {
		metric, _ := forecast[i][2].(string)
		predicted, _ := forecast[i][3].(float64)
		target, _ := forecast[i][1].(time.Time)
		if metric != want.metric || math.Abs(predicted-want.predicted) > 1e-9 {
			t.Fatalf("forecast row %d: metric %q predicted %v, want %q %v", i, metric, predicted, want.metric, want.predicted)
		}
		if s := target.Format(manualSeriesDateLayout); s != "2026-12-31" {
			t.Fatalf("forecast row %d: target_date %s, want 2026-12-31", i, s)
		}
	}
}

func TestInsertModelRunForecastFailureIsRetried(t *testing.T) {
	store := newFakeStore()
	store.failSend[forecastLogInsert] = errors.New("forecast_log unavailable")
	writer := newTestWriter(store)
	run := validModelRun()
	run.NavPerShare = 12500

	if err := writer.InsertModelRun(context.Background(), run); err == nil {
		t.Fatal("expected error when forecast_log insert fails")
	}
	if n := store.batchesFor(modelRunsInsert); n != 0 {
		t.Fatalf("model_runs written %d times after forecast_log failure, want 0", n)
	}

	delete(store.failSend, forecastLogInsert)
	if err := writer.InsertModelRun(context.Background(), run); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n := store.batchesFor(modelRunsInsert); n != 1 {
		t.Fatalf("model_runs written %d times after retry, want 1", n)
	}
	if n := store.batchesFor(forecastLogInsert); n != 1 {
		t.Fatalf("forecast_log batches %d after retry, want 1", n)
	}
}

func TestInsertModelRunConcurrentRetriesWriteOnce(t *testing.T) {
	store := newFakeStore()
	writer := newTestWriter(store)
	run := validModelRun()
	run.NavPerShare = 12500

	const attempts = 8
	errs := make([]error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = writer.InsertModelRun(context.Background(), run)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if n := store.batchesFor(modelRunsInsert); n != 1 {
		t.Fatalf("model_runs written %d times for %d concurrent POSTs with one run_id, want 1", n, attempts)
	}
	if n := store.batchesFor(forecastLogInsert); n != 1 {
		t.Fatalf("forecast_log batches %d, want 1", n)
	}
}
