package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	modelRunsInsert   = "INSERT INTO model_runs (run_id, run_date, trigger_type, trigger_ref, gold_scenario, usdrub_path, price_deck, discount_rate, wacc, nav_per_share, nav_bull, nav_base, nav_bear, market_price, upside_pct, nav_beta_gold, comment)"
	forecastLogInsert = "INSERT INTO forecast_log (forecast_date, target_date, metric, predicted, actual, error_pct)"
	macroSeriesInsert = "INSERT INTO macro_series (source, series, date, value)"
)

// chStore — узкий срез ClickHouse, который нужен InsertModelRun; в тестах подменяется фейком.
type chStore interface {
	runExists(ctx context.Context, runID uuid.UUID) (bool, error)
	prepareBatch(ctx context.Context, query string) (chBatch, error)
}

// chBatch — батч вставки: достаточно Append и Send из driver.Batch.
type chBatch interface {
	Append(v ...any) error
	Send() error
}

// connStore — реализация chStore поверх driver.Conn.
type connStore struct {
	conn driver.Conn
}

func (s connStore) runExists(ctx context.Context, runID uuid.UUID) (bool, error) {
	var count uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM model_runs FINAL WHERE run_id = ?", runID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s connStore) prepareBatch(ctx context.Context, query string) (chBatch, error) {
	return s.conn.PrepareBatch(ctx, query)
}

// ClickHouseWriter — BatchWriter поверх clickhouse-go: только батчи PrepareBatch/Append/Send.
type ClickHouseWriter struct {
	conn  driver.Conn
	store chStore
	mu    sync.Mutex // сериализует InsertModelRun, см. комментарий у метода
}

func NewClickHouseWriter(conn driver.Conn) *ClickHouseWriter {
	return &ClickHouseWriter{conn: conn, store: connStore{conn: conn}}
}

func (w *ClickHouseWriter) EnsureTables(ctx context.Context) error {
	return ensureTables(ctx, w.conn)
}

// InsertModelRun пишет прогон модели и производные строки forecast_log:
// metric='nav' (predicted = nav_per_share) и metric='xau_q_avg' (вероятностно-взвешенная
// точка сценариев золота), target_date — конец горизонта сценария, actual/error_pct = NULL.
//
// Идемпотентность по клиентскому run_id: повторный POST с уже записанным run_id пропускается.
// ORDER BY (run_date, run_id) в ReplacingMergeTree не схлопывает строки с разным run_date,
// поэтому дубли из-за повторного POST этим движком не убираются — защищает только проверка ниже.
// Поэтому вызовы сериализованы мьютексом (ingest — один инстанс): иначе два параллельных
// повтора оба пройдут проверку и оба вставят строки.
// Порядок записи: сначала forecast_log, последней — model_runs. Строка в model_runs — маркер
// фиксации прогона: если forecast_log записан, а model_runs упал, повтор не увидит run и
// допишет обе таблицы (forecast_log при повторе схлопывается по (metric, forecast_date) в пределах дня).
// Обратный порядок привёл бы к тому, что упавший forecast_log навсегда отсутствует при существующем run.
func (w *ClickHouseWriter) InsertModelRun(ctx context.Context, r ModelRun) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	runID, err := uuid.Parse(r.RunID)
	if err != nil {
		return fmt.Errorf("run_id %q is not a UUID: %w", r.RunID, err)
	}
	exists, err := w.store.runExists(ctx, runID)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("ingest: model_run %s already exists, skipping", r.RunID)
		return nil
	}
	goldScenario, err := json.Marshal(r.GoldScenario)
	if err != nil {
		return fmt.Errorf("gold_scenario: %w", err)
	}
	usdrubPath, err := json.Marshal(r.UsdrubPath)
	if err != nil {
		return fmt.Errorf("usdrub_path: %w", err)
	}
	targetDate, err := scenarioTargetDate(r.GoldScenario)
	if err != nil {
		return err
	}
	xauAvg, err := weightedGoldPoint(r.GoldScenario, r.Probabilities)
	if err != nil {
		return err
	}
	now := time.Now()

	forecastDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	forecastBatch, err := w.store.prepareBatch(ctx, forecastLogInsert)
	if err != nil {
		return err
	}
	for _, row := range []struct {
		metric    string
		predicted float64
	}{
		{"nav", r.NavPerShare},
		{"xau_q_avg", xauAvg},
	} {
		if err = forecastBatch.Append(forecastDate, targetDate, row.metric, row.predicted, nil, nil); err != nil {
			return err
		}
	}
	if err = forecastBatch.Send(); err != nil {
		return err
	}

	batch, err := w.store.prepareBatch(ctx, modelRunsInsert)
	if err != nil {
		return err
	}
	if err = batch.Append(
		runID, now, r.TriggerType, r.TriggerRef, string(goldScenario), string(usdrubPath),
		r.PriceDeck, r.DiscountRate, r.Wacc, r.NavPerShare, r.NavBull, r.NavBase, r.NavBear,
		r.MarketPrice, r.UpsidePct, nil, r.Comment,
	); err != nil {
		return err
	}
	return batch.Send()
}

// InsertManualSeries пишет вручную введённые точки в macro_series с фиксированным source='manual'.
func (w *ClickHouseWriter) InsertManualSeries(ctx context.Context, points []ManualSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, macroSeriesInsert)
	if err != nil {
		return err
	}
	for _, point := range points {
		date, err := time.Parse(manualSeriesDateLayout, point.Date)
		if err != nil {
			return fmt.Errorf("date %q: %w", point.Date, err)
		}
		if err = batch.Append(manualSeriesSource, point.Series, date, point.Value); err != nil {
			return err
		}
	}
	return batch.Send()
}

var (
	quarterHorizonRe = regexp.MustCompile(`^Q([1-4])-(\d{4})$`)
	yearHorizonRe    = regexp.MustCompile(`^(?:YE-)?(\d{4})$`)
)

// scenarioTargetDate — конец горизонта сценария: "Q4-2026" → конец квартала,
// "YE-2026"/"2026" → 31 декабря, "2026-12-31" — как есть.
// Используется и в ValidateModelRun (входные ошибки → 400), и в InsertModelRun.
func scenarioTargetDate(scenario map[string]any) (time.Time, error) {
	raw, ok := scenario["horizon"]
	if !ok {
		return time.Time{}, fmt.Errorf("gold_scenario.horizon is required for forecast_log target_date")
	}
	horizon, ok := raw.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("gold_scenario.horizon must be a string, got %v", raw)
	}
	if date, err := time.Parse(manualSeriesDateLayout, horizon); err == nil {
		return date, nil
	}
	if m := quarterHorizonRe.FindStringSubmatch(horizon); m != nil {
		quarter, err := strconv.Atoi(m[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("gold_scenario.horizon %q: %w", horizon, err)
		}
		year, err := strconv.Atoi(m[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("gold_scenario.horizon %q: %w", horizon, err)
		}
		return time.Date(year, time.Month(quarter*3)+1, 0, 0, 0, 0, 0, time.UTC), nil
	}
	if m := yearHorizonRe.FindStringSubmatch(horizon); m != nil {
		year, err := strconv.Atoi(m[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("gold_scenario.horizon %q: %w", horizon, err)
		}
		return time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("unsupported gold_scenario.horizon %q (want Q<n>-YYYY, YE-YYYY, YYYY or YYYY-MM-DD)", horizon)
}

// weightedGoldPoint — вероятностно-взвешенная точка золота: Σ prob×point / Σ prob.
// Используется и в ValidateModelRun (нет блока под вероятность → 400), и в InsertModelRun.
func weightedGoldPoint(scenario map[string]any, probabilities map[string]float64) (float64, error) {
	var weighted, sum float64
	for name, probability := range probabilities {
		point, err := scenarioPoint(scenario, name)
		if err != nil {
			return 0, err
		}
		weighted += probability * point
		sum += probability
	}
	if sum == 0 {
		return 0, fmt.Errorf("probabilities are empty")
	}
	return weighted / sum, nil
}

// scenarioPoint — точка сценария: "point", при её отсутствии — середина "low"/"high".
func scenarioPoint(scenario map[string]any, name string) (float64, error) {
	raw, ok := scenario[name]
	if !ok {
		return 0, fmt.Errorf("gold_scenario has no %q block for probability", name)
	}
	block, ok := raw.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("gold_scenario.%s must be an object, got %v", name, raw)
	}
	if point, ok := toFloat(block["point"]); ok {
		return point, nil
	}
	low, lowOk := toFloat(block["low"])
	high, highOk := toFloat(block["high"])
	if lowOk && highOk {
		return (low + high) / 2, nil
	}
	return 0, fmt.Errorf("gold_scenario.%s has no point and no low/high", name)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}
