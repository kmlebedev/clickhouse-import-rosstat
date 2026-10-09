package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// ClickHouseWriter — BatchWriter поверх clickhouse-go: только батчи PrepareBatch/Append/Send.
type ClickHouseWriter struct {
	conn driver.Conn
}

func NewClickHouseWriter(conn driver.Conn) *ClickHouseWriter {
	return &ClickHouseWriter{conn: conn}
}

func (w *ClickHouseWriter) EnsureTables(ctx context.Context) error {
	return ensureTables(ctx, w.conn)
}

// InsertModelRun пишет прогон модели и производные строки forecast_log:
// metric='nav' (predicted = nav_per_share) и metric='xau_q_avg' (вероятностно-взвешенная
// точка сценариев золота), target_date — конец горизонта сценария, actual/error_pct = NULL.
// Идемпотентность по клиентскому run_id: повторный POST пропускается — ORDER BY
// (run_date, run_id) в ReplacingMergeTree не схлопывает строки с разным run_date.
func (w *ClickHouseWriter) InsertModelRun(ctx context.Context, r ModelRun) error {
	runID, err := uuid.Parse(r.RunID)
	if err != nil {
		return fmt.Errorf("run_id %q is not a UUID: %w", r.RunID, err)
	}
	var existing uint64
	if err = w.conn.QueryRow(ctx, "SELECT count() FROM model_runs FINAL WHERE run_id = ?", runID).Scan(&existing); err != nil {
		return err
	}
	if existing > 0 {
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
	batch, err := w.conn.PrepareBatch(ctx, modelRunsInsert)
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
	if err = batch.Send(); err != nil {
		return err
	}
	forecastDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	forecastBatch, err := w.conn.PrepareBatch(ctx, forecastLogInsert)
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
	return forecastBatch.Send()
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

var quarterHorizonRe = regexp.MustCompile(`^Q([1-4])-(\d{4})$`)

// scenarioTargetDate — конец горизонта сценария: "Q4-2026" → конец квартала,
// "YE-2026"/"2026" → 31 декабря, "2026-12-31" — как есть.
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
	yearStr := strings.TrimPrefix(horizon, "YE-")
	if year, err := strconv.Atoi(yearStr); err == nil && len(yearStr) == 4 {
		return time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("unsupported gold_scenario.horizon %q (want Q<n>-YYYY, YE-YYYY, YYYY or YYYY-MM-DD)", horizon)
}

// weightedGoldPoint — вероятностно-взвешенная точка золота: Σ prob×point / Σ prob.
// Точка сценария — "point", при её отсутствии — середина "low"/"high".
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
