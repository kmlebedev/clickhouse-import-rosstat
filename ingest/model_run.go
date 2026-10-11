package ingest

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	probabilitiesSumExpected  = 100.0
	probabilitiesSumTolerance = 0.1
	manualSeriesDateLayout    = "2006-01-02"
	manualSeriesSource        = "manual"
	// Диапазон Date32 в ClickHouse: [1900-01-01, 2299-12-31]. Даты вне диапазона
	// должны отсекаться валидацией (400), а не падать вставкой батча (500).
	date32Min = "1900-01-01"
	date32Max = "2299-12-31"
)

// date32MinTime/date32MaxTime — границы диапазона Date32 (см. date32Min/date32Max).
var (
	date32MinTime = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	date32MaxTime = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
)

var validTriggerTypes = map[string]bool{
	"calendar": true,
	"news":     true,
	"manual":   true,
}

// validProbabilityKeys — сценарии золота, которым назначается вероятность (spec, ARCHITECTURE §6.4).
var validProbabilityKeys = map[string]bool{
	"bull": true,
	"base": true,
	"bear": true,
}

var validPriceDecks = map[string]bool{
	"spot_flat":    true,
	"consensus_lt": true,
	"own_scenario": true,
}

// ModelRun — один прогон модели «золото → DCF → NAV» (таблица model_runs).
type ModelRun struct {
	RunID         string             `json:"run_id"`
	TriggerType   string             `json:"trigger_type"`
	TriggerRef    string             `json:"trigger_ref"`
	GoldScenario  map[string]any     `json:"gold_scenario"`
	UsdrubPath    map[string]any     `json:"usdrub_path"`
	PriceDeck     string             `json:"price_deck"`
	DiscountRate  float64            `json:"discount_rate"`
	Wacc          float64            `json:"wacc"`
	NavPerShare   float64            `json:"nav_per_share"`
	NavBull       float64            `json:"nav_bull"`
	NavBase       float64            `json:"nav_base"`
	NavBear       float64            `json:"nav_bear"`
	MarketPrice   float64            `json:"market_price"`
	UpsidePct     float64            `json:"upside_pct"`
	Comment       string             `json:"comment"`
	Probabilities map[string]float64 `json:"probabilities"`
}

// ValidateModelRun проверяет вход модели до вставки в ClickHouse; все ошибки входа — 400.
// run_id клиентский, обязателен и должен быть UUID: повторный POST с тем же run_id идемпотентен
// (см. InsertModelRun). Горизонт и вероятности сверяются с gold_scenario теми же функциями,
// которыми InsertModelRun считает target_date и взвешенную точку, — чтобы вход, который не
// запишется, отсекался здесь, а не падал внутри записи.
func ValidateModelRun(r ModelRun) error {
	if strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("run_id is required")
	}
	if _, err := uuid.Parse(r.RunID); err != nil {
		return fmt.Errorf("run_id %q must be a UUID: %w", r.RunID, err)
	}
	if !validTriggerTypes[r.TriggerType] {
		return fmt.Errorf("trigger_type %q must be one of: calendar, news, manual", r.TriggerType)
	}
	if !validPriceDecks[r.PriceDeck] {
		return fmt.Errorf("price_deck %q must be one of: spot_flat, consensus_lt, own_scenario", r.PriceDeck)
	}
	if err := validateProbabilities(r.Probabilities); err != nil {
		return err
	}
	var sum float64
	for _, probability := range r.Probabilities {
		sum += probability
	}
	if math.Abs(sum-probabilitiesSumExpected) > probabilitiesSumTolerance {
		return fmt.Errorf("probabilities sum %.4f must be within %.1f of %.0f", sum, probabilitiesSumTolerance, probabilitiesSumExpected)
	}
	if r.GoldScenario == nil {
		return fmt.Errorf("gold_scenario is required")
	}
	if _, err := scenarioTargetDate(r.GoldScenario); err != nil {
		return err
	}
	if _, err := weightedGoldPoint(r.GoldScenario, r.Probabilities); err != nil {
		return err
	}
	return nil
}

// validateProbabilities проверяет ключи (только bull/base/bear) и веса (0 ≤ p ≤ 100).
// Ключи обходятся по возрастанию, чтобы текст ошибки был детерминированным.
func validateProbabilities(probabilities map[string]float64) error {
	keys := make([]string, 0, len(probabilities))
	for key := range probabilities {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !validProbabilityKeys[key] {
			return fmt.Errorf("probability key %q must be one of: bull, base, bear", key)
		}
		if p := probabilities[key]; p < 0 || p > 100 {
			return fmt.Errorf("probability %q = %v must be within 0..100", key, p)
		}
	}
	return nil
}

// sortedKeys — ключи булева словаря в детерминированном порядке для текстов ошибок.
func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// ManualSeriesPoint — вручную введённая точка ряда macro_series.
// source по умолчанию 'manual' (пустое значение); допустимы 'manual', 'webbridge', 'imf'.
type ManualSeriesPoint struct {
	Series string  `json:"series"`
	Date   string  `json:"date"`
	Value  float64 `json:"value"`
	Source string  `json:"source"`
}

// validManualSources — допустимые источники записи через /v1/manual_series.
var validManualSources = map[string]bool{
	"manual":    true,
	"webbridge": true,
	"imf":       true,
}

// ValidateManualSeriesPoint проверяет точку ряда. date — строго YYYY-MM-DD
// в диапазоне Date32 [1900-01-01, 2299-12-31] (даты до 1970 валидны: CPIAUCSL с 1947).
// Пустой source допустим — дефолт 'manual' применяет writer.
func ValidateManualSeriesPoint(p ManualSeriesPoint) error {
	if strings.TrimSpace(p.Series) == "" {
		return fmt.Errorf("series is required")
	}
	if _, err := parsePointDate(p.Date); err != nil {
		return err
	}
	if p.Source != "" && !validManualSources[p.Source] {
		return fmt.Errorf("source %q must be one of: %s", p.Source, strings.Join(sortedKeys(validManualSources), ", "))
	}
	return nil
}

// parsePointDate разбирает дату точки: строго YYYY-MM-DD и в диапазоне Date32.
func parsePointDate(raw string) (time.Time, error) {
	date, err := time.Parse(manualSeriesDateLayout, raw)
	if err != nil || date.Format(manualSeriesDateLayout) != raw {
		return time.Time{}, fmt.Errorf("date %q must be in YYYY-MM-DD format", raw)
	}
	if date.Before(date32MinTime) || date.After(date32MaxTime) {
		return time.Time{}, fmt.Errorf("date %q out of Date32 range [%s, %s]", raw, date32Min, date32Max)
	}
	return date, nil
}

// GoldPricePoint — точка цены золота по площадке (таблица gold_prices, ключ (venue, date)).
type GoldPricePoint struct {
	Venue string  `json:"venue"`
	Date  string  `json:"date"`
	Usd   float64 `json:"usd"`
}

// validGoldVenues — допустимые площадки записи через ingest. Словарь расширяемый:
// новая venue добавляется здесь вместе с описанием ряда в series_catalog.
var validGoldVenues = map[string]bool{
	"comex_front": true,
}

// ValidateGoldPricePoint проверяет точку цены до вставки; ошибки входа — 400.
// venue сверяется точно по whitelist (регистр значим), date — строго YYYY-MM-DD
// в диапазоне Date32, цена золота обязана быть положительной
// (отсутствующее поле usd даёт 0 → ошибка).
func ValidateGoldPricePoint(p GoldPricePoint) error {
	if !validGoldVenues[p.Venue] {
		return fmt.Errorf("venue %q must be one of: %s", p.Venue, strings.Join(sortedKeys(validGoldVenues), ", "))
	}
	if _, err := parsePointDate(p.Date); err != nil {
		return err
	}
	if p.Usd <= 0 {
		return fmt.Errorf("usd %v must be positive", p.Usd)
	}
	return nil
}
