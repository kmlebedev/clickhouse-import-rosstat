package ingest

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	probabilitiesSumExpected  = 100.0
	probabilitiesSumTolerance = 0.1
	manualSeriesDateLayout    = "2006-01-02"
	manualSeriesSource        = "manual"
)

var validTriggerTypes = map[string]bool{
	"calendar": true,
	"news":     true,
	"manual":   true,
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

// ValidateModelRun проверяет вход модели до вставки в ClickHouse.
// run_id клиентский и обязателен: повторный POST с тем же run_id идемпотентен
// (ReplacingMergeTree схлопывает по ORDER BY).
func ValidateModelRun(r ModelRun) error {
	if strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("run_id is required")
	}
	if !validTriggerTypes[r.TriggerType] {
		return fmt.Errorf("trigger_type %q must be one of: calendar, news, manual", r.TriggerType)
	}
	if !validPriceDecks[r.PriceDeck] {
		return fmt.Errorf("price_deck %q must be one of: spot_flat, consensus_lt, own_scenario", r.PriceDeck)
	}
	var sum float64
	for _, probability := range r.Probabilities {
		sum += probability
	}
	if math.Abs(sum-probabilitiesSumExpected) > probabilitiesSumTolerance {
		return fmt.Errorf("probabilities sum %.4f must be within %.1f of %.0f", sum, probabilitiesSumTolerance, probabilitiesSumExpected)
	}
	return nil
}

// ManualSeriesPoint — вручную введённая точка ряда macro_series (source='manual').
type ManualSeriesPoint struct {
	Series string  `json:"series"`
	Date   string  `json:"date"`
	Value  float64 `json:"value"`
}

// ValidateManualSeriesPoint проверяет точку ряда. date — строго YYYY-MM-DD;
// macro_series.date имеет тип Date32, поэтому даты до 1970 валидны.
func ValidateManualSeriesPoint(p ManualSeriesPoint) error {
	if strings.TrimSpace(p.Series) == "" {
		return fmt.Errorf("series is required")
	}
	date, err := time.Parse(manualSeriesDateLayout, p.Date)
	if err != nil || date.Format(manualSeriesDateLayout) != p.Date {
		return fmt.Errorf("date %q must be in YYYY-MM-DD format", p.Date)
	}
	return nil
}
