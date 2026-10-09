package ingest

import (
	"strings"
	"testing"
)

const testRunID = "11111111-2222-3333-4444-555555555555"

// validGoldScenario — сценарная сетка Q4-2026: точки 4800/4300/3900, low/high заданы.
func validGoldScenario() map[string]any {
	return map[string]any{
		"horizon": "Q4-2026",
		"bull":    map[string]any{"low": 4600.0, "high": 5000.0, "point": 4800.0},
		"base":    map[string]any{"low": 4000.0, "high": 4600.0, "point": 4300.0},
		"bear":    map[string]any{"low": 3750.0, "high": 4050.0, "point": 3900.0},
	}
}

func validModelRun() ModelRun {
	return ModelRun{
		RunID:         testRunID,
		TriggerType:   "manual",
		TriggerRef:    "unit test",
		GoldScenario:  validGoldScenario(),
		PriceDeck:     "own_scenario",
		Probabilities: map[string]float64{"bull": 25, "base": 40, "bear": 35},
	}
}

func TestValidateModelRunValid(t *testing.T) {
	if err := ValidateModelRun(validModelRun()); err != nil {
		t.Fatalf("valid run rejected: %v", err)
	}
}

// Все эти входы должны давать ошибку валидации (HTTP 400), а не падать внутри записи (500).
func TestValidateModelRunInputErrors(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*ModelRun)
		wantSubstr string
	}{
		{"run_id is not a UUID", func(r *ModelRun) { r.RunID = "not-a-uuid" }, "UUID"},
		{"gold_scenario is nil", func(r *ModelRun) { r.GoldScenario = nil }, "gold_scenario is required"},
		{"horizon is missing", func(r *ModelRun) { delete(r.GoldScenario, "horizon") }, "horizon"},
		{"horizon is not a string", func(r *ModelRun) { r.GoldScenario["horizon"] = 2026.0 }, "must be a string"},
		{"horizon is unparsable", func(r *ModelRun) { r.GoldScenario["horizon"] = "Q5-2026" }, "unsupported gold_scenario.horizon"},
		{"probability key has no gold block", func(r *ModelRun) {
			r.Probabilities = map[string]float64{"bull": 25, "base": 40, "tail": 35}
		}, "tail"},
		{"block has no point and no range", func(r *ModelRun) { r.GoldScenario["bear"] = map[string]any{"comment": "x"} }, "bear"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := validModelRun()
			tt.mutate(&run)
			err := ValidateModelRun(run)
			if err == nil || !strings.Contains(err.Error(), tt.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantSubstr, err)
			}
		})
	}
}

func TestValidateModelRunProbabilitiesSum(t *testing.T) {
	run := validModelRun()
	run.Probabilities = map[string]float64{"bull": 25, "base": 40, "bear": 25}
	err := ValidateModelRun(run)
	if err == nil || !strings.Contains(err.Error(), "probabilities") {
		t.Fatalf("expected probabilities sum error, got %v", err)
	}
}

func TestValidateModelRunProbabilitiesEmpty(t *testing.T) {
	run := validModelRun()
	run.Probabilities = nil
	if err := ValidateModelRun(run); err == nil {
		t.Fatal("expected error for empty probabilities")
	}
}

func TestValidateModelRunPriceDeck(t *testing.T) {
	run := validModelRun()
	run.PriceDeck = "wacc_guess"
	err := ValidateModelRun(run)
	if err == nil || !strings.Contains(err.Error(), "price_deck") {
		t.Fatalf("expected price_deck error, got %v", err)
	}
}

func TestValidateModelRunRunIDRequired(t *testing.T) {
	run := validModelRun()
	run.RunID = ""
	err := ValidateModelRun(run)
	if err == nil || !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("expected run_id error, got %v", err)
	}
}

func TestValidateModelRunTriggerType(t *testing.T) {
	run := validModelRun()
	run.TriggerType = "cron"
	err := ValidateModelRun(run)
	if err == nil || !strings.Contains(err.Error(), "trigger_type") {
		t.Fatalf("expected trigger_type error, got %v", err)
	}
}

func TestValidateManualSeriesPointPre1970(t *testing.T) {
	point := ManualSeriesPoint{Series: "CPIAUCSL", Date: "1947-01-01", Value: 21.48}
	if err := ValidateManualSeriesPoint(point); err != nil {
		t.Fatalf("pre-1970 date must be valid (Date32): %v", err)
	}
}

func TestValidateManualSeriesPointErrors(t *testing.T) {
	tests := []struct {
		name  string
		point ManualSeriesPoint
	}{
		{"empty series", ManualSeriesPoint{Series: "", Date: "2026-01-01", Value: 1}},
		{"bad month", ManualSeriesPoint{Series: "DFII10", Date: "2026-13-01", Value: 1}},
		{"not a date", ManualSeriesPoint{Series: "DFII10", Date: "yesterday", Value: 1}},
		{"short year", ManualSeriesPoint{Series: "DFII10", Date: "26-01-01", Value: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateManualSeriesPoint(tt.point); err == nil {
				t.Fatalf("expected error for %+v", tt.point)
			}
		})
	}
}
