package ingest

import (
	"strings"
	"testing"
)

const testRunID = "11111111-2222-3333-4444-555555555555"

func validModelRun() ModelRun {
	return ModelRun{
		RunID:         testRunID,
		TriggerType:   "manual",
		TriggerRef:    "unit test",
		PriceDeck:     "own_scenario",
		Probabilities: map[string]float64{"bull": 25, "base": 40, "bear": 35},
	}
}

func TestValidateModelRunValid(t *testing.T) {
	if err := ValidateModelRun(validModelRun()); err != nil {
		t.Fatalf("valid run rejected: %v", err)
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
