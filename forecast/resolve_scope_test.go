package forecast

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// Nil embedded store panics if scope validation touches any method.
type untouchedResolveStore struct{ resolveStore }

func TestResolveScopeRejectsBeforeQueries(t *testing.T) {
	for _, scope := range []string{"rates", "CBR_RATE", " cbr_rate", "xau_q_avg"} {
		if n, err := resolveForecastsWithScope(context.Background(), untouchedResolveStore{}, scope); err == nil || n != 0 || !strings.Contains(err.Error(), "FORECAST_RESOLVE_SCOPE") {
			t.Fatalf("scope%q: %d %v", scope, n, err)
		}
	}
	t.Setenv("FORECAST_RESOLVE_SCOPE", "invalid")
	if n, err := (&resolver{}).Import(context.Background(), nil); err == nil || n != 0 {
		t.Fatal("Import did not validate environment before conn", n, err)
	}
}

func TestResolveRateScopeIsolationAndRepeat(t *testing.T) {
	target := date(2026, time.October, 23)
	row := forecastRow{ForecastDate: date(2026, time.October, 19), TargetDate: target, Metric: "cbr_rate:ordered_logit:shadow", Predicted: 13.36, Probabilities: `{"hold":83,"cut":12,"hike":5}`, Model: "model", DataVersion: "v1", PromptVersion: 2}
	store := &fakeResolveStore{rateRows: []forecastRow{row}, rates: map[string]float64{"2026-10-23": 14}, resolvableErr: errors.New("gold must not be read"), windowErr: errors.New("gold must not be calculated")}
	n, err := resolveForecastsWithScope(context.Background(), store, "cbr_rate")
	if err != nil || n != 1 || len(store.writes) != 1 || len(store.windows) != 0 {
		t.Fatal(n, err, store.writes, store.windows)
	}
	got := store.writes[0][0]
	if got.Metric != row.Metric || got.ForecastDate != row.ForecastDate || got.TargetDate != row.TargetDate || got.Predicted != row.Predicted || got.Probabilities != row.Probabilities || got.Model != row.Model || got.DataVersion != row.DataVersion || got.PromptVersion != row.PromptVersion {
		t.Fatal("forecast metadata changed", got)
	}
	if got.Actual != 14 || got.ErrorBp == nil || math.Abs(*got.ErrorBp+64) > 1e-9 || got.ErrorPct != nil {
		t.Fatal("wrong rate resolution", got)
	}
	actual := got.Actual
	store.rateRows[0].Actual = &actual
	n, err = resolveForecastsWithScope(context.Background(), store, "cbr_rate")
	if err != nil || n != 0 || len(store.writes) != 1 || len(store.windows) != 0 {
		t.Fatal("repeat changed data", n, err)
	}
}

func TestResolveDefaultScopeKeepsBothFamilies(t *testing.T) {
	for _, scope := range []string{"", "all"} {
		t.Run(scope, func(t *testing.T) {
			target := date(2026, time.October, 23)
			store := &fakeResolveStore{rateRows: []forecastRow{{Metric: "cbr_rate:taylor", TargetDate: target, Predicted: 13.5}}, rates: map[string]float64{"2026-10-23": 14}, rows: []forecastRow{{TargetDate: date(2026, time.September, 30), Predicted: 4400}}, stats: windowStats{ComexCount: 1, ComexAvg: 4300}}
			n, err := resolveForecastsWithScope(context.Background(), store, scope)
			if err != nil || n != 2 || len(store.windows) != 1 || len(store.writes) != 1 || len(store.writes[0]) != 2 {
				t.Fatal(n, err, store.writes)
			}
			if store.writes[0][0].Metric != "cbr_rate:taylor" || store.writes[0][1].Metric != "xau_q_avg" {
				t.Fatal("default lost a metric family", store.writes)
			}
		})
	}
}
