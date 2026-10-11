package ingest

import (
	"strings"
	"testing"
)

// TestValidateGoldForecastErrors — таблица правил валидации строки gold_forecasts:
// каждая строка начинается с валидного point-прогноза и мутирует его одним полем;
// wantErr — подстрока ожидаемой ошибки (пусто — прогноз валиден).
func TestValidateGoldForecastErrors(t *testing.T) {
	valid := GoldForecast{
		Author:    "goldman",
		Published: "2026-10-07",
		Horizon:   "YE-2026",
		Scenario:  "point",
		Point:     4900,
	}
	tests := []struct {
		name   string
		mutate func(*GoldForecast)
		want   string
	}{
		{"valid point", func(*GoldForecast) {}, ""},
		{"valid range", func(f *GoldForecast) {
			f.Scenario, f.Point, f.Low, f.High = "range", 0, 5400, 5600
		}, ""},
		{"empty author", func(f *GoldForecast) { f.Author = "" }, "author is required"},
		{"blank author", func(f *GoldForecast) { f.Author = "   " }, "author is required"},
		{"published not a date", func(f *GoldForecast) { f.Published = "07.10.2026" }, "YYYY-MM-DD"},
		{"published before Date32", func(f *GoldForecast) { f.Published = "1899-12-31" }, "Date32 range"},
		{"empty horizon", func(f *GoldForecast) { f.Horizon = "" }, "horizon is required"},
		{"blank horizon", func(f *GoldForecast) { f.Horizon = " " }, "horizon is required"},
		{"scenario not in whitelist", func(f *GoldForecast) { f.Scenario = "foo" },
			`scenario "foo" must be one of: base, bear, bull, point, range`},
		{"all numbers zero", func(f *GoldForecast) { f.Point = 0 }, "at least one of low, high, point must be positive"},
		{"high below low", func(f *GoldForecast) {
			f.Scenario, f.Point, f.Low, f.High = "range", 0, 5600, 5400
		}, "must be greater than or equal to low"},
		{"probability above 100", func(f *GoldForecast) { f.Probability = 101 }, "within 0..100"},
		{"negative low", func(f *GoldForecast) { f.Low = -1 }, "non-negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forecast := valid
			tt.mutate(&forecast)
			err := ValidateGoldForecast(forecast)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("valid forecast rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}
