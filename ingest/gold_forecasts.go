package ingest

import (
	"fmt"
	"strings"
)

// validForecastScenarios — допустимые сценарии прогноза (gold_forecasts.scenario):
// банковский точечный/диапазонный прогноз и собственная сценарная сетка.
var validForecastScenarios = map[string]bool{
	"point": true,
	"range": true,
	"bull":  true,
	"base":  true,
	"bear":  true,
}

// GoldForecast — одна строка консенсус-прогноза по золоту (таблица gold_forecasts,
// ключ (author, published, horizon, scenario)). Цены — USD/унц: банковский range
// задаёт low/high без point, собственный сценарий — point без диапазона.
type GoldForecast struct {
	Author      string  `json:"author"`
	Published   string  `json:"published"`
	Horizon     string  `json:"horizon"`
	Scenario    string  `json:"scenario"`
	Low         float64 `json:"low"`
	High        float64 `json:"high"`
	Point       float64 `json:"point"`
	Probability float32 `json:"probability"`
}

// ValidateGoldForecast проверяет строку прогноза до вставки; ошибки входа — 400.
// published — строго YYYY-MM-DD в диапазоне Date32 (тот же parsePointDate, что у точек рядов).
// Все три цены неотрицательны и хотя бы одна положительна: строка из нулей — это
// незаполненный прогноз, а не цена. high сверяется с low только когда заданы оба —
// у точечного прогноза и одностороннего диапазона второй границы нет.
func ValidateGoldForecast(f GoldForecast) error {
	if strings.TrimSpace(f.Author) == "" {
		return fmt.Errorf("author is required")
	}
	if _, err := parsePointDate(f.Published); err != nil {
		return err
	}
	if strings.TrimSpace(f.Horizon) == "" {
		return fmt.Errorf("horizon is required")
	}
	if !validForecastScenarios[f.Scenario] {
		return fmt.Errorf("scenario %q must be one of: %s", f.Scenario, strings.Join(sortedKeys(validForecastScenarios), ", "))
	}
	if f.Low < 0 || f.High < 0 || f.Point < 0 {
		return fmt.Errorf("low %v, high %v, point %v must be non-negative", f.Low, f.High, f.Point)
	}
	if f.Low == 0 && f.High == 0 && f.Point == 0 {
		return fmt.Errorf("at least one of low, high, point must be positive")
	}
	if f.Low > 0 && f.High > 0 && f.High < f.Low {
		return fmt.Errorf("high %v must be greater than or equal to low %v", f.High, f.Low)
	}
	if f.Probability < 0 || f.Probability > 100 {
		return fmt.Errorf("probability %v must be within 0..100", f.Probability)
	}
	return nil
}
