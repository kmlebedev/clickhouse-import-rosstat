package ingest

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// keyRateMetric — базовое имя метрики журнала прогнозов ключевой ставки.
// Треки различаются суффиксом (`cbr_rate:taylor` и т.п.): ключ forecast_log —
// (metric, forecast_date), поэтому суффикс и есть измерение трека, и ORDER BY
// таблицы менять не нужно. Значение без суффикса оставлено для совместимости.
const keyRateMetric = "cbr_rate"

// keyRateTracks — треки, которые пишутся в журнал. Список закрытый: новый трек
// добавляется здесь вместе с writer'ом, иначе строки схлопнулись бы в один metric.
var keyRateTracks = map[string]bool{
	"taylor":        true,
	"ordered_logit": true,
	"ml":            true,
	"llm":           true,
}

// keyRateProbabilityAliases — допустимые ключи распределения и их канонический
// вид. Модель v0 отдаёт классы строками "-1"/"0"/"+1", LLM-трек (#189) — словами
// cut/hold/hike; endpoint принимает оба набора, чтобы writer не переписывал
// контракт артефакта, но внутри класс один.
var keyRateProbabilityAliases = map[string]string{
	"-1":   "cut",
	"0":    "hold",
	"+1":   "hike",
	"cut":  "cut",
	"hold": "hold",
	"hike": "hike",
}

// keyRateForecastProbabilitySum — распределение задаётся в процентах (как
// вероятности сценариев золота в ModelRun), сумма 100 ± 0.1.
const keyRateForecastProbabilitySum = 100.0

// keyRateForecastProbabilityTolerance — допуск суммы распределения.
const keyRateForecastProbabilityTolerance = 0.1

// keyRateForecastMaxRate — верхняя граница правдоподобной ставки, % годовых.
// Ключевая ставка ЦБ исторически 4.25–21 %, но контур не должен молча принять
// перепутанные единицы (шаг в б.п. вместо уровня).
const keyRateForecastMaxRate = 1000.0

// KeyRateForecast — одна строка журнала прогнозов ключевой ставки.
// `Predicted` — ожидаемая СТАВКА после заседания в % годовых (не шаг в б.п.):
// резолвер считает error_bp = (predicted − actual) × 100. `Probabilities` —
// необязательное распределение по классам {cut, hold, hike} в процентах.
//
// `Model`, `PromptVersion` и `DataVersion` пока не имеют колонок в forecast_log
// (см. спеку §4/§7): endpoint их валидирует и пишет в лог ingest, чтобы контракт
// writer'а и LLM-трека (#189) не менялся при добавлении колонок follow-up'ом.
type KeyRateForecast struct {
	ForecastDate  string             `json:"forecast_date"`
	TargetDate    string             `json:"target_date"`
	Metric        string             `json:"metric"`
	Predicted     float64            `json:"predicted"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	DataVersion   string             `json:"data_version"`
	Model         string             `json:"model"`
	PromptVersion uint16             `json:"prompt_version"`
}

// ValidateKeyRateForecast проверяет строку до вставки; все ошибки входа — 400.
// Даты объявлены Date (не Date32), поэтому проверяется диапазон [1970-01-01,
// 2149-06-06] поверх формата YYYY-MM-DD. target_date не раньше forecast_date:
// журнал хранит дату исходного прогноза, а не дату вставки, и вставка «задним
// числом» не должна выглядеть как прогноз, сделанный после заседания.
func ValidateKeyRateForecast(f KeyRateForecast) error {
	forecastDate, err := parseKeyRateForecastDate("forecast_date", f.ForecastDate)
	if err != nil {
		return err
	}
	targetDate, err := parseKeyRateForecastDate("target_date", f.TargetDate)
	if err != nil {
		return err
	}
	if targetDate.Before(forecastDate) {
		return fmt.Errorf("target_date %q is before forecast_date %q", f.TargetDate, f.ForecastDate)
	}

	track, ok := keyRateForecastTrack(f.Metric)
	if !ok {
		return fmt.Errorf("metric %q must be %q or %q:<track> with track one of: %s",
			f.Metric, keyRateMetric, keyRateMetric, strings.Join(sortedKeys(keyRateTracks), ", "))
	}

	if math.IsNaN(f.Predicted) || math.IsInf(f.Predicted, 0) || f.Predicted <= 0 || f.Predicted >= keyRateForecastMaxRate {
		return fmt.Errorf("predicted %v must be a finite rate in (0, %v) percent per annum", f.Predicted, keyRateForecastMaxRate)
	}

	if err := validateKeyRateProbabilities(f.Probabilities); err != nil {
		return err
	}

	if strings.TrimSpace(f.DataVersion) == "" {
		return fmt.Errorf("data_version is required: forecast is not published without a data version")
	}
	if strings.TrimSpace(f.Model) == "" {
		return fmt.Errorf("model is required")
	}
	if track == "llm" && f.PromptVersion == 0 {
		return fmt.Errorf("prompt_version is required for %s:llm", keyRateMetric)
	}

	return nil
}

// parseKeyRateForecastDate разбирает дату журнала: формат и диапазон Date32
// проверяет parsePointDate, поверх — диапазон Date (колонки forecast_log).
func parseKeyRateForecastDate(field, raw string) (time.Time, error) {
	date, err := parsePointDate(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", field, err)
	}
	if date.Before(dateMinTime) || date.After(dateMaxTime) {
		return time.Time{}, fmt.Errorf("%s %q out of Date range [%s, %s]", field, raw, dateMin, dateMax)
	}
	return date, nil
}

// keyRateForecastTrack — суффикс трека метрики; пустая строка — базовое имя.
func keyRateForecastTrack(metric string) (string, bool) {
	if metric == keyRateMetric {
		return "", true
	}
	track, found := strings.CutPrefix(metric, keyRateMetric+":")
	if !found || !keyRateTracks[track] {
		return "", false
	}
	return track, true
}

// validateKeyRateProbabilities проверяет распределение: ключи — только классы
// решения (или их числовые алиасы), каждый класс не больше одного раза, значения
// конечны и 0–100, сумма 100 ± 0.1. Пустой объект — «распределения нет» (у
// точечного правила Тейлора его не существует).
func validateKeyRateProbabilities(probabilities map[string]float64) error {
	if len(probabilities) == 0 {
		return nil
	}

	keys := make([]string, 0, len(probabilities))
	for key := range probabilities {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	canonical := make(map[string]float64, len(probabilities))
	for _, key := range keys {
		class, ok := keyRateProbabilityAliases[key]
		if !ok {
			return fmt.Errorf("probability key %q must be one of: -1, 0, +1, cut, hold, hike", key)
		}
		if _, duplicate := canonical[class]; duplicate {
			return fmt.Errorf("probability class %q is set twice (aliases -1/0/+1 and cut/hold/hike)", class)
		}
		value := probabilities[key]
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return fmt.Errorf("probability %q = %v must be within 0..100", key, value)
		}
		canonical[class] = value
	}
	if len(canonical) != len(keyRateProbabilityAliases)/2 {
		return fmt.Errorf("probabilities must contain all three classes: cut, hold, hike")
	}

	var sum float64
	for _, value := range canonical {
		sum += value
	}
	if math.Abs(sum-keyRateForecastProbabilitySum) > keyRateForecastProbabilityTolerance {
		return fmt.Errorf("probabilities sum %.4f must be within %.1f of %.0f",
			sum, keyRateForecastProbabilityTolerance, keyRateForecastProbabilitySum)
	}

	return nil
}
