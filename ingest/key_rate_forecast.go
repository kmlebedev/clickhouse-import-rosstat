package ingest

import (
	"encoding/json"
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

// keyRateStages — стадии промоушена (infra/4.3). Стадия — третий сегмент
// metric: `cbr_rate:<track>:shadow`/`:canary`; production — без суффикса.
// Список закрытый: ingest не должен принимать произвольные стадии.
var keyRateStages = map[string]bool{
	"shadow": true,
	"canary": true,
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

	track, _, ok := keyRateForecastTrack(f.Metric)
	if !ok {
		return fmt.Errorf("metric %q must be %q, %q:<track> or %q:<track>:shadow|canary with track one of: %s",
			f.Metric, keyRateMetric, keyRateMetric, keyRateMetric,
			strings.Join(sortedKeys(keyRateTracks), ", "))
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

// keyRateForecastTrack — разбор metric на трек и стадию.
//
// Грамматика (infra/4.3): `cbr_rate`, `cbr_rate:<track>`,
// `cbr_rate:<track>:shadow|canary`. Стадия допустима только вместе с треком:
// `cbr_rate:shadow` и `cbr_rate::shadow` отвергаются, чтобы стадия не
// выглядела как чужой трек. Пустой трек/стадия — ошибка.
func keyRateForecastTrack(metric string) (track, stage string, ok bool) {
	if metric == keyRateMetric {
		return "", "", true
	}
	suffix, found := strings.CutPrefix(metric, keyRateMetric+":")
	if !found {
		return "", "", false
	}
	parts := strings.Split(suffix, ":")
	if len(parts) > 2 {
		return "", "", false
	}
	track = parts[0]
	if !keyRateTracks[track] {
		return "", "", false
	}
	if len(parts) == 2 {
		stage = parts[1]
		if !keyRateStages[stage] {
			return "", "", false
		}
	}
	return track, stage, true
}

// validateKeyRateProbabilities проверяет распределение: ключи — только классы
// решения (или их числовые алиасы), каждый класс не больше одного раза, значения
// конечны и 0–100, сумма 100 ± 0.1. Пустой объект — «распределения нет» (у
// точечного правила Тейлора его не существует).
func validateKeyRateProbabilities(probabilities map[string]float64) error {
	if len(probabilities) == 0 {
		return nil
	}
	canonical, err := canonicalKeyRateProbabilities(probabilities)
	if err != nil {
		return err
	}
	if len(canonical) != len(keyRateProbabilityClasses) {
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

// keyRateProbabilityClasses — канонические имена классов в порядке отчёта.
var keyRateProbabilityClasses = []string{"cut", "hold", "hike"}

// canonicalKeyRateProbabilities нормализует ключи распределения к cut/hold/hike
// (алиасы -1/0/+1) и проверяет значения/дубли. Пустой вход возвращает пустую
// карту: «распределения нет» — валидное состояние точечного прогноза.
func canonicalKeyRateProbabilities(probabilities map[string]float64) (map[string]float64, error) {
	if len(probabilities) == 0 {
		return map[string]float64{}, nil
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
			return nil, fmt.Errorf("probability key %q must be one of: -1, 0, +1, cut, hold, hike", key)
		}
		if _, duplicate := canonical[class]; duplicate {
			return nil, fmt.Errorf("probability class %q is set twice (aliases -1/0/+1 and cut/hold/hike)", class)
		}
		value := probabilities[key]
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return nil, fmt.Errorf("probability %q = %v must be within 0..100", key, value)
		}
		canonical[class] = value
	}

	return canonical, nil
}

// keyRateProbabilitiesJSON — распределение для колонки forecast_log.probabilities:
// канонический JSON {"cut":..,"hold":..,"hike":..}; пустое распределение — '{}'.
// encoding/json сортирует ключи карты, поэтому текст детерминирован и повторная
// запись той же строки не меняет журнал.
func keyRateProbabilitiesJSON(probabilities map[string]float64) (string, error) {
	canonical, err := canonicalKeyRateProbabilities(probabilities)
	if err != nil {
		return "", err
	}
	if len(canonical) == 0 {
		return "{}", nil
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("probabilities: %w", err)
	}
	return string(encoded), nil
}
