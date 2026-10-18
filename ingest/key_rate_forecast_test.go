package ingest

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
)

// validKeyRateForecast — строка прогноза трека ordered_logit: распределение
// задано в процентах словами cut/hold/hike, сумма ровно 100.
func validKeyRateForecast() KeyRateForecast {
	return KeyRateForecast{
		ForecastDate: "2026-10-18",
		TargetDate:   "2026-10-23",
		Metric:       "cbr_rate:ordered_logit",
		Predicted:    13.3665,
		Probabilities: map[string]float64{
			"cut":  81.65,
			"hold": 16.24,
			"hike": 2.11,
		},
		DataVersion:   "key-rate-model-v0:2026-10-16:n=108",
		Model:         "ordered_logit",
		PromptVersion: 0,
	}
}

// TestValidateKeyRateForecast — контракт endpoint'а: даты, metric-трек, ставка,
// распределение, версия данных, модель и prompt_version LLM-трека.
func TestValidateKeyRateForecast(t *testing.T) {
	base := validKeyRateForecast()

	with := func(change func(*KeyRateForecast)) KeyRateForecast {
		forecast := base
		change(&forecast)
		return forecast
	}

	valid := []struct {
		name     string
		forecast KeyRateForecast
	}{
		{"ordered_logit с распределением", base},
		{"taylor без распределения", with(func(f *KeyRateForecast) {
			f.Metric = "cbr_rate:taylor"
			f.Model = "taylor"
			f.Probabilities = nil
		})},
		{"числовые алиасы классов", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"-1": 81.65, "0": 16.24, "+1": 2.11}
		})},
		{"базовое имя metric", with(func(f *KeyRateForecast) {
			f.Metric = "cbr_rate"
		})},
		{"llm с prompt_version", with(func(f *KeyRateForecast) {
			f.Metric = "cbr_rate:llm"
			f.Model = "zai-org/GLM-5.3-Flash"
			f.PromptVersion = 1
		})},
		{"target_date равен forecast_date", with(func(f *KeyRateForecast) {
			f.TargetDate = f.ForecastDate
		})},
		{"пустой объект вероятностей — распределения нет", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{}
		})},
	}

	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateKeyRateForecast(tc.forecast); err != nil {
				t.Fatalf("ValidateKeyRateForecast: %v", err)
			}
		})
	}

	invalid := []struct {
		name     string
		forecast KeyRateForecast
		wantText string
	}{
		{"пустая forecast_date", with(func(f *KeyRateForecast) { f.ForecastDate = "" }), "forecast_date"},
		{"forecast_date не ISO", with(func(f *KeyRateForecast) { f.ForecastDate = "18.10.2026" }), "forecast_date"},
		{"forecast_date вне Date", with(func(f *KeyRateForecast) { f.ForecastDate = "1960-01-01" }), "forecast_date"},
		{"target_date раньше forecast_date", with(func(f *KeyRateForecast) { f.TargetDate = "2026-10-17" }), "before forecast_date"},
		{"metric без cbr_rate", with(func(f *KeyRateForecast) { f.Metric = "xau_q_avg" }), "metric"},
		{"неизвестный трек", with(func(f *KeyRateForecast) { f.Metric = "cbr_rate:ensemble" }), "metric"},
		{"пустой трек", with(func(f *KeyRateForecast) { f.Metric = "cbr_rate:" }), "metric"},
		{"predicted = 0", with(func(f *KeyRateForecast) { f.Predicted = 0 }), "predicted"},
		{"predicted отрицательный", with(func(f *KeyRateForecast) { f.Predicted = -1 }), "predicted"},
		{"predicted NaN", with(func(f *KeyRateForecast) { f.Predicted = math.NaN() }), "predicted"},
		{"predicted Inf", with(func(f *KeyRateForecast) { f.Predicted = math.Inf(1) }), "predicted"},
		{"predicted подозрительно велик", with(func(f *KeyRateForecast) { f.Predicted = 1000 }), "predicted"},
		{"неизвестный ключ вероятности", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": 50, "hold": 50, "sideways": 0}
		}), "probability key"},
		{"класс задан дважды", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": 50, "-1": 50, "hold": 0, "hike": 0}
		}), "set twice"},
		{"не хватает класса", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": 50, "hold": 50}
		}), "all three classes"},
		{"вероятность отрицательна", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": -1, "hold": 101, "hike": 0}
		}), "probability"},
		{"вероятность больше 100", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": 101, "hold": 0, "hike": 0}
		}), "probability"},
		{"сумма вероятностей не 100", with(func(f *KeyRateForecast) {
			f.Probabilities = map[string]float64{"cut": 50, "hold": 49, "hike": 0}
		}), "sum"},
		{"пустой data_version", with(func(f *KeyRateForecast) { f.DataVersion = "" }), "data_version"},
		{"data_version из пробелов", with(func(f *KeyRateForecast) { f.DataVersion = "   " }), "data_version"},
		{"пустой model", with(func(f *KeyRateForecast) { f.Model = "" }), "model"},
		{"llm без prompt_version", with(func(f *KeyRateForecast) {
			f.Metric = "cbr_rate:llm"
			f.Model = "zai-org/GLM-5.3-Flash"
		}), "prompt_version"},
	}

	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateKeyRateForecast(tc.forecast)
			if err == nil {
				t.Fatalf("ValidateKeyRateForecast(%+v) = nil, want error", tc.forecast)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q does not contain %q", err, tc.wantText)
			}
		})
	}
}

const validKeyRateForecastBody = `{
	"forecast_date": "2026-10-18",
	"target_date": "2026-10-23",
	"metric": "cbr_rate:ordered_logit",
	"predicted": 13.3665,
	"probabilities": {"cut": 81.65, "hold": 16.24, "hike": 2.11},
	"data_version": "key-rate-model-v0:2026-10-16:n=108",
	"model": "ordered_logit",
	"prompt_version": 0
}`

func TestKeyRateForecastUnauthorized(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	for _, token := range []string{"", "wrong-token"} {
		status, _ := post(t, server.URL+"/v1/key_rate_forecast", token, validKeyRateForecastBody)
		if status != http.StatusUnauthorized {
			t.Fatalf("token %q: status %d, want 401", token, status)
		}
	}
	if len(writer.keyRate) != 0 {
		t.Fatal("writer must not be called without valid token")
	}
}

func TestKeyRateForecastInvalidJSONAndUnknownField(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed JSON", `{"forecast_date":`},
		{"unknown field", strings.Replace(validKeyRateForecastBody, `"model":`, `"error_bp": null, "model":`, 1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writer := &mockBatchWriter{}
			server := newTestServer(writer)
			defer server.Close()

			status, _ := post(t, server.URL+"/v1/key_rate_forecast", testToken, tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", status)
			}
			if len(writer.keyRate) != 0 {
				t.Fatal("writer must not be called on malformed body")
			}
		})
	}
}

func TestKeyRateForecastValidationError(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	body := strings.Replace(validKeyRateForecastBody, `"cbr_rate:ordered_logit"`, `"cbr_rate:ensemble"`, 1)
	status, payload := post(t, server.URL+"/v1/key_rate_forecast", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if !strings.Contains(string(payload), "metric") {
		t.Fatalf("expected metric in error text, got %s", payload)
	}
	if len(writer.keyRate) != 0 {
		t.Fatal("writer must not be called on validation error")
	}
}

func TestKeyRateForecastOK(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, payload := post(t, server.URL+"/v1/key_rate_forecast", testToken, validKeyRateForecastBody)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	var response struct {
		Inserted int `json:"inserted"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response is not JSON: %s", payload)
	}
	if response.Inserted != 1 {
		t.Fatalf("inserted = %d, want 1", response.Inserted)
	}
	if len(writer.keyRate) != 1 {
		t.Fatalf("writer called %d times, want 1", len(writer.keyRate))
	}
	got := writer.keyRate[0]
	if got.Metric != "cbr_rate:ordered_logit" || got.TargetDate != "2026-10-23" ||
		got.Predicted != 13.3665 || got.Probabilities["hold"] != 16.24 ||
		got.DataVersion != "key-rate-model-v0:2026-10-16:n=108" || got.Model != "ordered_logit" {
		t.Fatalf("writer got %+v", got)
	}
}

func TestKeyRateForecastWriterError(t *testing.T) {
	writer := &mockBatchWriter{err: errors.New("clickhouse unavailable")}
	server := newTestServer(writer)
	defer server.Close()

	status, payload := post(t, server.URL+"/v1/key_rate_forecast", testToken, validKeyRateForecastBody)
	if status != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500: %s", status, payload)
	}
	if strings.Contains(string(payload), "clickhouse unavailable") {
		t.Fatalf("internal error text leaked to the client: %s", payload)
	}
}

var _ BatchWriter = (*mockBatchWriter)(nil)

// TestForecastLogDdlAndMigrations — канонический DDL и идемпотентные миграции
// обязаны совпадать: CREATE IF NOT EXISTS не добавит колонки живой таблице, и
// без ALTER резолвер cbr_rate упал бы на вставке в проде, а метаданные прогноза
// (probabilities/model/data_version/prompt_version) потерялись бы.
func TestForecastLogDdlAndMigrations(t *testing.T) {
	for _, want := range []string{
		"error_bp Nullable(Float64)",
		"probabilities String DEFAULT '{}'",
		"model LowCardinality(String) DEFAULT ''",
		"data_version String DEFAULT ''",
		"prompt_version UInt16 DEFAULT 0",
	} {
		if !strings.Contains(forecastLogDdl, want) {
			t.Errorf("forecastLogDdl не содержит %q: %s", want, forecastLogDdl)
		}
	}
	joined := strings.Join(forecastLogMigrations, "\n")
	for _, want := range []string{
		"ADD COLUMN IF NOT EXISTS error_bp Nullable(Float64)",
		"ADD COLUMN IF NOT EXISTS probabilities String DEFAULT '{}'",
		"ADD COLUMN IF NOT EXISTS model LowCardinality(String) DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS data_version String DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS prompt_version UInt16 DEFAULT 0",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("forecastLogMigrations не содержат %q: %s", want, joined)
		}
	}
}
