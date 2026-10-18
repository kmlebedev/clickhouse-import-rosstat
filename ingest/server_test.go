package ingest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testToken = "unit-test-token"

type mockBatchWriter struct {
	runs          []ModelRun
	series        [][]ManualSeriesPoint
	goldPrices    [][]GoldPricePoint
	goldForecasts [][]GoldForecast
	keyRate       []KeyRateForecast
	err           error
}

func (m *mockBatchWriter) EnsureTables(_ context.Context) error {
	return m.err
}

func (m *mockBatchWriter) InsertModelRun(_ context.Context, r ModelRun) error {
	m.runs = append(m.runs, r)
	return m.err
}

func (m *mockBatchWriter) InsertManualSeries(_ context.Context, points []ManualSeriesPoint) error {
	m.series = append(m.series, points)
	return m.err
}

func (m *mockBatchWriter) InsertGoldPrices(_ context.Context, points []GoldPricePoint) error {
	m.goldPrices = append(m.goldPrices, points)
	return m.err
}

func (m *mockBatchWriter) InsertGoldForecasts(_ context.Context, forecasts []GoldForecast) error {
	m.goldForecasts = append(m.goldForecasts, forecasts)
	return m.err
}

func (m *mockBatchWriter) InsertKeyRateForecast(_ context.Context, forecast KeyRateForecast) error {
	m.keyRate = append(m.keyRate, forecast)
	return m.err
}

func newTestServer(writer BatchWriter) *httptest.Server {
	return httptest.NewServer(NewServer(writer, testToken, DefaultRatePerMin))
}

// manualClock — часы для теста ограничителя: время меняется только через Advance.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func post(t *testing.T, url, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, payload
}

const validRunBody = `{
	"run_id": "` + testRunID + `",
	"trigger_type": "manual",
	"trigger_ref": "unit test",
	"gold_scenario": {
		"horizon": "Q4-2026",
		"bull": {"low": 4600, "high": 5000, "point": 4800},
		"base": {"low": 4000, "high": 4600, "point": 4300},
		"bear": {"low": 3750, "high": 4050, "point": 3900}
	},
	"usdrub_path": {"Q4-2026": 81.5},
	"price_deck": "own_scenario",
	"discount_rate": 0.09,
	"wacc": 0.14,
	"nav_per_share": 12500,
	"nav_bull": 16000,
	"nav_base": 12500,
	"nav_bear": 9500,
	"market_price": 6800,
	"upside_pct": 83.8,
	"comment": "unit test",
	"probabilities": {"bull": 25, "base": 40, "bear": 35}
}`

func TestModelRunUnauthorized(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	for _, token := range []string{"", "wrong-token"} {
		status, _ := post(t, server.URL+"/v1/model_run", token, validRunBody)
		if status != http.StatusUnauthorized {
			t.Fatalf("token %q: status %d, want 401", token, status)
		}
	}
	if len(writer.runs) != 0 {
		t.Fatal("writer must not be called without valid token")
	}
}

func TestModelRunInvalidJSON(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, _ := post(t, server.URL+"/v1/model_run", testToken, `{"run_id":`)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", status)
	}
	if len(writer.runs) != 0 {
		t.Fatal("writer must not be called on malformed JSON")
	}
}

func TestModelRunValidationError(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	body := strings.Replace(validRunBody, `"bear": 35}`, `"bear": 25}`, 1)
	status, payload := post(t, server.URL+"/v1/model_run", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", status)
	}
	if !strings.Contains(string(payload), "probabilities") {
		t.Fatalf("expected validation error text, got %s", payload)
	}
	if len(writer.runs) != 0 {
		t.Fatal("writer must not be called on validation error")
	}
}

func TestModelRunOK(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, payload := post(t, server.URL+"/v1/model_run", testToken, validRunBody)
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
	if len(writer.runs) != 1 {
		t.Fatalf("writer called %d times, want 1", len(writer.runs))
	}
	if writer.runs[0].RunID != testRunID {
		t.Fatalf("run_id = %q, want %q", writer.runs[0].RunID, testRunID)
	}
}

func TestManualSeriesPre1970Date(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "CPIAUCSL", "date": "1947-01-01", "value": 21.48}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("pre-1970 date (Date32) must pass: status %d: %s", status, payload)
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
	if len(writer.series) != 1 || writer.series[0][0].Date != "1947-01-01" {
		t.Fatalf("writer got %+v", writer.series)
	}
}

func TestManualSeriesValidationError(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "CPIAUCSL", "date": "2026-13-01", "value": 1}]`
	status, _ := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", status)
	}
	if len(writer.series) != 0 {
		t.Fatal("writer must not be called on validation error")
	}
}

func TestManualSeriesUnauthorized(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, _ := post(t, server.URL+"/v1/manual_series", "", `[]`)
	if status != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", status)
	}
}

func TestGoldPricesOK(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[
		{"venue": "comex_front", "date": "2026-10-09", "usd": 4216.3},
		{"venue": "comex_front", "date": "2026-10-08", "usd": 4157.0}
	]`
	status, payload := post(t, server.URL+"/v1/gold_prices", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	var response struct {
		Inserted int `json:"inserted"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response is not JSON: %s", payload)
	}
	if response.Inserted != 2 {
		t.Fatalf("inserted = %d, want 2", response.Inserted)
	}
	if len(writer.goldPrices) != 1 || len(writer.goldPrices[0]) != 2 {
		t.Fatalf("writer got %+v", writer.goldPrices)
	}
	first := writer.goldPrices[0][0]
	if first.Venue != "comex_front" || first.Date != "2026-10-09" || first.Usd != 4216.3 {
		t.Fatalf("first point = %+v", first)
	}
}

func TestGoldPricesValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"venue case mismatch", `[{"venue": "Comex_Front", "date": "2026-10-09", "usd": 4216.3}]`},
		{"venue not in whitelist", `[{"venue": "spot", "date": "2026-10-09", "usd": 4216.3}]`},
		{"date not zero padded", `[{"venue": "comex_front", "date": "2026-1-1", "usd": 4216.3}]`},
		{"impossible date", `[{"venue": "comex_front", "date": "2026-13-01", "usd": 4216.3}]`},
		{"usd missing", `[{"venue": "comex_front", "date": "2026-10-09"}]`},
		{"zero usd", `[{"venue": "comex_front", "date": "2026-10-09", "usd": 0}]`},
		{"negative usd", `[{"venue": "comex_front", "date": "2026-10-09", "usd": -1}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &mockBatchWriter{}
			server := newTestServer(writer)
			defer server.Close()

			status, payload := post(t, server.URL+"/v1/gold_prices", testToken, tt.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status %d, want 400: %s", status, payload)
			}
			if len(writer.goldPrices) != 0 {
				t.Fatal("writer must not be called on validation error")
			}
		})
	}
}

func TestGoldPricesEmptyArray(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, payload := post(t, server.URL+"/v1/gold_prices", testToken, `[]`)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	var response struct {
		Inserted int `json:"inserted"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response is not JSON: %s", payload)
	}
	if response.Inserted != 0 {
		t.Fatalf("inserted = %d, want 0", response.Inserted)
	}
}

func TestModelRunUnknownFieldIsRejected(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	body := strings.Replace(validRunBody, `"comment": "unit test",`, `"comment": "unit test", "nav_pershare": 1,`, 1)
	status, payload := post(t, server.URL+"/v1/model_run", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if !strings.Contains(string(payload), "nav_pershare") {
		t.Fatalf("expected unknown field name in error, got %s", payload)
	}
	if len(writer.runs) != 0 {
		t.Fatal("writer must not be called with unknown field")
	}
}

func TestManualSeriesUnknownFieldIsRejected(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "CPIAUCSL", "date": "2026-01-01", "value": 1, "units": "pct"}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if len(writer.series) != 0 {
		t.Fatal("writer must not be called with unknown field")
	}
}

func TestManualSeriesSourceAccepted(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "ra_gold_pol", "date": "2026-08-01", "value": 45600, "source": "imf"}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	if len(writer.series) != 1 || writer.series[0][0].Source != "imf" {
		t.Fatalf("writer got %+v", writer.series)
	}
}

func TestManualSeriesSourceRejected(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "CPIAUCSL", "date": "2026-01-01", "value": 1, "source": "fred"}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if !strings.Contains(string(payload), "source") {
		t.Fatalf("expected source in error text, got %s", payload)
	}
	if len(writer.series) != 0 {
		t.Fatal("writer must not be called on validation error")
	}
}

func TestManualSeriesSourceDefault(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "dxy", "date": "2026-10-09", "value": 102.21}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	// Дефолт 'manual' применяет writer (source пустой) — здесь только проверяем,
	// что отсутствие поля валидно и точка доходит до writer'а без подстановки в handler.
	if len(writer.series) != 1 || writer.series[0][0].Source != "" {
		t.Fatalf("writer got %+v", writer.series)
	}
}

// Явная пустая строка source в JSON эквивалентна отсутствию поля: валидация пропускает,
// дефолт 'manual' применяет writer при вставке.
func TestManualSeriesExplicitEmptySourceDefaults(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"series": "crack_ulsd_proxy", "date": "2026-10-09", "value": 107.16, "source": ""}]`
	status, payload := post(t, server.URL+"/v1/manual_series", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	if len(writer.series) != 1 || writer.series[0][0].Source != "" {
		t.Fatalf("writer got %+v", writer.series)
	}
}

// Лимит: burst 2 при 60/мин (1 токен в секунду). Неавторизованные запросы токены не тратят.
func TestRateLimitReturns429AfterBurst(t *testing.T) {
	clock := &manualClock{now: time.Unix(1800000000, 0)}
	writer := &mockBatchWriter{}
	server := httptest.NewServer(newServer(writer, testToken, newTokenBucket(60, 2, clock.Now)))
	defer server.Close()

	for i := 0; i < 5; i++ {
		if status, _ := post(t, server.URL+"/v1/manual_series", "wrong-token", `[]`); status != http.StatusUnauthorized {
			t.Fatalf("unauthorized request %d: status %d, want 401", i+1, status)
		}
	}
	for i := 0; i < 2; i++ {
		if status, payload := post(t, server.URL+"/v1/manual_series", testToken, `[]`); status != http.StatusOK {
			t.Fatalf("request %d within burst: status %d, want 200: %s", i+1, status, payload)
		}
	}
	if status, _ := post(t, server.URL+"/v1/manual_series", testToken, `[]`); status != http.StatusTooManyRequests {
		t.Fatalf("request over burst: status %d, want 429", status)
	}
	if len(writer.series) != 2 {
		t.Fatalf("writer called %d times, want 2 (429 must not reach the writer)", len(writer.series))
	}

	clock.Advance(time.Second)
	if status, _ := post(t, server.URL+"/v1/manual_series", testToken, `[]`); status != http.StatusOK {
		t.Fatalf("request after 1s refill: status %d, want 200", status)
	}
	if status, _ := post(t, server.URL+"/v1/manual_series", testToken, `[]`); status != http.StatusTooManyRequests {
		t.Fatalf("second request after 1s refill: status %d, want 429", status)
	}
}

func TestGoldForecastsOK(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[
		{"author": "goldman", "published": "2026-10-07", "horizon": "2027", "scenario": "range", "low": 5400, "high": 5600},
		{"author": "own", "published": "2026-10-11", "horizon": "Q4-2026", "scenario": "bull", "point": 4800, "probability": 25}
	]`
	status, payload := post(t, server.URL+"/v1/gold_forecasts", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	var response struct {
		Inserted int `json:"inserted"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response is not JSON: %s", payload)
	}
	if response.Inserted != 2 {
		t.Fatalf("inserted = %d, want 2", response.Inserted)
	}
	if len(writer.goldForecasts) != 1 || len(writer.goldForecasts[0]) != 2 {
		t.Fatalf("writer got %+v", writer.goldForecasts)
	}
	first := writer.goldForecasts[0][0]
	want := GoldForecast{Author: "goldman", Published: "2026-10-07", Horizon: "2027", Scenario: "range", Low: 5400, High: 5600}
	if first != want {
		t.Fatalf("first forecast = %+v, want %+v", first, want)
	}
}

// Review Focus 1: валидация всего массива до записи — одна невалидная строка
// в середине массива отменяет запись целиком.
func TestGoldForecastsValidationError(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[
		{"author": "goldman", "published": "2026-10-07", "horizon": "2027", "scenario": "range", "low": 5400, "high": 5600},
		{"author": "own", "published": "2026-10-11", "horizon": "Q4-2026", "scenario": "bull", "point": 4800},
		{"author": "", "published": "2026-10-11", "horizon": "Q4-2026", "scenario": "bear", "point": 3900}
	]`
	status, payload := post(t, server.URL+"/v1/gold_forecasts", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if !strings.Contains(string(payload), "author") {
		t.Fatalf("expected author in error text, got %s", payload)
	}
	if len(writer.goldForecasts) != 0 {
		t.Fatalf("writer must not be called when any row is invalid, got %+v", writer.goldForecasts)
	}
}

func TestGoldForecastsEmptyArray(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	status, payload := post(t, server.URL+"/v1/gold_forecasts", testToken, `[]`)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	var response struct {
		Inserted int `json:"inserted"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response is not JSON: %s", payload)
	}
	if response.Inserted != 0 {
		t.Fatalf("inserted = %d, want 0", response.Inserted)
	}
}

func TestGoldForecastsUnknownFieldIsRejected(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[{"author": "own", "published": "2026-10-11", "horizon": "Q4-2026",
		"scenario": "bull", "point": 4800, "probability_pct": 25}]`
	status, payload := post(t, server.URL+"/v1/gold_forecasts", testToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", status, payload)
	}
	if !strings.Contains(string(payload), "probability_pct") {
		t.Fatalf("expected unknown field name in error, got %s", payload)
	}
	if len(writer.goldForecasts) != 0 {
		t.Fatal("writer must not be called with unknown field")
	}
}
