package ingest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testToken = "unit-test-token"

type mockBatchWriter struct {
	runs   []ModelRun
	series [][]ManualSeriesPoint
	err    error
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

func newTestServer(writer BatchWriter) *httptest.Server {
	return httptest.NewServer(NewServer(writer, testToken))
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
