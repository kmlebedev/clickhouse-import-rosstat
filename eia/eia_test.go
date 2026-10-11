package eia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

const fakeKey = "00000000-0000-0000-0000-000000000000"

// cannedResponse — снимок формы ответа EIA API v2 без сети: две серии, values строками.
const cannedResponse = `{"response":{"total":"4","data":[
	{"period":"2026-09-25","series":"WDISTUS1","value":"105138","units":"MBBL"},
	{"period":"2026-10-02","series":"WDISTUS1","value":"104500","units":"MBBL"},
	{"period":"2026-09-25","series":"WPULEUS3","value":"92.7","units":"%"},
	{"period":"2026-10-02","series":"WPULEUS3","value":"91.3","units":"%"}
]}}`

func TestSeriesMetaCoversAllSeries(t *testing.T) {
	described := make(map[string]bool, len(eiaSeriesMeta))
	for _, m := range eiaSeriesMeta {
		if m.Source != eiaSource || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Fatalf("incomplete catalog entry: %+v", m)
		}
		if m.Frequency != "W" {
			t.Errorf("series %s: frequency = %q, want W", m.Series, m.Frequency)
		}
		described[m.Series] = true
	}
	for _, s := range eiaSeries {
		if !described[s.series] {
			t.Errorf("EIA series %s (%s) has no series_catalog entry", s.code, s.series)
		}
	}
}

func TestParseResponse(t *testing.T) {
	page, err := parseResponse(strings.NewReader(cannedResponse))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if page.total != 4 {
		t.Fatalf("total = %d, want 4", page.total)
	}
	if page.rows != 4 {
		t.Fatalf("rows = %d, want 4", page.rows)
	}
	got := page.observations
	want := []observation{
		{series: "distillate_stocks", date: time.Date(2026, time.September, 25, 0, 0, 0, 0, time.UTC), value: 105138},
		{series: "distillate_stocks", date: time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC), value: 104500},
		{series: "refinery_utilization", date: time.Date(2026, time.September, 25, 0, 0, 0, 0, time.UTC), value: 92.7},
		{series: "refinery_utilization", date: time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC), value: 91.3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseResponseSkipsEmptyValuesAndUnknownSeries(t *testing.T) {
	const body = `{"response":{"total":"5","data":[
		{"period":"2026-09-25","series":"WDISTUS1","value":""},
		{"period":"2026-09-25","series":"WDISTUS1"},
		{"period":"2026-09-25","series":"WDISTUS1","value":"105138"},
		{"period":"2026-09-25","series":"NOTOURSERIES","value":"1"}
	]}}`
	page, err := parseResponse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if page.total != 5 {
		t.Fatalf("total = %d, want 5", page.total)
	}
	if page.rows != 4 {
		t.Fatalf("rows = %d, want 4", page.rows)
	}
	got := page.observations
	want := []observation{
		{series: "distillate_stocks", date: time.Date(2026, time.September, 25, 0, 0, 0, 0, time.UTC), value: 105138},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseResponseErrors(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantSubstr []string
	}{
		{
			name:       "bad period",
			body:       `{"response":{"total":"1","data":[{"period":"2026-13-40","series":"WDISTUS1","value":"1"}]}}`,
			wantSubstr: []string{"WDISTUS1", "2026-13-40", "period"},
		},
		{
			name:       "bad value",
			body:       `{"response":{"total":"1","data":[{"period":"2026-10-02","series":"WDISTUS1","value":"1,051"}]}}`,
			wantSubstr: []string{"WDISTUS1", "2026-10-02", "1,051", "value"},
		},
		{
			name:       "bad total",
			body:       `{"response":{"total":"many","data":[]}}`,
			wantSubstr: []string{"total", "many"},
		},
		{
			name:       "malformed json",
			body:       `{"response":`,
			wantSubstr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseResponse(strings.NewReader(tt.body))
			if err == nil {
				t.Fatal("expected error")
			}
			for _, substr := range tt.wantSubstr {
				if !strings.Contains(err.Error(), substr) {
					t.Errorf("error %q does not contain %q", err, substr)
				}
			}
		})
	}
}

func TestNextOffset(t *testing.T) {
	tests := []struct {
		name     string
		rows     int
		offset   int
		total    int
		wantNext int
		wantMore bool
	}{
		{"short page stops", 405, 0, 405, 0, false},
		{"full page below total continues", 5000, 0, 12000, 5000, true},
		{"full page reaching total stops", 5000, 5000, 10000, 0, false},
		{"full page with missing total stops", 5000, 0, 0, 0, false},
		{"last partial page stops", 1200, 10000, 12000, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, more := nextOffset(tt.rows, tt.offset, tt.total, eiaPageLength)
			if next != tt.wantNext || more != tt.wantMore {
				t.Fatalf("nextOffset(%d, %d, %d, %d) = (%d, %v), want (%d, %v)",
					tt.rows, tt.offset, tt.total, eiaPageLength, next, more, tt.wantNext, tt.wantMore)
			}
		})
	}
}

func TestMaskKey(t *testing.T) {
	transportErr := &url.Error{
		Op:  http.MethodGet,
		URL: "https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key=" + fakeKey + "&frequency=weekly",
		Err: errors.New("dial tcp: i/o timeout"),
	}
	masked := maskKey(transportErr, fakeKey)
	if strings.Contains(masked.Error(), fakeKey) {
		t.Fatalf("error leaks api key: %v", masked)
	}
	if !strings.Contains(masked.Error(), "***") {
		t.Fatalf("expected mask placeholder, got %v", masked)
	}
	if !strings.Contains(masked.Error(), "i/o timeout") {
		t.Fatalf("transport reason lost: %v", masked)
	}
}

func TestMaskKeyEmptyKeyKeepsError(t *testing.T) {
	orig := errors.New("boom")
	if got := maskKey(orig, ""); got.Error() != orig.Error() {
		t.Fatalf("maskKey with empty key = %v, want %v", got, orig)
	}
}

func TestFetchSeriesSendsQuery(t *testing.T) {
	var query url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = io.WriteString(w, `{"response":{"total":"1","data":[{"period":"2026-10-02","series":"WDISTUS1","value":"105138","units":"MBBL"}]}}`)
	}))
	defer server.Close()

	series := eiaSeries[0]
	series.url = server.URL

	observations, err := fetchSeries(context.Background(), fakeKey, series)
	if err != nil {
		t.Fatalf("fetchSeries: %v", err)
	}
	if len(observations) != 1 || observations[0].series != "distillate_stocks" {
		t.Fatalf("observations = %+v, want one distillate_stocks row", observations)
	}
	checks := map[string]string{
		"api_key":            fakeKey,
		"frequency":          "weekly",
		"data[0]":            "value",
		"facets[series][]":   "WDISTUS1",
		"start":              "2019-01-01",
		"sort[0][column]":    "period",
		"sort[0][direction]": "asc",
		"length":             "5000",
		"offset":             "0",
	}
	for k, want := range checks {
		if got := query.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestFetchSeriesPaginates(t *testing.T) {
	var offsets []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		offsets = append(offsets, offset)
		if offset == "0" {
			_, _ = io.WriteString(w, eiaResponseBody(eiaPageLength, "WDISTUS1", "2026-10-02", "105138", "5003"))
			return
		}
		_, _ = io.WriteString(w, eiaResponseBody(3, "WDISTUS1", "2026-10-09", "104000", "5003"))
	}))
	defer server.Close()

	series := eiaSeries[0]
	series.url = server.URL

	observations, err := fetchSeries(context.Background(), fakeKey, series)
	if err != nil {
		t.Fatalf("fetchSeries: %v", err)
	}
	if want := []string{"0", "5000"}; !reflect.DeepEqual(offsets, want) {
		t.Fatalf("offsets = %v, want %v", offsets, want)
	}
	if len(observations) != eiaPageLength+3 {
		t.Fatalf("observations = %d, want %d", len(observations), eiaPageLength+3)
	}
}

// eiaResponseBody собирает ответ EIA API v2 с rows одинаковыми строками data.
func eiaResponseBody(rows int, series, period, value, total string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"response":{"total":"%s","data":[`, total)
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"period":"%s","series":"%s","value":"%s","units":"MBBL"}`, period, series, value)
	}
	b.WriteString(`]}}`)
	return b.String()
}

// Локальная копия дескриптора: подмена eiaSeries[0].url мутировала бы
// package-level срез и текла бы в параллельные тесты.
func TestFetchSeriesMasksKeyInTransportError(t *testing.T) {
	series := eiaSeries[0]
	series.url = "http://127.0.0.1:1"

	_, err := fetchSeries(context.Background(), fakeKey, series)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), fakeKey) {
		t.Fatalf("error leaks api key: %v", err)
	}
}

// Тело не-200 обязано попасть в ошибку: 403 API_KEY_MISSING от EIA неотличим от
// WAF/геоблока по одному коду статуса, а ключ в теле (если он там окажется) — не
// утечь. Пустое тело оставляет прежний короткий текст без висящего двоеточия.
func TestFetchSeriesIncludesErrorBody(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantErr     string
		wantSubstrs []string
	}{
		{
			name:        "тело 403 объясняет отказ",
			status:      http.StatusForbidden,
			body:        `{"error":{"code":"API_KEY_MISSING","message":"key ` + fakeKey + ` is missing"}}`,
			wantSubstrs: []string{"403", "API_KEY_MISSING"},
		},
		{
			name:    "пустое тело — прежний текст ошибки",
			status:  http.StatusInternalServerError,
			body:    "",
			wantErr: "http status 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()

			series := eiaSeries[0]
			series.url = server.URL

			_, err := fetchSeries(context.Background(), fakeKey, series)
			if err == nil {
				t.Fatal("expected http status error")
			}
			if tt.wantErr != "" && err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err, tt.wantErr)
			}
			for _, substr := range tt.wantSubstrs {
				if !strings.Contains(err.Error(), substr) {
					t.Errorf("error %q does not contain %q", err, substr)
				}
			}
			if strings.Contains(err.Error(), fakeKey) {
				t.Fatalf("error leaks api key: %v", err)
			}
		})
	}
}

func TestImportRequiresKey(t *testing.T) {
	t.Setenv(eiaKeyEnv, "")
	_, err := (&eiaImport{}).Import(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), eiaKeyEnv) {
		t.Fatalf("expected missing key error, got %v", err)
	}
}
