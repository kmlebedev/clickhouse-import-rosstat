package bls

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSeriesMetaCoversAllSeries(t *testing.T) {
	described := make(map[string]bool, len(blsSeriesMeta))
	for _, m := range blsSeriesMeta {
		if m.Source != blsSource || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Fatalf("incomplete catalog entry: %+v", m)
		}
		described[m.Series] = true
	}
	for _, series := range blsSeries {
		if !described[series] {
			t.Errorf("series %s has no series_catalog entry", series)
		}
	}
}

func TestParseResponse(t *testing.T) {
	const succeeded = `{"status":"REQUEST_SUCCEEDED","message":[],"Results":{"series":[
		{"seriesID":"CUUR0000SA0","data":[
			{"year":"2025","period":"M02","value":"319.082"},
			{"year":"2025","period":"M01","value":"-"},
			{"year":"2024","period":"M13","value":"313.689"},
			{"year":"2024","period":"Q01","value":"312.0"}
		]}]}}`
	got, err := parseResponse(strings.NewReader(succeeded))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	want := []observation{
		{series: "CUUR0000SA0", date: time.Date(2025, time.February, 1, 0, 0, 0, 0, time.UTC), value: 319.082},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseResponseCatalogWarningIsNotError(t *testing.T) {
	const body = `{"status":"REQUEST_SUCCEEDED","message":["Unable to get Catalog Data for series X"],"Results":{"series":[
		{"seriesID":"X","data":[{"year":"2026","period":"M01","value":"4.3"}]}]}}`
	got, err := parseResponse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if len(got) != 1 || got[0].value != 4.3 {
		t.Fatalf("unexpected observations: %+v", got)
	}
}

func TestParseResponseErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"request not processed", `{"status":"REQUEST_NOT_PROCESSED","message":["daily threshold reached"],"Results":{}}`},
		{"bad value", `{"status":"REQUEST_SUCCEEDED","message":[],"Results":{"series":[{"seriesID":"X","data":[{"year":"2026","period":"M01","value":"abc"}]}]}}`},
		{"bad year", `{"status":"REQUEST_SUCCEEDED","message":[],"Results":{"series":[{"seriesID":"X","data":[{"year":"20x6","period":"M01","value":"1"}]}]}}`},
		{"malformed json", `{"status":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseResponse(strings.NewReader(tt.body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestYearWindows(t *testing.T) {
	got := yearWindows(2006, 2026)
	want := [][2]int{{2006, 2015}, {2016, 2025}, {2026, 2026}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, w := range got {
		if w[1]-w[0]+1 > blsWindowYears {
			t.Fatalf("window %v exceeds %d years", w, blsWindowYears)
		}
	}
}

func TestFetchWindowSendsPayload(t *testing.T) {
	var received blsRequest
	var contentType, method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		contentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("request body is not JSON: %v", err)
		}
		_, _ = io.WriteString(w, `{"status":"REQUEST_SUCCEEDED","message":[],"Results":{"series":[]}}`)
	}))
	defer server.Close()

	defer func(prev string) { blsBaseUrl = prev }(blsBaseUrl)
	blsBaseUrl = server.URL

	if _, err := fetchWindow(context.Background(), "test-key", 2016, 2025); err != nil {
		t.Fatalf("fetchWindow: %v", err)
	}
	if method != http.MethodPost {
		t.Errorf("method = %s, want POST", method)
	}
	if contentType != "application/json" {
		t.Errorf("Content-Type = %s, want application/json", contentType)
	}
	if received.StartYear != "2016" || received.EndYear != "2025" {
		t.Errorf("years = %s-%s, want 2016-2025", received.StartYear, received.EndYear)
	}
	if received.RegistrationKey != "test-key" {
		t.Errorf("registrationkey = %q, want test-key", received.RegistrationKey)
	}
	if !reflect.DeepEqual(received.SeriesIds, blsSeries) {
		t.Errorf("seriesid = %v, want %v", received.SeriesIds, blsSeries)
	}
}

func TestFetchWindowHttpError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	defer func(prev string) { blsBaseUrl = prev }(blsBaseUrl)
	blsBaseUrl = server.URL

	if _, err := fetchWindow(context.Background(), "", 2006, 2015); err == nil {
		t.Fatal("expected error on HTTP 403")
	}
}
