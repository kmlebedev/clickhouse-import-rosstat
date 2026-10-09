package bea

import (
	"context"
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

func TestSeriesMetaCoversAllLines(t *testing.T) {
	described := make(map[string]bool, len(beaSeriesMeta))
	for _, m := range beaSeriesMeta {
		if m.Source != beaSource || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Fatalf("incomplete catalog entry: %+v", m)
		}
		described[m.Series] = true
	}
	for line, series := range beaLines {
		if !described[series] {
			t.Errorf("line %s series %s has no series_catalog entry", line, series)
		}
	}
}

func TestParseResponse(t *testing.T) {
	const body = `{"BEAAPI":{"Results":{"Data":[
		{"LineNumber":"1","TimePeriod":"2025M12","DataValue":"125.123"},
		{"LineNumber":"25","TimePeriod":"2025M12","DataValue":"1,234.5"},
		{"LineNumber":"1","TimePeriod":"2025Q4","DataValue":"124.0"},
		{"LineNumber":"2","TimePeriod":"2025M12","DataValue":"99.9"},
		{"LineNumber":"1","TimePeriod":"2025M11","DataValue":""}
	]}}}`
	got, err := parseResponse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	want := []observation{
		{series: "PCE_PI", date: time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC), value: 125.123},
		{series: "PCE_PI_CORE", date: time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC), value: 1234.5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseResponseApiError(t *testing.T) {
	const body = `{"BEAAPI":{"Results":{"Error":{"APIErrorCode":"3","APIErrorDescription":"The BEA API UserID provided in the request does not exist."}}}}`
	_, err := parseResponse(strings.NewReader(body))
	if err == nil || !strings.Contains(err.Error(), "UserID") {
		t.Fatalf("expected api error, got %v", err)
	}
}

func TestParseResponseErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"bad value", `{"BEAAPI":{"Results":{"Data":[{"LineNumber":"1","TimePeriod":"2025M01","DataValue":"x"}]}}}`},
		{"bad month", `{"BEAAPI":{"Results":{"Data":[{"LineNumber":"1","TimePeriod":"2025M13","DataValue":"1"}]}}}`},
		{"malformed json", `{"BEAAPI":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseResponse(strings.NewReader(tt.body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestFetchTableSendsQuery(t *testing.T) {
	var query url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = io.WriteString(w, `{"BEAAPI":{"Results":{"Data":[]}}}`)
	}))
	defer server.Close()

	defer func(prev string) { beaBaseUrl = prev }(beaBaseUrl)
	beaBaseUrl = server.URL

	if _, err := fetchTable(context.Background(), fakeKey, 2006, 2008); err != nil {
		t.Fatalf("fetchTable: %v", err)
	}
	checks := map[string]string{
		"UserID":       fakeKey,
		"method":       "GetData",
		"datasetname":  "NIPA",
		"TableName":    "T20804",
		"Frequency":    "M",
		"Year":         "2006,2007,2008",
		"ResultFormat": "JSON",
	}
	for k, want := range checks {
		if got := query.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestFetchTableMasksKeyInTransportError(t *testing.T) {
	defer func(prev string) { beaBaseUrl = prev }(beaBaseUrl)
	beaBaseUrl = "http://127.0.0.1:1"

	_, err := fetchTable(context.Background(), fakeKey, 2006, 2006)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), fakeKey) {
		t.Fatalf("error leaks UserID: %v", err)
	}
}

func TestImportRequiresKey(t *testing.T) {
	t.Setenv(beaKeyEnv, "")
	_, err := (&beaImport{}).Import(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), beaKeyEnv) {
		t.Fatalf("expected missing key error, got %v", err)
	}
}
