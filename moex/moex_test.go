package moex

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSeriesMetaCoversAllSeries(t *testing.T) {
	described := make(map[string]bool, len(moexSeriesMeta))
	for _, m := range moexSeriesMeta {
		if m.Source != moexSource || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Fatalf("incomplete catalog entry: %+v", m)
		}
		described[m.Series] = true
	}
	if !described[plzlCode] {
		t.Errorf("series %s (stock_prices) has no series_catalog entry", plzlCode)
	}
	if !described[rgbiTenor] {
		t.Errorf("tenor %s (ofz_curve) has no series_catalog entry", rgbiTenor)
	}
	for period, tenor := range zcycPeriodToTenor {
		if !described[tenor] {
			t.Errorf("zcyc period %v tenor %s has no series_catalog entry", period, tenor)
		}
	}
}

func TestParseCandles(t *testing.T) {
	block := issBlock{
		Columns: []string{"open", "close", "high", "low", "value", "volume", "begin", "end"},
		Data: [][]any{
			{972.0, 960.4, 976.6, 955.6, 8.2e8, 854585.0, "2026-10-01 00:00:00", "2026-10-01 23:59:59"},
			{959.0, 959.8, 967.6, 950.0, 8.7e8, 908357.0, "2026-10-02 00:00:00", "2026-10-02 23:59:58"},
		},
	}
	got, err := parseCandles(block)
	if err != nil {
		t.Fatalf("parseCandles: %v", err)
	}
	day := func(s string) time.Time { d, _ := time.Parse(moexDate, s); return d }
	want := []candle{
		{date: day("2026-10-01"), open: 972, close: 960.4, high: 976.6, low: 955.6, volume: 854585},
		{date: day("2026-10-02"), open: 959, close: 959.8, high: 967.6, low: 950, volume: 908357},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseCandlesErrors(t *testing.T) {
	tests := []struct {
		name  string
		block issBlock
	}{
		{"missing column", issBlock{Columns: []string{"open", "close"}, Data: [][]any{{1, 2}}}},
		{"short row", issBlock{Columns: []string{"open", "close", "high", "low", "volume", "begin"}, Data: [][]any{{1, 2}}}},
		{"bad date", issBlock{
			Columns: []string{"open", "close", "high", "low", "volume", "begin"},
			Data:    [][]any{{1.0, 2.0, 3.0, 4.0, 5.0, "not-a-date"}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseCandles(tt.block); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestFetchCandlesPaginates(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("start") {
		case "0":
			var data []string
			for i := 0; i < moexPageLimit; i++ {
				data = append(data, `[1,2,3,4,5,6,"2026-10-01 00:00:00","2026-10-01 23:59:59"]`)
			}
			_, _ = io.WriteString(w, `{"candles":{"columns":["open","close","high","low","value","volume","begin","end"],"data":[`+
				strings.Join(data, ",")+`]}}`)
		default:
			_, _ = io.WriteString(w, `{"candles":{"columns":["open","close","high","low","value","volume","begin","end"],"data":[]}}`)
		}
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	from, _ := time.Parse(moexDate, "2026-10-01")
	candles, err := fetchCandles(context.Background(), "/iss/fake/PLZL/candles.json", from)
	if err != nil {
		t.Fatalf("fetchCandles: %v", err)
	}
	if len(candles) != moexPageLimit {
		t.Fatalf("got %d candles, want %d", len(candles), moexPageLimit)
	}
	if len(paths) != 2 || !strings.Contains(paths[0], "start=0") || !strings.Contains(paths[1], "start=100") {
		t.Fatalf("unexpected pagination requests %v", paths)
	}
	if !strings.Contains(paths[0], "from=2026-10-01") || !strings.Contains(paths[0], "interval=24") ||
		!strings.Contains(paths[0], "limit=100") {
		t.Fatalf("missing from/interval in %q", paths[0])
	}
}

func TestParseYearYields(t *testing.T) {
	block := issBlock{
		Columns: []string{"tradedate", "tradetime", "period", "value"},
		Data: [][]any{
			{"2026-10-09", "18:54:59", 0.25, 11.87},
			{"2026-10-09", "18:54:59", 1.0, 13.7592},
			{"2026-10-09", "18:54:59", 3.0, 15.9688},
			{"2026-10-09", "18:54:59", 5.0, 16.568},
			{"2026-10-09", "18:54:59", 10.0, 16.8486},
			{"2026-10-09", "18:54:59", 20.0, 16.92},
		},
	}
	got, err := parseYearYields(block)
	if err != nil {
		t.Fatalf("parseYearYields: %v", err)
	}
	day, _ := time.Parse(moexDate, "2026-10-09")
	want := []curvePoint{
		{date: day, tenor: "1y", value: 13.7592},
		{date: day, tenor: "3y", value: 15.9688},
		{date: day, tenor: "5y", value: 16.568},
		{date: day, tenor: "10y", value: 16.8486},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFetchYearYieldsSendsRequest(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.String()
		_, _ = io.WriteString(w, `{"yearyields":{"columns":["tradedate","tradetime","period","value"],"data":[["2026-10-09","18:54:59",5.0,16.5]]}}`)
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	points, err := fetchYearYields(context.Background())
	if err != nil {
		t.Fatalf("fetchYearYields: %v", err)
	}
	if len(points) != 1 || points[0].tenor != "5y" || points[0].value != 16.5 {
		t.Fatalf("unexpected points %+v", points)
	}
	if !strings.HasPrefix(path, "/iss/engines/stock/zcyc.json") {
		t.Fatalf("unexpected path %q", path)
	}
}
