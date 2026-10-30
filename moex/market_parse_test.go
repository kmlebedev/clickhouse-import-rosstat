package moex

import (
	"reflect"
	"testing"
	"time"
)

func mustDay(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse(moexDate, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return d
}

func TestParseContractMonth(t *testing.T) {
	tests := []struct {
		shortname string
		asset     string
		want      contractMonth
	}{
		{"BR-10.09", "BR", contractMonth{year: 2009, month: time.October}},
		{"BR-12.26", "BR", contractMonth{year: 2026, month: time.December}},
		{"COPPER-12.24", "COPPER", contractMonth{year: 2024, month: time.December}},
	}
	for _, tt := range tests {
		got, err := parseContractMonth(tt.shortname, tt.asset)
		if err != nil {
			t.Fatalf("%s: %v", tt.shortname, err)
		}
		if got != tt.want {
			t.Errorf("%s: got %+v, want %+v", tt.shortname, got, tt.want)
		}
	}
	for _, tt := range []struct {
		shortname string
		asset     string
	}{
		{"BR-13.09", "BR"},
		{"BR-10", "BR"},
		{"BR-10.09.01", "BR"},
		{"BRM-10.09", "BR"},
		{"COPPER-12.24", "BR"},
	} {
		if _, err := parseContractMonth(tt.shortname, tt.asset); err == nil {
			t.Errorf("%s: expected error", tt.shortname)
		}
	}
}

func TestParseFuturesHistory(t *testing.T) {
	block := issBlock{
		Columns: []string{"TRADEDATE", "SECID", "CLOSE", "SETTLEPRICE", "VOLUME", "OPENPOSITION", "SHORTNAME", "ASSETCODE"},
		Data: [][]any{
			{"2009-10-05", "BRV9_2009", 66.45, 66.45, 21486.0, 24756.0, "BR-10.09", "BR"},
			{"2009-10-05", "BRV9_2009BRX9_2009", 0.0, 0.0, 0.0, 0.0, "BR-10.09-11.09", ""},
			{"2009-10-05", "BRV9_2009BRX9_2009", 0.0, 0.0, 0.0, 0.0, "BR-10.09-11.09", nil},
			{"2009-10-05", "BRX9_2009", nil, 69.73, nil, nil, "BR-11.09", "BR"},
		},
	}
	bars, err := parseFuturesHistory(block, "BR")
	if err != nil {
		t.Fatalf("parseFuturesHistory: %v", err)
	}
	want := []futuresBar{
		{
			secid: "BRV9_2009", month: contractMonth{year: 2009, month: time.October},
			date: mustDay(t, "2009-10-05"), settle: 66.45, close: 66.45, volume: 21486, openPosition: 24756,
		},
		{
			secid: "BRX9_2009", month: contractMonth{year: 2009, month: time.November},
			date: mustDay(t, "2009-10-05"), settle: 69.73,
		},
	}
	if !reflect.DeepEqual(bars, want) {
		t.Fatalf("got %+v, want %+v", bars, want)
	}
}

func TestParseFuturesHistoryErrors(t *testing.T) {
	base := []string{"TRADEDATE", "SECID", "CLOSE", "SETTLEPRICE", "VOLUME", "OPENPOSITION", "SHORTNAME", "ASSETCODE"}
	tests := []struct {
		name  string
		block issBlock
	}{
		{"missing column", issBlock{Columns: base[:7], Data: [][]any{{}}}},
		{"short row", issBlock{Columns: base, Data: [][]any{{"2009-10-05"}}}},
		{"bad date", issBlock{Columns: base, Data: [][]any{{"05.10.2009", "BRV9", 1.0, 1.0, 0.0, 0.0, "BR-10.09", "BR"}}}},
		{"bad shortname", issBlock{Columns: base, Data: [][]any{{"2009-10-05", "BRV9", 1.0, 1.0, 0.0, 0.0, "BR-13.09", "BR"}}}},
		{"bad settle", issBlock{Columns: base, Data: [][]any{{"2009-10-05", "BRV9", 1.0, "x", 0.0, 0.0, "BR-10.09", "BR"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseFuturesHistory(tt.block, "BR"); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseFuturesSecids(t *testing.T) {
	block := issBlock{
		Columns: []string{"SECID", "SHORTNAME", "ASSETCODE"},
		Data: [][]any{
			{"BRF7", "BR-1.27", "BR"},
			{"BRG7", "BR-2.27", "BR"},
			{"BRF7BRG7", "BR-1.27-2.27", ""},
			{"BRF7", "BR-1.27", "BR"},
			{"CEH7", "COPPER-3.27", "COPPER"},
			{"BR98BK6", "BR-12.26M251126CA98", "BR"},
			{"BRN6", "BR-7.26", nil},
		},
	}
	got, err := parseFuturesSecids(block, "BR")
	if err != nil {
		t.Fatalf("parseFuturesSecids: %v", err)
	}
	want := []string{"BRF7", "BRG7"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFrontMonthPoints(t *testing.T) {
	d1 := mustDay(t, "2009-10-05")
	d2 := mustDay(t, "2009-10-06")
	d3 := mustDay(t, "2009-11-02")
	bars := []futuresBar{
		{secid: "BRX9", month: contractMonth{year: 2009, month: time.November}, date: d1, settle: 69.73},
		{secid: "BRV9", month: contractMonth{year: 2009, month: time.October}, date: d1, settle: 66.45},
		{secid: "BRX9", month: contractMonth{year: 2009, month: time.November}, date: d2, settle: 70.0},
		{secid: "BRV9", month: contractMonth{year: 2009, month: time.October}, date: d2, close: 66.5},
		{secid: "BRX9", month: contractMonth{year: 2009, month: time.November}, date: d3, settle: 71.0},
		{secid: "BRV9", month: contractMonth{year: 2009, month: time.October}, date: d3, settle: 0, close: 0},
	}
	got := frontMonthPoints(bars)
	want := []marketPoint{
		{date: d1, value: 66.45},
		{date: d2, value: 66.5},
		{date: d3, value: 71.0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFrontMonthPointsSkipsEmpty(t *testing.T) {
	got := frontMonthPoints([]futuresBar{
		{secid: "BRV9", date: mustDay(t, "2009-10-05")},
	})
	if len(got) != 0 {
		t.Fatalf("got %+v, want empty", got)
	}
}
