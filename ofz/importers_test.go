package ofz

import (
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

func TestOfzImportersRegistered(t *testing.T) {
	if len(ofzImporters) != 1 {
		t.Fatalf("importers = %d, want 1", len(ofzImporters))
	}
	for _, imp := range ofzImporters {
		if !strings.HasPrefix(imp.Name(), "ofz_") {
			t.Fatalf("importer %q is outside ofz_ namespace", imp.Name())
		}
	}
	found := false
	for _, stat := range chimport.Stats {
		if stat.Name() == ofzIssueAnalyticsTable {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("%s is not registered in chimport.Stats", ofzIssueAnalyticsTable)
	}
}

func fixedSource() securitySource {
	return securitySource{
		SecCode:       "SU26238RMFS9",
		Isin:          "RU000A10BQF8",
		SecName:       "ОФЗ-ПД 26238",
		Maturity:      date(2027, time.October, 24),
		NextCoupon:    date(2027, time.October, 24),
		CouponPeriod:  365,
		CouponValue:   100,
		FaceValue:     1000,
		Board:         "TQCB",
		ShortName:     "ОФЗ-ПД 26238",
		LotSize:       1,
		LastPrice:     100,
		LastTradeTime: date(2026, time.October, 24),
		TradeCount:    1,
		Notional:      100,
	}
}

func TestBuildAnalyticsRowsFixedCoupon(t *testing.T) {
	settle := date(2026, time.October, 24)
	rows := buildAnalyticsRows(settle, []securitySource{fixedSource()}, []curvePoint{
		{Years: 1, Yield: 8, Tenor: "1"},
	}, settle.Add(time.Hour))
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.CalculationStatus != "ok" {
		t.Fatalf("status = %q, want ok", row.CalculationStatus)
	}
	if row.YTM == nil || !almostEqual(*row.YTM, 10, 1e-6) {
		t.Fatalf("ytm = %v, want 10", row.YTM)
	}
	if row.Duration == nil || !almostEqual(*row.Duration, 1, 1e-6) {
		t.Fatalf("duration = %v, want 1", row.Duration)
	}
	if row.ModifiedDuration == nil || !almostEqual(*row.ModifiedDuration, 1.0/1.1, 1e-6) {
		t.Fatalf("modified duration = %v, want %.6f", row.ModifiedDuration, 1.0/1.1)
	}
	if row.SpreadToCurve == nil || !almostEqual(*row.SpreadToCurve, 2, 1e-6) {
		t.Fatalf("spread = %v, want 2", row.SpreadToCurve)
	}
	if row.CurveTenor != "1" {
		t.Fatalf("curve tenor = %q, want 1", row.CurveTenor)
	}
	if row.Turnover == nil || !almostEqual(*row.Turnover, 1000, 1e-6) {
		t.Fatalf("turnover = %v, want 1000", row.Turnover)
	}
	if row.TradeCount == nil || *row.TradeCount != 1 {
		t.Fatalf("trade count = %v, want 1", row.TradeCount)
	}
	if row.DaysSinceLastTrade == nil || *row.DaysSinceLastTrade != 0 {
		t.Fatalf("days since last trade = %v, want 0", row.DaysSinceLastTrade)
	}
}

func TestBuildAnalyticsRowsUnsupportedAndMissing(t *testing.T) {
	settle := date(2026, time.October, 24)
	tests := []struct {
		name   string
		mutate func(*securitySource)
		status string
	}{
		{
			name: "variable",
			mutate: func(sec *securitySource) {
				sec.ShortName = "ОФЗ-ПК 29006"
				sec.SecName = "ОФЗ-ПК 29006"
			},
			status: "unsupported_variable",
		},
		{
			name: "no price",
			mutate: func(sec *securitySource) {
				sec.LastPrice = 0
				sec.LastTradeTime = time.Time{}
			},
			status: "no_price",
		},
		{
			name: "missing coupon",
			mutate: func(sec *securitySource) {
				sec.CouponPeriod = 0
			},
			status: "missing_data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sec := fixedSource()
			test.mutate(&sec)
			rows := buildAnalyticsRows(settle, []securitySource{sec}, nil, settle)
			if len(rows) != 1 {
				t.Fatalf("rows = %d, want 1", len(rows))
			}
			if rows[0].CalculationStatus != test.status {
				t.Fatalf("status = %q, want %q", rows[0].CalculationStatus, test.status)
			}
			if rows[0].YTM != nil {
				t.Fatalf("ytm = %v, want nil", *rows[0].YTM)
			}
		})
	}
}

func TestBuildAnalyticsRowsSkipsNonOFZ(t *testing.T) {
	sec := fixedSource()
	sec.ShortName = "Газпром БО-01"
	sec.SecName = "Газпром БО-01"
	rows := buildAnalyticsRows(date(2026, time.October, 24), []securitySource{sec}, nil, time.Now())
	if len(rows) != 0 {
		t.Fatalf("rows = %d, want 0 for a corporate bond", len(rows))
	}
}
