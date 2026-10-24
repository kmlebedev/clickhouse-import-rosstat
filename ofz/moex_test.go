package ofz

import (
	"testing"
	"time"
)

func TestParseAndBuildMOEXBonds(t *testing.T) {
	response := moexBondResponse{}
	response.Securities.Columns = []string{
		"SECID", "BOARDID", "SHORTNAME", "MATDATE", "NEXTCOUPON", "COUPONVALUE",
		"COUPONPERIOD", "ACCRUEDINT", "FACEVALUE", "PREVWAPRICE", "YIELDATPREVWAPRICE",
	}
	response.Securities.Data = [][]any{{
		"SU26207RMFS9", "TQOB", "ОФЗ 26207", "2027-10-24", "2027-10-24",
		100.0, 365.0, 0.0, 1000.0, 100.0, 10.0,
	}}
	bonds, err := parseMOEXBonds(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(bonds) != 1 {
		t.Fatalf("bonds = %d, want 1", len(bonds))
	}
	if bonds[0].SecID != "SU26207RMFS9" || bonds[0].Board != "TQOB" || bonds[0].CouponPeriod != 365 {
		t.Fatalf("bond = %+v", bonds[0])
	}

	settle := date(2026, time.October, 24)
	rows := buildMOEXAnalyticsRows(settle, bonds, []curvePoint{
		{Years: 1, Yield: 8, Tenor: "1"},
	}, settle)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.Source != "moex" {
		t.Fatalf("source = %q, want moex", row.Source)
	}
	if row.CalculationStatus != "ok" {
		t.Fatalf("status = %q, want ok", row.CalculationStatus)
	}
	if row.YTM == nil || !almostEqual(*row.YTM, 10, 1e-6) {
		t.Fatalf("ytm = %v, want 10", row.YTM)
	}
	if row.Duration == nil || !almostEqual(*row.Duration, 1, 1e-6) {
		t.Fatalf("duration = %v, want 1", row.Duration)
	}
	if row.SpreadToCurve == nil || !almostEqual(*row.SpreadToCurve, 2, 1e-6) {
		t.Fatalf("spread = %v, want 2", row.SpreadToCurve)
	}
	if row.Turnover != nil || row.TradeCount != nil {
		t.Fatalf("MOEX fallback must not invent liquidity: turnover=%v trades=%v", row.Turnover, row.TradeCount)
	}
}

func TestParseMOEXBondsRejectsMissingColumns(t *testing.T) {
	response := moexBondResponse{}
	response.Securities.Columns = []string{"SECID"}
	if _, err := parseMOEXBonds(response); err == nil {
		t.Fatal("parseMOEXBonds accepted a response without required columns")
	}
}
