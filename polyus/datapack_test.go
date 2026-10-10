package polyus

import (
	"testing"
	"time"
)

func TestParseDatapackReadsAssetRows(t *testing.T) {
	rows, stats, err := parseDatapack("testdata/polyus_datapack_fy2025_new.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("no rows parsed")
	}
	// OLIMPIADA присутствует как таблица
	var found bool
	for _, r := range rows {
		if r.Table == "OLIMPIADA" {
			found = true
			break
		}
	}
	if !found {
		t.Error("OLIMPIADA table not found in parsed rows")
	}
	if stats.Rows == 0 {
		t.Error("stats.Rows is zero")
	}
}

func TestParseDatapackToleratesBadCell(t *testing.T) {
	// фикстура с ячейкой "n/a" вместо числа не должна ронять разбор
	rows, stats, err := parseDatapack("testdata/datapack_bad_cell.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack must not fail on a bad cell: %v", err)
	}
	if stats.Skipped == 0 {
		t.Error("expected at least one skipped cell to be counted")
	}
	if len(rows) == 0 {
		t.Error("good rows must still be parsed")
	}
	// Битый F13 = Total rock moved / ANNUAL 2007 (значение 49474 в эталонном
	// файле). Дата у годовой точки — 2008-01-01 (31.12.2007 + 1 месяц), и она
	// совпадает с QUARTERLY 4Q'07, поэтому проверяем пару (Data, Date).
	const goodTotalRockMovedFY2007 = 49474
	for _, r := range rows {
		if r.Table == "CONSOLIDATED OPERATING RESULTS" && r.Name == "Total rock moved" &&
			r.Data == "ANNUAL" && r.Date.Equal(time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("bad cell survived: Total rock moved ANNUAL FY2007 parsed as %v", r.Value)
		}
	}
	// Битый датапак обязан дать ровно на одну точку меньше эталонного:
	// значит пропуск — это именно выпавшее значение, а не шум в разборе.
	good, goodStats, err := parseDatapack("testdata/polyus_datapack_fy2025_new.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack(good): %v", err)
	}
	if got := len(good) - len(rows); got != 1 {
		t.Errorf("bad cell should drop exactly one data point, dropped %d", got)
	}
	if got := stats.Skipped - goodStats.Skipped; got != 1 {
		t.Errorf("bad fixture should add exactly one skipped cell, added %d", got)
	}
	// Контроль в обе стороны: в эталонном файле точка 49474 есть, иначе
	// проверка выше ничего не доказывала бы.
	var refSeen bool
	for _, r := range good {
		if r.Table == "CONSOLIDATED OPERATING RESULTS" && r.Name == "Total rock moved" &&
			r.Data == "ANNUAL" && r.Date.Equal(time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC)) && r.Value == goodTotalRockMovedFY2007 {
			refSeen = true
			break
		}
	}
	if !refSeen {
		t.Errorf("reference fixture must contain Total rock moved ANNUAL FY2007 = %d", goodTotalRockMovedFY2007)
	}
}
