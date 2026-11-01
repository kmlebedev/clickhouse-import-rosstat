package polyus

import (
	"math"
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

// TestParseDatapackDateConvention — дата точки датапака — первое число месяца,
// следующего за концом периода; у годового наблюдения это 1 января СЛЕДУЮЩЕГО
// года (FY2024 → 2025-01-01), а не 1 декабря отчётного.
//
// Конвенция проверяется на всех трёх типах периода, потому что дефект в ней не
// виден в логе: DCF читает год точной датой (dcf/mine_plans_import.go), и
// «1 декабря» молча подменило бы факт FY2025 (OLIMPIADA 926.5) фактом FY2024
// (1265.7) — именно это и произошло на staging. Квартальная и полугодовая ветки
// проверяются вместе с годовой, чтобы правка одной не разошлась с остальными.
func TestParseDatapackDateConvention(t *testing.T) {
	const wantTable = "OLIMPIADA"

	rows, _, err := parseDatapack("testdata/polyus_datapack_fy2025_new.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack: %v", err)
	}

	cases := []struct {
		name   string
		period string // колонка data
		date   time.Time
		value  float32
	}{
		// Подпись «2024» — FY2024, точка датирована 1 января 2025.
		{"Total refined gold output", "ANNUAL", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), 1265.7},
		// 4Q'07 — первое число месяца после конца квартала.
		{"Total rock moved", "QUARTERLY", time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC), 8438.8},
		// 2H'24 — первое число месяца после конца полугодия.
		{"Total rock moved", "SEMI-ANNUAL", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), 40830},
	}

	for _, tc := range cases {
		found := false
		for _, r := range rows {
			if r.Table != wantTable || r.Name != tc.name || r.Data != tc.period || !r.Date.Equal(tc.date) {
				continue
			}
			found = true
			// Допуск шире float32-погрешности: parseDatapack читает
			// ОТФОРМАТИРОВАННУЮ строку ячейки (excelize.GetRows), а квартальные
			// Total rock moved отформатированы в датапаке как `#,##0`. Поэтому
			// сырое 4Q'07 = 8438.8 приходит как 8439 (сдвиг 0.2), и допуск в
			// половину отображаемой единицы покрывает такую оцифровку, не мешая
			// поймать подмену года, ради которой написан тест.
			const valueTolerance = 0.5
			if diff := math.Abs(float64(r.Value) - float64(tc.value)); diff > valueTolerance {
				t.Errorf("%s/%s/%s %s: значение %v, want %v (diff %v)",
					wantTable, tc.name, tc.period, tc.date.Format("2006-01-02"), r.Value, tc.value, diff)
			}
		}
		if !found {
			t.Errorf("%s/%s/%s с датой %s не найдена: дата не по конвенции "+
				"«первое число месяца, следующего за концом периода»",
				wantTable, tc.name, tc.period, tc.date.Format("2006-01-02"))
		}
	}
}
