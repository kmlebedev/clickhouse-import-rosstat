package polyus

import (
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
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

// datapackFixtureWithLabel копирует эталонный датапак во временный файл и
// подменяет метку блока в B84 (в FY2025-файле там `VERNINSKOYE2`). Фикстура в
// репозитории не меняется: 1H25-датапак с `VERNINSKOYE**` — доказательство
// дефекта и тест-кейс, а не импортируемый в прод источник.
func datapackFixtureWithLabel(t *testing.T, label string) string {
	t.Helper()

	const src = "testdata/polyus_datapack_fy2025_new.xlsx"

	dst := filepath.Join(t.TempDir(), "datapack.xlsx")

	xlsx, err := excelize.OpenFile(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer func() { _ = xlsx.Close() }()

	// B84 — строка заголовка блока VERNINSKOYE2 (колонка B, строка 84).
	if err = xlsx.SetCellValue(datapackSheet, "B84", label); err != nil {
		t.Fatalf("set %s!B84 = %q: %v", datapackSheet, label, err)
	}

	if err = xlsx.SaveAs(dst); err != nil {
		t.Fatalf("save %s: %v", dst, err)
	}

	return dst
}

// datapackDuplicateKeys возвращает ключи (Table, Name, Data, Date), которые
// встретились больше одного раза.
func datapackDuplicateKeys(rows []datapackRow) map[datapackRowKey]int {
	counts := make(map[datapackRowKey]int, len(rows))
	for _, r := range rows {
		counts[datapackRowKey{Table: r.Table, Name: r.Name, Data: r.Data, Date: r.Date.Format("2006-01-02")}]++
	}

	dups := make(map[datapackRowKey]int)
	for key, n := range counts {
		if n > 1 {
			dups[key] = n
		}
	}

	return dups
}

// TestParseDatapackFootnoteAssetAlias — footnote-маркер в метке блока
// (`VERNINSKOYE**` из 1H25-датапака) снимается нормализацией, а alias
// возвращает каноническое `VERNINSKOYE2`; иначе строки остаются у предыдущего
// актива. На staging эта ошибка дала 208 строк TITIMUKHTA после 2020 со
// значениями Вернинского.
func TestParseDatapackFootnoteAssetAlias(t *testing.T) {
	path := datapackFixtureWithLabel(t, "VERNINSKOYE**")

	rows, _, err := parseDatapack(path)
	if err != nil {
		t.Fatalf("parseDatapack: %v", err)
	}

	const (
		wantTable = "VERNINSKOYE2"
		wantName  = "Ore processed"
		wantData  = "ANNUAL"
		wantValue = 3277
	)

	wantDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	found := false

	for _, r := range rows {
		if r.Table != wantTable || r.Name != wantName || r.Data != wantData || !r.Date.Equal(wantDate) {
			continue
		}

		found = true

		if diff := math.Abs(float64(r.Value) - wantValue); diff > 0.01 {
			t.Errorf("%s/%s/%s %s: значение %v, want %d (diff %v)",
				wantTable, wantName, wantData, wantDate.Format("2006-01-02"), r.Value, wantValue, diff)
		}
	}

	if !found {
		t.Fatalf("%s/%s/%s %s не найдена: метка блока не распознана",
			wantTable, wantName, wantData, wantDate.Format("2006-01-02"))
	}

	// Дефект staging: строки Вернинского уезжали в TITIMUKHTA и повторяли его
	// числа. В чистом датапаке у TITIMUKHTA после 2020 остаются только нули
	// полугодий (годовые ячейки — `-`, они пропускаются), поэтому ненулевое
	// значение здесь и есть утечка.
	after2020 := time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)

	for _, r := range rows {
		if r.Table != "TITIMUKHTA" || !r.Date.After(after2020) {
			continue
		}

		if r.Value != 0 {
			t.Errorf("TITIMUKHTA/%s/%s %s = %v: строки Вернинского утекли в TITIMUKHTA",
				r.Name, r.Data, r.Date.Format("2006-01-02"), r.Value)
		}
	}

	// Дублей ключа (Table, Name, Data, Date) в затронутых блоках быть не должно.
	// Во всём датапаке они есть только у KURANAKH (секции Mill и Heap-leach обе
	// содержат `Ore processed`) — это штатная структура, а не дефект меток.
	for key, n := range datapackDuplicateKeys(rows) {
		if key.Table == "TITIMUKHTA" || key.Table == wantTable {
			t.Errorf("duplicate key %s/%s/%s %s: %d строк", key.Table, key.Name, key.Data, key.Date, n)
		}
	}
}

// TestParseDatapackUnknownBlock — неизвестная all-caps метка после первого
// актива не должна молча относить строки к предыдущему блоку: разбор падает с
// номером строки и меткой.
func TestParseDatapackUnknownBlock(t *testing.T) {
	path := datapackFixtureWithLabel(t, "UNKNOWNASSET")

	_, _, err := parseDatapack(path)
	if err == nil {
		t.Fatal("parseDatapack must fail on an unknown all-caps block label")
	}

	for _, want := range []string{"UNKNOWNASSET", "line 84"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}
