package rosstat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// ppiVedBatchRecorder — подмена driver.Batch для пятиколоночной таблицы rosstat_ppi_ved
// (§11.6). Общий appendRecorder пакета рассчитан на три колонки (ряд, дата, значение) и
// здесь не подходит: таблица хранит ещё code и index_type.
type ppiVedBatchRecorder struct {
	rows []ppiVedPoint
}

var _ driver.Batch = (*ppiVedBatchRecorder)(nil)

func (r *ppiVedBatchRecorder) Append(v ...any) error {
	if len(v) != 5 {
		return fmt.Errorf("ppiVedBatchRecorder: Append с %d аргументами, ожидалось 5 (date, code, name, index_type, value)", len(v))
	}
	date, ok := v[0].(time.Time)
	if !ok {
		return fmt.Errorf("ppiVedBatchRecorder: дата %T, ожидалось time.Time", v[0])
	}
	code, ok := v[1].(string)
	if !ok {
		return fmt.Errorf("ppiVedBatchRecorder: code %T, ожидалась строка", v[1])
	}
	name, ok := v[2].(string)
	if !ok {
		return fmt.Errorf("ppiVedBatchRecorder: name %T, ожидалась строка", v[2])
	}
	indexType, ok := v[3].(string)
	if !ok {
		return fmt.Errorf("ppiVedBatchRecorder: index_type %T, ожидалась строка", v[3])
	}
	value, ok := v[4].(float64)
	if !ok {
		return fmt.Errorf("ppiVedBatchRecorder: value %T, ожидалось float64", v[4])
	}
	r.rows = append(r.rows, ppiVedPoint{code: code, name: name, indexType: indexType, date: date, value: value})
	return nil
}

func (r *ppiVedBatchRecorder) Abort() error { return nil }

func (r *ppiVedBatchRecorder) AppendStruct(any) error {
	return fmt.Errorf("ppiVedBatchRecorder: AppendStruct не поддерживается")
}

func (r *ppiVedBatchRecorder) Column(int) driver.BatchColumn { return nil }

func (r *ppiVedBatchRecorder) Columns() []column.Interface { return nil }

func (r *ppiVedBatchRecorder) Flush() error { return nil }

func (r *ppiVedBatchRecorder) Send() error { return nil }

func (r *ppiVedBatchRecorder) IsSent() bool { return false }

func (r *ppiVedBatchRecorder) Rows() int { return len(r.rows) }

func openPPIVedFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/ppi_ved.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

func parsePPIVedFixture(t *testing.T) []ppiVedPoint {
	t.Helper()
	points, err := parsePPIVed(openPPIVedFixture(t))
	if err != nil {
		t.Fatalf("parsePPIVed: %v", err)
	}
	return points
}

func ppiVedCounts(points []ppiVedPoint) map[string]int {
	counts := make(map[string]int, 4)
	for _, point := range points {
		counts[point.indexType]++
	}
	return counts
}

func ppiVedFind(points []ppiVedPoint, indexType string, date time.Time) (ppiVedPoint, bool) {
	for _, point := range points {
		if point.indexType == indexType && point.date.Equal(date) {
			return point, true
		}
	}
	return ppiVedPoint{}, false
}

func ppiVedRequireSpot(t *testing.T, points []ppiVedPoint, indexType string, date time.Time, want float64) {
	t.Helper()
	point, ok := ppiVedFind(points, indexType, date)
	if !ok {
		t.Fatalf("%s %s: точки нет", indexType, date.Format("2006-01-02"))
	}
	if point.value != want {
		t.Errorf("%s %s = %v, ожидалось %v", indexType, date.Format("2006-01-02"), point.value, want)
	}
}

// TestPPIVedFindLink проверяет селектор на реальной HTML-фикстуре страницы цен: на
// странице две ссылки, месячная и квартальная 2kv, и импортёр обязан взять месячную.
func TestPPIVedFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, ppiVedLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	want := "/storage/mediabank/Proizvoditeli_Ind_VED_08-2026.xlsx"
	if href != want {
		t.Errorf("FindLink() = %q, ожидалось %q (квартальная 2kv-ссылка не должна выбираться)", href, want)
	}
	if strings.Contains(href, "2kv-") {
		t.Errorf("FindLink() вернул квартальную выгрузку: %q", href)
	}

	// Реальная страница отдаёт месячную ссылку первой, поэтому один этот тест
	// прошёл бы и без :not. Ставим 2kv первой и проверяем фильтр отдельно.
	reordered := []byte(`<a href="/storage/mediabank/Proizvoditeli_Ind_VED_2kv-2026.xlsx">kv</a>` +
		`<a href="/storage/mediabank/Proizvoditeli_Ind_VED_08-2026.xlsx">month</a>`)
	href, err = util.FindLink(reordered, ppiVedLinkSelector)
	if err != nil {
		t.Fatalf("FindLink (2kv first): %v", err)
	}
	if href != want {
		t.Errorf("FindLink (2kv first) = %q, ожидалось %q: :not([href*=2kv-]) не работает", href, want)
	}
}

// TestPPIVedReadsFixture проверяет урезанный винтаж 08-2026: 1.1/2.1, годовые листы
// 3.1–3.4 (только РФ) и 8.1 (РФ + регионы). Число точек фикстурное: в ней нет годовых
// листов 4.x–7.x и 8.2–8.4, поэтому ytd/y/y/period кончаются 2021-12, а m/m — 2026-08.
func TestPPIVedReadsFixture(t *testing.T) {
	points := parsePPIVedFixture(t)
	if len(points) != 407 {
		t.Fatalf("точек %d, ожидалось 407", len(points))
	}
	counts := ppiVedCounts(points)
	wantCounts := map[string]int{
		ppiVedIndexMM:     344,
		ppiVedIndexYTD:    39,
		ppiVedIndexYY:     12,
		ppiVedIndexPeriod: 12,
	}
	for indexType, want := range wantCounts {
		if counts[indexType] != want {
			t.Errorf("%s: точек %d, ожидалось %d", indexType, counts[indexType], want)
		}
	}

	wantNames := map[string]string{
		ppiVedIndexMM:     "ИЦП по ВЭД, РФ, m/m",
		ppiVedIndexYTD:    "ИЦП по ВЭД, РФ, ytd",
		ppiVedIndexYY:     "ИЦП по ВЭД, РФ, y/y",
		ppiVedIndexPeriod: "ИЦП по ВЭД, РФ, period",
	}
	for _, point := range points {
		if point.code != ppiVedTotalCode {
			t.Fatalf("код точки %q, ожидался %q", point.code, ppiVedTotalCode)
		}
		if point.name != wantNames[point.indexType] {
			t.Errorf("%s: имя %q, ожидалось %q", point.indexType, point.name, wantNames[point.indexType])
		}
	}

	// Spot-значения: legacy-листы, приоритет 2.1 над 1.1 на 2013, приоритет годового
	// листа над 2.1 на 2021 и региональная строка РФ на 2026.
	ppiVedRequireSpot(t, points, ppiVedIndexMM, time.Date(1998, 1, 31, 0, 0, 0, 0, time.UTC), 100.8)
	ppiVedRequireSpot(t, points, ppiVedIndexMM, time.Date(2013, 1, 31, 0, 0, 0, 0, time.UTC), 99.9)
	ppiVedRequireSpot(t, points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), 103.36)
	ppiVedRequireSpot(t, points, ppiVedIndexMM, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), 98.45)
	ppiVedRequireSpot(t, points, ppiVedIndexMM, time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC), 100.86)
	ppiVedRequireSpot(t, points, ppiVedIndexYTD, time.Date(1998, 12, 31, 0, 0, 0, 0, time.UTC), 119.4)
	ppiVedRequireSpot(t, points, ppiVedIndexYTD, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), 103.36)
	ppiVedRequireSpot(t, points, ppiVedIndexYY, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), 106.69)
	ppiVedRequireSpot(t, points, ppiVedIndexPeriod, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), 106.69)

	// Приоритет источников: 1.1 на 2013-01 даёт 99.6, 2.1 — 99.9; 2.1 на 2021-01
	// даёт 103.4, годовой лист 3.1 — 103.36.
	if point, ok := ppiVedFind(points, ppiVedIndexMM, time.Date(2013, 1, 31, 0, 0, 0, 0, time.UTC)); !ok || point.value != 99.9 {
		t.Errorf("2013-01 m/m: %+v, ожидалось значение 99.9 из листа 2.1 (не 99.6 из 1.1)", point)
	}
	if point, ok := ppiVedFind(points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC)); !ok || point.value != 103.36 {
		t.Errorf("2021-01 m/m: %+v, ожидалось значение 103.36 из листа 3.1 (не 103.4 из 2.1)", point)
	}
}

// TestPPIVedCatalogContract — двусторонний контракт каталога с разбором: имена рядов
// объявлены кодом, поэтому каждый разобранный ряд описан и каждый описанный разобран.
func TestPPIVedCatalogContract(t *testing.T) {
	points := parsePPIVedFixture(t)
	parsed := make(map[string]bool)
	for _, point := range points {
		parsed[point.name] = true
	}
	described := make(map[string]bool, len(ppiVedSeriesMeta))
	for _, meta := range ppiVedSeriesMeta {
		described[meta.Series] = true
	}
	if len(parsed) != len(described) {
		t.Fatalf("разобрано рядов %d, описано %d: %v / %v", len(parsed), len(described), parsed, described)
	}
	for series := range parsed {
		if !described[series] {
			t.Errorf("ряд %q разобран, но не описан в ppiVedSeriesMeta", series)
		}
	}
	for series := range described {
		if !parsed[series] {
			t.Errorf("ряд %q описан, но не разобран", series)
		}
	}
}

// TestPPIVedImportAppends проверяет запись точек: пять аргументов в порядке колонок
// DDL, значение float64.
func TestPPIVedImportAppends(t *testing.T) {
	rec := &ppiVedBatchRecorder{}
	if err := ppiVedImport(openPPIVedFixture(t), rec); err != nil {
		t.Fatalf("ppiVedImport: %v", err)
	}
	if len(rec.rows) != 407 {
		t.Fatalf("записано строк %d, ожидалось 407", len(rec.rows))
	}
	for _, row := range rec.rows {
		if row.code != ppiVedTotalCode || row.name == "" || row.indexType == "" || row.date.IsZero() {
			t.Fatalf("неполная строка: %+v", row)
		}
	}
}

func TestPPIVedValue(t *testing.T) {
	cases := []struct {
		raw   string
		value float64
		ok    bool
	}{
		{"100.8", 100.8, true},
		{"98,121)", 98.12, true},
		{"103,361)2)", 103.36, true},
		{"", 0, false},
		{"…", 0, false},
		{"...", 0, false},
		{"-", 0, false},
		{"—", 0, false},
		{"х", 0, false},
	}
	for _, tc := range cases {
		value, ok, err := ppiVedValue(tc.raw)
		if err != nil {
			t.Errorf("ppiVedValue(%q): %v", tc.raw, err)
			continue
		}
		if ok != tc.ok || (ok && value != tc.value) {
			t.Errorf("ppiVedValue(%q) = (%v, %v), ожидалось (%v, %v)", tc.raw, value, ok, tc.value, tc.ok)
		}
	}
	if _, _, err := ppiVedValue("не число"); err == nil {
		t.Error("ppiVedValue(\"не число\") вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedDdl(t *testing.T) {
	for _, fragment := range []string{
		"ReplacingMergeTree",
		"date Date",
		"code LowCardinality(String)",
		"name LowCardinality(String)",
		"index_type LowCardinality(String)",
		"value Float64",
		"ORDER BY (date, code, index_type)",
	} {
		if !strings.Contains(ppiVedDdl, fragment) {
			t.Errorf("DDL не содержит %q:\n%s", fragment, ppiVedDdl)
		}
	}
}

func TestPPIVedMissingSheetFails(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	if err := xlsx.DeleteSheet("1.1"); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePPIVed(xlsx); err == nil {
		t.Fatal("parsePPIVed без листа 1.1 вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedMissingCodeFails(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	if err := xlsx.SetCellValue("3.1", "B10", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePPIVed(xlsx); err == nil {
		t.Fatal("parsePPIVed без кода 1323500.029.31 вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedMissingRussiaRowFails(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	if err := xlsx.SetCellValue("8.1", "A271", "не Российская Федерация"); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePPIVed(xlsx); err == nil {
		t.Fatal("parsePPIVed без строки «Российская Федерация» вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedNonNumericValueFails(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	if err := xlsx.SetCellValue("3.1", "C10", "не число"); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePPIVed(xlsx); err == nil {
		t.Fatal("parsePPIVed с нечисловым значением вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedEmptyWorkbookFails(t *testing.T) {
	xlsx := excelize.NewFile()
	t.Cleanup(func() { _ = xlsx.Close() })
	if _, err := parsePPIVed(xlsx); err == nil {
		t.Fatal("parsePPIVed на пустой книге вернул nil, ожидалась ошибка")
	}
}

func TestPPIVedBlankLeadingRowDoesNotPanic(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	if err := xlsx.InsertRows("1.1", 1, 1); err != nil {
		t.Fatal(err)
	}
	points, err := parsePPIVed(xlsx)
	if err != nil {
		t.Fatalf("parsePPIVed с пустой строкой над шапкой: %v", err)
	}
	if len(points) != 407 {
		t.Fatalf("точек %d, ожидалось 407: пустая строка сдвинула шапку", len(points))
	}
}

func TestPPIVedMarkerInCodeRowKeepsRussiaRow(t *testing.T) {
	xlsx := openPPIVedFixture(t)
	for col := 3; col <= 10; col++ {
		cell, err := excelize.CoordinatesToCellName(col, 270)
		if err != nil {
			t.Fatal(err)
		}
		if err := xlsx.SetCellValue("8.1", cell, "…"); err != nil {
			t.Fatal(err)
		}
	}
	points, err := parsePPIVed(xlsx)
	if err != nil {
		t.Fatalf("parsePPIVed с маркерами в строке кода 8.1: %v", err)
	}
	if len(points) != 407 {
		t.Fatalf("точек %d, ожидалось 407: строка РФ не прочитана из-за маркеров", len(points))
	}
	if _, ok := ppiVedFind(points, ppiVedIndexMM, time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)); !ok {
		t.Fatal("2026-08 m/m потерян: маркеры в строке кода подменили строку РФ")
	}
}

func TestPPIVedRestoreRefused(t *testing.T) {
	t.Setenv(util.EnvArchiveAction, util.ArchiveActionRestore)
	importer := &PPIVedStat{}
	if _, err := importer.Import(context.Background(), nil); err == nil {
		t.Fatal("Import в режиме restore вернул nil, ожидалась ошибка")
	} else if !strings.Contains(err.Error(), "восстановления") {
		t.Errorf("ошибка restore не называет режим: %v", err)
	}
}

func TestPPIVedAddRejectsConflict(t *testing.T) {
	points := make(map[ppiVedKey]float64)
	key := ppiVedKey{date: time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC), indexType: ppiVedIndexMM}
	if err := ppiVedAdd(points, key, 100, "test"); err != nil {
		t.Fatal(err)
	}
	if err := ppiVedAdd(points, key, 101, "test"); err == nil {
		t.Fatal("ppiVedAdd принял дубль с другим значением, ожидалась ошибка")
	}
}

func TestPPIVedCoverageRejectsGap(t *testing.T) {
	points := []ppiVedPoint{
		{indexType: ppiVedIndexMM, date: time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC)},
		{indexType: ppiVedIndexMM, date: time.Date(2020, 3, 31, 0, 0, 0, 0, time.UTC)},
		{indexType: ppiVedIndexYTD, date: time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC)},
		{indexType: ppiVedIndexYY, date: time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC)},
		{indexType: ppiVedIndexPeriod, date: time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC)},
	}
	if err := ppiVedValidateCoverage(points); err == nil {
		t.Fatal("ppiVedValidateCoverage принял пропуск 2020-02, ожидалась ошибка")
	}
}
