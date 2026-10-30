package rosstat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func openPPIProductFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/ppi_product.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

func parsePPIProductFixture(t *testing.T) []ppiProductPoint {
	t.Helper()
	points, err := parsePPIProductWithCoverage(openPPIProductFixture(t), false)
	if err != nil {
		t.Fatalf("parsePPIProductWithCoverage: %v", err)
	}
	return points
}

func ppiProductFind(points []ppiProductPoint, indexType string, date time.Time, okpd2 string) (ppiProductPoint, bool) {
	for _, point := range points {
		if point.indexType == indexType && point.date.Equal(date) && point.okpd2 == okpd2 {
			return point, true
		}
	}
	return ppiProductPoint{}, false
}

func ppiProductRequireSpot(t *testing.T, points []ppiProductPoint, indexType string, date time.Time, okpd2 string, want float64) {
	t.Helper()
	point, ok := ppiProductFind(points, indexType, date, okpd2)
	if !ok {
		t.Fatalf("%s %s %s: точки нет", indexType, date.Format("2006-01-02"), okpd2)
	}
	if point.value != want {
		t.Errorf("%s %s %s = %v, ожидалось %v", indexType, date.Format("2006-01-02"), okpd2, point.value, want)
	}
}

func TestPPIProductFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, ppiProductLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	want := "/storage/mediabank/Proizvoditeli_Ind_tov_08-2026.xlsx"
	if href != want {
		t.Errorf("FindLink() = %q, ожидалось %q", href, want)
	}
}

func TestPPIProductReadsFixture(t *testing.T) {
	points := parsePPIProductFixture(t)
	if len(points) != 68360 {
		t.Fatalf("точек %d, ожидалось 68 360", len(points))
	}
	counts := make(map[string]int, 4)
	codes := make(map[string]bool)
	minDate, maxDate := points[0].date, points[0].date
	for _, point := range points {
		counts[point.indexType]++
		codes[point.okpd2] = true
		if point.date.Before(minDate) {
			minDate = point.date
		}
		if point.date.After(maxDate) {
			maxDate = point.date
		}
	}
	wantCounts := map[string]int{
		ppiVedIndexMM:     24728,
		ppiVedIndexYTD:    14544,
		ppiVedIndexYY:     14544,
		ppiVedIndexPeriod: 14544,
	}
	for indexType, want := range wantCounts {
		if counts[indexType] != want {
			t.Errorf("%s: точек %d, ожидалось %d", indexType, counts[indexType], want)
		}
	}
	if len(codes) != 1323 {
		t.Errorf("кодов %d, ожидалось 1 323", len(codes))
	}
	if want := time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC); !minDate.Equal(want) {
		t.Errorf("min(date) = %s, ожидалось %s", minDate.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if want := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC); !maxDate.Equal(want) {
		t.Errorf("max(date) = %s, ожидалось %s", maxDate.Format("2006-01-02"), want.Format("2006-01-02"))
	}

	// Оба формата листа: 2021 — значения в строке кода, 2026 — в строке РФ.
	ppiProductRequireSpot(t, points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 101.74)
	ppiProductRequireSpot(t, points, ppiVedIndexMM, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 100.95)
	ppiProductRequireSpot(t, points, ppiVedIndexMM, time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 100.11)
	ppiProductRequireSpot(t, points, ppiVedIndexYTD, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 101.74)
	ppiProductRequireSpot(t, points, ppiVedIndexYY, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 108.75)
	ppiProductRequireSpot(t, points, ppiVedIndexPeriod, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11", 108.75)

	// Коды, которые есть только в 2021 или только в 2026.
	if _, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "03.12.12"); !ok {
		t.Error("03.12.12: нет точки 2021-01, хотя код есть в листе 2021")
	}
	if _, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), "03.12.12"); ok {
		t.Error("03.12.12: найдена точка 2026-01, хотя кода нет в листе 2026")
	}
	if _, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), "03.11.30"); !ok {
		t.Error("03.11.30: нет точки 2026-01, хотя код есть в листе 2026")
	}
	if _, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "03.11.30"); ok {
		t.Error("03.11.30: найдена точка 2021-01, хотя кода нет в листе 2021")
	}

	// Переименование: name хранится по точке, а не одной строкой на код.
	oldName, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "35.13.10.021.АГ")
	if !ok || !strings.Contains(oldName.name, "тарифам") {
		t.Errorf("35.13.10.021.АГ 2021-01: %+v, ожидалось название со «тарифам»", oldName)
	}
	newName, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), "35.13.10.021.АГ")
	if !ok || !strings.Contains(newName.name, "ценам") {
		t.Errorf("35.13.10.021.АГ 2026-01: %+v, ожидалось название со «ценам»", newName)
	}
}

func TestPPIProductEmptyCellSkipped(t *testing.T) {
	xlsx := openPPIProductFixture(t)
	if err := xlsx.SetCellValue("4", "C5", ""); err != nil {
		t.Fatal(err)
	}
	points, err := parsePPIProductWithCoverage(xlsx, false)
	if err != nil {
		t.Fatalf("parsePPIProductWithCoverage: %v", err)
	}
	if _, ok := ppiProductFind(points, ppiVedIndexMM, time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), "02.20.11"); ok {
		t.Error("очищенная ячейка стала точкой: пустая ячейка — пропуск, а не ноль")
	}
}

func TestPPIProductIndexType(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		{"на конец периода, в % к предыдущему месяцу", ppiVedIndexMM},
		{"на конец периода, в % к декабрю предыдущего года", ppiVedIndexYTD},
		{"на конец периода, в % к соответствующему месяцу предыдущего года", ppiVedIndexYY},
		{"за период с начала года, в % к соответствующему периоду предыдущего года", ppiVedIndexPeriod},
	}
	for _, tc := range cases {
		got, err := ppiProductIndexType([][]string{{"", ""}, {"", tc.text}})
		if err != nil {
			t.Errorf("ppiProductIndexType(%q): %v", tc.text, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ppiProductIndexType(%q) = %q, ожидалось %q", tc.text, got, tc.want)
		}
	}
	if _, err := ppiProductIndexType([][]string{{"неизвестная подпись"}}); err == nil {
		t.Error("ppiProductIndexType для неизвестной подписи вернул nil, ожидалась ошибка")
	}
}

func TestPPIProductImportAppends(t *testing.T) {
	rec := &ppiRecordingBatch{}
	if err := appendPPIProduct(parsePPIProductFixture(t), rec); err != nil {
		t.Fatalf("appendPPIProduct: %v", err)
	}
	if len(rec.rows) != 68360 {
		t.Fatalf("записано строк %d, ожидалось 68 360", len(rec.rows))
	}
	for _, row := range rec.rows {
		if len(row) != 5 {
			t.Fatalf("Append с %d аргументами, ожидалось 5", len(row))
		}
		if _, ok := row[0].(time.Time); !ok {
			t.Fatalf("date %T, ожидалось time.Time", row[0])
		}
		for _, i := range []int{1, 2, 3} {
			if _, ok := row[i].(string); !ok {
				t.Fatalf("колонка %d: %T, ожидалась строка", i, row[i])
			}
		}
		if _, ok := row[4].(float64); !ok {
			t.Fatalf("value %T, ожидалось float64", row[4])
		}
	}
}

func TestPPIProductDdl(t *testing.T) {
	for _, fragment := range []string{
		"ReplacingMergeTree",
		"date Date",
		"okpd2 LowCardinality(String)",
		"name LowCardinality(String)",
		"index_type LowCardinality(String)",
		"value Float64",
		"ORDER BY (date, okpd2, index_type)",
	} {
		if !strings.Contains(ppiProductDdl, fragment) {
			t.Errorf("DDL не содержит %q:\n%s", fragment, ppiProductDdl)
		}
	}
}

func TestPPIProductRestoreRefused(t *testing.T) {
	t.Setenv(util.EnvArchiveAction, util.ArchiveActionRestore)
	importer := &PPIProductStat{}
	if _, err := importer.Import(context.Background(), nil); err == nil {
		t.Fatal("Import в режиме restore вернул nil, ожидалась ошибка")
	} else if !strings.Contains(err.Error(), "восстановления") {
		t.Errorf("ошибка restore не называет режим: %v", err)
	}
}

func TestPPIProductAddRejectsConflict(t *testing.T) {
	points := make(map[ppiProductKey]float64)
	names := make(map[ppiProductKey]string)
	row := []string{"товар", "01.01.01", "100"}
	monthCols := map[int]time.Month{2: time.January}
	if err := addPPIProductRow(points, names, row, 1, "4", 2021, "01.01.01", "товар", ppiVedIndexMM, monthCols); err != nil {
		t.Fatal(err)
	}
	row[2] = "101"
	if err := addPPIProductRow(points, names, row, 2, "4", 2021, "01.01.01", "товар", ppiVedIndexMM, monthCols); err == nil {
		t.Fatal("addPPIProductRow принял дубль с другим значением, ожидалась ошибка")
	}
}

func TestPPIProductRequiresRussiaRow(t *testing.T) {
	rows := [][]string{
		{"Индексы цен производителей по товарам и товарным группам"},
		{"по Российской Федерации в 2024 г."},
		{"на конец периода, в % к предыдущему месяцу"},
		{"", "Код товара на основе ОКПД2", "январь"},
	}
	for i := 0; i < ppiProductMinCodes; i++ {
		rows = append(rows, []string{fmt.Sprintf("товар %d", i), fmt.Sprintf("%05d", i), "100"})
	}
	_, err := parsePPIProductSheet(rows, rows, "16", 3, 1, map[int]time.Month{2: time.January})
	if err == nil || !strings.Contains(err.Error(), "Российская Федерация") {
		t.Fatalf("parsePPIProductSheet без строки РФ: %v, ожидалась ошибка про строку «Российская Федерация»", err)
	}
}

func TestPPIProductCoverageRejectsGap(t *testing.T) {
	points := make([]ppiProductPoint, 0, 7000)
	january := time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC)
	february := time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC)
	indexTypes := []string{ppiVedIndexMM, ppiVedIndexYTD, ppiVedIndexYY, ppiVedIndexPeriod}
	for i := 0; i < ppiProductMinCodes; i++ {
		code := fmt.Sprintf("%05d", i)
		for _, indexType := range indexTypes {
			points = append(points, ppiProductPoint{okpd2: code, indexType: indexType, date: january, value: 100})
			if indexType != ppiVedIndexPeriod {
				points = append(points, ppiProductPoint{okpd2: code, indexType: indexType, date: february, value: 100})
			}
		}
	}
	if err := ppiProductValidateCoverage(points); err == nil {
		t.Fatal("ppiProductValidateCoverage принял пропуск февраля у period, ожидалась ошибка")
	}
}

func TestPPIProductCatalog(t *testing.T) {
	points := parsePPIProductFixture(t)
	seen := make(map[string]bool)
	rows := make([]ppiProductCatalogRow, 0, 5000)
	for _, point := range points {
		key := point.okpd2 + "\x00" + point.indexType
		if seen[key] {
			continue
		}
		seen[key] = true
		rows = append(rows, ppiProductCatalogRow{okpd2: point.okpd2, indexType: point.indexType, name: point.name})
	}
	metas := ppiProductSeriesMeta(rows)
	if len(metas) != 4959 {
		t.Fatalf("серий каталога %d, ожидалось 4 959", len(metas))
	}
	series := make(map[string]bool, len(metas))
	for _, meta := range metas {
		if !strings.HasPrefix(meta.Series, "okpd2:") {
			t.Errorf("series %q без префикса okpd2:", meta.Series)
		}
		if series[meta.Series] {
			t.Errorf("series %q встречается дважды", meta.Series)
		}
		series[meta.Series] = true
		if meta.Source != rosstatSource || meta.Frequency != "M" || meta.Unit == "" || meta.Title == "" || meta.Description == "" {
			t.Errorf("неполная запись каталога: %+v", meta)
		}
	}
}

func TestPPIProductHeaderAnnualAndContent(t *testing.T) {
	annualRows := [][]string{
		{""},
		{"", "Код товара на основе ОКПД2", "2017", "2018", "2019", "2020", "2021"},
	}
	_, _, monthCols, annual, found, err := ppiProductHeader(annualRows)
	if err != nil || !found || !annual {
		t.Fatalf("ppiProductHeader(annual) = found=%v annual=%v err=%v, ожидался годовой лист", found, annual, err)
	}
	if monthCols != nil {
		t.Errorf("годовой лист вернул месячные колонки: %v", monthCols)
	}

	_, _, _, _, found, err = ppiProductHeader([][]string{{"Содержание"}, {"1. Описание"}})
	if err != nil || found {
		t.Errorf("ppiProductHeader(Содержание) = found=%v err=%v, ожидался пропуск", found, err)
	}
}

func TestPPIProductOrphanRussiaRowFails(t *testing.T) {
	rows := [][]string{
		{"Индексы цен производителей по товарам и товарным группам"},
		{"по Российской Федерации в 2021 г."},
		{"на конец периода, в % к предыдущему месяцу"},
		{"", "Код товара на основе ОКПД2", "январь"},
		{"Российская Федерация", "", "100"},
	}
	if _, err := parsePPIProductSheet(rows, rows, "4", 3, 1, map[int]time.Month{2: time.January}); err == nil || !strings.Contains(err.Error(), "без кода") {
		t.Fatalf("parsePPIProductSheet с сиротской строкой РФ: %v, ожидалась ошибка про отсутствие кода", err)
	}
}

func TestPPIProductEmptyWorkbookFails(t *testing.T) {
	xlsx := excelize.NewFile()
	t.Cleanup(func() { _ = xlsx.Close() })
	if _, err := parsePPIProduct(xlsx); err == nil {
		t.Fatal("parsePPIProduct на пустой книге вернул nil, ожидалась ошибка")
	}
}
