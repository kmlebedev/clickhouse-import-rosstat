package minfin

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func debtFind(t *testing.T, rows []fedbudMesyatsRow, name string, date time.Time) float64 {
	t.Helper()
	row := fedbudMesyatsFind(t, rows, name, date)
	return row.value
}

func closeTo(got, want, tol float64) bool {
	return math.Abs(got-want) <= tol
}

func TestParseInternalDebtXls(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "Obem_gosdolga_s_garantiyami_god_polnostu_na_01_10_2026.xls"))
	if err != nil {
		t.Fatalf("чтение фикстуры внутреннего долга: %v", err)
	}
	rec := &fedbudMesyatsRecorder{}
	if err := parseInternalDebtXls(body, rec); err != nil {
		t.Fatalf("parseInternalDebtXls: %v", err)
	}
	if len(rec.rows) < 400 {
		t.Fatalf("точек %d, ожидалось не меньше 400 (18 лет × ~12 месяцев × 2 ряда)", len(rec.rows))
	}
	if got := debtFind(t, rec.rows, debtStockInternalTotal, fedbudMesyatsDay(2009, time.January, 1)); !closeTo(got, 1499.824, 0.001) {
		t.Errorf("внутренний долг 2009-01-01 = %v, ожидалось 1499.824", got)
	}
	if got := debtFind(t, rec.rows, debtStockInternalGuarantees, fedbudMesyatsDay(2009, time.January, 1)); !closeTo(got, 72.487, 0.001) {
		t.Errorf("гарантии 2009-01-01 = %v, ожидалось 72.487", got)
	}
	if got := debtFind(t, rec.rows, debtStockInternalTotal, fedbudMesyatsDay(2026, time.October, 1)); !closeTo(got, 36682.056, 0.001) {
		t.Errorf("внутренний долг 2026-10-01 = %v, ожидалось 36682.056", got)
	}
	if got := debtFind(t, rec.rows, debtStockInternalGuarantees, fedbudMesyatsDay(2026, time.October, 1)); !closeTo(got, 505.739, 0.001) {
		t.Errorf("гарантии 2026-10-01 = %v, ожидалось 505.739", got)
	}
	// 2018-01-01: в источнике пустая ячейка «всего», но гарантии есть. Парсер
	// обязан сохранить гарантию, не теряя её вместе с пустым итогом.
	if got := debtFind(t, rec.rows, debtStockInternalGuarantees, fedbudMesyatsDay(2018, time.January, 1)); !closeTo(got, 1442.515, 0.001) {
		t.Errorf("гарантии 2018-01-01 = %v, ожидалось 1442.515", got)
	}
}

func TestParseExternalDebtXlsx(t *testing.T) {
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", "GVD_20261001.xlsx"))
	if err != nil {
		t.Fatalf("открытие фикстуры внешнего долга: %v", err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	rec := &fedbudMesyatsRecorder{}
	if err := parseExternalDebtXlsx(xlsx, rec); err != nil {
		t.Fatalf("parseExternalDebtXlsx: %v", err)
	}
	if len(rec.rows) < 180 {
		t.Fatalf("точек %d, ожидалось не меньше 180 (2011+ × 2 ряда)", len(rec.rows))
	}
	if got := debtFind(t, rec.rows, debtStockExternalTotal, fedbudMesyatsDay(2026, time.January, 1)); !closeTo(got, 57270.6, 0.01) {
		t.Errorf("внешний долг 2026-01-01 = %v, ожидалось 57270.6", got)
	}
	if got := debtFind(t, rec.rows, debtStockExternalGuarantees, fedbudMesyatsDay(2026, time.January, 1)); !closeTo(got, 21912.8, 0.01) {
		t.Errorf("гарантии 2026-01-01 = %v, ожидалось 21912.8", got)
	}
}

func TestParseOfzStructure2022Xls(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "Struktura_gos_vnutr_dolga_ezhemesyachnye_na_01.01.2023.xls"))
	if err != nil {
		t.Fatalf("чтение структуры 2022: %v", err)
	}
	rec := &fedbudMesyatsRecorder{}
	if err := parseOfzStructure2022Xls(body, rec); err != nil {
		t.Fatalf("parseOfzStructure2022Xls: %v", err)
	}
	if len(rec.rows) < 60 {
		t.Fatalf("точек %d, ожидалось не меньше 60 (13 месяцев × 5 рядов)", len(rec.rows))
	}
	if got := debtFind(t, rec.rows, debtStockOfzTotal, fedbudMesyatsDay(2022, time.January, 1)); !closeTo(got, 15533.451, 0.001) {
		t.Errorf("ОФЗ 2022-01-01 = %v, ожидалось 15533.451", got)
	}
	if got := debtFind(t, rec.rows, debtStockOfzPk, fedbudMesyatsDay(2022, time.January, 1)); !closeTo(got, 4709.268, 0.001) {
		t.Errorf("ОФЗ-ПК 2022-01-01 = %v, ожидалось 4709.268", got)
	}
	if got := debtFind(t, rec.rows, debtStockOfzN, fedbudMesyatsDay(2022, time.January, 1)); !closeTo(got, 39.161, 0.001) {
		t.Errorf("ОФЗ-н 2022-01-01 = %v, ожидалось 39.161", got)
	}
	if got := debtFind(t, rec.rows, debtStockOfzTotal, fedbudMesyatsDay(2023, time.January, 1)); !closeTo(got, 17904.89, 0.001) {
		t.Errorf("ОФЗ 2023-01-01 = %v, ожидалось 17904.89", got)
	}
}

func TestParseOfzStructureMonthXlsx(t *testing.T) {
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", "debtDynamicsMonth_01.10.2026.xlsx"))
	if err != nil {
		t.Fatalf("открытие месячной структуры: %v", err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	rec := &fedbudMesyatsRecorder{}
	if err := parseOfzStructureMonthXlsx(xlsx, rec); err != nil {
		t.Fatalf("parseOfzStructureMonthXlsx: %v", err)
	}
	date := fedbudMesyatsDay(2026, time.October, 1)
	if got := debtFind(t, rec.rows, debtStockOfzTotal, date); !closeTo(got, 33636.318, 0.001) {
		t.Errorf("ОФЗ 2026-10-01 = %v, ожидалось 33636.318 (ГСО не входит)", got)
	}
	want := map[string]float64{
		debtStockOfzPk: 11846.969,
		debtStockOfzPd: 20295.098,
		debtStockOfzAd: 34.22,
		debtStockOfzIn: 1460.031,
	}
	for name, value := range want {
		if got := debtFind(t, rec.rows, name, date); !closeTo(got, value, 0.001) {
			t.Errorf("%s 2026-10-01 = %v, ожидалось %v", name, got, value)
		}
	}
}

func TestDebtStockLinkFixtures(t *testing.T) {
	internalPage, err := os.ReadFile(filepath.Join("testdata", "debt_internal_doc.html"))
	if err != nil {
		t.Fatalf("чтение debt_internal_doc.html: %v", err)
	}
	internalHref, err := util.FindLink(internalPage, debtInternalSelector)
	if err != nil {
		t.Fatalf("FindLink внутреннего .xls: %v", err)
	}
	if !strings.Contains(internalHref, "Obem_gosdolga_s_garantiyami_god_polnostu_na_01_10_2026.xls") {
		t.Errorf("внутренний .xls: %q", internalHref)
	}

	externalPage, err := os.ReadFile(filepath.Join("testdata", "debt_external_doc.html"))
	if err != nil {
		t.Fatalf("чтение debt_external_doc.html: %v", err)
	}
	externalHref, err := util.FindLink(externalPage, debtExternalSelector)
	if err != nil {
		t.Fatalf("FindLink внешнего .xlsx: %v", err)
	}
	if !strings.Contains(externalHref, "GVD_20261001.xlsx") {
		t.Errorf("внешний .xlsx: %q", externalHref)
	}
}

func TestParseOfzStructureMonthMissingHeader(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "за месяца"); err != nil {
		t.Fatal(err)
	}
	if err := f.SetCellValue("за месяца", "A1", "не та шапка"); err != nil {
		t.Fatal(err)
	}
	if err := parseOfzStructureMonthXlsx(f, &fedbudMesyatsRecorder{}); err == nil {
		t.Fatal("ожидалась ошибка: шапка не найдена")
	}
}
