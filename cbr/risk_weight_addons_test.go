package cbr

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestRiskWeightAddonsPage — landing ЦБ: 33 xlsx-матрицы, сортировка по дате
// начала действия документа (старые первыми), действующие файлы на месте.
func TestRiskWeightAddonsPage(t *testing.T) {
	body, err := os.ReadFile("testdata/risk_weight_addons_page.html")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	files, err := cbrRiskWeightAddonsParsePage(body)
	if err != nil {
		t.Fatalf("parse page: %v", err)
	}
	if len(files) != 33 {
		t.Fatalf("файлов %d, want 33", len(files))
	}
	if files[0].Name != "2018.10.01_2019.09.30.xlsx" {
		t.Fatalf("первый файл %q, want 2018.10.01_2019.09.30.xlsx", files[0].Name)
	}
	if files[len(files)-1].Name != "table2_20261015.xlsx" {
		t.Fatalf("последний файл %q, want table2_20261015.xlsx", files[len(files)-1].Name)
	}
	if !files[len(files)-1].DocumentFrom.Equal(time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("дата последнего файла %s", files[len(files)-1].DocumentFrom)
	}
	foundTable1 := false
	for _, f := range files {
		if f.Name == "table1_20260904.xlsx" {
			foundTable1 = true
			if f.Caption != "Таблица 1 с 04.09.2026" {
				t.Fatalf("подпись table1: %q", f.Caption)
			}
		}
	}
	if !foundTable1 {
		t.Fatal("на странице нет table1_20260904.xlsx")
	}
}

// TestRiskWeightAddonsParse — реальные матрицы ЦБ: розничная, корпоративная и
// старая общая. Проверяются период, вид матрицы, число и «н/п».
func TestRiskWeightAddonsParse(t *testing.T) {
	t1 := parseRiskFile(t, "testdata/risk_weight_addons_table1_20260904.xlsx",
		riskAddonFile{Name: "table1_20260904.xlsx", Caption: "Таблица 1 с 04.09.2026"})
	assertRiskPoint(t, t1, "retail", 2025, 9, 1, 2100, 1, 1, "2002.i", "1000.i", 1.3, 1)
	assertRiskPoint(t, t1, "retail", 2025, 9, 1, 2100, 1, 1, "2002.i", "1008.i", 0, 0)
	assertRiskPoint(t, t1, "retail", 2025, 9, 1, 2100, 1, 1, "2002.i", "1001.i", 0, 1)

	t2 := parseRiskFile(t, "testdata/risk_weight_addons_table2_20261015.xlsx",
		riskAddonFile{Name: "table2_20261015.xlsx", Caption: "Таблица 2 с 15.10.2026"})
	assertRiskPoint(t, t2, "corporate", 2026, 10, 15, 2100, 1, 1, "8002.i", "8002.i", 1, 1)

	legacy := parseRiskFile(t, "testdata/risk_weight_addons_2018.10.01_2019.09.30.xlsx",
		riskAddonFile{Name: "2018.10.01_2019.09.30.xlsx", Caption: "с 01.10.2018 по 30.09.2019"})
	assertRiskPoint(t, legacy, "legacy", 2019, 10, 1, 2100, 1, 1, "2004.i", "1000.i", 0.6, 1)
}

func parseRiskFile(t *testing.T, path string, file riskAddonFile) []riskAddonPoint {
	t.Helper()
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = xlsx.Close() }()
	points, err := cbrRiskWeightAddonsParseBook(xlsx, file)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(points) == 0 {
		t.Fatalf("%s: 0 точек", path)
	}
	return points
}

func assertRiskPoint(t *testing.T, points []riskAddonPoint, matrix string,
	fromYear int, fromMonth time.Month, fromDay int,
	toYear int, toMonth time.Month, toDay int,
	row, col string, wantValue float64, wantApplicable uint8) {
	t.Helper()
	from := time.Date(fromYear, fromMonth, fromDay, 0, 0, 0, 0, time.UTC)
	to := time.Date(toYear, toMonth, toDay, 0, 0, 0, 0, time.UTC)
	for _, p := range points {
		if p.matrix == matrix && p.validFrom.Equal(from) && p.validTo.Equal(to) &&
			p.rowCode == row && p.colCode == col {
			if p.value != wantValue || p.isApplicable != wantApplicable {
				t.Fatalf("%s %s..%s %s×%s: value=%v applicable=%d, want %v/%d",
					matrix, from.Format("2006-01-02"), to.Format("2006-01-02"), row, col,
					p.value, p.isApplicable, wantValue, wantApplicable)
			}
			return
		}
	}
	t.Fatalf("нет точки %s %s..%s %s×%s", matrix, from.Format("2006-01-02"),
		to.Format("2006-01-02"), row, col)
}

// TestRiskAddonVersionOrder — version определяется датой документа, а не
// временем прогона: переимпорт старого файла не должен обгонять более новый.
func TestRiskAddonVersionOrder(t *testing.T) {
	oldFile := riskAddonFile{Name: "2018.xlsx", DocumentFrom: time.Date(2018, 10, 1, 0, 0, 0, 0, time.UTC)}
	newFile := riskAddonFile{Name: "2022.xlsx", DocumentFrom: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)}

	oldV := riskAddonVersion(oldFile, 0, 0)
	newV := riskAddonVersion(newFile, 0, 0)
	if oldV >= newV {
		t.Fatalf("старый документ version %d >= новый %d", oldV, newV)
	}
	// Переимпорт старого файла после нового: версия растёт в пределах своей даты,
	// но остаётся ниже версии нового документа.
	oldReimport := riskAddonVersion(oldFile, 0, oldV)
	if oldReimport <= oldV {
		t.Fatalf("переимпорт не сдвинул version: %d -> %d", oldV, oldReimport)
	}
	if oldReimport >= newV {
		t.Fatalf("переимпорт старого документа %d обогнал новый %d", oldReimport, newV)
	}
}

// TestRiskWeightAddonsRestoreRejected — набор файлов берётся с landing, поэтому
// restore обязан отказать до сети, а не падать на GetPage недоступного cbr.ru.
func TestRiskWeightAddonsRestoreRejected(t *testing.T) {
	t.Setenv("ARCHIVE_ACTION", "restore")
	if _, err := cbrRiskWeightAddonsImporter.Import(context.Background(), nil); err == nil {
		t.Fatal("restore для набора файлов должен возвращать ошибку")
	}
}
