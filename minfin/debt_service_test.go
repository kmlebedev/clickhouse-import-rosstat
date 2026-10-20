package minfin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func TestDebtServiceBudgetImport(t *testing.T) {
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", "Prilozhenie_3_dannye_109-111_mes_202610.xlsx"))
	if err != nil {
		t.Fatalf("открытие бюджетной фикстуры: %v", err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	rec := &fedbudMesyatsRecorder{}
	if err := debtServiceBudgetImport(xlsx, rec); err != nil {
		t.Fatalf("debtServiceBudgetImport: %v", err)
	}
	if len(rec.rows) != 132 {
		t.Fatalf("точек %d, ожидалось 132 (2011-01 … 2021-12; с 2022 источник строку не заполняет)", len(rec.rows))
	}
	if got := debtFind(t, rec.rows, debtServiceBudgetSeries, fedbudMesyatsDay(2011, time.January, 1)); !closeTo(got, 25.46067436306, 0.0001) {
		t.Errorf("обслуживание 2011-01 = %v, ожидалось 25.46067436306", got)
	}
	last := rec.rows[len(rec.rows)-1]
	if !last.date.Equal(fedbudMesyatsDay(2021, time.December, 1)) {
		t.Errorf("последняя дата %s, ожидалась 2021-12-01", last.date.Format("2006-01-02"))
	}
}

func TestParseIndicatorsBudget(t *testing.T) {
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", "indicatorsBudget_01.10.2026.xlsx"))
	if err != nil {
		t.Fatalf("открытие фикстуры заимствований: %v", err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	rec := &fedbudMesyatsRecorder{}
	if err := parseIndicatorsBudget(xlsx, rec); err != nil {
		t.Fatalf("parseIndicatorsBudget: %v", err)
	}
	if len(rec.rows) != 36 {
		t.Fatalf("точек %d, ожидалось 36 (9 месяцев × 4 ряда)", len(rec.rows))
	}
	date := fedbudMesyatsDay(2026, time.January, 1)
	want := map[string]float64{
		"Привлечение":               141.112332,
		"Погашение основного долга": 0,
		"Чистое привлечение":        141.112332,
		"Расходы на обслуживание":   223.704221,
	}
	for name, value := range want {
		if got := debtFind(t, rec.rows, name, date); !closeTo(got, value, 0.000001) {
			t.Errorf("%s 2026-01 = %v, ожидалось %v", name, got, value)
		}
	}
	last := rec.rows[len(rec.rows)-1]
	if !last.date.Equal(fedbudMesyatsDay(2026, time.September, 1)) {
		t.Errorf("последняя дата %s, ожидалась 2026-09-01", last.date.Format("2006-01-02"))
	}
}

func TestParseIndicatorsBudgetAnnual(t *testing.T) {
	cases := []struct {
		file string
		year int
	}{
		{"indicatorsBudget_01.01.2025.xlsx", 2024},
		{"indicatorsBudget_01.01.2026.xlsx", 2025},
	}
	for _, c := range cases {
		xlsx, err := excelize.OpenFile(filepath.Join("testdata", c.file))
		if err != nil {
			t.Fatalf("открытие %s: %v", c.file, err)
		}
		rec := &fedbudMesyatsRecorder{}
		if err := parseIndicatorsBudget(xlsx, rec); err != nil {
			_ = xlsx.Close()
			t.Fatalf("%s: %v", c.file, err)
		}
		_ = xlsx.Close()
		if len(rec.rows) != 48 {
			t.Fatalf("%s: точек %d, ожидалось 48 (12 месяцев × 4 ряда)", c.file, len(rec.rows))
		}
		last := rec.rows[len(rec.rows)-1]
		if !last.date.Equal(fedbudMesyatsDay(c.year, time.December, 1)) {
			t.Errorf("%s: последняя дата %s, ожидалась %d-12-01", c.file, last.date.Format("2006-01-02"), c.year)
		}
	}
}

func TestDebtServiceIndicatorsLinkFixture(t *testing.T) {
	page, err := os.ReadFile(filepath.Join("testdata", "borrowing_doc_2026.html"))
	if err != nil {
		t.Fatalf("чтение borrowing_doc_2026.html: %v", err)
	}
	href, err := util.FindLink(page, debtServiceFileSel)
	if err != nil {
		t.Fatalf("FindLink indicatorsBudget: %v", err)
	}
	if !strings.Contains(href, "indicatorsBudget_01.10.2026.xlsx") {
		t.Errorf("ссылка indicatorsBudget: %q", href)
	}
}

func TestParseIndicatorsBudgetMissingHeader(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "Основные показатели"); err != nil {
		t.Fatal(err)
	}
	if err := f.SetCellValue("Основные показатели", "A1", "нет года и шапки"); err != nil {
		t.Fatal(err)
	}
	if err := parseIndicatorsBudget(f, &fedbudMesyatsRecorder{}); err == nil {
		t.Fatal("ожидалась ошибка: год/шапка не найдены")
	}
}
