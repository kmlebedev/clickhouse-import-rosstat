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

// Фикстуры — реальные артефакты источника (стандарт §11.1): выпуск 2026/10 и
// страница каталога федерального бюджета, с которой на него ведёт ссылка.
const (
	fedbudMesFixtureXlsx = "Prilozhenie_3_dannye_109-111_mes_202610.xlsx"
	fedbudMesFixturePage = "fedbud_page.html"
)

func openFedbudMesFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", fedbudMesFixtureXlsx))
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", fedbudMesFixtureXlsx, err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// newFedbudTestWorkbook — синтетическая книга только для веток fail-fast: основной
// разбор проверяется на реальной книге (стандарт §11.1).
func newFedbudTestWorkbook(t *testing.T, sheet string, rows [][]any) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", sheet); err != nil {
		t.Fatal(err)
	}
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.SetCellValue(sheet, cell, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	return f
}

func fedbudDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func TestFedbudMesLinkFixture(t *testing.T) {
	page, err := os.ReadFile(filepath.Join("testdata", fedbudMesFixturePage))
	if err != nil {
		t.Fatalf("чтение HTML-фикстуры: %v", err)
	}
	href, err := util.FindLink(page, fedbudMesLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if !strings.Contains(href, "_mes.xlsx") || !strings.Contains(href, "2026/10") {
		t.Fatalf("FindLink вернул %q, ожидалась ссылка на выпуск 2026/10", href)
	}
	if _, err = util.FindLink([]byte("<html><body></body></html>"), fedbudMesLinkSelector); err == nil {
		t.Fatal("ожидалась ошибка: ссылка не найдена")
	}
}

func TestFedbudMesImport(t *testing.T) {
	rec := &appendRecorder{}
	if err := fedBudImport(openFedbudMesFixture(t), rec); err != nil {
		t.Fatalf("fedBudImport: %v", err)
	}
	if len(rec.rows) != 1076 {
		t.Fatalf("точек %d, ожидалось 1076 (189×4 + 132 + 188)", len(rec.rows))
	}

	wantCounts := map[string]int{
		"Доходы, всего":            189,
		"Расходы, всего":           189,
		"Акцизы":                   189,
		"Национальная оборона":     132,
		"привлечение":              188,
		"Дефицит (-)/Профицит (+)": 189,
	}
	for name, want := range wantCounts {
		got := 0
		for _, row := range rec.rows {
			if row.name == name {
				got++
			}
		}
		if got != want {
			t.Errorf("ряд %q: точек %d, ожидалось %d", name, got, want)
		}
	}
	for _, row := range rec.rows {
		if strings.Contains(row.name, "*") {
			t.Errorf("в имени ряда осталась сноска: %q", row.name)
		}
		if row.name == "Акцизы на ввозимые товары" {
			t.Errorf("строка «Акцизы на ввозимые товары» не должна попадать в набор")
		}
	}

	cases := []struct {
		name  string
		date  time.Time
		value float64
	}{
		{"Доходы, всего", fedbudDay(2011, time.January, 1), 804.8582643055},
		{"Доходы, всего", fedbudDay(2026, time.September, 1), 3687.049691145603},
		{"Расходы, всего", fedbudDay(2026, time.September, 1), 3072.7807109954847},
		{"Акцизы", fedbudDay(2026, time.September, 1), 153.07289659012986},
		{"Дефицит (-)/Профицит (+)", fedbudDay(2026, time.September, 1), 614.2689801501183},
		{"Национальная оборона", fedbudDay(2021, time.December, 1), 921.0929112883105},
		{"привлечение", fedbudDay(2026, time.August, 1), 0},
	}
	for _, c := range cases {
		var found bool
		for _, row := range rec.rows {
			if row.name != c.name || !row.date.Equal(c.date) {
				continue
			}
			found = true
			if math.Abs(row.value-c.value) > 1e-9 {
				t.Errorf("%s @ %s = %.15f, ожидалось %.15f",
					c.name, c.date.Format("2006-01-02"), row.value, c.value)
			}
		}
		if !found {
			t.Errorf("нет точки %s @ %s", c.name, c.date.Format("2006-01-02"))
		}
	}

	maxDates := make(map[string]time.Time)
	for _, row := range rec.rows {
		if maxDates[row.name].Before(row.date) {
			maxDates[row.name] = row.date
		}
	}
	for _, name := range []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Дефицит (-)/Профицит (+)"} {
		if !maxDates[name].Equal(fedbudDay(2026, time.September, 1)) {
			t.Errorf("%s: max date %s, ожидалось 2026-09-01", name, maxDates[name].Format("2006-01-02"))
		}
	}
	if !maxDates["Национальная оборона"].Equal(fedbudDay(2021, time.December, 1)) {
		t.Errorf("Национальная оборона: max date %s, ожидалось 2021-12-01", maxDates["Национальная оборона"].Format("2006-01-02"))
	}
	if !maxDates["привлечение"].Equal(fedbudDay(2026, time.August, 1)) {
		t.Errorf("привлечение: max date %s, ожидалось 2026-08-01", maxDates["привлечение"].Format("2006-01-02"))
	}
}

func TestFedbudMesImportMissingSheet(t *testing.T) {
	xlsx := excelize.NewFile()
	defer func() { _ = xlsx.Close() }()
	err := fedBudImport(xlsx, &appendRecorder{})
	if err == nil {
		t.Fatal("ожидалась ошибка: лист «месяц» отсутствует")
	}
	if !strings.Contains(err.Error(), fedbudMesTable) || !strings.Contains(err.Error(), fedbudSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, fedbudMesTable, fedbudSheet)
	}
}

func TestFedbudImportMissingHeader(t *testing.T) {
	rows := [][]any{
		{"", "Не показатель", "янв.11", "фев.11"},
		{"", "Доходы, всего", 100, 120},
	}
	err := fedBudImport(newFedbudTestWorkbook(t, fedbudSheet, rows), &appendRecorder{})
	if err == nil {
		t.Fatal("разбор книги без шапки прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "не найдена шапка") {
		t.Errorf("ошибка %q не называет причину «нет шапки»", err)
	}
}

func TestFedbudImportMissingExpectedRow(t *testing.T) {
	rows := [][]any{
		{"", "Показатель", "янв.11", "фев.11"},
		{"", "Доходы, всего", 100, 120},
	}
	err := fedBudImport(newFedbudTestWorkbook(t, fedbudSheet, rows), &appendRecorder{})
	if err == nil {
		t.Fatal("разбор книги без ожидаемых строк прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "не найдена ожидаемая строка") {
		t.Errorf("ошибка %q не называет пропавшую подпись строки", err)
	}
}

func TestFedbudImportFailsOnEmptyCellInsideRow(t *testing.T) {
	layout := fedbudLayout{Table: fedbudMesTable, Fields: []string{"Доходы, всего"}}
	rows := [][]any{
		{"", "Показатель", "янв.11", "фев.11"},
		{"", "Доходы, всего", "", 120},
	}
	err := importFedbud(newFedbudTestWorkbook(t, fedbudSheet, rows), &appendRecorder{}, layout)
	if err == nil {
		t.Fatal("разбор с пустой ячейкой внутри накопительного ряда прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "пустая ячейка") {
		t.Errorf("ошибка %q не называет пустую ячейку", err)
	}
}

func TestFedbudImportFailsWithoutPoints(t *testing.T) {
	layout := fedbudLayout{Table: fedbudMesTable, Fields: []string{"Доходы, всего"}}
	rows := [][]any{
		{"", "Показатель", "янв.11", "фев.11"},
		{"", "Доходы, всего"},
	}
	err := importFedbud(newFedbudTestWorkbook(t, fedbudSheet, rows), &appendRecorder{}, layout)
	if err == nil {
		t.Fatal("разбор без единой точки прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "не дала ни одной точки") {
		t.Errorf("ошибка %q не называет причину «ноль точек»", err)
	}
}
