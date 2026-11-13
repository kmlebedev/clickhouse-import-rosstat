package rosstat

import (
	"fmt"
	"os"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// cpiSPRTestBook собирает книгу в памяти: синтетические листы проверяют макет,
// границы и ошибки разбора без бинарных фикстур.
func cpiSPRTestBook(t *testing.T, sheets map[string][][]string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	names := make([]string, 0, len(sheets))
	for name := range sheets {
		names = append(names, name)
	}
	if len(names) == 0 {
		t.Fatal("книга без листов")
	}
	if err := f.SetSheetName("Sheet1", names[0]); err != nil {
		t.Fatal(err)
	}
	for _, name := range names[1:] {
		if _, err := f.NewSheet(name); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range names {
		for i, row := range sheets[name] {
			if err := f.SetSheetRow(name, fmt.Sprintf("A%d", i+1), &row); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// cpiSPRTestSheet строит лист-месяц в формате публикации: заголовок выпуска,
// двухстрочная шапка, агрегат и n товарных строк с заданным весом.
func cpiSPRTestSheet(monthLabel string, rowWeight float64, n int) [][]string {
	rows := [][]string{
		{"К содержанию"},
		{},
		{"Индексы потребительских цен \nпо Российской Федерации за " + monthLabel + " г.1)"},
		{},
		{
			"Наименование групп товаров (услуг)\nи товаров(услуг)-представителей\n",
			"Локальный код группы / товара (услуги)-представителя ",
			"Структура потребительских расходов населения,\nв % к итогу",
			"Индексы потребительских цен, в %", "", "",
		},
		{"", "", "", "к предыдущему месяцу", "к декабрю предыдущего года", "к соответствующему  месяцу предыдущего года"},
		{"Все товары", "2", "100.0000", "100.25", "103.93", "105.73"},
	}
	for i := 0; i < n; i++ {
		rows = append(rows, []string{
			fmt.Sprintf("Товар %d", i),
			fmt.Sprintf("%d", 1000+i),
			fmt.Sprintf("%.4f", rowWeight),
			"100.10", "101.10", "102.10",
		})
	}
	return rows
}

func TestCPISPRSheetMetaFromTitle(t *testing.T) {
	rows := [][]string{{"К содержанию"}, {}, {"Индексы потребительских цен по Российской Федерации за сентябрь 2025 г.1)"}}
	year, month, ok := cpiSPRSheetMeta(rows, "0922025")
	if !ok || year != 2025 || month != 9 {
		t.Fatalf("получено %d-%02d ok=%v, ожидалось 2025-09", year, month, ok)
	}
}

func TestCPISPRSheetMetaFromName(t *testing.T) {
	year, month, ok := cpiSPRSheetMeta([][]string{{"без даты"}}, "112026")
	if !ok || year != 2026 || month != 11 {
		t.Fatalf("получено %d-%02d ok=%v, ожидалось 2026-11", year, month, ok)
	}
	if _, _, ok = cpiSPRSheetMeta([][]string{{"Содержание"}}, "Содержание"); ok {
		t.Fatal("лист без даты и без имени MMYYYY не должен разбираться")
	}
}

func TestCPISPRFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page_spr.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, cpiSPRLinkSelector)
	if err != nil {
		t.Fatalf("%s: %v", cpiSPRLinkSelector, err)
	}
	if want := "/storage/mediabank/ipc_spr_09-2026.xlsx"; href != want {
		t.Errorf("%q, ожидалось %q", href, want)
	}
	// Флаг i: имя в другом регистре тоже должно находиться.
	upper := []byte(`<a href="/storage/mediabank/IPC_SPR_10-2026.XLSX">x</a>`)
	if href, err = util.FindLink(upper, cpiSPRLinkSelector); err != nil || href != "/storage/mediabank/IPC_SPR_10-2026.XLSX" {
		t.Errorf("регистронезависимый поиск: %q, %v", href, err)
	}
}

func TestCPISPRTypoSheetName(t *testing.T) {
	book := cpiSPRTestBook(t, map[string][][]string{
		"082025":  cpiSPRTestSheet("август 2025", 1.3, 700),
		"0922025": cpiSPRTestSheet("сентябрь 2025", 1.3, 700),
	})
	points, err := parseCPISPR(book)
	if err != nil {
		t.Fatalf("parseCPISPR: %v", err)
	}
	months := map[uint16]bool{}
	for _, point := range points {
		months[point.month] = true
	}
	if !months[8] || !months[9] {
		t.Fatalf("месяцы %v, ожидались 8 и 9", months)
	}
}

func TestCPISPRMissingMonth(t *testing.T) {
	book := cpiSPRTestBook(t, map[string][][]string{
		"012026": cpiSPRTestSheet("январь 2026", 1.3, 700),
		"032026": cpiSPRTestSheet("март 2026", 1.3, 700),
	})
	if _, err := parseCPISPR(book); err == nil {
		t.Fatal("пропуск февраля должен давать ошибку")
	}
}

func TestCPISPRDuplicateConflict(t *testing.T) {
	rows := cpiSPRTestSheet("январь 2026", 1.3, 700)
	rows = append(rows, []string{"Дубль", "1500", "0.5", "100", "100", "100"})
	book := cpiSPRTestBook(t, map[string][][]string{
		"012026": rows,
		"022026": cpiSPRTestSheet("февраль 2026", 1.3, 700),
	})
	if _, err := parseCPISPR(book); err == nil {
		t.Fatal("дубль кода с другим весом должен давать ошибку")
	}
}

func TestCPISPRSumOutOfRange(t *testing.T) {
	book := cpiSPRTestBook(t, map[string][][]string{
		"012026": cpiSPRTestSheet("январь 2026", 0.05, 700),
		"022026": cpiSPRTestSheet("февраль 2026", 0.05, 700),
	})
	if _, err := parseCPISPR(book); err == nil {
		t.Fatal("сумма листа вне диапазона должна давать ошибку")
	}
}

func TestCPISPRSmallSheetRejected(t *testing.T) {
	book := cpiSPRTestBook(t, map[string][][]string{
		"012026": cpiSPRTestSheet("январь 2026", 10, 50),
		"022026": cpiSPRTestSheet("февраль 2026", 10, 50),
	})
	if _, err := parseCPISPR(book); err == nil {
		t.Fatal("малое число позиций должно давать ошибку")
	}
}

func TestCPISPRIndicesNullable(t *testing.T) {
	rows := cpiSPRTestSheet("январь 2026", 1.3, 700)
	rows = append(rows, []string{"Без индексов", "9001", "0.5", "-", "", "-"})
	good := cpiSPRTestBook(t, map[string][][]string{
		"012026": rows,
		"022026": cpiSPRTestSheet("февраль 2026", 1.3, 700),
	})
	points, err := parseCPISPR(good)
	if err != nil {
		t.Fatalf("parseCPISPR: %v", err)
	}
	found := false
	for _, point := range points {
		if point.code != "9001" {
			continue
		}
		found = true
		if point.indexPrev != nil || point.indexYTD != nil || point.indexYOY != nil {
			t.Errorf("ожидались NULL-индексы, получено %v / %v / %v", point.indexPrev, point.indexYTD, point.indexYOY)
		}
	}
	if !found {
		t.Fatal("строка без индексов потеряна")
	}

	bad := cpiSPRTestSheet("январь 2026", 1.3, 700)
	bad = append(bad, []string{"Мусор в индексе", "9002", "0.5", "abc", "100", "100"})
	if _, err = parseCPISPR(cpiSPRTestBook(t, map[string][][]string{
		"012026": bad,
		"022026": cpiSPRTestSheet("февраль 2026", 1.3, 700),
	})); err == nil {
		t.Fatal("нечисловой индекс должен давать ошибку")
	}
}

func TestCPISPRWeightOutOfRange(t *testing.T) {
	for _, weight := range []string{"0", "150"} {
		rows := cpiSPRTestSheet("январь 2026", 1.3, 700)
		rows = append(rows, []string{"Плохой вес", "9003", weight, "100", "100", "100"})
		if _, err := parseCPISPR(cpiSPRTestBook(t, map[string][][]string{
			"012026": rows,
			"022026": cpiSPRTestSheet("февраль 2026", 1.3, 700),
		})); err == nil {
			t.Errorf("вес %s должен давать ошибку", weight)
		}
	}
}

func TestCPISPRRealFixture(t *testing.T) {
	f, err := excelize.OpenFile("testdata/cpi_spr.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	points, err := parseCPISPR(f)
	if err != nil {
		t.Fatalf("parseCPISPR: %v", err)
	}
	if len(points) != 2*778 {
		t.Fatalf("точек %d, ожидалось %d", len(points), 2*778)
	}
	byKey := map[string]cpiSPRPoint{}
	for _, point := range points {
		byKey[fmt.Sprintf("%d-%02d-%s", point.year, point.month, point.code)] = point
	}
	if got := byKey["2026-01-2"].weight; got != 71.774 {
		t.Errorf("2026-01 код 2 = %v, ожидалось 71.774", got)
	}
	if got := byKey["2026-01-111"].weight; got != 0.638 {
		t.Errorf("2026-01 код 111 = %v, ожидалось 0.638", got)
	}
	total := byKey["2026-01-1"]
	if total.weight != 100 || total.indexPrev == nil || *total.indexPrev != 101.62 || total.indexYOY == nil || *total.indexYOY != 106 {
		t.Errorf("2026-01 код 1: %+v", total)
	}
	nulls := 0
	for _, point := range points {
		if point.indexPrev == nil || point.indexYTD == nil || point.indexYOY == nil {
			nulls++
		}
	}
	if nulls == 0 {
		t.Error("ожидались строки без индексов (в источнике 10–11 на лист)")
	}
}

func TestCPISPRImporterDeclared(t *testing.T) {
	decl := cpiSprImporter.Publication()
	if decl.Source != rosstatCPISPRTable || decl.CountPoints != cpiSPRCountPoints {
		t.Errorf("объявление публикации: %+v", decl)
	}
	if cpiSprImporter.Name() != rosstatCPISPRTable {
		t.Errorf("Name() = %q", cpiSprImporter.Name())
	}
}
