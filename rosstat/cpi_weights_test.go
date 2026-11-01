package rosstat

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// cpiWeightsTestBook собирает книгу в памяти: без бинарных фикстур тест
// проверяет оба задокументированных макета и граничные случаи разбора.
func cpiWeightsTestBook(t *testing.T, sheets map[string][][]string) *excelize.File {
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

// cpiWeightsItemsRows строит 400 товарных строк (ровно порог) с суммой 100 %
// и итоговую строку «Все товары и услуги», которая в сумму не входит.
func cpiWeightsItemsRows(combined bool, weights float64) [][]string {
	rows := make([][]string, 0, 402)
	if combined {
		rows = append(rows, []string{"Код и наименование товара (услуги)-представителя", "СПР, %"})
		rows = append(rows, []string{"1 Все товары и услуги", "100"})
	} else {
		rows = append(rows, []string{"Код товара (услуги)-представителя", "Наименование товара (услуги)-представителя", "СПР, %"})
		rows = append(rows, []string{"1", "Все товары и услуги", "100"})
	}
	for i := 0; i < 400; i++ {
		code := fmt.Sprintf("%d", 1000+i)
		name := fmt.Sprintf("Товар %d", i)
		switch i {
		case 0:
			code, name = "803", "Масло подсолнечное, л"
		case 1:
			code, name = "930.АГ", "Услуги телекоммуникационные"
		}
		value := fmt.Sprintf("%.4f", weights)
		if combined {
			rows = append(rows, []string{code + " " + name, value})
		} else {
			rows = append(rows, []string{code, name, value})
		}
	}
	return rows
}

func cpiWeightsGroupsRows(levelColumn bool, weights float64) [][]string {
	header := []string{"Код", "Наименование группы (товара)", "СПР, %"}
	if levelColumn {
		header = []string{"Код", "Наименование группы (товара)", "Уровень", "СПР, %"}
	}
	rows := [][]string{header}
	for i := 0; i < 60; i++ {
		code := fmt.Sprintf("%d", 10+i)
		name := fmt.Sprintf("Группа %d", i)
		if i == 0 {
			code, name = "930.АГ", "Услуги телекоммуникационные"
		}
		if levelColumn {
			rows = append(rows, []string{code, name, fmt.Sprintf("%d", i%3+1), fmt.Sprintf("%.4f", weights)})
		} else {
			rows = append(rows, []string{code, name, fmt.Sprintf("%.4f", weights)})
		}
	}
	return rows
}

func TestCPIWeightsFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		selector string
		want     string
	}{
		{cpiWeightsItemsSelector, "/storage/mediabank/CPR_tov_RF_2004-2026.xlsx"},
		{cpiWeightsGroupsSelector, "/storage/mediabank/CPR_Gr-Tov-RF_2001-2026.xlsx"},
	} {
		href, err := util.FindLink(page, test.selector)
		if err != nil {
			t.Fatalf("%s: %v", test.selector, err)
		}
		if href != test.want {
			t.Errorf("%s: %q, ожидалось %q", test.selector, href, test.want)
		}
	}
}

func TestCPIWeightsItemsSeparateLayout(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": cpiWeightsItemsRows(false, 0.25),
		"2025": cpiWeightsItemsRows(false, 0.25),
		"2026": cpiWeightsItemsRows(false, 0.25),
	})
	points, err := parseCPIWeights(book, cpiWeightsFileItems)
	if err != nil {
		t.Fatalf("parseCPIWeights: %v", err)
	}
	if len(points) != 3*401 {
		t.Fatalf("точек %d, ожидалось %d", len(points), 3*401)
	}
	variants := map[string]bool{}
	byCode := map[string]cpiWeightPoint{}
	for _, point := range points {
		variants[point.variant] = true
		if point.year == 2026 {
			byCode[point.code] = point
		}
	}
	if len(variants) != 1 || !variants[""] {
		t.Errorf("варианты %v, ожидался только пустой", variants)
	}
	for _, code := range []string{"803", "930.АГ", "1002", "1"} {
		if _, ok := byCode[code]; !ok {
			t.Errorf("в 2026 нет кода %s", code)
		}
	}
	if got := byCode["803"].name; got != "Масло подсолнечное, л" {
		t.Errorf("название 803 = %q", got)
	}
}

func TestCPIWeightsItemsCombinedLayout(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": cpiWeightsItemsRows(true, 0.25),
		"2025": cpiWeightsItemsRows(true, 0.25),
		"2026": cpiWeightsItemsRows(true, 0.25),
	})
	points, err := parseCPIWeights(book, cpiWeightsFileItems)
	if err != nil {
		t.Fatalf("parseCPIWeights: %v", err)
	}
	found := false
	for _, point := range points {
		if point.code == "930.АГ" {
			found = true
			if point.name != "Услуги телекоммуникационные" {
				t.Errorf("название 930.АГ = %q", point.name)
			}
		}
	}
	if !found {
		t.Error("код 930.АГ не разобран из составной ячейки")
	}
}

func TestCPIWeights2022Variants(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2022 до 01.04": cpiWeightsItemsRows(false, 0.25),
		"2022 с 01.04":  cpiWeightsItemsRows(false, 0.25),
		"2023":          cpiWeightsItemsRows(false, 0.25),
		"2024":          cpiWeightsItemsRows(false, 0.25),
		"2025":          cpiWeightsItemsRows(false, 0.25),
		"2026":          cpiWeightsItemsRows(false, 0.25),
	})
	points, err := parseCPIWeights(book, cpiWeightsFileItems)
	if err != nil {
		t.Fatalf("parseCPIWeights: %v", err)
	}
	variants := map[string]int{}
	for _, point := range points {
		if point.year == 2022 {
			variants[point.variant]++
		}
	}
	if variants[cpiWeightsVariantBefore] != 401 || variants[cpiWeightsVariantFrom] != 401 {
		t.Errorf("варианты 2022: %v, ожидалось по 401", variants)
	}
}

func TestCPIWeightsDuplicateConflict(t *testing.T) {
	rows := cpiWeightsItemsRows(false, 0.25)
	rows = append(rows, []string{"1002", "Дубль", "0.30"})
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": rows, "2025": cpiWeightsItemsRows(false, 0.25), "2026": cpiWeightsItemsRows(false, 0.25),
	})
	if _, err := parseCPIWeights(book, cpiWeightsFileItems); err == nil ||
		!strings.Contains(err.Error(), "встречается дважды") {
		t.Fatalf("ожидалась ошибка дубля, получено %v", err)
	}
}

func TestCPIWeightsMissingYear(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": cpiWeightsItemsRows(false, 0.25),
		"2026": cpiWeightsItemsRows(false, 0.25),
	})
	if _, err := parseCPIWeights(book, cpiWeightsFileItems); err == nil ||
		!strings.Contains(err.Error(), "нет листа за 2025") {
		t.Fatalf("ожидалась ошибка пропуска года, получено %v", err)
	}
}

func TestCPIWeightsSumOutOfTolerance(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": cpiWeightsItemsRows(false, 0.20),
		"2025": cpiWeightsItemsRows(false, 0.20),
		"2026": cpiWeightsItemsRows(false, 0.20),
	})
	if _, err := parseCPIWeights(book, cpiWeightsFileItems); err == nil ||
		!strings.Contains(err.Error(), "сумма весов") {
		t.Fatalf("ожидалась ошибка суммы, получено %v", err)
	}
}

func TestCPIWeightsGroupsAllLevels(t *testing.T) {
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": cpiWeightsGroupsRows(true, 2.0),
		"2025": cpiWeightsGroupsRows(true, 2.0),
		"2026": cpiWeightsGroupsRows(true, 2.0),
	})
	points, err := parseCPIWeights(book, cpiWeightsFileGroups)
	if err != nil {
		t.Fatalf("parseCPIWeights: %v", err)
	}
	if len(points) != 3*60 {
		t.Fatalf("точек %d, ожидалось %d", len(points), 3*60)
	}
	levels := map[uint8]bool{}
	for _, point := range points {
		levels[point.level] = true
	}
	if !levels[1] || !levels[3] {
		t.Errorf("уровни %v: ожидались 1 и 3", levels)
	}
}

func TestCPIWeightsSmallSheetRejected(t *testing.T) {
	rows := cpiWeightsItemsRows(false, 0.25)[:100]
	book := cpiWeightsTestBook(t, map[string][][]string{
		"2024": rows, "2025": cpiWeightsItemsRows(false, 0.25), "2026": cpiWeightsItemsRows(false, 0.25),
	})
	if _, err := parseCPIWeights(book, cpiWeightsFileItems); err == nil ||
		!strings.Contains(err.Error(), "позиций") {
		t.Fatalf("ожидалась ошибка числа позиций, получено %v", err)
	}
}

func TestCPIWeightsImporterDeclared(t *testing.T) {
	if got := cpiWeightsImporter.Name(); got != rosstatCPIWeightsTable {
		t.Errorf("Name() = %q", got)
	}
	pub := cpiWeightsImporter.Publication()
	if pub.Source != rosstatCPIWeightsTable || pub.CountPoints != cpiWeightsCountAll {
		t.Errorf("Publication() = %+v", pub)
	}
}
