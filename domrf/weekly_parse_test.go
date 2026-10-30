package domrf

import (
	"os"
	"strings"
	"testing"
)

func domrfTestPage(t *testing.T) []byte {
	t.Helper()
	page, err := os.ReadFile("testdata/domrf_mortgage_page.html")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return page
}

func domrfTestWeeklyBlock(t *testing.T) domrfWeeklyBlockJSON {
	t.Helper()
	blocks, err := domrfBlocks(domrfTestPage(t))
	if err != nil {
		t.Fatalf("domrfBlocks: %v", err)
	}
	block, err := domrfBlock[domrfWeeklyBlockJSON](blocks, domrfWeeklyBlock)
	if err != nil {
		t.Fatalf("domrfBlock: %v", err)
	}
	return block
}

// TestDomrfWeeklyPoints — разбор реального блока: 254 даты по убыванию и 1 154 точки; «Льготная
// ипотека» короче остальных, потому что программа завершилась.
func TestDomrfWeeklyPoints(t *testing.T) {
	block := domrfTestWeeklyBlock(t)
	if got := len(block.Date); got != 254 {
		t.Fatalf("дат %d, ожидалось 254", got)
	}
	if block.Date[0] != "16.10.2026" || block.Date[len(block.Date)-1] != "03.09.2021" {
		t.Fatalf("крайние даты %q … %q", block.Date[0], block.Date[len(block.Date)-1])
	}
	points, err := domrfWeeklyPoints(block)
	if err != nil {
		t.Fatalf("domrfWeeklyPoints: %v", err)
	}
	if len(points) != 1154 {
		t.Fatalf("точек %d, ожидалось 1154", len(points))
	}
	perProduct := make(map[string]int, len(block.Titles))
	var newest domrfWeeklyPoint
	for _, point := range points {
		perProduct[point.Product]++
		if point.Date.Format("2006-01-02") == "2026-10-16" && point.Product == "Новостройка" {
			newest = point
		}
	}
	for _, product := range domrfWeeklyProducts {
		if perProduct[product] == 0 {
			t.Errorf("продукт %s без точек", product)
		}
	}
	if perProduct["Льготная ипотека"] != 138 {
		t.Errorf("Льготная ипотека: %d точек, ожидалось 138", perProduct["Льготная ипотека"])
	}
	if newest.Rate != 18.76 {
		t.Errorf("Новостройка 2026-10-16 = %v, ожидалось 18.76", newest.Rate)
	}
}

func TestDomrfWeeklyPointsGuards(t *testing.T) {
	base := domrfTestWeeklyBlock(t)

	longer := base
	longer.Values = make(map[string]string, len(base.Values))
	for key, value := range base.Values {
		longer.Values[key] = value
	}
	longer.Values["Новостройка"] = longer.Values["Новостройка"] + ",1"

	broken := base
	broken.Values = make(map[string]string, len(base.Values))
	for key, value := range base.Values {
		broken.Values[key] = value
	}
	broken.Values["Новостройка"] = strings.Replace(base.Values["Новостройка"], ",", ",,", 1)

	unsorted := base
	unsorted.Date = append([]string{}, base.Date...)
	unsorted.Date[0], unsorted.Date[1] = unsorted.Date[1], unsorted.Date[0]

	missing := base
	missing.Values = make(map[string]string, len(base.Values))
	for key, value := range base.Values {
		if key != "Новостройка" {
			missing.Values[key] = value
		}
	}

	cases := map[string]domrfWeeklyBlockJSON{
		"значений больше дат":  longer,
		"пустое значение":      broken,
		"даты не по убыванию":  unsorted,
		"продукт без значений": missing,
	}
	for name, block := range cases {
		if _, err := domrfWeeklyPoints(block); err == nil {
			t.Errorf("%s: ошибка не поймана", name)
		}
	}
}
