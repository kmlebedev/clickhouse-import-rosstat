package fao

import (
	"os"
	"strings"
	"testing"
)

func TestFaoSeriesCatalogContract(t *testing.T) {
	body, err := os.ReadFile("testdata/food_price_indices_data.csv")
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}
	rec := &faoRecorder{}
	if err = parseFaoFoodPrice(body, rec); err != nil {
		t.Fatalf("parseFaoFoodPrice: %v", err)
	}
	parsed := make(map[string]bool)
	for _, row := range rec.rows {
		parsed[row.series] = true
	}
	described := make(map[string]bool)
	for _, meta := range faoSeriesMeta {
		if meta.Source != faoSource {
			t.Errorf("ряд %q: source %q, ожидался %q", meta.Series, meta.Source, faoSource)
		}
		if meta.Title == "" || meta.Unit == "" || meta.Frequency == "" || meta.Origin == "" || meta.Description == "" {
			t.Errorf("ряд %q: не заполнены обязательные поля каталога", meta.Series)
		}
		if meta.Frequency != "M" {
			t.Errorf("ряд %q: frequency %q, ожидалось M", meta.Series, meta.Frequency)
		}
		if !strings.Contains(meta.Unit, "2014-2016") {
			t.Errorf("ряд %q: unit %q не содержит базу 2014-2016", meta.Series, meta.Unit)
		}
		described[meta.Series] = true
	}
	for series := range parsed {
		if !described[series] {
			t.Errorf("ряд %q разобран, но не описан в series_catalog", series)
		}
	}
	for series := range described {
		if !parsed[series] {
			t.Errorf("ряд %q описан, но не разобран на фикстуре", series)
		}
	}
}

func TestFaoViewContract(t *testing.T) {
	if faoFoodPriceView.Name != "v_fao_food_price" {
		t.Errorf("витрина %q, ожидалась v_fao_food_price", faoFoodPriceView.Name)
	}
	if len(faoFoodPriceView.Tables) != 1 || faoFoodPriceView.Tables[0] != faoFoodPriceTable {
		t.Errorf("Tables = %v, ожидалась [%s]", faoFoodPriceView.Tables, faoFoodPriceTable)
	}
	for _, want := range []string{"source", "series", "date", "period_date", "value", "toFloat64(price_index)"} {
		if !strings.Contains(faoFoodPriceView.Select, want) {
			t.Errorf("SELECT витрины не содержит %q", want)
		}
	}
	for _, column := range []string{"source", "series", "date", "period_date", "value"} {
		if faoFoodPriceView.Columns[column] == "" {
			t.Errorf("у колонки %q нет комментария", column)
		}
	}
}

func TestFaoSeriesHeadersUnique(t *testing.T) {
	seen := make(map[string]bool)
	for _, s := range faoSeriesByHeader {
		if seen[s.Header] {
			t.Errorf("заголовок %q встречается дважды", s.Header)
		}
		seen[s.Header] = true
	}
	if len(faoSeriesByHeader) != 6 {
		t.Errorf("рядов %d, ожидалось 6", len(faoSeriesByHeader))
	}
}
