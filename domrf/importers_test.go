package domrf

import (
	"slices"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// approvedNames — утверждённый литеральный набор имён импортёров пакета. Имя импортёра — контракт
// расписания (CLICKHOUSE_IMPORT_STAT и id шага dagu/domrf.yaml). Литералы, а не константы пакета:
// сравнение константы с самой собой прошло бы и после переименования таблицы.
var approvedNames = []string{"domrf_mortgage_market", "domrf_mortgage_weekly", "domrf_price_index"}

// TestImporterRegistry — реестр содержит ровно три импортёра пакета и ни одного чужого.
func TestImporterRegistry(t *testing.T) {
	found := make(map[string]bool, len(approvedNames))
	for _, stat := range chimport.Stats {
		if slices.Contains(approvedNames, stat.Name()) {
			found[stat.Name()] = true
		}
	}
	for _, name := range approvedNames {
		if !found[name] {
			t.Errorf("импортёр %s не зарегистрирован", name)
		}
	}
}

// TestImporterTables — Name() каждого импортёра равен литералу таблицы, а DDL создаёт именно её.
// Имя — контракт данных: по нему ходят витрины и агент MCP.
func TestImporterTables(t *testing.T) {
	if got := (&domrfMortgageWeekly{}).Name(); got != "domrf_mortgage_weekly" {
		t.Errorf("weekly Name() = %q", got)
	}
	if got := (&domrfPriceIndex{}).Name(); got != "domrf_price_index" {
		t.Errorf("price index Name() = %q", got)
	}
	market := newDomrfMortgageMarketImporter()
	if got := market.Name(); got != "domrf_mortgage_market" {
		t.Errorf("market Name() = %q", got)
	}
	// Шаблон подставляет имя в DDL через fmt.Sprintf — без %s таблица осталась бы без имени, а
	// лишний аргумент Sprintf дописал бы «%!(EXTRA …)» в конец SQL.
	if !strings.Contains(domrfMarketDdl, "%s") {
		t.Error("DDL рынка без подстановки имени: шаблон не подставит таблицу")
	}
	if domrfMarketCountPoints != "SELECT count() FROM domrf_mortgage_market FINAL" {
		t.Errorf("count-запрос рынка %q", domrfMarketCountPoints)
	}
	if !strings.Contains(domrfWeeklyDdl, "domrf_mortgage_weekly") {
		t.Error("DDL недельных ставок без имени таблицы")
	}
	if !strings.Contains(domrfPriceIndexDdl, "domrf_price_index") {
		t.Error("DDL индекса цен без имени таблицы")
	}
}

// TestSeriesCatalogCoversAll — каждый ряд и продукт описаны каталогом: ряд без описания агент
// прочитает без единиц. Тест держит обе стороны: список рядов и карту описаний.
func TestSeriesCatalogCoversAll(t *testing.T) {
	meta := domrfSeriesMeta()
	described := make(map[string]bool, len(meta))
	for _, entry := range meta {
		if entry.Series == "" || entry.Title == "" || entry.Unit == "" || entry.Description == "" || entry.Origin == "" {
			t.Errorf("ряд %s/%s описан не полностью: %+v", entry.Source, entry.Series, entry)
		}
		described[entry.Source+"|"+entry.Series] = true
	}
	for _, series := range domrfMarketSeriesList {
		if !described[domrfSourceMarket+"|"+series.Series] {
			t.Errorf("ряд %s не описан в каталоге", series.Series)
		}
	}
	for _, product := range domrfWeeklyProducts {
		if !described[domrfSourceWeekly+"|"+product] {
			t.Errorf("продукт %s не описан в каталоге", product)
		}
	}
	if len(meta) != len(domrfMarketSeriesList)+len(domrfWeeklyProducts) {
		t.Errorf("каталог %d рядов, ожидалось %d", len(meta), len(domrfMarketSeriesList)+len(domrfWeeklyProducts))
	}
}

// TestWeeklyProductsMatchSource — список продуктов каталога совпадает с набором блока источника:
// смена состава (новый продукт или переименование) должна ломать прогон, а не заводить продукт
// без описания.
func TestWeeklyProductsMatchSource(t *testing.T) {
	block := domrfTestWeeklyBlock(t)
	got := append([]string{}, block.Titles...)
	want := append([]string{}, domrfWeeklyProducts...)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("продукты источника %v, каталог %v", got, want)
	}
}

// TestViewsDeclared — витрины объявлены над своими таблицами и содержат их литералами.
func TestViewsDeclared(t *testing.T) {
	if domrfMortgageView.Name != "v_domrf_mortgage" {
		t.Errorf("имя витрины %q", domrfMortgageView.Name)
	}
	for _, table := range []string{"domrf_mortgage_market", "domrf_mortgage_weekly"} {
		if !slices.Contains(domrfMortgageView.Tables, table) || !strings.Contains(domrfMortgageView.Select, table) {
			t.Errorf("витрина рынка не объявляет таблицу %s", table)
		}
	}
	if domrfPriceIndexView.Name != "v_domrf_price_index" || !strings.Contains(domrfPriceIndexView.Select, "domrf_price_index") {
		t.Error("витрина индекса цен объявлена неверно")
	}
	for name, view := range map[string]struct {
		comment string
		columns map[string]string
	}{"v_domrf_mortgage": {domrfMortgageView.Comment, domrfMortgageView.Columns}, "v_domrf_price_index": {domrfPriceIndexView.Comment, domrfPriceIndexView.Columns}} {
		if view.comment == "" || len(view.columns) == 0 {
			t.Errorf("%s без комментариев", name)
		}
	}
}
