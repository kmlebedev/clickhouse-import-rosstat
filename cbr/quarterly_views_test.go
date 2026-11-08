package cbr

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// TestCbrDipQuarterlyViewSQL: квартальная витрина ДИП — среднее месячных SAAR по выпуску
// (спека 2026-11-08-cpi-quarterly-metrics §4.1). Контракт проверяется фрагментами SQL, а не
// живым запросом: тест обязан падать при правке, которая меняет смысл числа — потерянный FINAL
// (ReplacingMergeTree посчитал бы несмёрженные части), отбор не того ряда, потерянная
// винтажность (release/publication_date в GROUP BY) или квартальный ключ.
func TestCbrDipQuarterlyViewSQL(t *testing.T) {
	if cbrDipQuarterlyView.Name != "v_cbr_dip_quarterly" {
		t.Fatalf("имя витрины %q, ожидалось v_cbr_dip_quarterly", cbrDipQuarterlyView.Name)
	}
	if len(cbrDipQuarterlyView.Tables) != 1 || cbrDipQuarterlyView.Tables[0] != cbrDipTable {
		t.Fatalf("таблицы витрины %v, ожидалась [%s]", cbrDipQuarterlyView.Tables, cbrDipTable)
	}
	for _, fragment := range []string{
		"FROM cbr_dip_bulletin FINAL",
		"series = 'saar_all_goods'",
		"toStartOfQuarter(date) AS quarter_start",
		"GROUP BY release, release_id, publication_date, quarter_start",
		"count() AS months",
		"avg(value) AS saar_q",
		"'к'",
	} {
		if !strings.Contains(cbrDipQuarterlyView.Select, fragment) {
			t.Errorf("SQL витрины не содержит %q", fragment)
		}
	}
	if strings.Contains(cbrDipQuarterlyView.Select, "^") {
		t.Error("SQL витрины содержит ^: в ClickHouse это XOR, а не степень")
	}
	for _, column := range []string{"release", "release_id", "publication_date", "quarter_start", "quarter", "months", "saar_q"} {
		if cbrDipQuarterlyView.Columns[column] == "" {
			t.Errorf("нет комментария колонки %q", column)
		}
	}
	for _, want := range []string{"months = 3", "неполный", "ревизия", "2026-07-01 = III кв. 2026"} {
		if !strings.Contains(cbrDipQuarterlyView.Comment, want) {
			t.Errorf("комментарий витрины не содержит %q: агент не узнает, как читать квартал", want)
		}
	}
}

// TestCbrCoreQuarterlyViewSQL: квартальная базовая — среднее годовых темпов из SA м/м
// (спека §4.2). pow, а не ^: в ClickHouse ^ — побитовый XOR, и формула спеки в таком виде падает
// с SYNTAX_ERROR. Винтажей у файла нет — это должно быть сказано в комментарии, иначе агент
// прочитает квартал как point-in-time слой, которого нет.
func TestCbrCoreQuarterlyViewSQL(t *testing.T) {
	if cbrCoreQuarterlyView.Name != "v_cbr_core_quarterly" {
		t.Fatalf("имя витрины %q, ожидалось v_cbr_core_quarterly", cbrCoreQuarterlyView.Name)
	}
	if len(cbrCoreQuarterlyView.Tables) != 1 || cbrCoreQuarterlyView.Tables[0] != cbrIndicatorsCpdTable {
		t.Fatalf("таблицы витрины %v, ожидалась [%s]", cbrCoreQuarterlyView.Tables, cbrIndicatorsCpdTable)
	}
	for _, fragment := range []string{
		"FROM cbr_indicators_cpd FINAL",
		"name = 'Базовый ИПЦ'",
		"pow(1 + value / 100, 12)",
		"count() AS months",
		"max(date) AS last_month",
		"GROUP BY quarter_start",
	} {
		if !strings.Contains(cbrCoreQuarterlyView.Select, fragment) {
			t.Errorf("SQL витрины не содержит %q", fragment)
		}
	}
	if strings.Contains(cbrCoreQuarterlyView.Select, "^") {
		t.Error("SQL витрины содержит ^: в ClickHouse это XOR, а не степень")
	}
	for _, column := range []string{"quarter_start", "quarter", "months", "saar_q", "last_month"} {
		if cbrCoreQuarterlyView.Columns[column] == "" {
			t.Errorf("нет комментария колонки %q", column)
		}
	}
	for _, want := range []string{"14.11", "Винтажей нет", "текущая редакция", "2026-07-01 = III кв. 2026"} {
		if !strings.Contains(cbrCoreQuarterlyView.Comment, want) {
			t.Errorf("комментарий витрины не содержит %q", want)
		}
	}
}

// TestCbrQuarterlySeriesMeta: каталог квартальных рядов. Их два, и они живут в разных источниках
// (ДИП — под именем таблицы, базовая — под cbr, как её месячные ряды в v_cbr_macro). Единица
// «% годовых», частота «Q», и в описании — привязка даты (quarter_start) и метка quarter:
// без этого читатель применит к витрине правило каталога «1-е число следующего квартала» и
// сдвинет квартал на один.
func TestCbrQuarterlySeriesMeta(t *testing.T) {
	all := make([]util.SeriesMeta, 0, 2)
	all = append(all, cbrDipQuarterlySeriesMeta...)
	all = append(all, cbrCoreQuarterlySeriesMeta...)
	if len(all) != 2 {
		t.Fatalf("квартальных рядов каталога %d, ожидалось 2", len(all))
	}
	for _, item := range all {
		if item.Unit != "% годовых" {
			t.Errorf("ряд %s/%s: unit %q, ожидалось «%% годовых»", item.Source, item.Series, item.Unit)
		}
		if item.Frequency != "Q" {
			t.Errorf("ряд %s/%s: frequency %q, ожидалось Q", item.Source, item.Series, item.Frequency)
		}
		if item.Title == "" || item.Origin == "" || item.Description == "" {
			t.Errorf("ряд %s/%s: неполное описание", item.Source, item.Series)
		}
		for _, want := range []string{"quarter_start", "quarter", "months"} {
			if !strings.Contains(item.Description, want) {
				t.Errorf("ряд %s/%s: описание не содержит %q", item.Source, item.Series, want)
			}
		}
	}
	if len(cbrDipQuarterlySeriesMeta) != 1 || cbrDipQuarterlySeriesMeta[0].Series != "saar_q" {
		t.Fatalf("каталог ДИП: ожидался один ряд saar_q, получено %v", cbrDipQuarterlySeriesMeta)
	}
	if cbrDipQuarterlySeriesMeta[0].Source != cbrDipTable {
		t.Errorf("каталог ДИП: source %q, ожидался %q", cbrDipQuarterlySeriesMeta[0].Source, cbrDipTable)
	}
	if len(cbrCoreQuarterlySeriesMeta) != 1 || cbrCoreQuarterlySeriesMeta[0].Series != "Базовый ИПЦ:SAAR_q" {
		t.Fatalf("каталог базовой: ожидался один ряд «Базовый ИПЦ:SAAR_q», получено %v", cbrCoreQuarterlySeriesMeta)
	}
	if cbrCoreQuarterlySeriesMeta[0].Source != cbrSource {
		t.Errorf("каталог базовой: source %q, ожидался %q", cbrCoreQuarterlySeriesMeta[0].Source, cbrSource)
	}
	for _, item := range cbrDipSeriesMeta {
		if item.Series == "saar_q" {
			t.Error("квартальный ряд попал в cbrDipSeriesMeta: у месячных рядов unit «%» и frequency «M»")
		}
	}
}
