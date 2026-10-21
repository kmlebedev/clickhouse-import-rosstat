package ppi

import (
	"strings"
	"testing"
)

func TestPPIView(t *testing.T) {
	if ppiView.Name != "v_rosstat_ppi" {
		t.Fatalf("имя витрины %q", ppiView.Name)
	}
	wantTables := []string{"rosstat_ppi_ved", "rosstat_ppi_product", "rosstat_ppi_weights", "ppi_sa"}
	if len(ppiView.Tables) != len(wantTables) {
		t.Fatalf("таблиц %d, want %d", len(ppiView.Tables), len(wantTables))
	}
	for i, table := range wantTables {
		if ppiView.Tables[i] != table {
			t.Errorf("таблица %d = %q, want %q", i, ppiView.Tables[i], table)
		}
	}
	for _, column := range []string{"source", "series", "date", "value", "unit", "frequency"} {
		if _, ok := ppiView.Columns[column]; !ok {
			t.Errorf("нет комментария колонки %q", column)
		}
	}
	for _, fragment := range []string{
		"FROM rosstat_ppi_ved FINAL",
		"FROM rosstat_ppi_product FINAL",
		"FROM rosstat_ppi_weights FINAL",
		"FROM ppi_sa FINAL",
		"concat('ИЦП по ВЭД, РФ, ', index_type)",
		"concat('okpd2:', okpd2, ':', index_type)",
		"concat('ved-weight:', code, ':', weight_type)",
		"concat('ppi_sa:', series, ':sa')",
		"concat('ppi_sa:', series, ':mm_sa')",
		"concat('ppi_sa:', series, ':saar')",
		"concat('ppi_sa:', series, ':saar3')",
		"makeDate(year, 1, 1)",
		"toNullable(toFloat64(value))",
	} {
		if !strings.Contains(ppiView.Select, fragment) {
			t.Errorf("SQL витрины не содержит %q", fragment)
		}
	}
}

func TestPPIViewSeriesMeta(t *testing.T) {
	base := ppiSASeriesMeta()
	meta := ppiSAViewSeriesMeta()
	if len(base) != 5 {
		t.Fatalf("базовых рядов %d, want 5", len(base))
	}
	if len(meta) != 20 {
		t.Fatalf("рядов витрины %d, want 20", len(meta))
	}
	metrics := map[string]string{
		"sa":    "индекс (100 = первый месяц ряда)",
		"mm_sa": "%",
		"saar":  "%",
		"saar3": "%",
	}
	seen := map[string]bool{}
	for _, item := range meta {
		seen[item.Series] = true
		if item.Source != ppiSATable || item.Frequency != "M" {
			t.Errorf("%s: source=%q frequency=%q", item.Series, item.Source, item.Frequency)
		}
		if item.Title == "" || item.Origin == "" || item.Description == "" {
			t.Errorf("%s: неполное описание", item.Series)
		}
		found := false
		for metric, unit := range metrics {
			if strings.HasSuffix(item.Series, ":"+metric) {
				found = true
				if item.Unit != unit {
					t.Errorf("%s: unit=%q, want %q", item.Series, item.Unit, unit)
				}
			}
		}
		if !found {
			t.Errorf("%s: неизвестная метрика", item.Series)
		}
	}
	for _, item := range base {
		for metric := range metrics {
			name := "ppi_sa:" + item.Series + ":" + metric
			if !seen[name] {
				t.Errorf("нет ряда %s", name)
			}
		}
	}
}

func TestPPIViewSeriesInViewSQL(t *testing.T) {
	// Имена 20 рядов строятся тем же concat, что и SQL витрины: проверяем
	// префиксы/метрики, чтобы правка одной стороны не разошлась с другой.
	for _, metric := range []string{"sa", "mm_sa", "saar", "saar3"} {
		if !strings.Contains(ppiView.Select, "':"+metric+"')") {
			t.Errorf("SQL витрины не содержит метрику %q", metric)
		}
	}
	if !strings.Contains(ppiView.Select, "'ppi_sa'") {
		t.Error("SQL витрины не помечает source='ppi_sa'")
	}
}

func TestPPIDerivedCatalogSize(t *testing.T) {
	if got := len(ppiSASeriesMeta()) + len(ppiSAViewSeriesMeta()); got != 25 {
		t.Fatalf("каталог ppi_sa = %d записей, want 25 (5 базовых + 20 метрик)", got)
	}
}
