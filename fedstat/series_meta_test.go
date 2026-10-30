package fedstat

import (
	"strings"
	"testing"
)

func TestFedstatSeriesMetaComplete(t *testing.T) {
	meta := fedstatSeriesMeta()
	if len(meta) != fedstatExpectedProducts {
		t.Fatalf("catalog rows = %d, want %d", len(meta), fedstatExpectedProducts)
	}
	seen := make(map[string]bool, len(meta))
	historical, y2026, only2026, onlyHistorical := 0, 0, 0, 0
	for _, row := range meta {
		if row.Series == "" || row.Title == "" {
			t.Errorf("empty series/title: %+v", row)
		}
		if seen[row.Series] {
			t.Errorf("duplicate series %q", row.Series)
		}
		seen[row.Series] = true
		if row.Source != fedstatSource {
			t.Errorf("%q: source = %q, want %q", row.Series, row.Source, fedstatSource)
		}
		if row.Unit != "рубль" {
			t.Errorf("%q: unit = %q, want рубль", row.Series, row.Unit)
		}
		if row.Frequency != "W" {
			t.Errorf("%q: frequency = %q, want W", row.Series, row.Frequency)
		}
		if !strings.Contains(row.Origin, "s_grtov=") || !strings.Contains(row.Origin, fedstatRegionRFCode) || !strings.Contains(row.Origin, fedstatRegionRFNewCode) {
			t.Errorf("%q: origin = %q", row.Series, row.Origin)
		}
		if !strings.Contains(row.Description, "2023–2025") {
			t.Errorf("%q: description без оговорки о 2023–2025", row.Series)
		}
	}

	for _, product := range fedstatProducts {
		if product.historical {
			historical++
			if !product.y2026 {
				onlyHistorical++
			}
		} else {
			only2026++
		}
		if product.y2026 {
			y2026++
		}
	}
	if historical != 143 || y2026 != 114 || only2026 != 15 || onlyHistorical != 44 {
		t.Errorf("coverage flags: historical=%d (143), 2026=%d (114), only2026=%d (15), onlyHistorical=%d (44)", historical, y2026, only2026, onlyHistorical)
	}
	for _, row := range meta {
		product, ok := fedstatProductByCode[codeOfSeries(t, row.Series)]
		if !ok {
			t.Errorf("%q: нет в fedstatProductByCode", row.Series)
			continue
		}
		if !product.historical && !strings.Contains(row.Description, "В 2008–2022 значений нет") {
			t.Errorf("%q: description без оговорки об отсутствии истории", row.Series)
		}
		if !product.y2026 && !strings.Contains(row.Description, "В 2026 не публикуется") {
			t.Errorf("%q: description без оговорки об отсутствии 2026", row.Series)
		}
	}
}

func codeOfSeries(t *testing.T, series string) string {
	t.Helper()
	for _, product := range fedstatProducts {
		if product.name == series {
			return product.code
		}
	}
	t.Fatalf("series %q not found in fedstatProducts", series)
	return ""
}

func TestFedstatDdlAndView(t *testing.T) {
	for _, want := range []string{"Float64", "ReplacingMergeTree", "LowCardinality(String)", "ORDER BY (product, region, date)"} {
		if !strings.Contains(fedstatDdl, want) {
			t.Errorf("fedstatDdl missing %q", want)
		}
	}
	if fedstatWeeklyPricesView.Name != "v_fedstat_weekly_prices" {
		t.Errorf("view name = %q", fedstatWeeklyPricesView.Name)
	}
	if len(fedstatWeeklyPricesView.Tables) != 1 || fedstatWeeklyPricesView.Tables[0] != fedstatTable {
		t.Errorf("view tables = %v", fedstatWeeklyPricesView.Tables)
	}
	if !strings.Contains(fedstatWeeklyPricesView.Select, "fedstat_weekly_prices FINAL") {
		t.Errorf("view select = %q", fedstatWeeklyPricesView.Select)
	}
	for _, column := range []string{"source", "series", "region", "date", "value"} {
		if fedstatWeeklyPricesView.Columns[column] == "" {
			t.Errorf("view column %q has no comment", column)
		}
	}
}
