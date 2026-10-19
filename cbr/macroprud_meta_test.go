package cbr

import (
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// TestMacroprudentialSeriesMeta — каталог трёх новых источников: ожидаемое
// число рядов, заполненные поля и уникальность (source, series). Отдельный тест,
// потому что source у них — имя таблицы, а не литерал 'cbr' из cbrAllSeriesMeta.
func TestMacroprudentialSeriesMeta(t *testing.T) {
	cases := []struct {
		name string
		meta []util.SeriesMeta
		want int
	}{
		{"cbr_ccyb_indicators", cbrCcybSeriesMeta, 24},
		{"cbr_risk_weight_addons", cbrRiskWeightAddonsSeriesMeta, 3},
		{"cbr_mpl", cbrMPLSeriesMeta, 7},
	}
	for _, tc := range cases {
		if len(tc.meta) != tc.want {
			t.Errorf("%s: рядов %d, want %d", tc.name, len(tc.meta), tc.want)
		}
		seen := make(map[string]bool, len(tc.meta))
		for _, m := range tc.meta {
			if m.Source != tc.name {
				t.Errorf("%s: source %q", tc.name, m.Source)
			}
			for field, v := range map[string]string{
				"series": m.Series, "title": m.Title, "unit": m.Unit,
				"frequency": m.Frequency, "origin": m.Origin, "description": m.Description,
			} {
				if v == "" {
					t.Errorf("%s/%s: пустое поле %s", tc.name, m.Series, field)
				}
			}
			key := m.Source + "\x00" + m.Series
			if seen[key] {
				t.Errorf("%s: дубль ряда %q", tc.name, m.Series)
			}
			seen[key] = true
		}
	}
}
