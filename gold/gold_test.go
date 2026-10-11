package gold

import "testing"

// TestSeriesMetaWellFormed: у ряда gold (venue импортёра) заполнены все поля.
// Полнота набора рядов (moex_fix_usd импортёра + comex_front ingest) — в series_meta_test.go.
func TestSeriesMetaWellFormed(t *testing.T) {
	idx := -1
	for i := range goldSeriesMeta {
		if goldSeriesMeta[i].Series == goldVenue {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatalf("ряд %q не описан в series_meta", goldVenue)
	}
	m := goldSeriesMeta[idx]
	if m.Source != goldSource {
		t.Errorf("source %q != %q", m.Source, goldSource)
	}
	for field, v := range map[string]string{
		"title": m.Title, "unit": m.Unit, "frequency": m.Frequency,
		"origin": m.Origin, "description": m.Description,
	} {
		if v == "" {
			t.Errorf("пустое поле %s", field)
		}
	}
}

// TestViewCoversTable: витрина v_gold_prices строится над той таблицей, которую пишет импортёр.
func TestViewCoversTable(t *testing.T) {
	if len(goldPricesView.Tables) != 1 || goldPricesView.Tables[0] != goldTable {
		t.Errorf("view tables %v != [%s]", goldPricesView.Tables, goldTable)
	}
}
