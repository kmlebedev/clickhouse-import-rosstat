package gold

import "testing"

// TestSeriesMetaWellFormed: у ряда gold заполнены все поля и он совпадает с venue импортёра.
func TestSeriesMetaWellFormed(t *testing.T) {
	if len(goldSeriesMeta) != 1 {
		t.Fatalf("ожидается один ряд gold, есть %d", len(goldSeriesMeta))
	}
	m := goldSeriesMeta[0]
	if m.Series != goldVenue {
		t.Errorf("series %q != venue импортёра %q", m.Series, goldVenue)
	}
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
