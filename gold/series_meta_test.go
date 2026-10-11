package gold

import "testing"

// Каталог обязан описывать все venue, в которые пишет контур: импортёр gold — moex_fix_usd,
// ingest (/v1/gold_prices) — comex_front. Ряд без описания невидим агенту в v_series_catalog.
func TestSeriesMetaCoversAllVenues(t *testing.T) {
	want := []string{goldVenue, "comex_front"}

	described := make(map[string]bool, len(goldSeriesMeta))
	for _, m := range goldSeriesMeta {
		if m.Source != goldSource || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q заполнено не полностью", m.Series)
		}
		if described[m.Series] {
			t.Errorf("ряд %q описан дважды", m.Series)
		}
		described[m.Series] = true
	}

	for _, venue := range want {
		if !described[venue] {
			t.Errorf("venue %q не описан в series_meta", venue)
		}
	}
}
