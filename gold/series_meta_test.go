package gold

import (
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Каталог обязан описывать все venue, в которые пишет контур: импортёр gold — moex_fix_usd,
// ingest (/v1/gold_prices) — comex_front, импортёр bold_gold_price (пакет bold) — bold. Ряд без
// описания невидим агенту в v_series_catalog.
func TestSeriesMetaCoversAllVenues(t *testing.T) {
	want := []string{goldVenue, "comex_front", boldVenue}

	// Записи каталога живут в двух видах: срез goldSeriesMeta и отдельная BoldPriceSeriesMeta()
	// (её объявляет этот пакет, а публикует импортёр пакета bold). Проверка полноты обязана
	// покрывать оба: иначе переезд записи в отдельную функцию тихо вывел бы её из-под теста.
	described := make(map[string]bool, len(goldSeriesMeta)+1)
	for _, m := range append([]util.SeriesMeta{BoldPriceSeriesMeta()}, goldSeriesMeta...) {
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
