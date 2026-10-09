package fred

import "testing"

func TestSeriesMetaCoversAllSeries(t *testing.T) {
	described := make(map[string]bool, len(fredSeriesMeta))
	for _, m := range fredSeriesMeta {
		if m.Source != fredSource || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Fatalf("incomplete catalog entry: %+v", m)
		}
		described[m.Series] = true
	}
	for _, series := range fredSeries {
		if !described[series] {
			t.Errorf("series %s has no series_catalog entry", series)
		}
	}
}
