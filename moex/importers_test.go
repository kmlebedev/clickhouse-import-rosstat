package moex

import (
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

func TestImportersRegistry(t *testing.T) {
	want := map[string]bool{
		stockPricesTable: true,
		ofzTable:         true,
		keyrateTable:     true,
		moexMarketTable:  true,
	}
	if len(chimport.Stats) != len(want) {
		t.Fatalf("registered %d importers, want %d", len(chimport.Stats), len(want))
	}
	for _, importer := range chimport.Stats {
		if !want[importer.Name()] {
			t.Errorf("unexpected importer %q", importer.Name())
		}
		delete(want, importer.Name())
	}
	for name := range want {
		t.Errorf("importer %q is not registered", name)
	}
}
