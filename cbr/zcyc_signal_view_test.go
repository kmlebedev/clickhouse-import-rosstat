package cbr

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// TestOfzSignalViewUsesCpiPublicationDate — витрина ждёт series_publication и датирует
// ИПЦ датой доступности, а не концом отчётного месяца (дефект Ф2, #626).
func TestOfzSignalViewUsesCpiPublicationDate(t *testing.T) {
	hasSeriesPublication := false
	for _, table := range cbrZcycSignalView.Tables {
		if table == util.SeriesPublicationTable {
			hasSeriesPublication = true
		}
	}
	if !hasSeriesPublication {
		t.Fatalf("витрина не ждёт %s: %v", util.SeriesPublicationTable, cbrZcycSignalView.Tables)
	}
	for _, needle := range []string{
		"available_at",
		"c.date >= i.available_at",
		"source = 'rosstat'",
		"source = 'cbr' AND series = 'Все товары и услуги'",
		"addMonths(toStartOfMonth(w.date), 2) - 1",
	} {
		if !strings.Contains(cbrZcycSignalView.Select, needle) {
			t.Errorf("в SQL витрины нет %q", needle)
		}
	}
	if strings.Contains(cbrZcycSignalView.Select, "c.date >= i.date") {
		t.Error("витрина всё ещё датирует ИПЦ концом отчётного месяца")
	}
}
