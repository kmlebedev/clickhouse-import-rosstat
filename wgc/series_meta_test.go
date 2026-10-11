package wgc

import (
	"strings"
	"testing"
)

// Каталог — единственный контракт ряда для MCP-агента (правило 11): по нему агент
// узнаёт единицы, знак потока, связь с etf_flows_month и приоритет источников
// витрины. Проверки идут по самому wgcEtfCatalog(), а не по копии списка в тесте:
// копия пропустила бы ряд, забытый в каталоге.
func TestEtfCatalogDescribesEverySeries(t *testing.T) {
	const want = 20

	catalog := wgcEtfCatalog()
	if len(catalog) != want {
		t.Fatalf("wgcEtfCatalog(): %d записей, ожидалось %d", len(catalog), want)
	}

	described := make(map[string]bool, want)

	for _, meta := range catalog {
		if meta.Source != wgcSource {
			t.Errorf("ряд %s: source = %q, ожидался %q", meta.Series, meta.Source, wgcSource)
		}
		if meta.Frequency != "M" {
			t.Errorf("ряд %s: frequency = %q, ожидалось M (месячные точки)", meta.Series, meta.Frequency)
		}

		for field, value := range map[string]string{
			"title":       meta.Title,
			"unit":        meta.Unit,
			"origin":      meta.Origin,
			"description": meta.Description,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("ряд %s: пустое поле %s", meta.Series, field)
			}
		}

		if described[meta.Series] {
			t.Errorf("ряд %s описан дважды", meta.Series)
		}
		described[meta.Series] = true

		if !strings.Contains(meta.Description, "wgc_etf_flows") {
			t.Errorf("ряд %s: описание не называет импортёр wgc_etf_flows", meta.Series)
		}
		if !strings.Contains(meta.Description, "wgc > webbridge > manual") {
			t.Errorf("ряд %s: описание не фиксирует приоритет источников wgc > webbridge > manual", meta.Series)
		}
	}

	for _, metric := range wgcEtfMetrics {
		for _, region := range wgcEtfRegions {
			if name := wgcEtfSeriesName(metric, region); !described[name] {
				t.Errorf("ряд %s не описан в каталоге", name)
			}
		}
	}

	// origin обязан называть лист выгрузки своей метрики: копипаста с чужим листом
	// отправила бы агента читать не тот блок файла.
	sheets := map[string]string{
		etfMetricFlowUsdMln:     etfFlowsSheet,
		etfMetricFlowTonnes:     etfDemandSheet,
		etfMetricHoldingsTonnes: etfHoldingsSheet,
		etfMetricAumUsdBn:       etfChartsSheet,
	}
	wantSheet := make(map[string]string, want)

	for metric, sheet := range sheets {
		for _, region := range wgcEtfRegions {
			wantSheet[wgcEtfSeriesName(metric, region)] = sheet
		}
	}

	for _, meta := range catalog {
		if sheet, ok := wantSheet[meta.Series]; !ok || !strings.Contains(meta.Origin, sheet) {
			t.Errorf("ряд %s: origin %q не называет лист %q", meta.Series, meta.Origin, sheet)
		}
	}

	// До 2007-03 у world нет значений всех четырёх регионов (Азия появляется
	// только в 2007-03), поэтому мировой ряд начинается с 2007-03; агент обязан
	// узнать это из каталога, а не из пропусков в данных.
	for _, meta := range catalog {
		if strings.HasSuffix(meta.Series, "_"+etfWorldRegion) && !strings.Contains(meta.Description, "2007-03") {
			t.Errorf("ряд %s: описание world не фиксирует начало ряда с 2007-03", meta.Series)
		}
	}
}
