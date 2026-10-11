package wgc

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
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

// Каталог ЦБ — единственный контракт рядов для MCP-агента: единицы, направление дельты,
// оговорки стран и «не путать с v_imf_macro» (там USD и только Китай). Проверки идут по
// самому wgcCbCatalog(), а не по копии списка в тесте: копия пропустила бы ряд, забытый
// в каталоге.
func TestCbCatalogDescribesEverySeries(t *testing.T) {
	names := CbSeriesNames()

	catalog := wgcCbCatalog()
	if len(catalog) != len(names) {
		t.Fatalf("wgcCbCatalog(): %d записей, ожидалось %d", len(catalog), len(names))
	}

	bySeries := make(map[string]util.SeriesMeta, len(catalog))
	for _, meta := range catalog {
		if meta.Source != wgcSource {
			t.Errorf("ряд %s: source = %q, ожидался %q", meta.Series, meta.Source, wgcSource)
		}
		if meta.Unit != "тонны" {
			t.Errorf("ряд %s: unit = %q, ожидались тонны", meta.Series, meta.Unit)
		}
		if meta.Frequency != "M" {
			t.Errorf("ряд %s: frequency = %q, ожидалось M (месячные точки)", meta.Series, meta.Frequency)
		}
		for field, value := range map[string]string{
			"title":       meta.Title,
			"origin":      meta.Origin,
			"description": meta.Description,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("ряд %s: пустое поле %s", meta.Series, field)
			}
		}
		if _, dup := bySeries[meta.Series]; dup {
			t.Errorf("ряд %s описан дважды", meta.Series)
		}
		bySeries[meta.Series] = meta
	}

	// Порядок записей — как в CbSeriesNames(): по странам словаря, внутри страны дельты,
	// затем уровни; на него опирается и витрина v_cb_gold_dynamics (Task 5).
	for i, name := range names {
		if catalog[i].Series != name {
			t.Errorf("каталог[%d].Series = %q, ожидался %q (порядок CbSeriesNames)", i, catalog[i].Series, name)
		}
	}

	for _, c := range CbCountries() {
		change := bySeries[wgcCbSeriesName(cbMetricChangeTonnes, c.ISO)]
		holdings := bySeries[wgcCbSeriesName(cbMetricHoldingsTonnes, c.ISO)]

		if !strings.Contains(change.Title, "Золотые резервы ЦБ: изменение за месяц") || !strings.Contains(change.Title, c.Country) {
			t.Errorf("ряд %s: title %q не называет метрику и страну", change.Series, change.Title)
		}
		if !strings.Contains(holdings.Title, "Золотые резервы ЦБ: уровень") || !strings.Contains(holdings.Title, c.Country) {
			t.Errorf("ряд %s: title %q не называет метрику и страну", holdings.Series, holdings.Title)
		}

		// origin обязан называть файл и лист своей метрики: копипаста с чужим листом
		// отправила бы агента читать не тот блок файла.
		if !strings.Contains(change.Origin, cbChangesMask) || !strings.Contains(change.Origin, cbChangesSheet) {
			t.Errorf("ряд %s: origin %q не называет файл %s и лист %s", change.Series, change.Origin, cbChangesMask, cbChangesSheet)
		}
		if !strings.Contains(holdings.Origin, cbHoldingsMask) || !strings.Contains(holdings.Origin, cbHoldingsSheet) {
			t.Errorf("ряд %s: origin %q не называет файл %s и лист %s", holdings.Series, holdings.Origin, cbHoldingsMask, cbHoldingsSheet)
		}

		// Дельта и уровень — разные ряды из разных файлов: без взаимной ссылки агент
		// прочитал бы их как одно число.
		if !strings.Contains(change.Description, holdings.Series) {
			t.Errorf("ряд %s: описание не ссылается на уровень %s", change.Series, holdings.Series)
		}
		if !strings.Contains(holdings.Description, change.Series) {
			t.Errorf("ряд %s: описание не ссылается на дельты %s", holdings.Series, change.Series)
		}
		if !strings.Contains(holdings.Description, "отставать") {
			t.Errorf("ряд %s: описание не предупреждает, что дата уровня может отставать от дельт", holdings.Series)
		}

		for _, meta := range []util.SeriesMeta{change, holdings} {
			for _, want := range []string{"wgc_cb_reserves", "v_imf_macro", "IMF", "отчётного месяца"} {
				if !strings.Contains(meta.Description, want) {
					t.Errorf("ряд %s: описание не содержит %q", meta.Series, want)
				}
			}
		}
	}

	// Оговорки стран (§3.2 спеки): пропуски RUS, net-ряд TUR, покрытие UZB. Без них агент
	// прочитал бы пропуск как ноль, а gross-строку — как официальные резервы.
	notes := map[string][]string{
		"RUS": {"2025-12", "2026-03", "пропущены"},
		"TUR": {"Turkey*", "net"},
		"UZB": {"2013-02"},
	}
	for iso, wants := range notes {
		for _, metric := range cbMetrics {
			meta := bySeries[wgcCbSeriesName(metric, iso)]
			for _, want := range wants {
				if !strings.Contains(meta.Description, want) {
					t.Errorf("ряд %s: описание не содержит оговорку %q", meta.Series, want)
				}
			}
		}
	}

	// Ruling 5: покрытие UZB — фактическое (2013-02); комментарий файла «Data first
	// available in January 2018» данным не соответствует и в каталоге не тиражируется.
	if meta := bySeries[wgcCbSeriesName(cbMetricChangeTonnes, "UZB")]; strings.Contains(meta.Description, "2018") {
		t.Errorf("ряд %s: описание тиражирует противоречащий данным комментарий файла «Data first available in January 2018»", meta.Series)
	}
}
