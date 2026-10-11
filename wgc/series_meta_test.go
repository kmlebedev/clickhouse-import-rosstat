package wgc

import (
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// etfFallbackFile — имя файла ручной выгрузки, которым origin называет вторую дорогу ряда.
// Копия литерала из etfOrigin* сделана намеренно: тест пинит контракт (агент по origin обязан
// видеть и файл, и лист), а не строку реализации, — переименование файла потребует осознанной
// правки обеих сторон.
const etfFallbackFile = "ETF_Flows_*.xlsx"

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
	bySeries := make(map[string]util.SeriesMeta, want)

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
		bySeries[meta.Series] = meta

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

	// origin обязан называть эндпоинт API своей метрики и — как вторую дорогу ряда — файл и
	// лист ручной выгрузки: копипаста с чужим эндпоинтом или листом отправила бы агента
	// читать чужую метрику или не тот блок файла (flows-chart2 несёт потоки, holdings-chart2 —
	// запасы и AUM; Fund flows by month, Demand by month, Holdings by month и блок Charts Data
	// файла — разные блоки с разными числами).
	type etfOriginWant struct {
		endpoint string
		sheet    string
	}
	origins := map[string]etfOriginWant{
		etfMetricFlowUsdMln:     {etfAPIFlowsPath, etfFlowsSheet},
		etfMetricFlowTonnes:     {etfAPIFlowsPath, etfDemandSheet},
		etfMetricHoldingsTonnes: {etfAPIHoldingsPath, etfHoldingsSheet},
		etfMetricAumUsdBn:       {etfAPIHoldingsPath, etfChartsSheet},
	}

	wantOrigin := make(map[string]etfOriginWant, want)
	for _, metric := range wgcEtfMetrics {
		origin, ok := origins[metric]
		if !ok {
			t.Errorf("метрика %s: тест не знает происхождения ряда — дополни origins", metric)
			continue
		}
		for _, region := range wgcEtfRegions {
			wantOrigin[wgcEtfSeriesName(metric, region)] = origin
		}
	}

	for _, meta := range catalog {
		origin, ok := wantOrigin[meta.Series]
		if !ok {
			// Без этого гварда ряд, которого нет в карте, проверялся бы пустыми ожиданиями:
			// strings.Contains(x, "") всегда истинно, и утверждение стало бы вакуумным.
			t.Errorf("ряд %s: тест не знает ожидаемого origin — дополни origins", meta.Series)
			continue
		}
		for _, wantText := range []string{origin.endpoint, origin.sheet, etfFallbackFile} {
			if !strings.Contains(meta.Origin, wantText) {
				t.Errorf("ряд %s: origin %q не называет %q", meta.Series, meta.Origin, wantText)
			}
		}
	}

	// Начало мировых рядов разное по метрикам: потоки API отдаёт с 2003-02 (там
	// нули явные, и до 2007-03 world = NA + EU + Other — азиатских фондов ещё
	// нет), а запасы и AUM — с 2007-03, первого месяца со всеми четырьмя
	// регионами. Агент обязан узнать это из каталога, а не из пропусков в данных.
	worldSince := map[string]string{
		etfMetricFlowUsdMln:     "2003-02",
		etfMetricFlowTonnes:     "2003-02",
		etfMetricHoldingsTonnes: "2007-03",
		etfMetricAumUsdBn:       "2007-03",
	}

	for _, metric := range wgcEtfMetrics {
		since, ok := worldSince[metric]
		if !ok {
			t.Errorf("метрика %s: тест не знает начала мирового ряда — дополни worldSince", metric)
			continue
		}

		series := wgcEtfSeriesName(metric, etfWorldRegion)
		if !strings.Contains(bySeries[series].Description, since) {
			t.Errorf("ряд %s: описание world не фиксирует начало ряда с %s", series, since)
		}
	}

	// Оговорка об округлении обязательна для всех пяти рядов запасов: тоннажные стоки
	// API приходят округлёнными до 0,01 т, а ручная выгрузка в фолбэке даёт полную
	// точность. Без неё агент прочитал бы расхождение ≤ 0,01 т между источниками за
	// дефект импорта. Пин — по коротким подстрокам, а не по целой фразе: текст оговорки
	// редактируется, требование к нему — нет.
	for _, region := range wgcEtfRegions {
		series := wgcEtfSeriesName(etfMetricHoldingsTonnes, region)

		for _, want := range []string{"0,01", "округл"} {
			if !strings.Contains(bySeries[series].Description, want) {
				t.Errorf("ряд %s: описание запасов не несёт оговорки об округлении источника (%q)", series, want)
			}
		}
	}
}

// Каталог ЦБ — единственный контракт рядов для MCP-агента: единицы, направление дельты,
// оговорки стран и «не путать с v_imf_macro» (там USD и только Китай). Проверки идут по
// самому каталогу на полном наборе точек, а не по копии списка в тесте: копия пропустила
// бы ряд, забытый в каталоге.
func TestCbCatalogDescribesEverySeries(t *testing.T) {
	names := CbSeriesNames()

	catalog := cbCatalogForPoints(cbAllSeriesPoints())
	if len(catalog) != len(names) {
		t.Fatalf("каталог: %d записей, ожидалось %d", len(catalog), len(names))
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

// cbAllSeriesPoints — по точке на каждый ряд набора: так выглядит полный прогон с обеими
// выгрузками (каталогу важны имена рядов, значения не читаются).
func cbAllSeriesPoints() []seriesPoint {
	points := make([]seriesPoint, 0, len(cbCountries)*len(cbMetrics))
	for _, name := range CbSeriesNames() {
		points = append(points, seriesPoint{Series: name, Date: cbDate(2026, time.August, 31), Value: 1})
	}
	return points
}

// Каталог публикует только метрики, у которых в прогоне есть точки: выгрузки ЦБ
// появляются в wgc/data по отдельности, и описания рядов метрики без данных обещали бы
// агенту ряды, которых в macro_series нет. Гранулярность — метрика: одна точка метрики
// описывает все её ряды (страна на месяц выпадает из топ-100 holdings, и удалять её
// описание из каталога было бы потерей карты данных).
func TestCbCatalogForPointsFiltersByMetrics(t *testing.T) {
	point := func(series string) seriesPoint {
		return seriesPoint{Series: series, Date: cbDate(2026, time.August, 31), Value: 1}
	}

	for _, tc := range []struct {
		name   string
		points []seriesPoint
		want   map[string]int // метрика → записей в каталоге
	}{
		{"только дельты", []seriesPoint{point(wgcCbSeriesName(cbMetricChangeTonnes, "POL"))},
			map[string]int{cbMetricChangeTonnes: len(cbCountries), cbMetricHoldingsTonnes: 0}},
		{"только уровни", []seriesPoint{point(wgcCbSeriesName(cbMetricHoldingsTonnes, "POL"))},
			map[string]int{cbMetricChangeTonnes: 0, cbMetricHoldingsTonnes: len(cbCountries)}},
		{"обе метрики", cbAllSeriesPoints(),
			map[string]int{cbMetricChangeTonnes: len(cbCountries), cbMetricHoldingsTonnes: len(cbCountries)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := cbCatalogForPoints(tc.points)

			wantLen := 0
			for _, n := range tc.want {
				wantLen += n
			}
			if len(catalog) != wantLen {
				t.Fatalf("cbCatalogForPoints: %d записей, ожидалось %d", len(catalog), wantLen)
			}

			got := make(map[string]int, len(cbMetrics))
			for _, meta := range catalog {
				metric := ""
				for _, m := range cbMetrics {
					if strings.HasPrefix(meta.Series, wgcCbSeriesName(m, "")) {
						metric = m
						break
					}
				}
				if metric == "" {
					t.Fatalf("ряд %s не принадлежит ни одной метрике набора", meta.Series)
				}
				got[metric]++
			}

			for _, metric := range cbMetrics {
				if got[metric] != tc.want[metric] {
					t.Errorf("метрика %s: %d записей, ожидалось %d", metric, got[metric], tc.want[metric])
				}
			}
		})
	}
}
