package wgc

import (
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Единицы рядов — те же строки, что в сиде webbridge-фолбэка
// (sql/wgc_etf_flows_catalog.sql): агент сравнивает unit посимвольно, и
// «млн USD» против «млн $» — уже разные единицы.
const (
	etfUnitUsdMln = "млн USD"
	etfUnitTonnes = "тонны"
	etfUnitUsdBn  = "млрд USD"

	// cbUnitTonnes — единицы рядов ЦБ: та же строка, что у ETF-тонн (агент сравнивает
	// unit посимвольно, и «т» против «тонны» — уже разные единицы).
	cbUnitTonnes = etfUnitTonnes
)

// origin называет лист выгрузки своей метрики: без него агент не отличит поток
// из «Fund flows by month» от запасов из «Holdings by month» — соседние листы
// несут разные метрики в одних и тех же единицах.
const (
	etfOriginFlowUsdMln     = "WGC Gold ETF Flows, файл ETF_Flows_*.xlsx, лист Fund flows by month"
	etfOriginFlowTonnes     = "WGC Gold ETF Flows, файл ETF_Flows_*.xlsx, лист Demand by month"
	etfOriginHoldingsTonnes = "WGC Gold ETF Flows, файл ETF_Flows_*.xlsx, лист Holdings by month"
	etfOriginAum            = "WGC Gold ETF Flows, файл ETF_Flows_*.xlsx, лист Charts Data (блок Area chart - monthly holdings in US$)"
)

// etfCatalogWorldSince — оговорка мировых рядов: world пишется только за месяцы,
// где есть значения всех четырёх регионов, а Азия появляется с 2007-03. Без неё
// агент принял бы отсутствие точек до 2007-03 за нулевой поток.
const etfCatalogWorldSince = "Ряд начинается с 2007-03 — первого месяца, когда в выгрузке есть значения у всех четырёх регионов (Азия — с 2007-03): более ранние месяцы у world отсутствуют, а не нулевые."

// etfCatalogImportNote — общий хвост описания: кто наполняет ряд и как витрина
// выбирает источник. Повторяется в каждой записи намеренно — агент читает
// description по одному ряду и соседние записи не видит.
const etfCatalogImportNote = "Импортёр wgc_etf_flows (расписание dagu/wgc.yaml, пн 20:17) читает свежайший по mtime файл ETF_Flows*.xlsx из wgc/data (скачивается с gold.org/goldhub вручную) и пишет ряд с source='wgc'; приоритет источников в v_gold_etf_flows — wgc > webbridge > manual (webbridge — сессионный сбор, manual — ручной фолбэк)."

// etfDescription склеивает части описания ряда в один абзац.
func etfDescription(parts ...string) string {
	return strings.Join(parts, " ")
}

// wgcEtfCatalog — каталог 20 рядов набора (4 метрики × 5 регионов) для
// series_catalog с source='wgc'. Смыслы — как в сиде webbridge-фолбэка
// sql/wgc_etf_flows_catalog.sql (знак потока, сток vs поток, «не путать с
// etf_flows_month»), но происхождение и обновление другие: ряд пишет импортёр
// из выгрузки WGC, а не сессия через POST /v1/manual_series. Порядок — по
// регионам, как в сиде: строки каталога читаются блоками «мир, затем регионы».
func wgcEtfCatalog() []util.SeriesMeta {
	return []util.SeriesMeta{
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowUsdMln, etfWorldRegion),
			Title:     "WGC Gold ETF: чистый поток, мир",
			Unit:      etfUnitUsdMln,
			Frequency: "M",
			Origin:    etfOriginFlowUsdMln,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF, млн USD, мир: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Мировой итог — сумма четырёх регионов (north_america + europe + asia + other).",
				etfCatalogWorldSince,
				"Не путать с etf_flows_month (source='manual', млрд USD — индикатор v_gold_dashboard): это тот же мировой поток, но другой ряд и другие единицы — не складывать.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowTonnes, etfWorldRegion),
			Title:     "WGC Gold ETF: чистый поток в тоннах, мир",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginFlowTonnes,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF в тоннах, мир: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Тот же поток, что etf_flow_usd_mln_world, только в тоннах: знаки мировых рядов обязаны совпадать. Мировой итог — сумма четырёх регионов.",
				etfCatalogWorldSince,
				"Изменение запасов etf_holdings_tonnes_world месяц к месяцу соответствует этому потоку.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricHoldingsTonnes, etfWorldRegion),
			Title:     "WGC Gold ETF: запасы (holdings) в тоннах, мир",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginHoldingsTonnes,
			Description: etfDescription(
				"Запасы (holdings) физически обеспеченных золотых ETF на конец месяца, тонны, мир: сток, а не поток — значение неотрицательно и направления не несёт. Дата — последний день отчётного месяца. Мировой итог — сумма запасов четырёх регионов (north_america + europe + asia + other).",
				etfCatalogWorldSince,
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricAumUsdBn, etfWorldRegion),
			Title:     "WGC Gold ETF: активы под управлением (AUM), мир",
			Unit:      etfUnitUsdBn,
			Frequency: "M",
			Origin:    etfOriginAum,
			Description: etfDescription(
				"Активы под управлением (AUM) физически обеспеченных золотых ETF на конец месяца, млрд USD, мир: сток, а не поток; включает переоценку запасов по рыночной цене золота, поэтому меняется и в месяцы без потока. Дата — последний день отчётного месяца. Мировой итог — сумма четырёх регионов.",
				etfCatalogWorldSince,
				"Не путать с потоком etf_flow_usd_mln_world (приток/отток за месяц, млн USD) и с etf_flows_month (source='manual', млрд USD — поток, индикатор v_gold_dashboard).",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowUsdMln, etfRegionNorthAmerica),
			Title:     "WGC Gold ETF: чистый поток, Северная Америка",
			Unit:      etfUnitUsdMln,
			Frequency: "M",
			Origin:    etfOriginFlowUsdMln,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF, млн USD, регион «Северная Америка»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Региональная часть мирового etf_flow_usd_mln_world: сумма north_america + europe + asia + other обязана совпадать с мировым рядом — расхождение означает пропущенный регион.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowTonnes, etfRegionNorthAmerica),
			Title:     "WGC Gold ETF: чистый поток в тоннах, Северная Америка",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginFlowTonnes,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF в тоннах, регион «Северная Америка»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Тот же поток, что etf_flow_usd_mln_north_america, только в тоннах: знаки обоих рядов обязаны совпадать — цена месяца не переводит приток в отток. Региональная часть мирового etf_flow_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricHoldingsTonnes, etfRegionNorthAmerica),
			Title:     "WGC Gold ETF: запасы (holdings) в тоннах, Северная Америка",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginHoldingsTonnes,
			Description: etfDescription(
				"Запасы (holdings) физически обеспеченных золотых ETF на конец месяца, тонны, регион «Северная Америка»: сток, а не поток — значение неотрицательно и направления не несёт. Дата — последний день отчётного месяца. Изменение запасов месяц к месяцу соответствует потоку etf_flow_tonnes_north_america. Региональная часть мирового etf_holdings_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricAumUsdBn, etfRegionNorthAmerica),
			Title:     "WGC Gold ETF: активы под управлением (AUM), Северная Америка",
			Unit:      etfUnitUsdBn,
			Frequency: "M",
			Origin:    etfOriginAum,
			Description: etfDescription(
				"Активы под управлением (AUM) физически обеспеченных золотых ETF на конец месяца, млрд USD, регион «Северная Америка»: сток, а не поток; включает переоценку запасов по рыночной цене золота, поэтому меняется и в месяцы без потока. Дата — последний день отчётного месяца. Не путать с потоком etf_flow_usd_mln_north_america (приток/отток за месяц, млн USD): AUM — это размер фондов, а не движение денег. Региональная часть мирового etf_aum_usd_bn_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowUsdMln, etfRegionEurope),
			Title:     "WGC Gold ETF: чистый поток, Европа",
			Unit:      etfUnitUsdMln,
			Frequency: "M",
			Origin:    etfOriginFlowUsdMln,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF, млн USD, регион «Европа»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Региональная часть мирового etf_flow_usd_mln_world: сумма north_america + europe + asia + other обязана совпадать с мировым рядом — расхождение означает пропущенный регион.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowTonnes, etfRegionEurope),
			Title:     "WGC Gold ETF: чистый поток в тоннах, Европа",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginFlowTonnes,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF в тоннах, регион «Европа»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Тот же поток, что etf_flow_usd_mln_europe, только в тоннах: знаки обоих рядов обязаны совпадать — цена месяца не переводит приток в отток. Региональная часть мирового etf_flow_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricHoldingsTonnes, etfRegionEurope),
			Title:     "WGC Gold ETF: запасы (holdings) в тоннах, Европа",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginHoldingsTonnes,
			Description: etfDescription(
				"Запасы (holdings) физически обеспеченных золотых ETF на конец месяца, тонны, регион «Европа»: сток, а не поток — значение неотрицательно и направления не несёт. Дата — последний день отчётного месяца. Изменение запасов месяц к месяцу соответствует потоку etf_flow_tonnes_europe. Региональная часть мирового etf_holdings_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricAumUsdBn, etfRegionEurope),
			Title:     "WGC Gold ETF: активы под управлением (AUM), Европа",
			Unit:      etfUnitUsdBn,
			Frequency: "M",
			Origin:    etfOriginAum,
			Description: etfDescription(
				"Активы под управлением (AUM) физически обеспеченных золотых ETF на конец месяца, млрд USD, регион «Европа»: сток, а не поток; включает переоценку запасов по рыночной цене золота, поэтому меняется и в месяцы без потока. Дата — последний день отчётного месяца. Не путать с потоком etf_flow_usd_mln_europe (приток/отток за месяц, млн USD): AUM — это размер фондов, а не движение денег. Региональная часть мирового etf_aum_usd_bn_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowUsdMln, etfRegionAsia),
			Title:     "WGC Gold ETF: чистый поток, Азия",
			Unit:      etfUnitUsdMln,
			Frequency: "M",
			Origin:    etfOriginFlowUsdMln,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF, млн USD, регион «Азия»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Региональная часть мирового etf_flow_usd_mln_world: сумма north_america + europe + asia + other обязана совпадать с мировым рядом — расхождение означает пропущенный регион.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowTonnes, etfRegionAsia),
			Title:     "WGC Gold ETF: чистый поток в тоннах, Азия",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginFlowTonnes,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF в тоннах, регион «Азия»: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Тот же поток, что etf_flow_usd_mln_asia, только в тоннах: знаки обоих рядов обязаны совпадать — цена месяца не переводит приток в отток. Региональная часть мирового etf_flow_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricHoldingsTonnes, etfRegionAsia),
			Title:     "WGC Gold ETF: запасы (holdings) в тоннах, Азия",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginHoldingsTonnes,
			Description: etfDescription(
				"Запасы (holdings) физически обеспеченных золотых ETF на конец месяца, тонны, регион «Азия»: сток, а не поток — значение неотрицательно и направления не несёт. Дата — последний день отчётного месяца. Изменение запасов месяц к месяцу соответствует потоку etf_flow_tonnes_asia. Региональная часть мирового etf_holdings_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricAumUsdBn, etfRegionAsia),
			Title:     "WGC Gold ETF: активы под управлением (AUM), Азия",
			Unit:      etfUnitUsdBn,
			Frequency: "M",
			Origin:    etfOriginAum,
			Description: etfDescription(
				"Активы под управлением (AUM) физически обеспеченных золотых ETF на конец месяца, млрд USD, регион «Азия»: сток, а не поток; включает переоценку запасов по рыночной цене золота, поэтому меняется и в месяцы без потока. Дата — последний день отчётного месяца. Не путать с потоком etf_flow_usd_mln_asia (приток/отток за месяц, млн USD): AUM — это размер фондов, а не движение денег. Региональная часть мирового etf_aum_usd_bn_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowUsdMln, etfRegionOther),
			Title:     "WGC Gold ETF: чистый поток, прочие регионы",
			Unit:      etfUnitUsdMln,
			Frequency: "M",
			Origin:    etfOriginFlowUsdMln,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF, млн USD, регион «прочие регионы» (other — всё, что WGC не относит к Северной Америке, Европе и Азии): знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Региональная часть мирового etf_flow_usd_mln_world: сумма north_america + europe + asia + other обязана совпадать с мировым рядом — расхождение означает пропущенный регион.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricFlowTonnes, etfRegionOther),
			Title:     "WGC Gold ETF: чистый поток в тоннах, прочие регионы",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginFlowTonnes,
			Description: etfDescription(
				"Чистый месячный поток физически обеспеченных золотых ETF в тоннах, регион «прочие регионы» (other): знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Тот же поток, что etf_flow_usd_mln_other, только в тоннах: знаки обоих рядов обязаны совпадать — цена месяца не переводит приток в отток. Региональная часть мирового etf_flow_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricHoldingsTonnes, etfRegionOther),
			Title:     "WGC Gold ETF: запасы (holdings) в тоннах, прочие регионы",
			Unit:      etfUnitTonnes,
			Frequency: "M",
			Origin:    etfOriginHoldingsTonnes,
			Description: etfDescription(
				"Запасы (holdings) физически обеспеченных золотых ETF на конец месяца, тонны, регион «прочие регионы» (other): сток, а не поток — значение неотрицательно и направления не несёт. Дата — последний день отчётного месяца. Изменение запасов месяц к месяцу соответствует потоку etf_flow_tonnes_other. Региональная часть мирового etf_holdings_tonnes_world.",
				etfCatalogImportNote),
		},
		{
			Source:    wgcSource,
			Series:    wgcEtfSeriesName(etfMetricAumUsdBn, etfRegionOther),
			Title:     "WGC Gold ETF: активы под управлением (AUM), прочие регионы",
			Unit:      etfUnitUsdBn,
			Frequency: "M",
			Origin:    etfOriginAum,
			Description: etfDescription(
				"Активы под управлением (AUM) физически обеспеченных золотых ETF на конец месяца, млрд USD, регион «прочие регионы» (other): сток, а не поток; включает переоценку запасов по рыночной цене золота, поэтому меняется и в месяцы без потока. Дата — последний день отчётного месяца. Не путать с потоком etf_flow_usd_mln_other (приток/отток за месяц, млн USD): AUM — это размер фондов, а не движение денег. Региональная часть мирового etf_aum_usd_bn_world.",
				etfCatalogImportNote),
		},
	}
}

// Единицы рядов ЦБ и происхождение каждого ряда. origin называет источник, файл и лист:
// без листа агент не отличил бы дельты (Changes/Monthly) от уровней (holdings/PDF) —
// это разные файлы с разными датами и числами.
const (
	cbOriginChanges  = "WGC Gold Reserves by Country (IMF IFS + правки WGC), файл " + cbChangesMask + ", лист " + cbChangesSheet
	cbOriginHoldings = "WGC Gold Reserves by Country (IMF IFS + правки WGC), файл " + cbHoldingsMask + ", лист " + cbHoldingsSheet
)

// cbCatalogNotImf — общая оговорка каталога: рядом живёт imf-ряд в USD и только по
// Китаю; агент не должен их складывать или считать одним источником.
const cbCatalogNotImf = "Не путать с v_imf_macro (IMF IRCFL, source='imf'): там денежное золото в USD и через datasource заполнен только Китай; здесь — тонны по семи странам (POL, CZE, TUR, RUS, KAZ, UZB, CHN) из выгрузки WGC."

// cbCatalogImportNote — общий хвост описания: откуда берутся файлы и кто пишет ряды.
// Повторяется в каждой записи намеренно — агент читает description по одному ряду и
// соседние записи не видит.
const cbCatalogImportNote = "Источник — WGC Gold Reserves by Country (IMF IFS + правки WGC); импортёр wgc_cb_reserves (расписание dagu/wgc.yaml, пн 20:17) читает в wgc/data свежайшие по mtime выгрузки " + cbChangesMask + " и " + cbHoldingsMask + " (скачивание с gold.org/goldhub — только браузером) и пишет ряды с source='wgc'."

// cbCountryNotes — оговорки стран набора: особенность данных, о которой агент обязан
// узнать из каталога, а не из пропусков в ряду. Пустая строка — особенностей нет.
// Оговорка входит в описания обеих метрик страны намеренно (см. cbCatalogImportNote).
var cbCountryNotes = map[string]string{
	"TUR": "У Турции в выгрузке две строки: Turkey — net-ряд официального сектора (импортируется), Turkey* — gross, включая банки (не импортируется): ряд показывает изменение официальных резервов, а не валовые покупки сектора.",
	"RUS": "У России в выгрузке пропущены месяцы 2025-12, 2026-01, 2026-02 и 2026-03 (пустая ячейка — нет данных за месяц, а не нулевое изменение): точек за них нет.",
	"UZB": "У Узбекистана значения начинаются с 2013-02 (фактическое покрытие выгрузки): точек за более ранние месяцы нет, и это не нули.",
}

// cbDescription склеивает непустые части описания ряда в один абзац: у стран без
// оговорок часть пуста и в текст не попадает.
func cbDescription(parts ...string) string {
	nonEmpty := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	return strings.Join(nonEmpty, " ")
}

// cbCatalogForPoints — каталог ровно по тем метрикам, у которых в прогоне есть точки:
// выгрузки ЦБ появляются в wgc/data по отдельности, и на первом запуске может быть только
// одна из них — описания рядов второй метрики обещали бы данные, которых в macro_series
// нет. Полный набор (обе выгрузки) даёт все 14 записей. Порядок — как в CbSeriesNames():
// по странам словаря, внутри страны сначала дельты, затем уровни. Гранулярность — метрика,
// а не отдельный ряд: страна на месяц выпадает из топ-100 holdings (это warn парсера), и
// пропажа её описания из каталога теряла бы карту данных. Смыслы — из спеки §3.2/§3.3.
func cbCatalogForPoints(points []seriesPoint) []util.SeriesMeta {
	metricsWithData := make(map[string]bool, len(cbMetrics))
	for _, metric := range cbMetrics {
		// Префикс имён рядов метрики: cb_<метрика>_.
		prefix := wgcCbSeriesName(metric, "")
		for _, p := range points {
			if strings.HasPrefix(p.Series, prefix) {
				metricsWithData[metric] = true
				break
			}
		}
	}

	catalog := make([]util.SeriesMeta, 0, len(cbCountries)*len(cbMetrics))
	for _, c := range cbCountries {
		for _, metric := range cbMetrics {
			if metricsWithData[metric] {
				catalog = append(catalog, cbSeriesMeta(c, metric))
			}
		}
	}
	return catalog
}

// cbSeriesMeta — описание одной метрики страны: различаются только название, происхождение
// и тело, оговорки источника и страны у всех записей общие.
func cbSeriesMeta(c CbCountry, metric string) util.SeriesMeta {
	var title, origin, body string

	switch metric {
	case cbMetricChangeTonnes:
		title = "Золотые резервы ЦБ: изменение за месяц — " + c.Country
		origin = cbOriginChanges
		body = "Месячное изменение официальных золотых резервов ЦБ, тонны, " + c.Country +
			": знак — направление (плюс — покупка, минус — продажа). Дата — последний день отчётного месяца. " +
			"Пустая ячейка выгрузки — нет данных за месяц (точки нет), а не нулевое изменение; явный 0 — значение. " +
			"Уровень резервов — отдельный ряд " + wgcCbSeriesName(cbMetricHoldingsTonnes, c.ISO) + "."
	case cbMetricHoldingsTonnes:
		title = "Золотые резервы ЦБ: уровень — " + c.Country
		origin = cbOriginHoldings
		body = "Официальные золотые резервы ЦБ, тонны, " + c.Country +
			": сток на дату «Holdings as of» страны, приведённую к концу отчётного месяца. " +
			"Дата уровня может отставать от последней дельты ряда " + wgcCbSeriesName(cbMetricChangeTonnes, c.ISO) +
			": уровни и дельты приходят из разных выгрузок, и их даты не обязаны совпадать."
	}

	return util.SeriesMeta{
		Source:      wgcSource,
		Series:      wgcCbSeriesName(metric, c.ISO),
		Title:       title,
		Unit:        cbUnitTonnes,
		Frequency:   "M",
		Origin:      origin,
		Description: cbDescription(body, cbCountryNotes[c.ISO], cbCatalogNotImf, cbCatalogImportNote),
	}
}
