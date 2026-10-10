package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// companyFinancialsTable — общая для сектора таблица метрик компаний. Числа в неё
// кладёт импортёр polyus/ (KPI-релизы и МСФО-отчёты), а читает их агент через
// витрины ниже: сырая таблица ему не выдана.
const companyFinancialsTable = "company_financials"

// companyFinancialsCreateTable — DDL таблицы с ПОДСТАВЛЕННЫМ именем: витрины
// создаёт импортёр company_views, который исполняет companyViewsDDL() сырым
// conn.Exec, без fmt.Sprintf. Поэтому имени здесь нет ни в форме %s, ни
// напечатанным руками: оно приходит в текст через конкатенацию с константой
// companyFinancialsTable (см. строку ниже), и искать в этом литерале плейсхолдер
// или отдельно записанное имя не нужно — его там нет.
//
// Список колонок, движок и ORDER BY обязаны совпадать с шаблоном из
// polyus/import.go: таблица одна на два импортёра, и разошедшаяся копия DDL
// завела бы её в двух формах.
const companyFinancialsCreateTable = `CREATE TABLE IF NOT EXISTS ` + companyFinancialsTable + `
(
    company     LowCardinality(String),
    metric      LowCardinality(String),
    period      String,
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    source_kind LowCardinality(String),
    value       Nullable(Float64),
    unit        LowCardinality(String),
    source_url  LowCardinality(String),
    source_page UInt16,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period, source_kind)`

// companyFinancialsView — основной вход DCF: метрика компании за период, когда
// значений несколько — одно, разрешённое по правилу spec §3.4.
//
// Разрешение идёт по ключу ПЕРВИЧНОГО индекса: argMax выбирает строку с
// наибольшим кортежем
//
//	(source_kind = 'ifrs', loaded_at, source_url)
//
// и вместе с ней возвращает значение и провенанс. Порядок полей кортежа — и есть
// правило: сначала аудированная отчётность (ifrs = 1) перебивает пресс-релиз
// (kpi = 0), затем побеждает более поздняя загрузка, а source_url стоит третьим
// только затем, чтобы выдача не «плавала» между прогонами при равных первых двух
// ключах.
//
// Внутренние агрегаты проецируются под именами r_* (r_value, r_source_kind, ...)
// и получают настоящие имена колонок только во внешнем SELECT. Это не
// косметика, а обход ловушки: если агрегат назвать так же, как его исходную
// колонку (argMax(value, ...) AS value), ClickHouse привязывает имя внутри
// кортежа-ключа к АЛИАСУ агрегата, а не к колонке таблицы, и падает на создании
// витрины:
//
//	Code: 184. DB::Exception: Aggregate function argMax(source_kind,
//	(source_kind = 'ifrs', loaded_at, source_url)) AS source_kind is found inside
//	another aggregate function in query. (ILLEGAL_AGGREGATION)
//
// Сравнение source_kind = 'ifrs' при этом совершенно ни при чём: clickhouse-go
// вставляет LowCardinality(String), в запросе колонка сравнивается со строковым
// литералом, и это работает как есть — никакого if() для этого не нужно.
//
// Отвергнутая альтернатива — row_number() OVER (PARTITION BY company, metric,
// period ORDER BY ...). Оконные функции над FINAL считаются одним потоком:
// 347 строк это терпит, но миллионы строк металлургов (см. §9 спеки) — уже нет,
// а argMax остаётся потоковой агрегацией.
var companyFinancialsView = util.View{
	Name:   "v_company_financials",
	Tables: []string{companyFinancialsTable},
	Select: `SELECT
    company,
    metric,
    period,
    r_period_type AS period_type,
    r_source_kind AS source_kind,
    r_value AS value,
    r_unit AS unit,
    r_source_url AS source_url,
    r_source_page AS source_page,
    r_loaded_at AS loaded_at
FROM (
    SELECT
        company,
        metric,
        period,
        argMax(period_type, (source_kind = 'ifrs', loaded_at, source_url)) AS r_period_type,
        argMax(source_kind, (source_kind = 'ifrs', loaded_at, source_url)) AS r_source_kind,
        argMax(value, (source_kind = 'ifrs', loaded_at, source_url)) AS r_value,
        argMax(unit, (source_kind = 'ifrs', loaded_at, source_url)) AS r_unit,
        argMax(source_url, (source_kind = 'ifrs', loaded_at, source_url)) AS r_source_url,
        argMax(source_page, (source_kind = 'ifrs', loaded_at, source_url)) AS r_source_page,
        argMax(loaded_at, (source_kind = 'ifrs', loaded_at, source_url)) AS r_loaded_at
    FROM ` + companyFinancialsTable + ` FINAL
    GROUP BY company, metric, period
)`,
	Comment: "Разрешённые метрики компаний по документам: одна строка на (company, metric, period). Приоритет источника: ifrs (аудит) > kpi (пресс-релиз), затем свежесть loaded_at, затем source_url — детерминированный тай-брейк. source_kind и source_url указывают, из какого документа взято значение. Проигравшие значения видны в v_company_metric_sources",
	Columns: map[string]string{
		"company":     "Компания-эмитент: 'PLZL' — Полюс. Пока заполняется только она; схема заводилась общей для сектора",
		"metric":      "Показатель: 'gold_output' — производство золота, koz; 'revenue' — выручка, USD млн, и др. Словарь метрик — polyus/metrics.go",
		"period":      "Отчётный период в канонической форме: '2026H1', '2024FY', '2024Q4'. Формируется из шапки страницы документа, сравнивать периоды между собой нужно как строки одного словаря",
		"period_type": "Тип периода: Q — квартал, H — полугодие, FY — год, LTM — скользящие 12 месяцев. Берётся из документа, а не выводится из period",
		"source_kind": "Вид документа, из которого взято значение: 'ifrs' — аудированная отчётность (побеждает), 'kpi' — пресс-релиз, 'legacy' — строки, перенесённые из polyus_financial_metrics. Идентифицирует документ-победитель вместе с source_url",
		"value":       "Значение показателя в единицах колонки unit; NULL — документ печатал прочерк или значение не прочиталось",
		"unit":        "Единица значения: 'koz' (тыс. тройских унций), 'USD million', 'USD/share', 'x' (коэффициент), '%'. Из словаря метрик",
		"source_url":  "URL документа, из которого взято значение (PDF релиза или отчёта). Вместе с source_kind и source_page задаёт провенанс документа-победителя",
		"source_page": "Номер страницы PDF, на которой напечатано значение; 0 — страница не донесена. Может быть на единицу больше номера в PDF: адрес страницы и номер листа считаются по-разному",
		"loaded_at":   "Момент загрузки строки в ClickHouse (UTC). Одно время на весь батч импортёра; позже побеждает — свежесть загрузки стоит вторым ключом разрешения",
	},
}

// companyMetricSourcesView — аудит расхождений: ВСЕ версии значения, включая
// проигравшие в v_company_financials.
//
// Правило разрешения здесь не применяется намеренно: два документа за один
// период печатают разные числа (у Полюса 2 902 и 2 799 за 2023FY), и витрина,
// которая оставила бы только победителя, сделала бы расхождение невидимым — агент
// не смог бы обнаружить спор и не смог бы его объяснить. Поэтому ни argMax,
// ни row_number, ни LIMIT BY: только FINAL и сортировка по ключу витрины.
var companyMetricSourcesView = util.View{
	Name:   "v_company_metric_sources",
	Tables: []string{companyFinancialsTable},
	Select: `SELECT
    company,
    metric,
    period,
    period_type,
    source_kind,
    value,
    unit,
    source_url,
    source_page,
    loaded_at
FROM ` + companyFinancialsTable + ` FINAL
ORDER BY company, metric, period, source_kind`,
	Comment: "Аудит расхождений company_financials: все версии метрики за период, включая проигравшую в v_company_financials, — сравнение документов. Правило приоритета не применяется: витрина нужна затем, чтобы расхождение документов (сравнительная база одного релиза против другого) оставалось видимым. source_kind и source_url идентифицируют документ каждой строки",
	Columns: map[string]string{
		"company":     "Компания-эмитент: 'PLZL' — Полюс. Пока заполняется только она; схема заводилась общей для сектора",
		"metric":      "Показатель: 'gold_output' — производство золота, koz; 'revenue' — выручка, USD млн, и др. Словарь метрик — polyus/metrics.go",
		"period":      "Отчётный период в канонической форме: '2026H1', '2024FY', '2024Q4'. Один период встречается в нескольких документах — это и есть предмет сверки",
		"period_type": "Тип периода: Q — квартал, H — полугодие, FY — год, LTM — скользящие 12 месяцев. Берётся из документа, а не выводится из period",
		"source_kind": "Вид документа, из которого пришла строка: 'ifrs' — аудированная отчётность, 'kpi' — пресс-релиз, 'legacy' — строки, перенесённые из polyus_financial_metrics. Идентифицирует документ вместе с source_url; по нему видно, чья версия победила в v_company_financials",
		"value":       "Значение показателя в единицах колонки unit у ЭТОГО документа; NULL — документ печатал прочерк или значение не прочиталось",
		"unit":        "Единица значения: 'koz' (тыс. тройских унций), 'USD million', 'USD/share', 'x' (коэффициент), '%'. Из словаря метрик",
		"source_url":  "URL документа, из которого пришла строка (PDF релиза или отчёта). Именно по нему видно, что сравниваются два разных документа, а не дубликат одной строки",
		"source_page": "Номер страницы PDF, на которой напечатано значение; 0 — страница не донесена. Может быть на единицу больше номера в PDF: адрес страницы и номер листа считаются по-разному",
		"loaded_at":   "Момент загрузки строки в ClickHouse (UTC); у двух документов за один период метки обычно разные — по ним видно, какая версия загружена позже",
	},
}

// companyOperatingView — операционка по активам из датапака Polyus: вход
// mine_plans. Это ДРУГАЯ гранулярность, чем у релизных метрик, и смешивать их
// нельзя.
//
// Датапак меряет показатель по активу (месторождению) за квартал, полугодие или
// год; релиз — консолидированно по компании за отчётный период. Имена показателей
// при этом совпадают: 'gold_output' есть и там, и там, но в датапаке это добыча
// Олимпады, а в релизе — всего Полюса. Поэтому источник и вынесен в отдельную
// витрину: UNION ALL заставил бы агента фильтровать по типу источника в каждом
// запросе, а забытый фильтр молча складывал бы добычу актива с добычей компании.
// company берётся константой: у датапака Полюса нет колонки эмитента, и на этой
// итерации датапак только один.
var companyOperatingView = util.View{
	Name:   "v_company_operating",
	Tables: []string{"databook_polyus"},
	Select: `SELECT
    'PLZL' AS company,
    table AS asset,
    name AS metric,
    data AS frequency,
    date,
    value
FROM databook_polyus FINAL`,
	Comment: "Операционка по активам Полюса из датапака (xlsx), вход mine_plans: одна строка на (asset, metric, frequency, date). Все периоды с 2007: кварталы, полугодия и годы. Это ДРУГАЯ гранулярность, чем у v_company_financials: asset — месторождение или сводная таблица, а метрика показана за один период без сравнительной базы. С релизными метриками НЕ смешивать: одинаковые имена (например gold_output) означают здесь добычу актива, а не компании",
	Columns: map[string]string{
		"company":   "Компания-эмитент: всегда 'PLZL' — Полюс, константой. У датапака нет колонки эмитента, а датапак на этой итерации один",
		"asset":     "Актив: 'CONSOLIDATED OPERATING RESULTS' — сводные итоги компании, остальные — месторождения ('OLIMPIADA', 'BLAGODATNOYE', 'TITIMUKHTA', 'VERNINSKOYE2', 'ALLUVIALS', 'KURANAKH', 'ZAPADNOYE', 'NATALKA', 'Sukhoi Log'). Это ДРУГАЯ гранулярность, чем метрики релиза: показатель отнесён к активу, а не ко всей компании, и смешивать эти два уровня нельзя — у них совпадают имена метрик",
		"metric":    "Показатель датапака по активу: 'Total rock moved', 'Ore processed', 'Total Dore gold output', 'Total refined gold output' — плюс сводные ('Total gold produced'). Имена могут совпадать с релизными, но смысл другой: здесь это величина по активу, там — консолидированная",
		"frequency": "Тип периода датапака: 'ANNUAL', 'QUARTERLY', 'SEMI-ANNUAL'. Ключ датапака — (актив, метрика, частота, дата): квартальный и годовой ряд одной метрики соседствуют, и пара (metric, date) у них совпадает, различает только эта колонка. Частота — не гранулярность витрины релиза: 2H'25 здесь означает полугодие актива, а не период отчётности",
		"date":      "Дата периода: годовое наблюдение — 1 декабря отчётного года, квартальное и полугодовое — первое число месяца, следующего за концом периода (4Q'07 → 2008-01-01). Это дата КОНЦА периода, а не дата публикации",
		"value":     "Значение показателя в единицах самого показателя (koz, kT, 000 m3, g/t, % — датапак печатает их подписью строки). Единица отдельной колонкой не вынесена: у датапака её нет. Значения с десятичной точкой — пересчёт из датапака, а не печать отчёта",
	},
}

// companyViewsDDL возвращает DDL таблицы витрин одной строкой на оператор —
// именно в том виде, в каком его исполняет импортёр company_views (raw conn.Exec,
// без fmt.Sprintf). Первым идёт CREATE TABLE: витрины читают её, и создавать их
// раньше незачем.
func companyViewsDDL() []string {
	return []string{companyFinancialsCreateTable}
}
