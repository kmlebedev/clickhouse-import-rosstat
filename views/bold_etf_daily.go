package views

import (
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// boldETFSeriesPair — пара «код ряда macro_series → колонка витрины» одного из семи
// рядов контура bold.report (source='bold', спека §4.1; описания рядов — в
// bold/series_meta.go).
type boldETFSeriesPair struct {
	Series string
	Metric string
}

// boldETFSeries — семь пар контура в порядке колонок витрины: поток дня (тонны),
// тот же поток в USD, окна 7/30/90 календарных дней и два стока (запасы и AUM).
// Срез один и на mapping-массив пивота, и на IN-фильтр: ряд, выпавший из него,
// исчезает из выдачи молча — колонка осталась бы в SELECT и вечно отдавала NULL,
// потому что «пусто» и «не публикуется» в длинном формате неразличимы. Поэтому
// набор сверяется тестом не с копией самого себя, а с объявлениями имён в файле
// импортёра (../bold/parse.go), который эти ряды пишет.
var boldETFSeries = []boldETFSeriesPair{
	{"etf_bold_flow_tonnes", "flow_tonnes"},
	{"etf_bold_flow_usd_mln", "flow_usd_mln"},
	{"etf_bold_flow_7d_tonnes", "flow_7d_tonnes"},
	{"etf_bold_flow_30d_tonnes", "flow_30d_tonnes"},
	{"etf_bold_flow_90d_tonnes", "flow_90d_tonnes"},
	{"etf_bold_holdings_tonnes", "holdings_tonnes"},
	{"etf_bold_aum_usd_bn", "aum_usd_bn"},
}

// boldETFSeriesNamesSQL — те же семь кодов строкой для IN (...). Ограничение списка
// именами набора, а не LIKE 'etf_bold_%': посторонний ряд с тем же префиксом (ручной
// ввод, будущий импортёр) не должен попадать в пивот.
func boldETFSeriesNamesSQL() string {
	quoted := make([]string, 0, len(boldETFSeries))

	for _, pair := range boldETFSeries {
		quoted = append(quoted, "'"+pair.Series+"'")
	}

	return strings.Join(quoted, ", ")
}

// boldETFSeriesMapSQL — пары (series, metric) для ARRAY JOIN: из этого массива
// витрина узнаёт, какой колонке принадлежит значение ряда. Без разметки пивот не
// смог бы разложить длинный формат macro_series (одно значение на ряд) по колонкам.
func boldETFSeriesMapSQL() string {
	rows := make([]string, 0, len(boldETFSeries))

	for _, pair := range boldETFSeries {
		rows = append(rows, "('"+pair.Series+"', '"+pair.Metric+"')")
	}

	return strings.Join(rows, ", ")
}

// goldEtfFlowsDailyView — дневной пивот семи рядов контура bold.report: одна строка
// на торговый день источника с восемью колонками. Структура — как у v_gold_etf_flows
// (mapping-массив (series, metric) + IN (...) с именами набора): macro_series хранит
// длинный формат, и раскладывает его по колонкам сама витрина.
//
// Отличия от месячной WGC-витрины принципиальны и обе — свойства источника, а не
// дефекты: (1) источник один — source='bold', поэтому приоритет источников
// (coalesce по wgc > webbridge > manual) здесь не нужен, а версии ReplacingMergeTree
// схлопывает FINAL; (2) maxIfOrNull, а не maxIf: maxIf при отсутствии совпадений
// вернул бы 0 и подменил бы «ряда за этот день нет» нулевым потоком. Вложенный
// подзапрос отдаёт по одной строке на (date, series): FINAL — по определению
// ReplacingMergeTree, а WHERE pin-ит source, поэтому дублей ключа быть не может.
var goldEtfFlowsDailyView = util.View{
	Name:   "v_gold_etf_flows_daily",
	Tables: []string{"macro_series"},
	Select: `SELECT
    v.date AS date,
    maxIfOrNull(v.value, m.metric = 'flow_tonnes') AS flow_tonnes,
    maxIfOrNull(v.value, m.metric = 'flow_usd_mln') AS flow_usd_mln,
    maxIfOrNull(v.value, m.metric = 'flow_7d_tonnes') AS flow_7d_tonnes,
    maxIfOrNull(v.value, m.metric = 'flow_30d_tonnes') AS flow_30d_tonnes,
    maxIfOrNull(v.value, m.metric = 'flow_90d_tonnes') AS flow_90d_tonnes,
    maxIfOrNull(v.value, m.metric = 'holdings_tonnes') AS holdings_tonnes,
    maxIfOrNull(v.value, m.metric = 'aum_usd_bn') AS aum_usd_bn
FROM (
    SELECT x.1 AS series, x.2 AS metric
    FROM (SELECT arrayJoin([` + boldETFSeriesMapSQL() + `]) AS x)
) AS m
INNER JOIN (
    SELECT date, series, value
    FROM macro_series FINAL
    WHERE source = 'bold' AND series IN (` + boldETFSeriesNamesSQL() + `)
) AS v ON v.series = m.series
GROUP BY date
ORDER BY date DESC`,
	Comment: "Дневной агрегат золотых ETF контура bold.report (ByteTree): одна строка на торговый день источника — пивот семи рядов etf_bold_* из macro_series (source='bold'). " +
		"Ряды пишет импортёр bold_etf_flows (расписание dagu/bold.yaml, ежедневно 08:07 МСК; данные T+1, ключей у источника нет, режим щадящий — 4 запроса на инкрементный прогон), описания рядов и единиц — в v_series_catalog (source='bold'). " +
		"Агрегат 73 фондов (в списке GLD, IAU, IGLN, GLDM, SGLD, 4GLD, PHYS, 518880, GOLD, 1540, ZGLD, SGOL) — ≈83 % мировой величины WGC: не складывать и не сравнивать напрямую с etf_holdings_tonnes_world/etf_flow_tonnes_world (там все фонды мира и месячная частота). " +
		"flow_tonnes — дневной поток: Δ совокупных запасов (units) между соседними торговыми днями, переведённая в тонны (÷32150,7465686); знак — направление (плюс приток); у первой точки ряда потока нет, поэтому колонка NULL. flow_usd_mln — тот же поток, оценённый ценой дня источника ((units_t − units_{t−1}) × price_t / 1e6): это не стоимость сделок и не объём торгов; нет цены дня — нет и точки USD-потока (тоннажный поток при этом пишется). " +
		"flow_7d_tonnes/flow_30d_tonnes/flow_90d_tonnes — окна источника как опубликованы (culm7day/culm30day/culm90day ÷32150,7465686), без пересчёта из нашего дневного ряда: окна календарные (7/30/90 дней), поэтому сумма пяти торговых дней ≠ culm7day, а окна пересекаются — значения соседних дней суммировать нельзя. " +
		"holdings_tonnes (units ÷32150,7465686) и aum_usd_bn (aum ÷1e9) — стоки на конец дня; AUM источник считает по своим ценам фондов, поэтому с нашим рядом gold/bold он может расходиться (метод цены источником не раскрыт — «Futures Price»). " +
		"Глубина: строк до 2015-04-01 у источника нет вовсе, а первые 64 дня истории (2015-01-01…2015-03-31) несут только цену — точек в витрине у них не появляется; выходные и праздники источник опускает, поэтому день без ряда в выдаче отсутствует (а не NULL). NULL в колонке означает «ряда за этот день нет» (не 0: ноль означал бы нулевой поток). Порядок строк — date DESC (детерминированная выдача). " +
		"Пер-фондовая детализация того же дня — в v_gold_etf_funds_daily (73 фонда; там унции и USD, здесь тонны и млрд USD)",
	Columns: map[string]string{
		"date":            "Торговый день источника (Date32) — дата точки рядов etf_bold_*; за выходные и праздники строк нет",
		"flow_tonnes":     "Дневной поток: Δ совокупных запасов 73 фондов между соседними торговыми днями, тонны; знак — направление (плюс приток); NULL — у первой точки ряда потока нет или ряда за день нет",
		"flow_usd_mln":    "Тот же дневной поток, оценённый ценой дня источника, млн USD: не стоимость сделок и не объём торгов; NULL — ряда или цены дня нет",
		"flow_7d_tonnes":  "Поток за 7 календарных дней, как публикует источник (culm7day), тонны; окно календарное — сумма 5 торговых дней ему не равна; NULL — ряда за день нет",
		"flow_30d_tonnes": "Поток за 30 календарных дней, как публикует источник (culm30day), тонны; окно календарное и пересекается с 7/90-дневным — суммировать нельзя; NULL — ряда за день нет",
		"flow_90d_tonnes": "Поток за 90 календарных дней, как публикует источник (culm90day), тонны; окно календарное (отклонение от Δ за 90 календарных дней — свойство источника); NULL — ряда за день нет",
		"holdings_tonnes": "Совокупные запасы 73 фондов на конец дня (сток), тонны: ≈83 % мировой величины WGC — не складывать с etf_holdings_tonnes_world (все фонды мира, месячная частота); NULL — ряда за день нет",
		"aum_usd_bn":      "Совокупный AUM 73 фондов на конец дня (сток), млрд USD: источник считает его по своим ценам фондов, с нашим gold/bold возможны расхождения; NULL — ряда за день нет",
	},
}

// v_gold_etf_funds_daily — пер-фондовый срез контура bold.report: одна строка на
// (дата, фонд), 73 фонда за торговый день. Таблица-источник — bold_fund_snapshot
// (её пишет тот же импортёр bold_etf_flows), а не macro_series: у среза свой ключ
// (date, ticker, name), потому что тикер у источника не уникален (GLD в одном срезе
// трижды). FINAL схлопывает версии ReplacingMergeTree по этому ключу. Витрина
// отдаёт 10 из 11 колонок среза: price_change_usd в срез пишется, но в выдачу не
// выведен (цену дня агент берёт колонкой price_usd).
var boldFundsDailyView = util.View{
	Name:   "v_gold_etf_funds_daily",
	Tables: []string{"bold_fund_snapshot"},
	Select: `SELECT
    date,
    ticker,
    name,
    units_oz,
    aum_usd,
    price_usd,
    units_change_oz,
    aum_change_usd,
    shares,
    shares_change
FROM bold_fund_snapshot FINAL
ORDER BY date DESC, ticker, name`,
	Comment: "Пер-фондовый дневной срез золотых ETF контура bold.report (ByteTree): одна строка на (дата, фонд) — 73 фонда источника. " +
		"Источник — bold.report/api/v1/gold/summary (у файла среза нет поля даты: дату приносит ответ «последнего дня», а расхождение дат — ошибка импортёра, а не тихая запись под чужим числом); пишет срез импортёр bold_etf_flows (расписание dagu/bold.yaml, ежедневно 08:07 МСК), ключей у источника нет. " +
		"История копится с первого прогона: у источника пер-фондовой истории нет — только последний день, поэтому глубина витрины растёт по одному торговому дню за прогон, начиная с 2026-10. " +
		"Тикер фонда у источника НЕ уникален: в одном срезе три разных фонда называются GLD (KTAM Gold ETF Tracker Class GLD, NewGold Issuer Ltd, SPDR Gold Shares), поэтому фонд определяется парой (ticker, name), а не тикером, и в выдаче одного дня GLD встречается трижды. " +
		"Агрегат 73 фондов — ≈83 % мировой величины WGC: не складывать и не сравнивать напрямую с etf_holdings_tonnes_world/etf_flow_tonnes_world (там все фонды мира и месячная частота). " +
		"units_oz — металл фонда в унциях (Total Units, Oz); aum_usd — AUM фонда в USD; price_usd — цена фонда источника («Futures Price»): конкретный метод, контракт и время снимка источником не раскрыты, это не спот и не наша цена золота comex_front; units_change_oz, aum_change_usd, shares, shares_change — дневные изменения и число паёв как публикует источник. " +
		"Единицы этой витрины — унции и USD; агрегат контура в тоннах и млрд USD — в v_gold_etf_flows_daily. Все поля среза у источника обязательны, поэтому NULL в колонках витрины не бывает: отсутствует не значение, а строка — день, который источник не публиковал (выходные и праздники он опускает). Порядок строк — date DESC, ticker, name (детерминированная выдача)",
	Columns: map[string]string{
		"date":            "Дата среза (Date32) из ответа «последнего дня» источника: под ней записан весь пер-фондовый срез, у самого файла среза даты нет",
		"ticker":          "Тикер фонда как публикует источник; НЕ уникален — в срезе три фонда с тикером GLD, поэтому строку определяет пара (ticker, name)",
		"name":            "Имя фонда источника: различает фонды с одинаковым тикером (для GLD — KTAM Gold ETF Tracker Class GLD, NewGold Issuer Ltd, SPDR Gold Shares)",
		"units_oz":        "Металл фонда в унциях (Total Units, Oz) на дату среза: сток, не поток",
		"aum_usd":         "Активы фонда (AUM) в USD на дату среза: сток; источник считает его по своим ценам фондов",
		"price_usd":       "Цена фонда источника («Futures Price»), USD: метод, контракт и время снимка источником не раскрыты — это не спот и не цена закрытия",
		"units_change_oz": "Изменение металла фонда к предыдущему торговому дню, унции: знак — направление (плюс приток); как публикует источник",
		"aum_change_usd":  "Изменение AUM фонда к предыдущему торговому дню, USD: как публикует источник (включает переоценку по цене)",
		"shares":          "Число паёв (shares) фонда на дату среза: как публикует источник",
		"shares_change":   "Изменение числа паёв фонда к предыдущему торговому дню: как публикует источник",
	},
}
