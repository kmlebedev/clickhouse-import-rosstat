package views

import (
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/kmlebedev/clickhouse-import-rosstat/wgc"
)

// cbGoldChangePrefix / cbGoldHoldingsPrefix — префиксы имён рядов набора ЦБ:
// в macro_series лежат cb_gold_change_tonnes_<iso3> (месячные дельты, со знаком) и
// cb_gold_holdings_tonnes_<iso3> (уровни). Витрина отрезает префикс от имени ряда,
// поэтому IN-список и length(...) в substring — части одного контракта: тест сверяет
// оба с wgc.CbSeriesNames(), и переименование метрики в импортёре роняет тест, а не
// молча отдаёт пустой iso3 (запрос-то остаётся синтаксически валидным).
const (
	cbGoldChangePrefix   = "cb_gold_change_tonnes_"
	cbGoldHoldingsPrefix = "cb_gold_holdings_tonnes_"
)

// cbGoldSeriesSQL — IN-список рядов одной метрики (по префиксу) в порядке
// wgc.CbSeriesNames(). Ограничение списка именами набора, а не LIKE 'cb_gold_%':
// чужие/посторонние ряды с тем же префиксом не должны попадать в выдачу стран.
func cbGoldSeriesSQL(prefix string) string {
	names := wgc.CbSeriesNames()
	quoted := make([]string, 0, len(names))

	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			quoted = append(quoted, "'"+name+"'")
		}
	}

	return strings.Join(quoted, ", ")
}

// cbGoldIsoSQL — выражение iso3 из имени ряда: отрезает префикс метрики. Длина
// префикса берётся из той же константы, что и IN-список (см. выше).
func cbGoldIsoSQL(prefix string) string {
	return "substring(series, length('" + prefix + "') + 1)"
}

// cbGoldCountriesSQL — mapping-массив пар (iso3, русское имя) в порядке словаря
// wgc.CbCountries(): из него витрина берёт страны выдачи. LEFT JOIN от этого массива
// даёт ровно одну строку на страну, даже когда рядов страны в macro_series нет
// (тогда метрики NULL): пропавшая страна видна, а не исчезает из выдачи.
func cbGoldCountriesSQL() string {
	countries := wgc.CbCountries()
	pairs := make([]string, 0, len(countries))

	for _, c := range countries {
		pairs = append(pairs, "('"+c.ISO+"', '"+c.Country+"')")
	}

	return strings.Join(pairs, ", ")
}

// cbGoldDynamicsView — динамика официальных золотых резервов семи ЦБ: одна строка на
// страну, MoM (последняя дельта) и YTD (сумма дельт с начала года последней точки) +
// последний уровень резервов и его дата. macro_series хранит длинный формат (одно
// значение на дату и ряд), поэтому витрина сама сводит два ряда страны в строку.
// Уровни приходят из отдельной выгрузки и датируются её датой «Holdings as of»
// (нормализованной к концу месяца), поэтому holdings_date может отставать от
// last_change_date — это факт источника, а не дефект витрины.
//
// YTD считается по каждому ряду отдельно: год берётся у max(date) самого ряда
// (окно OVER (PARTITION BY series)), а не у всей выборки, — у стран разные последние
// месяцы (RUS с пропусками, UZB короче), и общий год посчитал бы отставшим странам
// неполный YTD. Вложенный агрегат (max(max_d) внутри sumIf) ClickHouse не принимает,
// поэтому дата последней точки приходит столбцом из подзапроса.
//
// Присоединённые метрики отдаются через if(n > 0, …, NULL): LEFT JOIN ClickHouse по
// умолчанию (join_use_nulls = 0) заполняет несовпавшую строку значениями по умолчанию,
// и страна без ряда получила бы mom_change_tonnes = 0 и last_change_date = 1970-01-01 —
// «нулевое изменение» вместо «данных нет» (та же конвенция, что в v_gold_dashboard,
// где значение оборачивается в if(n > 0, …, NULL)). n — счётчик точек страны,
// приходящий из того же подзапроса, что и метрики.
var cbGoldDynamicsView = util.View{
	Name:   "v_cb_gold_dynamics",
	Tables: []string{"macro_series"},
	Select: `SELECT
    m.iso3 AS iso3,
    m.country AS country,
    if(c.n > 0, c.last_date, NULL) AS last_change_date,
    if(c.n > 0, c.mom_change_tonnes, NULL) AS mom_change_tonnes,
    if(c.n > 0, c.ytd_change_tonnes, NULL) AS ytd_change_tonnes,
    if(h.n > 0, h.holdings_date, NULL) AS holdings_date,
    if(h.n > 0, h.holdings_tonnes, NULL) AS holdings_tonnes
FROM (
    SELECT x.1 AS iso3, x.2 AS country
    FROM (SELECT arrayJoin([` + cbGoldCountriesSQL() + `]) AS x)
) AS m
LEFT JOIN (
    SELECT iso3,
           count() AS n,
           max(d) AS last_date,
           argMax(v, d) AS mom_change_tonnes,
           sumIf(v, d >= toStartOfYear(md)) AS ytd_change_tonnes
    FROM (
        SELECT ` + cbGoldIsoSQL(cbGoldChangePrefix) + ` AS iso3,
               date AS d,
               value AS v,
               max(date) OVER (PARTITION BY series) AS md
        FROM macro_series FINAL
        WHERE source = 'wgc' AND series IN (` + cbGoldSeriesSQL(cbGoldChangePrefix) + `)
    )
    GROUP BY iso3
) AS c ON c.iso3 = m.iso3
LEFT JOIN (
    SELECT ` + cbGoldIsoSQL(cbGoldHoldingsPrefix) + ` AS iso3,
           count() AS n,
           max(date) AS holdings_date,
           argMax(value, date) AS holdings_tonnes
    FROM macro_series FINAL
    WHERE source = 'wgc' AND series IN (` + cbGoldSeriesSQL(cbGoldHoldingsPrefix) + `)
    GROUP BY iso3
) AS h ON h.iso3 = m.iso3
ORDER BY iso3`,
	Comment: "Динамика официальных золотых резервов семи центральных банков (POL, CZE, TUR, RUS, KAZ, UZB, CHN): одна строка на страну словаря набора. " +
		"Источник — WGC Gold Reserves by Country (IMF IFS + правки WGC), ряды лежат в macro_series с source='wgc': их пишет импортёр wgc_cb_reserves (расписание dagu/wgc.yaml, ежедневно 20:17 МСК) из выгрузок Changes_latest_as_of_*.xlsx (месячные изменения, лист Monthly) и World_official_gold_holdings_as_of_*.xlsx (уровни, лист PDF) в папке wgc/data — скачивание с gold.org/goldhub только браузером, вручную. Прогон читает свежайший по mtime файл, поэтому ряды ЦБ обновляются после очередного ручного скачивания, а не по расписанию: пока выгрузки на месте, ежедневный прогон идемпотентен — перезаписываются те же ключи и значения, поэтому в логе каждый раз Imported N rows of wgc_cb_reserves, а точки за новый отчётный месяц появляются только после очередной выгрузки в wgc/data; когда файлов нет вовсе, прогон печатает warn и 0 строк, и dagu остаётся зелёным. " +
		"mom_change_tonnes — изменение резервов за последний отчётный месяц (MoM, тонны): знак — направление (плюс покупка, минус продажа), last_change_date — дата этой дельты (последний день отчётного месяца). " +
		"ytd_change_tonnes — сумма месячных дельт с 01.01 года последней точки страны включительно (YTD, тонны): накопленное с начала года изменение резервов. " +
		"holdings_tonnes — последний уровень (сток) резервов, holdings_date — его дата «Holdings as of» выгрузки уровней, приведённая к концу отчётного месяца: уровень может отставать от последней дельты (разные выгрузки). " +
		"Оговорки стран: TUR — net официального сектора (gross-ряд Turkey*, включая банки, не импортируется); RUS — в выгрузке пропущены месяцы 2025-12, 2026-01, 2026-02 и 2026-03, поэтому YTD считается по фактическим точкам; UZB — точки с 2013-02. " +
		"NULL в колонке — ряда за страну в macro_series нет (не 0: ноль означал бы нулевое изменение/уровень). Порядок строк — iso3 (детерминированная выдача). " +
		"Описания обоих рядов страны (единицы, происхождение, оговорки) — в v_series_catalog (source='wgc'): не путать с v_imf_macro (IMF IRFCL, USD, только Китай)",
	Columns: map[string]string{
		"iso3":              "ISO-код страны словаря набора ЦБ: POL, CZE, TUR, RUS, KAZ, UZB, CHN (ключ строки витрины)",
		"country":           "Русское имя страны (Польша, Чехия, Турция, Россия, Казахстан, Узбекистан, Китай)",
		"last_change_date":  "Дата последней месячной дельты резервов (последний день отчётного месяца); NULL — дельт за страну нет",
		"mom_change_tonnes": "Изменение резервов за последний отчётный месяц (MoM), тонны: знак — направление (плюс покупка, минус продажа); NULL — дельт нет",
		"ytd_change_tonnes": "Сумма месячных дельт с 01.01 года последней точки страны включительно (YTD), тонны: накопленное с начала года изменение резервов; NULL — дельт нет",
		"holdings_date":     "Дата последнего уровня резервов «Holdings as of» (конец отчётного месяца); может отставать от last_change_date — уровни и дельты из разных выгрузок; NULL — уровня нет",
		"holdings_tonnes":   "Уровень (сток) официальных золотых резервов на holdings_date, тонны; NULL — уровня нет",
	},
}
