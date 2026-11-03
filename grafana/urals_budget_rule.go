package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/barchart"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// UralsBudgetRuleUID — uid дашборда «Urals: реальная цена и бюджетное правило» (пункт infra/5.15).
// Артефакт — dashboard/<uid>.json; uid попадает и в spec, и в metadata.name манифеста.
const UralsBudgetRuleUID = "urals-budget-rule"

// Id рядов вне диапазона панелей 1..7: нулевой id Grafana заменит своим, а совпавший с панелью
// сделает артефакт неотличимым от кода.
const (
	firstUralsBudgetRuleRowID  = 21
	secondUralsBudgetRuleRowID = 22
)

// Высота панелей в строках сетки Grafana: ECharts-графики — 10, stat-снимки — 4,
// месячный бар — 8 (спека §3.4; порядок и ширина — контракт).
const (
	uralsBudgetChartHeight = 10
	uralsBudgetStatHeight  = 4
	uralsBudgetBarHeight   = 8
)

// SQL-контракт панелей (спека §4.2–§4.8, дословно). Панели читают только витрины v_*: у датасорса
// стенда нет грантов на сырые таблицы — это же требует Validate.
//
// П1 и П5 делят дефлятор ИПЦ: накопленный уровень (`exp(sum(log(value / 100)) OVER …)`), база —
// последний опубликованный уровень (`argMax`). SETTINGS join_use_nulls = 1 обязателен: без него
// LEFT JOIN подставляет 0 вместо NULL, и оконный anyLast уносит нулевой дефлятор в октябрь.

// uralsBudgetRealPriceSQL — П1: реальная цена Urals в постоянных рублях по месяцам 2018–2026.
// $__dateFilter стоит в финальном SELECT на сырой date: окно дашборда не должно менять сезонную
// норму 2018–2022 (она считается по полному ряду).
const uralsBudgetRealPriceSQL = `WITH
    ipc AS (
        SELECT toDate32(date) AS date,
               exp(sum(log(value / 100)) OVER (ORDER BY date)) AS level
        FROM v_fedstat_ipc
        WHERE series = 'ipc31074/Все товары и услуги'
          AND (region = 'Российская Федерация' OR date >= '2023-01-01')
    ),
    base AS (SELECT argMax(level, date) AS level FROM ipc),
    usd AS (
        SELECT toDate32(toStartOfMonth(date)) AS month, avg(value) AS usd_avg
        FROM v_cbr_macro
        WHERE series = 'cbr_currency_usd'
        GROUP BY month
    ),
    monthly AS (
        SELECT u.date AS date, u.value AS urals, d.usd_avg AS usd_avg, i.level AS ipc_level
        FROM v_minec_macro AS u
        INNER JOIN usd AS d ON d.month = u.date
        LEFT JOIN ipc AS i ON i.date = u.date
        WHERE u.series = 'urals_avg_price'
    ),
    filled AS (
        SELECT date, urals, usd_avg,
               anyLast(ipc_level) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS level_used,
               toUInt8(ipc_level IS NULL) AS is_estimate
        FROM monthly
    ),
    seasonal AS (
        SELECT toMonth(date) AS month,
               round(avg(urals * usd_avg * (SELECT level FROM base) / level_used), 0) AS seasonal_norm
        FROM filled
        WHERE toYear(date) BETWEEN 2018 AND 2022
        GROUP BY month
    )
SELECT
    f.date AS "Месяц",
    toYear(f.date) AS "Год",
    toMonth(f.date) AS "Месяц года",
    round(f.urals * f.usd_avg * (SELECT level FROM base) / f.level_used, 0) AS "Реальная цена, ₽/барр.",
    s.seasonal_norm AS "Сезонная норма 2018–2022, ₽/барр.",
    f.is_estimate AS "Оценка"
FROM filled AS f
INNER JOIN seasonal AS s ON s.month = toMonth(f.date)
WHERE $__dateFilter(f.date)
ORDER BY f.date
SETTINGS join_use_nulls = 1
`

// uralsBudgetRuleSQL — П2: бюджетное правило Минфина по месяцам с 2018: Urals $/барр., часть до
// $59 (в бюджет) и превышение (в ФНБ). $59 — цена отсечения бюджетного правила Минфина для Urals
// (действует до 2027; The Bell, mn.ru; снижение до $50 обсуждается только с 2027). Это параметр
// правила, а не ряд витрины: в v_series_catalog его нет.
const uralsBudgetRuleSQL = `-- $59 — цена отсечения бюджетного правила Минфина для Urals (действует до 2027;
-- The Bell, mn.ru; снижение до $50 обсуждается только с 2027). Это параметр правила,
-- а не ряд витрины: в v_series_catalog его нет.
SELECT
    date AS "Месяц",
    value AS "Urals, $/барр.",
    least(value, 59) AS "В бюджет (≤ $59)",
    greatest(value - 59, 0) AS "В ФНБ (> $59)"
FROM v_minec_macro
WHERE series = 'urals_avg_price'
  AND $__dateFilter(date)
ORDER BY date
`

// uralsBudgetSpotSQL — П3: снимок последнего опубликованного месяца. argMax вместо
// ORDER BY … LIMIT 1: снимок без LIMIT (конвенция панелей-снимков, как fpModelRunSQL).
const uralsBudgetSpotSQL = `SELECT round(argMax(value, date), 2) AS "Urals, $/барр."
FROM v_minec_macro
WHERE series = 'urals_avg_price'
`

// uralsBudgetFxSQL — П4: средний за тот же месяц курс ЦБ. Дневной ряд усредняется по месяцу
// (toStartOfMonth + avg); месяц — последний Urals, а не «последний курс»: ряды публикуются в
// разные даты, и снимок обязан относиться к одному месяцу.
const uralsBudgetFxSQL = `SELECT round(avg(value), 2) AS "Курс, ₽/$"
FROM v_cbr_macro
WHERE series = 'cbr_currency_usd'
  AND toStartOfMonth(date) = (SELECT max(toDate(date)) FROM v_minec_macro WHERE series = 'urals_avg_price')
`

// uralsBudgetRealSnapshotSQL — П5: Urals в рублях за последний месяц и два г/г темпа (номинальный
// и реальный). Дефлятор — тот же, что в П1: октябрь считается по уровню сентября (октябрьский
// ИПЦ ещё не опубликован). SETTINGS join_use_nulls = 1 обязателен — без него LEFT JOIN дал бы
// нулевой дефлятор и реальный темп потерялся бы.
const uralsBudgetRealSnapshotSQL = `WITH
    ipc AS (
        SELECT toDate32(date) AS date,
               exp(sum(log(value / 100)) OVER (ORDER BY date)) AS level
        FROM v_fedstat_ipc
        WHERE series = 'ipc31074/Все товары и услуги'
          AND (region = 'Российская Федерация' OR date >= '2023-01-01')
    ),
    base AS (SELECT argMax(level, date) AS level FROM ipc),
    usd AS (
        SELECT toDate32(toStartOfMonth(date)) AS month, avg(value) AS usd_avg
        FROM v_cbr_macro
        WHERE series = 'cbr_currency_usd'
        GROUP BY month
    ),
    monthly AS (
        SELECT u.date AS date, u.value AS urals, d.usd_avg AS usd_avg, i.level AS ipc_level
        FROM v_minec_macro AS u
        INNER JOIN usd AS d ON d.month = u.date
        LEFT JOIN ipc AS i ON i.date = u.date
        WHERE u.series = 'urals_avg_price'
    ),
    filled AS (
        SELECT date, urals, usd_avg,
               anyLast(ipc_level) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS level_used
        FROM monthly
    ),
    cur AS (SELECT max(date) AS date FROM filled)
SELECT
    round(anyIf(urals * usd_avg, date = (SELECT date FROM cur)), 0) AS "₽/барр.",
    round(100 * (anyIf(urals * usd_avg, date = (SELECT date FROM cur)) /
                 anyIf(urals * usd_avg, date = addYears((SELECT date FROM cur), -1)) - 1), 1) AS "г/г номинально, %",
    round(100 * (anyIf(urals * usd_avg * (SELECT level FROM base) / level_used, date = (SELECT date FROM cur)) /
                 anyIf(urals * usd_avg * (SELECT level FROM base) / level_used, date = addYears((SELECT date FROM cur), -1)) - 1), 1) AS "г/г реально, %"
FROM filled
SETTINGS join_use_nulls = 1
`

// uralsBudgetOilGasYtdSQL — П6: нефтегазовые доходы января–сентября 2026 и г/г темп к тому же
// периоду 2025. Снимок без $__dateFilter: сравнение фиксировано и окном дашборда не меняется.
const uralsBudgetOilGasYtdSQL = `SELECT
    round(sumIf(value, toYear(date) = 2026), 0) AS "Янв–сен 2026, млрд ₽",
    round(100 * (sumIf(value, toYear(date) = 2026) / sumIf(value, toYear(date) = 2025) - 1), 1) AS "г/г, %"
FROM v_minfin_budget
WHERE source = 'minfin_oil_gas'
  AND series = 'Нефтегазовые доходы, всего'
  AND toMonth(date) <= 9
  AND toYear(date) IN (2025, 2026)
`

// uralsBudgetOilGasMonthsSQL — П7: те же доходы по месяцам, пивот по годам делает SQL. nullIf
// превращает пустой sumIf после сентября 2026 в NULL, а не в 0: пропуск публикации не должен
// рисоваться нулевым баром (пропуск ≠ ноль).
const uralsBudgetOilGasMonthsSQL = `SELECT
    toMonth(date) AS "Месяц",
    round(sumIf(value, toYear(date) = 2025), 1) AS "2025",
    nullIf(round(sumIf(value, toYear(date) = 2026), 1), 0) AS "2026"
FROM v_minfin_budget
WHERE source = 'minfin_oil_gas'
  AND series = 'Нефтегазовые доходы, всего'
  AND toYear(date) IN (2025, 2026)
  AND $__dateFilter(date)
GROUP BY "Месяц"
ORDER BY "Месяц"
`

// uralsBudgetRealPriceJS — тело getOption панели 1 (формат плагина 7.2.5, echarts.go): данные
// только из data frames таргета, литеральных рядов нет. Ось X — 12 календарных месяцев (подписи —
// презентационный массив, не данные); синяя линия — сезонная норма 2018–2022; линии 2023–2026 —
// пивот (Год, Месяц года). Строки с "Оценка" = 1 (октябрь 2026) в линии годов НЕ входят: они
// рисуются отдельной scatter-серией полым маркером (symbol emptyCircle) с подписью значения.
// Линии не соединяют пропуски (connectNulls: false): 2018-06/07 в источнике отсутствуют.
const uralsBudgetRealPriceJS = `const s = context.panel.data.series[0];
if (!s) { return {}; }
function column(name) {
  const f = s.fields.find(function (x) { return x.name === name; });
  if (!f) { return null; }
  return f.values && f.values.buffer ? f.values.buffer : f.values;
}
const years = column('Год');
const months = column('Месяц года');
const real = column('Реальная цена, ₽/барр.');
const seasonal = column('Сезонная норма 2018–2022, ₽/барр.');
const estimate = column('Оценка');
if (!years || !months || !real || !seasonal || !estimate) { return {}; }
const ruMonths = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
const norm = new Array(12).fill(null);
const lines = {
  2023: new Array(12).fill(null),
  2024: new Array(12).fill(null),
  2025: new Array(12).fill(null),
  2026: new Array(12).fill(null)
};
const estimates = [];
for (let i = 0; i < months.length; i++) {
  const m = Number(months[i]) - 1;
  if (m < 0 || m > 11) { continue; }
  if (norm[m] === null && seasonal[i] !== null) { norm[m] = seasonal[i]; }
  if (Number(estimate[i]) === 1) {
    estimates.push({ value: [m, real[i]], year: Number(years[i]), month: m + 1 });
    continue;
  }
  const y = Number(years[i]);
  if (lines[y] && lines[y][m] === null) { lines[y][m] = real[i]; }
}
const series = [{
  name: 'Сезонная норма 2018–2022',
  type: 'line',
  data: norm,
  connectNulls: false,
  lineStyle: { type: 'dashed', width: 2 },
  itemStyle: { color: '#1c7ed6' }
}];
for (const y of [2023, 2024, 2025, 2026]) {
  if (!lines[y] || lines[y].every(function (v) { return v === null; })) { continue; }
  series.push({
    name: String(y),
    type: 'line',
    data: lines[y],
    connectNulls: false
  });
}
series.push({
  name: 'Оценка',
  type: 'scatter',
  symbol: 'emptyCircle',
  symbolSize: 14,
  data: estimates,
  itemStyle: { color: '#e8590c', borderWidth: 2 },
  label: { show: true, position: 'top', formatter: function (p) { return String(p.data.value[1]); } }
});
return {
  tooltip: {
    trigger: 'axis',
    formatter: function (ps) {
      if (!ps.length) { return ''; }
      const rows = [];
      for (const p of ps) {
        if (p.data === null || p.data === undefined) { continue; }
        let year = p.seriesName;
        let month = ruMonths[p.dataIndex];
        let value = p.data;
        if (Array.isArray(p.data)) {
          year = String(p.data.year);
          month = ruMonths[p.data.month - 1];
          value = p.data[1];
        }
        if (value === null || value === undefined) { continue; }
        rows.push(year + ' ' + month + ': <b>' + Number(value).toFixed(0) + '</b> ₽/барр.');
      }
      return rows.join('<br/>');
    }
  },
  legend: { bottom: 0 },
  grid: { left: 64, right: 24, top: 24, bottom: 64 },
  xAxis: { type: 'category', data: ruMonths },
  yAxis: { type: 'value', name: '₽/барр.' },
  series: series
};`

// uralsBudgetRuleJS — тело getOption панели 2: xAxis type 'time', основная линия Urals с подписью
// последней точки (endLabel), два area-ряда «в бюджет / в ФНБ» со stack 'rule' (стопка = цена
// Urals) и markLine на цене отсечения. Числовое значение 59 задано одной константой с
// комментарием-источником (имена колонок с $59 — контракт SQL, а не значения); литеральных рядов
// данных нет, разрывы не соединяются.
const uralsBudgetRuleJS = `const s = context.panel.data.series[0];
if (!s) { return {}; }
function field(name) {
  return s.fields.find(function (x) { return x.name === name; });
}
function values(f) {
  return f && f.values && f.values.buffer ? f.values.buffer : f.values;
}
const timeField = field('Месяц');
const uralsField = field('Urals, $/барр.');
const budgetField = field('В бюджет (≤ $59)');
const fundField = field('В ФНБ (> $59)');
if (!timeField || !uralsField || !budgetField || !fundField) { return {}; }
const times = values(timeField);
const urals = values(uralsField);
const toBudget = values(budgetField);
const toFund = values(fundField);
// $59 — цена отсечения бюджетного правила Минфина для Urals (действует до 2027; The Bell, mn.ru;
// снижение до $50 обсуждается только с 2027). Это параметр правила, а не ряд витрины.
const BUDGET_RULE_CUTOFF = 59;
const price = [];
const budget = [];
const fund = [];
for (let i = 0; i < times.length; i++) {
  if (times[i] === null || urals[i] === null) { continue; }
  price.push([times[i], urals[i]]);
  budget.push([times[i], toBudget[i]]);
  fund.push([times[i], toFund[i]]);
}
return {
  tooltip: {
    trigger: 'axis',
    formatter: function (ps) {
      if (!ps.length) { return ''; }
      const rows = [];
      for (const p of ps) {
        if (p.data === null || p.data === undefined) { continue; }
        rows.push(p.seriesName + ': <b>' + Number(p.data[1]).toFixed(2) + '</b> $/барр.');
      }
      return rows.join('<br/>');
    }
  },
  legend: { bottom: 0 },
  grid: { left: 64, right: 72, top: 24, bottom: 56 },
  xAxis: { type: 'time' },
  yAxis: { type: 'value', name: '$/барр.' },
  series: [
    {
      name: uralsField.name,
      type: 'line',
      data: price,
      connectNulls: false,
      itemStyle: { color: '#1c7ed6' },
      endLabel: {
        show: true,
        formatter: function (p) { return Number(p.value[1]).toFixed(2); }
      },
      markLine: {
        symbol: 'none',
        silent: true,
        lineStyle: { type: 'dashed', color: '#e03131', width: 2 },
        label: {
          formatter: 'Бюджетное правило: $' + BUDGET_RULE_CUTOFF,
          position: 'insideEndTop'
        },
        data: [{ yAxis: BUDGET_RULE_CUTOFF }]
      }
    },
    {
      name: budgetField.name,
      type: 'line',
      stack: 'rule',
      data: budget,
      connectNulls: false,
      areaStyle: { opacity: 0.25 },
      lineStyle: { width: 0 },
      itemStyle: { color: '#40c057' }
    },
    {
      name: fundField.name,
      type: 'line',
      stack: 'rule',
      data: fund,
      connectNulls: false,
      areaStyle: { opacity: 0.25 },
      lineStyle: { width: 0 },
      itemStyle: { color: '#fab005' }
    }
  ]
};`

// uralsBudgetStatDefaults — общие настройки stat-панелей: одно значение последней строки
// (lastNotNull), без списка значений. Подписи полей приходят из алиасов SQL.
func uralsBudgetStatDefaults(builder *stat.PanelBuilder) *stat.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		ReduceOptions(common.NewReduceDataOptionsBuilder().Calcs([]string{"lastNotNull"}).Values(false)).
		TextMode(common.BigValueTextModeAuto)
}

// NewUralsBudgetRule собирает дашборд «Urals: реальная цена и бюджетное правило» (спека §3–§4):
// 7 панелей в 2 рядах. Ряд 21 «Реальная цена Urals и бюджетное правило» — два ECharts-графика во
// всю ширину: реальная цена в постоянных рублях (П1) и бюджетное правило $59 (П2). Ряд 22
// «Октябрь 2026 и нефтегазовые доходы» — четыре stat-снимка (П3–П6) и месячный бар доходов (П7).
// Окно дашборда — с 2018-01-01: график публикации начинается с 2018. Порядок вызовов
// WithRow/WithPanel задаёт нумерацию и место в сетке; ширины и высоты — Span/Height панелей.
func NewUralsBudgetRule() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Urals: реальная цена и бюджетное правило").
		Uid(UralsBudgetRuleUID).
		Tags([]string{"infra", "urals", "budget"}).
		Refresh("1h").
		Time("2018-01-01", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Реальная цена Urals и бюджетное правило").Id(firstUralsBudgetRuleRowID)).
		WithPanel(uralsBudgetRealPricePanel()).
		WithPanel(uralsBudgetRulePanel()).
		WithRow(dashboard.NewRowBuilder("Октябрь 2026 и нефтегазовые доходы").Id(secondUralsBudgetRuleRowID)).
		WithPanel(uralsBudgetSpotPanel()).
		WithPanel(uralsBudgetFxPanel()).
		WithPanel(uralsBudgetRealSnapshotPanel()).
		WithPanel(uralsBudgetOilGasYtdPanel()).
		WithPanel(uralsBudgetOilGasMonthsPanel())
}

// uralsBudgetRealPricePanel — панель 1: Urals в постоянных рублях, сезонная норма и линии годов.
// Октябрь-2026 — оценка по дефлятору сентября (ИПЦ за октябрь ещё не опубликован); она исключена
// из линий годов и рисуется полым маркером (см. uralsBudgetRealPriceJS).
func uralsBudgetRealPricePanel() *dashboard.PanelBuilder {
	return echartsPanel(1,
		"Urals в постоянных рублях (база — сентябрь 2026)",
		"Urals в постоянных рублях (база — последний опубликованный ИПЦ, сейчас сентябрь 2026): "+
			"real = Urals $/барр. × средний курс ЦБ за месяц × уровень ИПЦ базы ÷ уровень месяца. "+
			"Источники: v_minec_macro (urals_avg_price, месячное зерно), v_cbr_macro "+
			"(cbr_currency_usd, среднее по дням месяца), v_fedstat_ipc (ipc31074/Все товары и услуги, "+
			"накопленный уровень). Ось X — 12 календарных месяцев: синяя линия — сезонная норма "+
			"2018–2022, линии 2023–2026 — годы ряда. Октябрь 2026 — оценка по дефлятору сентября "+
			"(ИПЦ за октябрь ещё не опубликован): строка исключена из линий годов и нарисована полым "+
			"маркером. 2018-06/07 в источнике отсутствуют — точек нет (пропуск ≠ ноль). Когда Росстат "+
			"опубликует ИПЦ за октябрь, база сдвинется и весь ряд пересчитается в рубли октября. "+
			"Значения — ₽/барр. в постоянных рублях.",
		uralsBudgetRealPriceJS,
		tableQuery("A", uralsBudgetRealPriceSQL),
	).Unit(units.Rubles).Height(uralsBudgetChartHeight).Span(12)
}

// uralsBudgetRulePanel — панель 2: бюджетное правило Минфина — Urals $/барр. по месяцам с 2018,
// горизонталь $59, stacked area «в бюджет / в ФНБ» и подпись последней точки.
func uralsBudgetRulePanel() *dashboard.PanelBuilder {
	return echartsPanel(2,
		"Бюджетное правило: цена отсечения $59",
		"Бюджетное правило Минфина: Urals $/барр. по месяцам с 2018 (v_minec_macro, series = "+
			"'urals_avg_price'). $59 — цена отсечения бюджетного правила для Urals (действует до 2027; "+
			"The Bell, mn.ru; снижение до $50 обсуждается только с 2027): это параметр правила, а не "+
			"ряд витрины — в каталоге рядов его нет. Stacked area: часть цены до $59 уходит в бюджет, "+
			"превышение — в ФНБ; горизонтальная markLine $59 — граница правила. Подпись последней "+
			"точки — цена последнего месяца. Разрывы линий не соединяются. Значения — $/барр.",
		uralsBudgetRuleJS,
		tableQuery("A", uralsBudgetRuleSQL),
	).Unit(units.Dollars).Height(uralsBudgetChartHeight).Span(12)
}

// uralsBudgetSpotPanel — панель 3: снимок последнего опубликованного месяца, $/барр.
func uralsBudgetSpotPanel() *stat.PanelBuilder {
	return uralsBudgetStatDefaults(stat.NewPanelBuilder()).
		Id(3).
		Title("Urals, $/барр. — октябрь 2026").
		Description("Снимок последнего опубликованного месяца: средняя цена Urals, $/барр. " +
			"(v_minec_macro, series = 'urals_avg_price'; argMax по date — последний месяц, сейчас " +
			"октябрь 2026). Зерно — месяц; значения — доллары за баррель. Диапазон дашборда панель " +
			"не учитывает: снимок последнего месяца не должен зависеть от окна.").
		Span(6).
		Height(uralsBudgetStatHeight).
		Unit(units.Dollars).
		WithTarget(tableQuery("A", uralsBudgetSpotSQL))
}

// uralsBudgetFxPanel — панель 4: снимок среднего курса ЦБ за тот же месяц, ₽/$.
func uralsBudgetFxPanel() *stat.PanelBuilder {
	return uralsBudgetStatDefaults(stat.NewPanelBuilder()).
		Id(4).
		Title("Курс ЦБ, ₽/$ — октябрь 2026").
		Description("Средний за последний опубликованный месяц Urals курс ЦБ (v_cbr_macro, series = " +
			"'cbr_currency_usd': дневной ряд, среднее по дням месяца через toStartOfMonth/avg). Месяц " +
			"берётся тот же, что у последнего Urals (максимум даты в v_minec_macro), а не последний " +
			"опубликованный курс: ряды выходят в разные даты, и снимок обязан относиться к одному " +
			"месяцу. Значения — ₽ за 1 USD (не сумма), поэтому единица short; снимок без фильтра " +
			"диапазона.").
		Span(6).
		Height(uralsBudgetStatHeight).
		Unit(units.Short).
		Decimals(4).
		WithTarget(tableQuery("A", uralsBudgetFxSQL))
}

// uralsBudgetRealSnapshotPanel — панель 5: Urals в рублях за последний месяц и два г/г темпа.
// Панельная единица — рубли, у процентных колонок — override percent по имени поля.
func uralsBudgetRealSnapshotPanel() *stat.PanelBuilder {
	return uralsBudgetStatDefaults(stat.NewPanelBuilder()).
		Id(5).
		Title("Urals в рублях, ₽/барр. — октябрь 2026").
		Description("Urals в рублях за последний опубликованный месяц: цена $/барр. × средний курс ЦБ "+
			"месяца, плюс два темпа г/г — номинальный и реальный (в постоянных рублях базы, сейчас "+
			"сентябрь 2026; октябрь считается по дефлятору сентября). Источники: v_minec_macro "+
			"(urals_avg_price), v_cbr_macro (cbr_currency_usd), v_fedstat_ipc "+
			"(ipc31074/Все товары и услуги). Зерно — месяц; колонки «₽/барр.» и два г/г темпа приходят "+
			"одной строкой, подписи — из алиасов SQL. Проценты — override'ом по имени поля, иначе темп "+
			"печатался бы в рублях. Снимок последнего месяца, без фильтра диапазона.").
		Span(6).
		Height(uralsBudgetStatHeight).
		Unit(units.Rubles).
		OverrideByName("г/г номинально, %", []dashboard.DynamicConfigValue{{Id: "unit", Value: units.Percent}}).
		OverrideByName("г/г реально, %", []dashboard.DynamicConfigValue{{Id: "unit", Value: units.Percent}}).
		WithTarget(tableQuery("A", uralsBudgetRealSnapshotSQL))
}

// uralsBudgetOilGasYtdPanel — панель 6: нефтегазовые доходы января–сентября 2026 и темп г/г.
// Панельная единица — рубли (млрд ₽), у колонки темпа — override percent.
func uralsBudgetOilGasYtdPanel() *stat.PanelBuilder {
	return uralsBudgetStatDefaults(stat.NewPanelBuilder()).
		Id(6).
		Title("Нефтегазовые доходы, млрд ₽ — янв–сен 2026").
		Description("Нефтегазовые доходы федерального бюджета, млрд ₽: сумма января–сентября 2026 "+
			"(v_minfin_budget, source = 'minfin_oil_gas', series = 'Нефтегазовые доходы, всего') и темп "+
			"г/г к тому же периоду 2025. Зерно — месяц, на панели — сумма за 9 месяцев; значения ЗА "+
			"период, не накопленным итогом. Сравнение фиксировано на янв–сен 2026 и окном дашборда не "+
			"меняется. Темп — override'ом percent по имени поля (иначе печатался бы в рублях).").
		Span(6).
		Height(uralsBudgetStatHeight).
		Unit(units.Rubles).
		OverrideByName("г/г, %", []dashboard.DynamicConfigValue{{Id: "unit", Value: units.Percent}}).
		WithTarget(tableQuery("A", uralsBudgetOilGasYtdSQL))
}

// uralsBudgetOilGasMonthsPanel — панель 7: доходы по месяцам, 2025 vs 2026. NULL после сентября
// 2026 — публикация ещё не вышла; нулевыми барами пропуск не подменяется.
func uralsBudgetOilGasMonthsPanel() *barchart.PanelBuilder {
	return barchart.NewPanelBuilder().
		Id(7).
		Title("Нефтегазовые доходы федерального бюджета по месяцам, млрд ₽").
		Description("Нефтегазовые доходы федерального бюджета по месяцам, млрд ₽: 2025 и 2026 " +
			"(v_minfin_budget, source = 'minfin_oil_gas', series = 'Нефтегазовые доходы, всего'), пивот " +
			"по годам делает SQL. Ось X — месяцы 1–12. Значения за месяц, не накопленным итогом. NULL " +
			"после сентября 2026 — публикация ещё не вышла: пустые месяцы не рисуются нулевыми барами " +
			"(пропуск ≠ ноль), честный ноль дохода отличить от пропуска в источнике нельзя.").
		Span(spanFull).
		Height(uralsBudgetBarHeight).
		Datasource(datasourceRef()).
		XField("Месяц").
		Unit(units.Rubles).
		WithTarget(tableQuery("A", uralsBudgetOilGasMonthsSQL))
}
