package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/bargauge"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// TransaqTradesUID — uid дашборда ленты сделок Transaq (пункт ofz-market/1.6). Артефакт —
// dashboard/<uid>.json; uid попадает и в spec, и в metadata.name манифеста.
const TransaqTradesUID = "transaq-trades"

// Id рядов вне диапазона панелей 1..6: нулевой id Grafana заменит своим, а совпавший с панелью
// сделает артефакт неотличимым от кода.
const firstTransaqTradesRowID = 21

// Запросы панелей 1–2 читают ДНЕВНУЮ витрину: top-10 за диапазон — это сумма по дням, и
// сканировать минуты незачем (time-series практика «зерно под задачу», спека §7). $__dateFilter —
// макрос плагина для Date-колонки; $__timeFilter здесь дал бы неявный каст Date → DateTime.
const (
	ttNetBuysSQL = `SELECT
    sec_code AS "Инструмент",
    sum(net_value_rub) AS "Нетто покупок, ₽"
FROM v_transaq_net_flows_daily
WHERE instrument_kind = 'share'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) > 0
ORDER BY "Нетто покупок, ₽" DESC
LIMIT 10
`

	ttNetSellsSQL = `SELECT
    sec_code AS "Инструмент",
    -sum(net_value_rub) AS "Нетто продаж, ₽"
FROM v_transaq_net_flows_daily
WHERE instrument_kind = 'share'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) < 0
ORDER BY "Нетто продаж, ₽" DESC
LIMIT 10
`

	// Графики читают минутную витрину; $__timeInterval разворачивается в toStartOfInterval с
	// адаптивным шагом панели. LIMIT на графике нет: ряд не обрезается, агрегация — в ClickHouse.
	ttNetFlowSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(net_value_rub) AS "Нетто, ₽"
FROM v_transaq_net_flows
WHERE instrument_kind = 'share'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`

	ttImoexVolumesSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE instrument_kind = 'share'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`

	ttOfzVolumesSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE instrument_kind = 'ofz'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`

	// Теплокарта читает дневную витрину за период дашборда: площадь плитки — оборот,
	// цвет — относительный перевес покупок/продаж. Срок ОФЗ — max(maturity_date) из
	// v_ofz_issue_analytics: в ленте TQOB лежат и SU…RMFS…, и CNY-выпуски с sec_code = ISIN,
	// поэтому join идёт по sec_code, а не по префиксу. nullIf превращает дефолт LEFT JOIN
	// (1970-01-01) в NULL: выпуск без строки аналитики уходит в «срок н/д», а не в «до 1 года».
	// Фьючерсы (instrument_kind='future') осознанно вне карты: на staging их номинал — ~52%
	// оборота, и в общем treemap они задавили бы cash-рынок; это отдельный пункт/панель.
	// instrument_kind='other' оставлен резервом: TQCB в подписке нет, тенор для него не считается.
	ttHeatmapSQL = `SELECT
    multiIf(
        instrument_kind = 'share', 'Акции',
        instrument_kind = 'fund', 'Фонды денежного рынка',
        instrument_kind = 'ofz' AND days_left IS NULL, 'ОФЗ (срок н/д)',
        instrument_kind = 'ofz' AND days_left <= 365, '≤1 года',
        instrument_kind = 'ofz' AND days_left <= 1095, '1–3',
        instrument_kind = 'ofz' AND days_left <= 1825, '3–5',
        instrument_kind = 'ofz' AND days_left <= 3650, '5–10',
        instrument_kind = 'ofz', '10+',
        'Прочие'
    ) AS "Кластер",
    "Тикер", "Покупки, ₽", "Продажи, ₽", "Оборот, ₽", "Нетто, ₽", "Сделок"
FROM (
    SELECT
        d.sec_code AS "Тикер",
        argMax(d.instrument_kind, d.date) AS instrument_kind,
        sum(d.buy_value_rub) AS "Покупки, ₽",
        sum(d.sell_value_rub) AS "Продажи, ₽",
        sum(d.buy_value_rub + d.sell_value_rub) AS "Оборот, ₽",
        sum(d.buy_value_rub - d.sell_value_rub) AS "Нетто, ₽",
        sum(d.trade_count) AS "Сделок",
        dateDiff('day', max(d.date), nullIf(max(m.maturity_date), toDate('1970-01-01'))) AS days_left
    FROM v_transaq_net_flows_daily AS d
    LEFT JOIN (
        SELECT sec_code, max(maturity_date) AS maturity_date
        FROM v_ofz_issue_analytics
        GROUP BY sec_code
    ) AS m ON m.sec_code = d.sec_code
    WHERE $__dateFilter(d.date)
      AND d.instrument_kind IN ('share', 'fund', 'ofz', 'other')
    GROUP BY d.sec_code
)
WHERE "Оборот, ₽" > 0
ORDER BY "Оборот, ₽" DESC
`
)

// ttHeatmapJS — тело getOption панели 6 (формат плагина 7.2.5, echarts.go): две рыночные карты
// в одной панели — слева акции и фонды денежного рынка, справа ОФЗ по срокам. Площадь — оборот,
// цвет — относительный перевес (buy − sell) / (buy + sell), а не абсолютное нетто: иначе одна
// крупная бумага перекрасила бы всю карту. Шкала симметричная по 95-му перцентилю |ratio| —
// выброс не обесцвечивает остальные плитки; ноль — серый. Две карты, а не одна: на staging
// фьючерсы дают ~52% оборота, акции ~38%, ОФЗ ~0,8% — в общем treemap ОФЗ были бы невидимы;
// фьючерсы в карту не входят, у каждой карты своя шкала площади. Литеральных тикеров и сумм в JS
// нет: все значения приходят из SQL, кластеры — только подписи. itemStyle.color задаётся
// per-item, visualMap не используется: он красил бы по value (оборот), а нам нужен цвет по ratio.
const ttHeatmapJS = `const s = context.panel.data.series[0];
if (!s) { return {}; }
function column(name) {
  const f = s.fields.find(function (x) { return x.name === name; });
  if (!f) { return null; }
  // plain array не имеет .buffer и остаётся как есть; Grafana Vector отдаёт значения в .buffer.
  return f.values && f.values.buffer ? f.values.buffer : f.values;
}
const clustersField = column('Кластер');
const tickers = column('Тикер');
const buys = column('Покупки, ₽');
const sells = column('Продажи, ₽');
const grosses = column('Оборот, ₽');
const nets = column('Нетто, ₽');
const trades = column('Сделок');
if (!clustersField || !tickers || !buys || !sells || !grosses || !nets || !trades) { return {}; }
const rows = [];
for (let i = 0; i < tickers.length; i++) {
  const gross = Number(grosses[i]);
  if (!(gross > 0)) { continue; }
  rows.push({
    cluster: String(clustersField[i]),
    ticker: String(tickers[i]),
    buy: Number(buys[i]),
    sell: Number(sells[i]),
    gross: gross,
    net: Number(nets[i]),
    trades: Number(trades[i])
  });
}
if (!rows.length) { return {}; }
const cashOrder = ['Акции', 'Фонды денежного рынка', 'Прочие'];
const bondOrder = ['≤1 года', '1–3', '3–5', '5–10', '10+', 'ОФЗ (срок н/д)'];
const byCluster = {};
for (let i = 0; i < rows.length; i++) {
  const r = rows[i];
  if (!byCluster[r.cluster]) {
    byCluster[r.cluster] = { name: r.cluster, gross: 0, net: 0, children: [] };
  }
  byCluster[r.cluster].gross += r.gross;
  byCluster[r.cluster].net += r.net;
  byCluster[r.cluster].children.push(r);
}
const cashNames = cashOrder.filter(function (n) { return byCluster[n]; });
Object.keys(byCluster).forEach(function (n) {
  if (cashOrder.indexOf(n) < 0 && bondOrder.indexOf(n) < 0) { cashNames.push(n); }
});
const bondNames = bondOrder.filter(function (n) { return byCluster[n]; });
const ratios = rows.map(function (r) { return Math.abs(r.net / r.gross); }).sort(function (a, b) { return a - b; });
const p95 = ratios.length ? ratios[Math.floor((ratios.length - 1) * 0.95)] : 0.02;
const scale = Math.min(1, Math.max(0.02, p95));
function colorFor(ratio) {
  if (!ratio) { return '#ced4da'; }
  const t = Math.sqrt(Math.min(1, Math.abs(ratio) / scale));
  const hue = ratio > 0 ? 142 : 0;
  const light = 80 - 48 * t;
  return 'hsl(' + hue + ', 65%, ' + light.toFixed(0) + '%)';
}
function shortName(name) {
  return name.replace(/RMFS\d$/, '');
}
function money(v) {
  const a = Math.abs(v);
  if (a >= 1e9) { return (a / 1e9).toFixed(2).replace('.', ',') + ' млрд'; }
  if (a >= 1e6) { return (a / 1e6).toFixed(1).replace('.', ',') + ' млн'; }
  if (a >= 1e3) { return (a / 1e3).toFixed(0) + ' тыс.'; }
  return String(Math.round(a));
}
function signedMoney(v) {
  if (v > 0) { return '+' + money(v); }
  if (v < 0) { return '−' + money(v); }
  return '0';
}
function ratioPct(v) {
  if (Math.abs(v * 100) < 0.05) { return '0,0%'; }
  return (v > 0 ? '+' : '−') + Math.abs(v * 100).toFixed(1).replace('.', ',') + '%';
}
function clusterNode(name) {
  const c = byCluster[name];
  const children = c.children.map(function (r) {
    const ratio = r.net / r.gross;
    return {
      name: r.ticker,
      value: r.gross,
      net: r.net,
      buy: r.buy,
      sell: r.sell,
      gross: r.gross,
      trades: r.trades,
      ratio: ratio,
      cluster: name,
      isLeaf: true,
      itemStyle: { color: colorFor(ratio), borderColor: '#fff', borderWidth: 1 }
    };
  });
  return {
    name: name,
    value: c.gross,
    net: c.net,
    gross: c.gross,
    ratio: c.gross > 0 ? c.net / c.gross : 0,
    isLeaf: false,
    itemStyle: { color: '#343a40', borderColor: '#fff', borderWidth: 2 },
    children: children
  };
}
const cashData = cashNames.map(function (n) { return clusterNode(n); });
const bondData = bondNames.map(function (n) { return clusterNode(n); });
if (!cashData.length && !bondData.length) { return {}; }
function treemapSeries(title, data, left, width) {
  return {
    type: 'treemap',
    name: title,
    data: data,
    left: left,
    width: width,
    top: 24,
    bottom: 28,
    roam: false,
    nodeClick: false,
    breadcrumb: { show: false },
    leafDepth: 2,
    sort: false,
    label: {
      show: true,
      position: 'inside',
      formatter: function (p) {
        if (!p.data || !p.data.isLeaf) { return ''; }
        return '{name|' + shortName(p.name) + '}\n{net|' + signedMoney(p.data.net) + '}';
      },
      rich: {
        name: { fontSize: 12, fontWeight: 'bold', color: '#fff', lineHeight: 15, textBorderColor: 'rgba(0,0,0,0.45)', textBorderWidth: 2 },
        net: { fontSize: 11, color: '#fff', lineHeight: 14, textBorderColor: 'rgba(0,0,0,0.45)', textBorderWidth: 2 }
      },
      overflow: 'truncate',
      ellipsis: '…'
    },
    levels: [
      { label: { show: false }, upperLabel: { show: false } },
      { itemStyle: { borderColor: '#fff', borderWidth: 2, gapWidth: 2 }, upperLabel: { show: true, formatter: '{b}', height: 20, color: '#fff', fontSize: 12, fontWeight: 'bold', backgroundColor: 'rgba(33,37,41,0.85)', padding: [3, 6] }, label: { show: false } },
      { itemStyle: { borderColor: '#fff', borderWidth: 1, gapWidth: 1 }, upperLabel: { show: false }, label: { show: true } }
    ]
  };
}
const series = [];
const graphic = [];
const hasCash = cashData.length > 0;
const hasBonds = bondData.length > 0;
const cashLeft = '0%';
const bondLeft = hasCash ? '59%' : '0%';
if (hasCash) {
  series.push(treemapSeries('Акции и фонды', cashData, cashLeft, hasBonds ? '57%' : '100%'));
  graphic.push({ type: 'text', left: cashLeft, top: 2, style: { text: 'Акции и фонды', fill: '#495057', font: 'bold 11px sans-serif' } });
}
if (hasBonds) {
  series.push(treemapSeries('ОФЗ по срокам', bondData, bondLeft, hasCash ? '41%' : '100%'));
  graphic.push({ type: 'text', left: bondLeft, top: 2, style: { text: 'ОФЗ по срокам', fill: '#495057', font: 'bold 11px sans-serif' } });
}
return {
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'item',
    formatter: function (p) {
      const d = p.data;
      if (!d) { return p.name; }
      if (d.isLeaf) {
        return '<b>' + d.name + '</b><br/>' + d.cluster +
          '<br/>Покупки: ' + money(d.buy) + ' ₽' +
          '<br/>Продажи: ' + money(d.sell) + ' ₽' +
          '<br/>Нетто: <b>' + signedMoney(d.net) + ' ₽</b> (' + ratioPct(d.ratio) + ' оборота)' +
          '<br/>Оборот: ' + money(d.gross) + ' ₽' +
          '<br/>Сделок: ' + d.trades;
      }
      return '<b>' + d.name + '</b>' +
        '<br/>Оборот: ' + money(d.gross) + ' ₽' +
        '<br/>Нетто: ' + signedMoney(d.net) + ' ₽ (' + ratioPct(d.ratio) + ' оборота)';
    }
  },
  series: series,
  graphic: graphic.concat([{
    type: 'text',
    left: 'center',
    bottom: 2,
    style: {
      text: 'Цвет — перевес покупок/продаж, % оборота; площадь — оборот, ₽',
      fill: '#868e96',
      font: '11px sans-serif'
    }
  }])
};`

// NewTransaqTrades собирает дашборд «Сделки MOEX — нетто покупок и продаж» (пункты ofz-market/1.6
// и 1.9, спеки 2026-10-31-grafana-transaq-trades-design.md §5 и
// 2026-11-09-transaq-trades-heatmap-design.md §5): 6 панелей в 3 рядах. Ряд 23 «Теплокарта
// периода» — две ECharts-карты cash-рынка (акции/фонды и ОФЗ по срокам; площадь — оборот,
// цвет — относительный перевес); ряд 21 «Итоги периода» — top-10 нетто покупок/продаж IMOEX
// по дневной витрине;
// ряд 22 «Объёмы по интервалам» — нетто-поток IMOEX и покупки/продажи IMOEX и ОФЗ по минутной
// витрине. Порядок WithRow/WithPanel задаёт нумерацию, Span — ширину в сетке 24.
//
// Окно по умолчанию — 7 дней: «итоги дня» на нерабочий день были бы пусты, а неделя покрывает
// последнюю торговую сессию. Refresh 5m — лента обновляется импортёром каждые 15 минут.
func NewTransaqTrades() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Сделки MOEX — нетто покупок и продаж").
		Uid(TransaqTradesUID).
		Tags([]string{"transaq", "moex", "ofz-market"}).
		Refresh("5m").
		Time("now-7d", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Теплокарта периода").Id(firstTransaqTradesRowID + 2)).
		WithPanel(ttHeatmapPanel()).
		WithRow(dashboard.NewRowBuilder("Итоги периода").Id(firstTransaqTradesRowID)).
		WithPanel(ttNetBuysPanel()).
		WithPanel(ttNetSellsPanel()).
		WithRow(dashboard.NewRowBuilder("Объёмы по интервалам").Id(firstTransaqTradesRowID + 1)).
		WithPanel(ttNetFlowPanel()).
		WithPanel(ttImoexVolumesPanel()).
		WithPanel(ttOfzVolumesPanel())
}

// ttPanelDefaults — конвенции графиков: база timeseriesDefaults (SpanNulls=false — пропуск в витрине,
// то есть нет сделок или разрыв ленты, честно остаётся разрывом: нули не подставляются, иначе
// отсутствие данных читалось бы как нулевой объём) с легендой сумма/среднее/максимум, сглаженная
// линия, рубли без дробной части.
func ttPanelDefaults(builder *timeseries.PanelBuilder) *timeseries.PanelBuilder {
	return timeseriesDefaults(builder, "sum", "mean", "max").
		LineInterpolation(common.LineInterpolationSmooth).
		Unit(units.Rubles).
		Decimals(0)
}

// ttBargaugeDefaults — общие настройки top-10 панелей: горизонтальные бары, значения красит
// значение, шкала тянется на всю высоту. ReduceOptions.values=true — в бары идут значения строк
// табличного ответа, а не вычисляемая агрегация.
func ttBargaugeDefaults(builder *bargauge.PanelBuilder) *bargauge.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		Orientation(common.VizOrientationHorizontal).
		DisplayMode(common.BarGaugeDisplayModeBasic).
		ValueMode(common.BarGaugeValueModeColor).
		ShowUnfilled(true).
		ReduceOptions(common.NewReduceDataOptionsBuilder().Values(true)).
		Unit(units.Rubles).
		Decimals(0)
}

// ttNetBuysPanel — панель 1: top-10 нетто покупок акций IMOEX за выбранный период. Значение —
// оценка рублей через lotsize, а не биржевой оборот. Фильтр — instrument_kind = 'share': фонды
// денежного рынка тоже торгуются на TQBR и с оборотом в миллиарды заняли бы топ (ofz-market/1.7).
func ttNetBuysPanel() *bargauge.PanelBuilder {
	return ttBargaugeDefaults(bargauge.NewPanelBuilder()).
		Id(1).
		Title("Нетто покупки в IMOEX, ₽").
		Span(spanHalf).
		Height(9).
		WithTarget(tableQuery("A", ttNetBuysSQL)).
		Description("Сумма нетто-объёма (оценка покупок минус продаж) по коду бумаги за выбранный период; " +
			"только акции IMOEX (instrument_kind = 'share'; фонды денежного рынка и фьючерсы не входят). " +
			"Топ-10 по величине нетто покупок. Рублёвая оценка — " +
			"Σ цена × лоты × lotsize из снимка transaq_securities, а не биржевой оборот: сделки без " +
			"lotsize в сумму не входят. Только фактические сделки " +
			"ленты: разрыв сессии не догоняется, провал может означать пропуск данных, а не рынок. " +
			"Это аналитика, а не сигнал и не рекомендация.")
}

// ttNetSellsPanel — панель 2: top-10 нетто продаж акций IMOEX. Знак снят намеренно: бар показывает
// величину перевеса продавцов, а не отрицательное число.
func ttNetSellsPanel() *bargauge.PanelBuilder {
	return ttBargaugeDefaults(bargauge.NewPanelBuilder()).
		Id(2).
		Title("Нетто продажи в IMOEX, ₽").
		Span(spanHalf).
		Height(9).
		WithTarget(tableQuery("A", ttNetSellsSQL)).
		Description("Сумма нетто-объёма (продажи минус покупки) по коду бумаги за выбранный период; " +
			"только акции IMOEX (instrument_kind = 'share'). Топ-10 по величине перевеса продавцов; значения " +
			"показаны положительными, знак минуса снят намеренно. Рублёвая оценка — Σ цена × лоты × " +
			"lotsize из снимка transaq_securities: сделки без lotsize в сумму не входят. Только " +
			"фактические сделки ленты, разрывы не догоняются. Это аналитика, а не сигнал и не рекомендация.")
}

// ttNetFlowPanel — панель 3: нетто buy − sell по интервалам для IMOEX. Нулевая линия разделяет
// перевес покупателей и продавцов; бакеты адаптивные ($__timeInterval).
func ttNetFlowPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(3).
		Title("Нетто-поток IMOEX по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttNetFlowSQL)).
		Description("Нетто buy − sell по бакетам, которые Grafana выбирает под ширину панели и диапазон " +
			"($__timeInterval). Нулевая линия разделяет перевес покупателей и продавцов. Пропуск в " +
			"ряду — нет сделок в бакете или разрыв ленты: нули не подставляются и линия через пропуск " +
			"не проводится. Значение — рублёвая оценка через lotsize; это не биржевой оборот и не сигнал.")
}

// ttImoexVolumesPanel — панель 4: покупки и продажи IMOEX по инициативной стороне сделки.
// Продажи отрицательные, чтобы перевес читался по нулевой линии, как в источнике.
func ttImoexVolumesPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(4).
		Title("Покупки и продажи IMOEX по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttImoexVolumesSQL)).
		Description("Покупки и продажи по инициативной стороне сделки (buy_sell ленты) в рублях; продажи " +
			"показаны со знаком минус, чтобы перевес читался по нулевой линии. Это не оборот MOEX, а " +
			"оценка по сделкам, попавшим в ленту Transaq: разрывы не догоняются, нули не подставляются.")
}

// ttOfzVolumesPanel — панель 5: то же для ОФЗ (instrument_kind = 'ofz', режим TQOB). Заменяет
// ETF-панель источника; фонды денежного рынка в ленте с ofz-market/1.7, их панель — отдельный пункт.
func ttOfzVolumesPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(5).
		Title("Покупки и продажи ОФЗ (TQOB) по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttOfzVolumesSQL)).
		Description("Покупки и продажи по инициативной стороне сделки в режиме TQOB — государственные " +
			"облигации (ОФЗ). Продажи показаны со знаком минус. Для облигаций лоты и денежный объём " +
			"не совпадают, а lotsize берётся из снимка каталога, поэтому значения — ориентир, а не " +
			"точный оборот; разрывы ленты не догоняются.")
}

// ttHeatmapPanel — панель 6: две теплокарты cash-рынка за выбранный период. Слева — акции
// IMOEX и фонды денежного рынка, справа — ОФЗ по срокам до 1/3/5/10 лет (срок —
// max(maturity_date) из v_ofz_issue_analytics); у каждой карты своя шкала площади. Площадь
// плитки — оборот (buy + sell), цвет — относительный перевес (buy − sell) / (buy + sell):
// зелёный — покупки, красный — продажи, серый — баланс. Фьючерсы вне карты: их номинал на
// staging ~52% оборота и задавил бы cash-рынок; «Прочие» — резерв под режимы вне подписки.
func ttHeatmapPanel() *dashboard.PanelBuilder {
	return echartsPanel(6,
		"Теплокарта cash-рынка: акции, фонды, ОФЗ",
		"Две карты фактических сделок за выбранный период (v_transaq_net_flows_daily, "+
			"$__dateFilter по date): слева акции IMOEX и фонды денежного рынка, справа ОФЗ по "+
			"сроку до погашения (до 1 года, 1–3, 3–5, 5–10, 10+ лет; срок — max(maturity_date) "+
			"из v_ofz_issue_analytics). Площадь плитки — оборот (покупки + продажи), цвет — "+
			"относительный перевес (покупки − продажи) / оборот, зелёный — перевес покупателей, "+
			"красный — продавцов; у каждой карты своя шкала площади, цвет общий. Фьючерсы в карту "+
			"не входят: их номинал задавил бы cash-рынок, это отдельная панель/пункт. Рублёвый "+
			"объём — оценка Σ цена × лоты × lotsize, а не биржевой оборот; сделки без lotsize в "+
			"сумму не входят. Лента событийная: разрыв сессии не догоняется, пропуск не считается "+
			"нулём. Корпоративных облигаций (TQCB) в подписке нет — их теноры не считаются; для "+
			"прочих режимов остаётся кластер «Прочие». Это аналитика, а не сигнал и не рекомендация.",
		ttHeatmapJS,
		tableQuery("A", ttHeatmapSQL),
	).Unit(units.Rubles).Decimals(0).Height(16).Span(spanFull)
}
