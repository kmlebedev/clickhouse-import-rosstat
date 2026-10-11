package wgc

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// etfAPIBaseURL — база JSON API WGC: те же ряды ETF, что и в ручной выгрузке xlsx,
	// но машинно и без ключа (403 у goldhub относится к HTML и скачиванию файлов).
	etfAPIBaseURL = "https://fscdn.gold.org/api/v11/charts/etfv2/revised/"
	// etfAPIFlowsPath и etfAPIHoldingsPath — эндпоинты метрик: потоки и стоки/AUM.
	etfAPIFlowsPath    = "flows-chart2"
	etfAPIHoldingsPath = "holdings-chart2"
	// etfAPIPriceSeries — ценовой ряд в списке потоков: известный не-регион, в импорт
	// не входит. Любое другое имя вне словаря регионов — ошибка, а не молчаливый пропуск.
	etfAPIPriceSeries = "Gold Price (rhs)"
	// etfAPIHoldingsPriceColumn — цена золота в шапке таблицы стоков: тоже не регион;
	// имя закреплено, чтобы колонка не привязывалась к позиции.
	etfAPIHoldingsPriceColumn = "Gold, US$/oz"
	// etfAPIBreakCacheFormat — формат break-cache: сайт goldhub просит свежий ответ CDN
	// датой вида 12Oct26, иначе сразу после публикации CDN отдаёт прошлый месяц.
	etfAPIBreakCacheFormat = "02Jan06"
)

// etfAPIScale — делитель значений метрики: API отдаёт доллары абсолютными (потоки и AUM),
// тоннажные метрики идут как есть. Метрики нет в словаре — значения без делителя.
var etfAPIScale = map[string]float64{etfMetricFlowUsdMln: 1e6, etfMetricAumUsdBn: 1e9}

// etfAPISystem — system ответа WGC: cached говорит, отдал ли CDN ответ из кэша (в лог).
type etfAPISystem struct {
	Cached bool `json:"cached"`
}

// etfAPIFlowsChart — конверт flows-chart2. Шапка system/chartData/asOfDate у обоих
// эндпоинтов одинакова, различается только тело data.Monthly, поэтому конвертов два.
type etfAPIFlowsChart struct {
	System    etfAPISystem `json:"system"`
	ChartData struct {
		AsOfDate string          `json:"asOfDate"`
		Data     etfAPIFlowsBody `json:"data"`
	} `json:"chartData"`
}

// etfAPIFlowsBody — chartData.data эндпоинта потоков.
type etfAPIFlowsBody struct {
	Monthly etfAPIMonthlyFlows `json:"Monthly"`
}

// etfAPIMonthlyFlows — тело Monthly потоков: series.usd и series.tonnes — по списку рядов.
type etfAPIMonthlyFlows struct {
	Series map[string][]etfAPIFlowSeries `json:"series"`
}

// etfAPIFlowSeries — один ряд потоков: имя (регион или цена) и точки [ms, value].
// usd — абсолютные доллары, tonnes — тонны. Data — точки [ms, value]: указатели отличают
// null (данных за месяц нет) от явного нуля (0 — значение, точка есть) — в потоках ровно
// так же, как в стоках, где null ловится указателем.
type etfAPIFlowSeries struct {
	Name string       `json:"name"`
	Data [][]*float64 `json:"data"`
}

// etfAPIHoldingsChart — конверт holdings-chart2.
type etfAPIHoldingsChart struct {
	System    etfAPISystem `json:"system"`
	ChartData struct {
		AsOfDate string             `json:"asOfDate"`
		Data     etfAPIHoldingsBody `json:"data"`
	} `json:"chartData"`
}

// etfAPIHoldingsBody — chartData.data эндпоинта стоков.
type etfAPIHoldingsBody struct {
	Monthly etfAPIMonthlyHoldings `json:"Monthly"`
}

// etfAPIMonthlyHoldings — тело Monthly стоков: таблицы tonnes (запасы) и usd (AUM).
type etfAPIMonthlyHoldings struct {
	Tonnes etfAPIHoldingsTable `json:"tonnes"`
	USD    etfAPIHoldingsTable `json:"usd"`
}

// etfAPIHoldingsTable — таблица стоков: columns — шапка (дата, четыре региона, цена),
// set — строки. Указатели в set отличают null (данных нет) от явного нуля: в стоках
// отсутствие данных именно null, а в потоках — 0, который точкой остаётся.
type etfAPIHoldingsTable struct {
	Columns []string     `json:"columns"`
	Set     [][]*float64 `json:"set"`
}

// etfPointsFromAPI скачивает и разбирает оба JSON-эндпоинта WGC (потоки и стоки/AUM) в точки
// того же вида, что даёт xlsx-парсер: etf_<метрика>_<регион>, дата — конец месяца UTC. Ряда
// world в JSON нет — его добавляют существующие хелперы регионов. Пустая метрика — ошибка
// (fail-fast, урок eia): тихий «0 точек» оставил бы dagu зелёным, а ряды — без обновления.
// Проверка идёт по каждой из четырёх метрик, а не по сумме точек: конверт с корректными
// именами регионов, но пустыми data даёт точки стоков, и импорт «успешно» записал бы свои
// ряды, оставив десять потоковых без обновления (зеркально — пустой set стоков).
func etfPointsFromAPI(baseURL string, now time.Time) ([]seriesPoint, error) {
	var flows etfAPIFlowsChart
	if err := util.GetJSON(etfAPIURL(baseURL, etfAPIFlowsPath, now), &flows); err != nil {
		return nil, fmt.Errorf("%s: %w", etfAPIFlowsPath, err)
	}
	flowsPoints, err := etfFlowPoints(flows.ChartData.Data.Monthly)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", etfAPIFlowsPath, err)
	}

	var holdings etfAPIHoldingsChart
	if err := util.GetJSON(etfAPIURL(baseURL, etfAPIHoldingsPath, now), &holdings); err != nil {
		return nil, fmt.Errorf("%s: %w", etfAPIHoldingsPath, err)
	}
	holdingsPoints, err := etfHoldingsPoints(holdings.ChartData.Data.Monthly)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", etfAPIHoldingsPath, err)
	}

	points := append(flowsPoints, holdingsPoints...)
	if empty := etfEmptyMetrics(points); len(empty) > 0 {
		return nil, fmt.Errorf("выгрузка без точек: метрики %s не дали ни одной точки — их ряды остались бы без обновления",
			strings.Join(empty, ", "))
	}

	log.Infof("wgc etf api: потоки asOfDate=%s (cached=%t) + стоки asOfDate=%s (cached=%t), точек %d",
		flows.ChartData.AsOfDate, flows.System.Cached,
		holdings.ChartData.AsOfDate, holdings.System.Cached, len(points))

	return points, nil
}

// etfEmptyMetrics — метрики набора, у которых в разборе нет ни одной точки, в порядке
// wgcEtfMetrics (текст ошибки детерминирован). Считается по префиксу имени ряда, а не по
// сумме точек: «ноль точек всего» не ловит случай, когда одна пара эндпоинтов вернула
// конверт без точек — импорт записал бы вторую половину рядов и остался зелёным.
func etfEmptyMetrics(points []seriesPoint) []string {
	empty := make([]string, 0, len(wgcEtfMetrics))
	for _, metric := range wgcEtfMetrics {
		prefix := wgcEtfSeriesName(metric, "")
		metricHasPoints := false
		for _, p := range points {
			if strings.HasPrefix(p.Series, prefix) {
				metricHasPoints = true
				break
			}
		}
		if !metricHasPoints {
			empty = append(empty, metric)
		}
	}
	return empty
}

// etfAPIURL — адрес эндпоинта с break-cache: свежий ответ CDN нужен, потому что кэш может
// вернуть прошлый месяц уже после публикации, и обновление рядов молча потеряется.
func etfAPIURL(baseURL, path string, now time.Time) string {
	return baseURL + path + "?break-cache=" + now.Format(etfAPIBreakCacheFormat)
}

// etfFlowPoints собирает точки потоков из series.usd и series.tonnes: регионы распознаются
// словарём, ценовой ряд пропускается по имени, world добавляет appendRegionPoints.
func etfFlowPoints(monthly etfAPIMonthlyFlows) ([]seriesPoint, error) {
	metrics := []struct {
		series string
		metric string
	}{
		{"usd", etfMetricFlowUsdMln},
		{"tonnes", etfMetricFlowTonnes},
	}
	var points []seriesPoint
	for _, m := range metrics {
		sums, err := etfFlowSums(monthly, m.series, etfAPIScaleFor(m.metric))
		if err != nil {
			return nil, err
		}
		for _, date := range etfAPISortedDates(sums) {
			points = appendRegionPoints(points, m.metric, date, sums[date])
		}
	}
	return points, nil
}

// etfFlowSums собирает значения потоков метрики apiKey по месяцам: map[конец месяца][регион],
// и проверяет полноту набора (fail-fast): ключ метрики обязан быть в ответе, а в её списке
// должны распознаться ровно четыре региона словаря (ценовой etfAPIPriceSeries не считается).
// Без этого проверки пропавшая метрика или регион молча дали бы частичный импорт: половина
// рядов осталась бы без обновления, а dagu — зелёным. Имя ряда ищется в etfRegionsByName;
// любое другое имя вне словаря — ошибка: иначе в macro_series появился бы ряд вида
// etf_flow_tonnes_antarctica. null в значении — отсутствие данных за месяц (точки у региона
// нет, и world за этот месяц не появится), а не нулевой поток: 0 в JSON — это значение.
func etfFlowSums(monthly etfAPIMonthlyFlows, apiKey string, scale float64) (map[time.Time]map[string]float64, error) {
	series, ok := monthly.Series[apiKey]
	if !ok {
		return nil, fmt.Errorf("в ответе нет метрики %s: набор метрик потоков неполон", apiKey)
	}
	sums := make(map[time.Time]map[string]float64, len(series))
	regions := make(map[string]bool, len(etfRegionsByName))
	for _, s := range series {
		if s.Name == etfAPIPriceSeries {
			continue
		}
		region, ok := etfRegionsByName[s.Name]
		if !ok {
			return nil, fmt.Errorf("метрика %s: ряд %q вне словаря регионов (известны: %s)",
				apiKey, s.Name, strings.Join(etfAPIKnownRegions(), ", "))
		}
		// Два ряда одного региона противоречивы: без ошибки второй молча перезаписал бы первый.
		if regions[region] {
			return nil, fmt.Errorf("метрика %s: регион %q встречается дважды — набор регионов противоречив",
				apiKey, s.Name)
		}
		regions[region] = true
		for _, p := range s.Data {
			if len(p) < 2 {
				return nil, fmt.Errorf("метрика %s: ряд %q: точка без пары [ms, value] (%d элементов)",
					apiKey, s.Name, len(p))
			}
			if p[1] == nil {
				continue // null — данных за месяц нет: точки нет вовсе (не путать с явным нулём)
			}
			if p[0] == nil {
				// Дата null при живом значении — дефект выгрузки: без ошибки точка ушла бы
				// в 1970-01-31 и легла бы в ряд как настоящий месяц.
				return nil, fmt.Errorf("метрика %s: ряд %q: значение %v без даты (null вместо ms)",
					apiKey, s.Name, *p[1])
			}
			date := etfAPIMonthEnd(*p[0])
			if sums[date] == nil {
				sums[date] = make(map[string]float64, len(etfRegionsByName))
			}
			sums[date][region] = *p[1] / scale
		}
	}
	if len(regions) != len(etfRegionsByName) {
		return nil, fmt.Errorf("метрика %s: распознано %d из %d регионов — набор регионов неполон",
			apiKey, len(regions), len(etfRegionsByName))
	}
	return sums, nil
}

// etfHoldingsPoints собирает точки стоков и AUM из таблиц tonnes и usd одного []seriesPoint.
func etfHoldingsPoints(monthly etfAPIMonthlyHoldings) ([]seriesPoint, error) {
	tables := []struct {
		table  etfAPIHoldingsTable
		metric string
	}{
		{monthly.Tonnes, etfMetricHoldingsTonnes},
		{monthly.USD, etfMetricAumUsdBn},
	}
	var points []seriesPoint
	for _, t := range tables {
		tablePoints, err := etfHoldingsTablePoints(t.table, t.metric)
		if err != nil {
			return nil, err
		}
		points = append(points, tablePoints...)
	}
	return points, nil
}

// etfHoldingsTablePoints — точки одной таблицы стоков: null у региона — данных нет и точки
// нет, 0 — значение (точка есть); world добавляет appendRegionPoints, когда есть все четыре.
func etfHoldingsTablePoints(table etfAPIHoldingsTable, metric string) ([]seriesPoint, error) {
	cols, err := etfAPIHoldingsColumns(table.Columns)
	if err != nil {
		return nil, fmt.Errorf("метрика %s: %w", metric, err)
	}
	scale := etfAPIScaleFor(metric)

	var points []seriesPoint
	for i, row := range table.Set {
		if len(row) < len(table.Columns) {
			return nil, fmt.Errorf("метрика %s: строка %d короче шапки (%d < %d)", metric, i, len(row), len(table.Columns))
		}
		if row[0] == nil {
			return nil, fmt.Errorf("метрика %s: строка %d без даты", metric, i)
		}
		date := etfAPIMonthEnd(*row[0])
		sums := make(map[string]float64, len(cols))
		for _, c := range cols {
			if row[c.col] == nil {
				continue
			}
			sums[c.region] = *row[c.col] / scale
		}
		points = appendRegionPoints(points, metric, date, sums)
	}
	return points, nil
}

// etfAPIHoldingsColumns размечает колонки таблицы стоков по шапке columns: нулевая — дата
// etfDateHeader (её значение и есть ms строки), дальше регионы словарём etfRegionsByName,
// замыкающая — цена etfAPIHoldingsPriceColumn. Все четыре региона обязаны быть в шапке и ни
// один не повторяется, а нулевая колонка обязана быть колонкой дат: пропавшая колонка молча
// остановила бы обновление ряда, имя вне словаря — дрейф выгрузки, а дубликат региона (шапка
// ["Date","North America","Europe","Asia","Asia","Gold, US$/oz"]) перезаписал бы значения
// первой колонки и выбросил бы Other, оставив набор «полным» из тех же четырёх регионов: ряды
// holdings_tonnes_other/world и aum_usd_bn_other/world молча перестали бы писаться.
func etfAPIHoldingsColumns(columns []string) ([]regionColumn, error) {
	if len(columns) == 0 {
		return nil, errors.New("шапка таблицы стоков пуста: колонок нет вовсе")
	}
	if date := strings.TrimSpace(columns[0]); !strings.EqualFold(date, etfDateHeader) {
		return nil, fmt.Errorf("нулевая колонка шапки %q — не %s: колонка дат пропала или сдвинулась",
			columns[0], etfDateHeader)
	}

	cols := make([]regionColumn, 0, len(etfRegionsByName))
	seen := make(map[string]bool, len(etfRegionsByName))
	for i, name := range columns {
		name = strings.TrimSpace(name)
		if i == 0 {
			continue
		}
		if region, ok := etfRegionsByName[name]; ok {
			// Два столбца одного региона противоречивы: без ошибки второй молча перезаписал бы
			// значения первого, а регион, чьего столбца в шапке нет, выпал бы из сумм.
			if seen[region] {
				return nil, fmt.Errorf("колонка %d %q: регион встречается дважды — шапка противоречива", i, name)
			}
			seen[region] = true
			cols = append(cols, regionColumn{col: i, region: region})
			continue
		}
		if name == etfAPIHoldingsPriceColumn {
			continue
		}
		return nil, fmt.Errorf("колонка %d %q вне словаря регионов и не цена (%s)",
			i, name, etfAPIHoldingsPriceColumn)
	}
	if len(cols) != len(etfRegionsByName) {
		return nil, fmt.Errorf("в шапке %d регионов из %d: колонка региона пропала", len(cols), len(etfRegionsByName))
	}
	return cols, nil
}

// etfAPIScaleFor — делитель значений метрики (etfAPIScale); метрики нет в словаре —
// значения идут как есть (тоннажные метрики).
func etfAPIScaleFor(metric string) float64 {
	if scale := etfAPIScale[metric]; scale != 0 {
		return scale
	}
	return 1
}

// etfAPIMonthEnd переводит ms-таймстемп точки API в конец месяца UTC: WGC отдаёт
// миллисекунды, а месяц считается в UTC — иначе результат зависел бы от таймзоны машины.
func etfAPIMonthEnd(ms float64) time.Time {
	return monthEnd(time.UnixMilli(int64(ms)).UTC())
}

// etfAPISortedDates — месяцы map по возрастанию: точки идут хронологически, как строки xlsx.
func etfAPISortedDates(sums map[time.Time]map[string]float64) []time.Time {
	dates := make([]time.Time, 0, len(sums))
	for date := range sums {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	return dates
}

// etfAPIKnownRegions — имена регионов словаря в стабильном порядке: список идёт в текст
// ошибки, а перебор map давал бы разный текст от запуска к запуску.
func etfAPIKnownRegions() []string {
	names := make([]string, 0, len(etfRegionsByName))
	for name := range etfRegionsByName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
