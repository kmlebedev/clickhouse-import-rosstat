// Импортёры пакета: bold_etf_flows — ежедневный T+1 срез агрегата 73 золотых ETF (bold.report,
// ByteTree) в семь рядов etf_bold_* (macro_series, source='bold') и пер-фондовый дневной срез
// bold_fund_snapshot; bold_gold_price — ежедневная цена золота источника в общую с пакетом gold/
// таблицу gold_prices новой площадкой venue='bold'. Чистый разбор ответов и конверсии — в
// parse.go; здесь сеть (только util.GetJSON), выбор режима (инкремент или полная история) и
// запись батчами. Документация пакета — в parse.go (пакет не должен иметь двух
// package-комментариев).

package bold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/gold"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// boldSource — source рядов пакета в macro_series и series_catalog.
	boldSource = "bold"

	// boldEtfName — имя импортёра: Name() (контракт dagu и make import STAT=…), префикс логов
	// и ошибок хелперов, которые самого boldEtfFlows не видят.
	boldEtfName = "bold_etf_flows"

	// boldFundTable — пер-фондовый дневной срез (спека §4.3). Ключ включает name (Ruling 3):
	// тикер у источника не уникален — в живом срезе 73 строки при 71 уникальном тикере (GLD
	// встречается трижды: KTAM Gold ETF Tracker Class GLD (Bangkok), NewGold Issuer Ltd,
	// SPDR Gold Shares), и ключ (date, ticker) схлопнул бы два реальных фонда. Витрина
	// сортирует так же — (date DESC, ticker, name).
	boldFundTable = "bold_fund_snapshot"

	// macroSeriesTable — таблица длинных рядов (общая со всеми импортёрами macro_series).
	// Здесь лежит своя копия канонического DDL, как в wgc/etf.go: импортёр обязан работать и
	// на чистой базе, где macro_series ещё не создана. Копии обязаны совпадать — менять обе
	// (ARCHITECTURE.md §6.2).
	macroSeriesTable = "macro_series"

	// boldFullFetchGapHours — порог «инкремент не может исключить пропуск дней»: 4 календарных
	// дня (план, Review Focus 3). Пятница → понедельник — это 3 дня (< 96 ч), и инкремент
	// закрывает такой разрыв сам: Δ считается от стока, прочитанного из ClickHouse, а не от
	// предыдущей строки ответа.
	boldFullFetchGapHours = 96
)

// DDL и INSERT-ы пакета. Все DDL выполняются до батчей: на чистой базе таблиц ещё нет.
const (
	// boldMacroSeriesDdl — копия канона macro_series из wgc/etf.go (§4.1 спеки).
	boldMacroSeriesDdl = `CREATE TABLE IF NOT EXISTS ` + macroSeriesTable + ` (
			  source LowCardinality(String)
			, series LowCardinality(String)
			, date Date32
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
	`
	boldMacroSeriesInsert = "INSERT INTO " + macroSeriesTable + " (source, series, date, value)"

	// boldFundDdl — копия канона bold_fund_snapshot из спеки §4.3 (ключ — (date, ticker, name),
	// см. boldFundTable).
	boldFundDdl = `CREATE TABLE IF NOT EXISTS ` + boldFundTable + ` (
			  date Date32
			, ticker LowCardinality(String)
			, name LowCardinality(String)
			, units_oz Float64
			, units_change_oz Float64
			, aum_usd Float64
			, aum_change_usd Float64
			, price_usd Float64
			, price_change_usd Float64
			, shares Float64
			, shares_change Float64
		) ENGINE = ReplacingMergeTree ORDER BY (date, ticker, name);
	`
	boldFundInsert = "INSERT INTO " + boldFundTable + " (date, ticker, name, units_oz, units_change_oz," +
		" aum_usd, aum_change_usd, price_usd, price_change_usd, shares, shares_change)"
)

// Гвардейские запросы к ClickHouse. Их текст — часть контракта (пинится тестами): у
// kimi_reader доступа к macro_series, bold_fund_snapshot и gold_prices нет (последняя видна ему
// только витриной v_gold_prices), и проверить иначе будет нечем.
const (
	// boldStoredSeriesMaxQuery — максимум дат рядов ETF-контура. LIKE с экранированным «_»:
	// «_» в LIKE — одиночный символ, и без экранирования шаблон поймал бы чужие ряды.
	boldStoredSeriesMaxQuery = "SELECT maxOrNull(date) FROM " + macroSeriesTable +
		" FINAL WHERE source = '" + boldSource + "' AND series LIKE 'etf\\_bold\\_%'"

	// boldStoredFundMaxQuery — максимум дат пер-фондового среза: вторая половина «сохранённого
	// максимума» (Ruling 7).
	boldStoredFundMaxQuery = "SELECT maxOrNull(date) FROM " + boldFundTable + " FINAL"
)

// Контур цены (спека §4.2): цена золота источника живёт в общей с пакетом gold/ таблице
// gold_prices новой площадкой, поэтому константы, копия канонического DDL и гвардейский запрос
// стоят отдельной секцией — читателю видно, что это не второй ряд ETF-контура.
const (
	// boldGoldPriceName — имя импортёра цены: Name() (контракт dagu и make import STAT=…),
	// префикс логов и ошибок хелперов.
	boldGoldPriceName = "bold_gold_price"

	// boldPriceTable — таблица цены золота: та же, что у пакета gold/ (площадки moex_fix_usd и
	// comex_front), — цена источника добавляется новой площадкой, а не вторым рядом цены.
	boldPriceTable = "gold_prices"

	// boldPriceVenue — площадка цены bold: ключ gold_prices — (venue, date), и она же series
	// записи каталога (в gold_prices series == venue, как у moex_fix_usd).
	boldPriceVenue = "bold"
)

// boldGoldPricesDdl — копия канонического DDL gold_prices из gold/gold.go (та же копия лежит в
// ingest/schema.go: у таблицы три независимых писателя). Копии обязаны совпадать, как у копий
// macro_series (ARCHITECTURE.md §6.2): импортёр цены обязан отработать и на чистой базе, где
// пакет gold/ ещё не прогонялся.
const (
	boldGoldPricesDdl = `CREATE TABLE IF NOT EXISTS ` + boldPriceTable + ` (
			  venue LowCardinality(String)
			, date Date32
			, usd Float64
		) ENGINE = ReplacingMergeTree ORDER BY (venue, date);
	`
	boldGoldPricesInsert = "INSERT INTO " + boldPriceTable + " (venue, date, usd)"
)

// boldPriceStoredMaxQuery — максимум дат площадки bold: сохранённый максимум импортёра цены.
// Площадка в условии обязательна: gold_prices делят три источника (gold/, ingest и этот), и
// максимум по всей таблице увёл бы прогон в «нечего обновлять» из-за чужих свежих точек.
const boldPriceStoredMaxQuery = "SELECT maxOrNull(date) FROM " + boldPriceTable +
	" FINAL WHERE venue = '" + boldPriceVenue + "'"

// boldEtfFlows — импортёр bold_etf_flows. Каждый набор ответов — четыре запроса: инкремент
// (обычный прогон) читает units-latest, aum-latest, flows/all-latest и summary, полный режим —
// три файла истории и summary. Полный прогон начинается с инкрементного набора (он даёт дату для
// решения no-op и для пер-фондового среза), поэтому в сумме делает восемь запросов; запись — два
// батча: macro_series (7 рядов) и bold_fund_snapshot (73 фонда).
type boldEtfFlows struct {
	// apiBase — база API с тем же смыслом, что apiBase в wgc: пустая означает боевую
	// boldBaseURL, в тестах подставляется httptest-сервер.
	apiBase string
	// now — часы прогона (nil → time.Now): ими инструментирован warn об отставании даты данных
	// от дня прогона. Шов для тестов, как apiBase.
	now func() time.Time
}

func (s *boldEtfFlows) Name() string { return boldEtfName }

// base — база API прогона: пустое поле означает боевой адрес.
func (s *boldEtfFlows) base() string {
	if s.apiBase == "" {
		return boldBaseURL
	}
	return s.apiBase
}

// clock — время прогона: nil-поле означает прод (time.Now).
func (s *boldEtfFlows) clock() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

// Import — один прогон импортёра. Порядок шагов важен: (1) сохранённый максимум из ClickHouse
// (ошибка чтения — warn и нулевое время, как в wgc: диагностический запрос не повод терять
// обновление); (2) четыре запроса «последнего дня» — они нужны и на полном пути: их дата
// решает no-op и датирует пер-фондовый срез; (3) решение no-op — дата источника не новее
// сохранённого максимума значит, что писать нечего, и батч не открывается вовсе; (4) выбор
// режима — полная история, если инкремент не может исключить пропуск дней (boldNeedsFullFetch)
// или если сохранённые максимумы ряда и среза разошлись (Ruling 8: инкремент посчитал бы
// многодневный поток за дневной); (5) точки фондов
// считаются до батчей (ошибка здесь — смена формы ответа summary.json); (6) обе DDL; (7) два
// батча; (8) каталог — только после успешных Send (описание ряда, которого в таблице нет, —
// обещание данных, которых агент не найдёт; прецедент dcf_mine_plans).
func (s *boldEtfFlows) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	base := s.base()

	seriesMax, fundMax, serr := boldStoredBounds(ctx, conn)
	if serr != nil {
		log.Warnf("%s: сохранённый максимум дат не прочитан (%v) — считаю ряды пустыми и беру полную историю", boldEtfName, serr)
		seriesMax, fundMax = time.Time{}, time.Time{}
	}
	stored := boldMinDate(seriesMax, fundMax)

	latest, funds, err := boldEtfFetchLatest(base)
	if err != nil {
		return 0, err
	}
	latestDate, err := parseBoldDate(latest.Date)
	if err != nil {
		return 0, err
	}

	if boldSourceLooksStale(latestDate, s.clock()) {
		log.Warnf("%s: дата источника %s отстаёт от дня прогона больше чем на %d ч — возможен застой источника или его кэша (импорт не отменяется)",
			boldEtfName, latestDate.Format(boldDateLayout), boldFullFetchGapHours)
	}

	// Идемпотентный no-op: дата источника не новее сохранённого максимума (или равна ему) —
	// писать те же точки незачем, и соединение дальше чтения максимума не трогается.
	// Расхождение половин (seriesMax != fundMax) no-op-ом не закрывается: это разбирает шаг ниже.
	diverged := !seriesMax.Equal(fundMax)
	if !stored.IsZero() && !latestDate.After(stored) {
		if diverged {
			log.Warnf("%s: ряд и срез разошлись (%s против %s), но дата источника %s не новее обоих — догонять нечего",
				boldEtfName, boldFormatDate(seriesMax), boldFormatDate(fundMax), latestDate.Format(boldDateLayout))
		}
		log.Infof("%s: дата источника %s не новее сохранённого максимума %s — нечего обновлять (0 строк)",
			boldEtfName, latestDate.Format(boldDateLayout), stored.Format(boldDateLayout))
		return 0, nil
	}

	var points []point
	// Полный путь — не только «пропуск дней» (boldNeedsFullFetch), но и расхождение половин
	// сохранённого максимума (Ruling 8): частичный сбой прошлого прогона (батч рядов прошёл, батч
	// среза упал) оставил срез за 08.10 при рядах за 09.10. Инкремент в такой день посчитал бы Δ
	// от ряда, отставшего на торговый день, — то есть многодневный поток, выданный за дневной, и
	// молча: база `stored` выглядит «предыдущим торговым днём». Полный путь пишет и ряды, и срез за
	// дату источника; при обычном совпадении максимума истории с датой источника это выравнивает
	// S = F = L, и предусловие инкремента снова выполнено (если файл истории отстал от ответа
	// *-latest, срез уйдёт вперёд и следующий прогон снова пойдёт полным путём — это лишние
	// запросы, но не потеря данных: полный путь идемпотентен).
	if boldNeedsFullFetch(latestDate, stored) || diverged {
		rows, fullFunds, ferr := boldEtfFullFetch(base)
		if ferr != nil {
			return 0, ferr
		}
		funds = fullFunds
		if points, err = boldETFSets(rows, nil, time.Time{}); err != nil {
			return 0, err
		}
		if diverged {
			log.Infof("%s: полный режим: ряд и срез разошлись (%s против %s) — выравниваю по дате источника %s, дней истории %d",
				boldEtfName, boldFormatDate(seriesMax), boldFormatDate(fundMax), latestDate.Format(boldDateLayout), len(rows))
		} else {
			log.Infof("%s: полный режим: сохранённый максимум %s, дата источника %s, дней истории %d",
				boldEtfName, boldFormatDate(stored), latestDate.Format(boldDateLayout), len(rows))
		}
	} else {
		// Инкремент: база для Δ — сток предыдущего торгового дня из ClickHouse. Ошибка чтения
		// останавливает импорт: без предыдущего стока поток молча стал бы нулём (Review Focus 4).
		prevUnits, uerr := boldStoredUnits(ctx, conn, stored)
		if uerr != nil {
			return 0, uerr
		}
		if points, err = boldETFSets([]boldRow{latest}, &prevUnits, stored); err != nil {
			return 0, err
		}
	}

	fundPoints, err := boldFundPoints(funds, latestDate)
	if err != nil {
		return 0, err
	}

	if err = conn.Exec(ctx, boldFundDdl); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldEtfName, boldFundTable, err)
	}
	if err = conn.Exec(ctx, boldMacroSeriesDdl); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldEtfName, macroSeriesTable, err)
	}

	count, err = boldWritePoints(ctx, conn, points)
	if err != nil {
		return count, err
	}
	if err = boldWriteFunds(ctx, conn, fundPoints); err != nil {
		return count, err
	}

	if err = util.UpsertSeriesCatalog(ctx, conn, boldETFSeriesMeta()); err != nil {
		return count, fmt.Errorf("%s: каталог рядов: %w", boldEtfName, err)
	}

	log.Infof("Imported %d rows of %s (bold ETF: %d точек, %d фондов)", count, boldEtfName, len(points), len(fundPoints))

	return count, nil
}

// boldFormatDate — дата в лог: нулевое время означает «в таблице данных нет», и печатать
// 0001-01-01 в логе полного режима незачем.
func boldFormatDate(date time.Time) string {
	if date.IsZero() {
		return "(таблицы пусты)"
	}
	return date.Format(boldDateLayout)
}

// boldSourceLooksStale — решение warn-а о застое источника: дата данных отстала от дня прогона
// больше чем на порог полного пути (4 календарных дня). Это не отказ импорта: данные T+1, и
// перезапись тех же точек идемпотентна, а ложный отказ оставил бы ряды без обновления (тот же
// принцип, что у guard-а устаревшего CDN в wgc_etf_flows). Вынесено чистой функцией: связка
// «дата источника ↔ часы прогона» проверяется табличным тестом без сети и соединения.
func boldSourceLooksStale(latest, now time.Time) bool {
	if latest.IsZero() || now.IsZero() {
		return false
	}
	return now.Sub(latest).Hours() > boldFullFetchGapHours
}

// boldNeedsFullFetch — «инкремент не может исключить пропуск дней»: таблицы пусты (нулевой
// сохранённый максимум) или дата источника ушла вперёд больше чем на 4 календарных дня (сбой
// джобы, длинные выходные, отпуск). Равенство дат сюда не доходит — его отсекает no-op в Import.
func boldNeedsFullFetch(latest, storedMax time.Time) bool {
	if storedMax.IsZero() {
		return true
	}
	return latest.Sub(storedMax).Hours() > boldFullFetchGapHours
}

// boldFetchBody скачивает тело эндпоинта. Разбор — в parse.go: тело отдаётся ему как есть,
// поэтому единственный разбор здесь — json.RawMessage (проверку статуса, пустого тела и
// дедлайна делает util.GetJSON). Ошибка называет путь эндпоинта: по «пустое тело» без имени
// файла не понять, какой из четырёх ответов сломался.
func boldFetchBody(baseURL, path string) ([]byte, error) {
	var body json.RawMessage
	if err := util.GetJSON(baseURL+path, &body); err != nil {
		return nil, fmt.Errorf("bold: %s: %w", path, err)
	}
	return body, nil
}

// boldFetchLatestRow — ответ «последнего дня» одной метрики.
func boldFetchLatestRow(baseURL, path string, metric boldMetric) (boldRow, error) {
	body, err := boldFetchBody(baseURL, path)
	if err != nil {
		return boldRow{}, err
	}
	return parseLatest(body, metric)
}

// boldFetchHistory — файл истории одной метрики.
func boldFetchHistory(baseURL, path string, metric boldMetric) ([]boldRow, error) {
	body, err := boldFetchBody(baseURL, path)
	if err != nil {
		return nil, err
	}
	return parseHistory(body, metric)
}

// boldFetchFunds — пер-фондовый срез (gold/summary). Даты в ответе нет: её приносит
// «последний день», и расхождение с ним обязано быть ошибкой, а не записью под чужой датой.
func boldFetchFunds(baseURL string) ([]boldFundRow, error) {
	body, err := boldFetchBody(baseURL, boldPathSummary)
	if err != nil {
		return nil, err
	}
	return parseFundSnapshot(body)
}

// boldEtfFetchLatest — инкремент: четыре запроса (units-latest, aum-latest, flows-latest,
// summary) и одна слитая строка. Поля берутся из своих файлов: units и price — из units,
// aum — из aum, culm* — из flows; фонды — из среза.
func boldEtfFetchLatest(baseURL string) (boldRow, []boldFundRow, error) {
	units, err := boldFetchLatestRow(baseURL, boldPathUnitsLatest, boldMetricUnits)
	if err != nil {
		return boldRow{}, nil, err
	}
	aum, err := boldFetchLatestRow(baseURL, boldPathAumLatest, boldMetricAum)
	if err != nil {
		return boldRow{}, nil, err
	}
	flows, err := boldFetchLatestRow(baseURL, boldPathFlowsLatest, boldMetricFlows)
	if err != nil {
		return boldRow{}, nil, err
	}

	merged, err := boldMergeLatest(units, aum, flows)
	if err != nil {
		return boldRow{}, nil, err
	}

	funds, err := boldFetchFunds(baseURL)
	if err != nil {
		return boldRow{}, nil, err
	}
	return merged, funds, nil
}

// boldMergeLatest сливает три ответа «последнего дня» в одну строку. Даты и цена обязаны
// совпасть (Review Focus 2): под датой строки пишется ещё и пер-фондовый срез, а цена входит
// множителем в etf_bold_flow_usd_mln — «дата из одного файла, цена из другого» посчитала бы
// USD-поток по чужой цене и записала бы срез под чужим днём. Три ответа приходят из одной
// публикации источника, поэтому равенство цены — не придирка, а сигнал рассогласованной пары
// файлов; сравниваются значения, а не указатели.
func boldMergeLatest(units, aum, flows boldRow) (boldRow, error) {
	if units.Date != aum.Date || units.Date != flows.Date {
		return boldRow{}, fmt.Errorf("bold: даты ответов «последнего дня» разошлись: %s за %s, %s за %s, %s за %s — день не собран",
			boldPathUnitsLatest, units.Date, boldPathAumLatest, aum.Date, boldPathFlowsLatest, flows.Date)
	}
	prices := []struct {
		path  string
		price *float64
	}{
		{boldPathUnitsLatest, units.Price},
		{boldPathAumLatest, aum.Price},
		{boldPathFlowsLatest, flows.Price},
	}
	for _, p := range prices {
		if p.price == nil {
			return boldRow{}, fmt.Errorf("bold: в ответе %s за %s нет price — USD-поток дня посчитать нечем", p.path, units.Date)
		}
	}
	if *units.Price != *aum.Price || *units.Price != *flows.Price {
		return boldRow{}, fmt.Errorf("bold: цена дня разошлась между ответами «последнего дня» за %s: %s %v, %s %v, %s %v",
			units.Date, boldPathUnitsLatest, *units.Price, boldPathAumLatest, *aum.Price, boldPathFlowsLatest, *flows.Price)
	}

	return boldRow{
		Date:   units.Date,
		Units:  units.Units,
		Aum:    aum.Aum,
		Culm7:  flows.Culm7,
		Culm30: flows.Culm30,
		Culm90: flows.Culm90,
		Price:  units.Price,
	}, nil
}

// boldEtfFullFetch — полный режим: три файла истории (units, aum, flows/all) и пер-фондовый
// срез. Четыре запроса, как и у инкремента.
func boldEtfFullFetch(baseURL string) ([]boldRow, []boldFundRow, error) {
	rows, err := boldEtfHistory(baseURL)
	if err != nil {
		return nil, nil, err
	}
	funds, err := boldFetchFunds(baseURL)
	if err != nil {
		return nil, nil, err
	}
	return rows, funds, nil
}

// boldEtfHistory скачивает и сливает три файла истории в строки по дате (порядок — по дате).
func boldEtfHistory(baseURL string) ([]boldRow, error) {
	units, err := boldFetchHistory(baseURL, boldPathUnits, boldMetricUnits)
	if err != nil {
		return nil, err
	}
	aum, err := boldFetchHistory(baseURL, boldPathAum, boldMetricAum)
	if err != nil {
		return nil, err
	}
	flows, err := boldFetchHistory(baseURL, boldPathFlows, boldMetricFlows)
	if err != nil {
		return nil, err
	}
	return boldMergeHistory(units, aum, flows)
}

// boldHistoryField — поле строки истории с именем для диагностики и установщиком в слитую
// строку.
type boldHistoryField struct {
	name  string
	value *float64
	set   func(*boldRow, *float64)
}

// boldHistoryFields перечисляет заполненные поля строки истории. Список строится по строке, а
// не по эндпоинту: владение полем — свойство здорового источника (units и price несёт файл
// units, aum — файл aum, окна — файл flows), и проверять его дешевле по факту, чем по таблице
// ожиданий — так рассогласованная пара файлов видна, а не игнорируется.
func boldHistoryFields(r boldRow) []boldHistoryField {
	return []boldHistoryField{
		{name: "units", value: r.Units, set: func(m *boldRow, v *float64) { m.Units = v }},
		{name: "aum", value: r.Aum, set: func(m *boldRow, v *float64) { m.Aum = v }},
		{name: "culm7day", value: r.Culm7, set: func(m *boldRow, v *float64) { m.Culm7 = v }},
		{name: "culm30day", value: r.Culm30, set: func(m *boldRow, v *float64) { m.Culm30 = v }},
		{name: "culm90day", value: r.Culm90, set: func(m *boldRow, v *float64) { m.Culm90 = v }},
		{name: "price", value: r.Price, set: func(m *boldRow, v *float64) { m.Price = v }},
	}
}

// boldHistorySlot — кто и с каким значением положил поле в слитую строку: нужен для
// диагностики дубля (дата, поле, оба файла, оба значения).
type boldHistorySlot struct {
	path  string
	value float64
}

// boldMergeHistory сливает строки трёх файлов истории по дате. Проверки (Ruling 5) — до записи,
// потому что цена ошибки здесь невидима: день, где есть units, но нет aum (или наоборот), дал бы
// ряд стока без второго ряда, и «полдня» ушло бы в базу как факт.
//
// Второй класс ошибки — дубликат даты между файлами: поле, пришедшее из двух файлов с разными
// значениями, означает рассогласованную пару файлов (источник публикует одну и ту же цену дня
// и одни и те же значения во всех файлах), и молчаливый выбор одного из двух значений спрятал бы
// это. Совпавшее значение дублем не считается: запись по ключу (date, …) идемпотентна.
//
// Строки, у которых нет ни одного ETF-поля (первые 64 дня истории несут только price),
// отбрасываются: это цена золота, её ведёт импортёр bold_gold_price, и в ряды этого импортёра
// такая строка не даёт ни одной точки. Пустой результат — ошибка: молчаливый ноль оставил бы
// dagu зелёным, а ряды без обновления (урок eia).
func boldMergeHistory(units, aum, flows []boldRow) ([]boldRow, error) {
	merged := map[string]*boldRow{}
	slots := map[string]map[string]boldHistorySlot{}

	for _, file := range []struct {
		path string
		rows []boldRow
	}{
		{boldPathUnits, units},
		{boldPathAum, aum},
		{boldPathFlows, flows},
	} {
		for _, r := range file.rows {
			m, ok := merged[r.Date]
			if !ok {
				m = &boldRow{Date: r.Date}
				merged[r.Date] = m
				slots[r.Date] = map[string]boldHistorySlot{}
			}
			for _, field := range boldHistoryFields(r) {
				if field.value == nil {
					continue
				}
				if prev, ok := slots[r.Date][field.name]; ok {
					if prev.value != *field.value {
						return nil, fmt.Errorf("bold: дата %s пришла из двух файлов истории: поле %s в %s = %v, а в %s = %v — слияние неоднозначно",
							r.Date, field.name, prev.path, prev.value, file.path, *field.value)
					}
					continue
				}
				slots[r.Date][field.name] = boldHistorySlot{path: file.path, value: *field.value}
				field.set(m, field.value)
			}
		}
	}

	type datedRow struct {
		date time.Time
		row  boldRow
	}
	dated := make([]datedRow, 0, len(merged))
	for date, m := range merged {
		if (m.Units == nil) != (m.Aum == nil) {
			present, missing := "units", "aum"
			if m.Units == nil {
				present, missing = "aum", "units"
			}
			return nil, fmt.Errorf("bold: день %s неполон: есть %s, но нет %s — сток и AUM обязаны приходить вместе (файлы %s и %s)",
				date, present, missing, boldPathUnits, boldPathAum)
		}
		if m.Units == nil && m.Aum == nil && m.Culm7 == nil && m.Culm30 == nil && m.Culm90 == nil {
			continue
		}
		d, err := parseBoldDate(date)
		if err != nil {
			return nil, err
		}
		dated = append(dated, datedRow{date: d, row: *m})
	}
	if len(dated) == 0 {
		return nil, errors.New("bold: в истории нет ни одного дня с ETF-полями (units, aum или окна culm*)")
	}
	sort.Slice(dated, func(i, j int) bool { return dated[i].date.Before(dated[j].date) })

	rows := make([]boldRow, 0, len(dated))
	for _, item := range dated {
		rows = append(rows, item.row)
	}
	return rows, nil
}

// boldStoredBounds читает обе половины сохранённого максимума: максимум дат рядов source='bold'
// в macro_series и максимум дат bold_fund_snapshot. Обе нужны вызывающему: минимум выбирает
// режим (Ruling 7), а само расхождение половин — признак частичного сбоя прошлого прогона
// (Ruling 8). Нулевое время половины означает «данных нет» (в том числе таблицы ещё нет);
// ошибки не проглатываются, решение «warn и полный путь» принимает Import.
func boldStoredBounds(ctx context.Context, conn driver.Conn) (seriesMax, fundMax time.Time, err error) {
	seriesMax, err = boldScanMaxDate(ctx, conn, boldStoredSeriesMaxQuery)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	fundMax, err = boldScanMaxDate(ctx, conn, boldStoredFundMaxQuery)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return seriesMax, fundMax, nil
}

// boldStoredMaxDate — «сохранённый максимум» для выбора режима: минимум из максимума дат рядов
// source='bold' в macro_series и максимума дат bold_fund_snapshot (Ruling 7). Минимум, а не
// максимум: ряды и срез фондов пишутся двумя батчами, и частичный сбой (батч рядов прошёл, батч
// фондов упал) оставил бы срез позади — по максимуму следующий прогон счёл бы день уже
// обработанным, и пер-фондовый срез отстал бы навсегда. Минимум заставляет такой прогон
// догнать день. Нулевое время любой из половин («данных нет», в том числе таблицы ещё нет)
// означает полный путь.
func boldStoredMaxDate(ctx context.Context, conn driver.Conn) (time.Time, error) {
	seriesMax, fundMax, err := boldStoredBounds(ctx, conn)
	if err != nil {
		return time.Time{}, err
	}
	return boldMinDate(seriesMax, fundMax), nil
}

// boldScanMaxDate выполняет диагностический maxOrNull(date) и нормализует ответ пустой таблицы
// в нулевое время: NULL — это факт из ответа, а не вывод из года (прежняя эвристика
// max(date).Year() <= 1970 объявляла пустой и таблицу с настоящими точками 1970-01; поэтому
// Date сканируется в указатель). Отсутствие таблицы — тоже «данных нет»: на чистой базе её ещё
// нет, и первый прогон не должен получать ложный warn о непрочитанном максимуме. FINAL — по
// определению ReplacingMergeTree: без него запрос читал бы физический хвост таблицы.
func boldScanMaxDate(ctx context.Context, conn driver.Conn, query string) (time.Time, error) {
	var maxDate *time.Time
	if err := conn.QueryRow(ctx, query).Scan(&maxDate); err != nil {
		if boldTableMissing(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	if maxDate == nil {
		return time.Time{}, nil
	}
	// Дата приводится к UTC: clickhouse-go отдаёт Date с таймзоной сессии, а даты точек — UTC,
	// и сравнение с датой источника иначе зависело бы от настроек соединения.
	return time.Date(maxDate.Year(), maxDate.Month(), maxDate.Day(), 0, 0, 0, 0, time.UTC), nil
}

// boldTableMissing — «таблицы ещё нет»: UNKNOWN_TABLE (код 60 ClickHouse, текст ошибки
// содержит имя ошибки). Строковая проверка вместо разбора *clickhouse.Exception намеренно:
// пакету источников не нужен корневой пакет драйвера (как и wgc), а ClickHouse не менял имя
// ошибки с 2013 года. Формулировка «does not exist» добавлена как второй вариант текста.
func boldTableMissing(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "UNKNOWN_TABLE") || strings.Contains(message, "does not exist")
}

// boldMinDate — минимум двух дат с учётом нулевого времени: ноль означает «в этой таблице
// данных нет», и минимум с любой датой — ноль (уводит на полный путь; см. boldStoredMaxDate).
func boldMinDate(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if a.Before(b) {
		return a
	}
	return b
}

// boldStoredUnits читает сток предыдущего торгового дня — базу для дневного потока. В базе ряд
// etf_bold_holdings_tonnes в тоннах, а boldETFSets считает Δ в унциях источника, поэтому
// конверсия обратно здесь: без неё Δ была бы в 32150 раз меньше унции, поток оказался бы
// абсурдно отрицательным, а ряд — «на месте». Нет точки за эту дату — ошибка, а не ноль:
// без предыдущего стока поток молча стал бы нулём или ценой дня (Review Focus 4).
func boldStoredUnits(ctx context.Context, conn driver.Conn, date time.Time) (float64, error) {
	query := "SELECT maxOrNull(value) FROM " + macroSeriesTable + " FINAL WHERE source = '" + boldSource +
		"' AND series = '" + boldSeriesHoldingsTonnes + "' AND date = '" + date.Format(boldDateLayout) + "'"
	var tonnes *float64
	if err := conn.QueryRow(ctx, query).Scan(&tonnes); err != nil {
		return 0, fmt.Errorf("%s: предыдущий сток за %s не прочитан: %w", boldEtfName, date.Format(boldDateLayout), err)
	}
	if tonnes == nil {
		return 0, fmt.Errorf("%s: нет запасов (%s) за %s — дневной поток (Δ) посчитать нечем",
			boldEtfName, boldSeriesHoldingsTonnes, date.Format(boldDateLayout))
	}
	return *tonnes * ouncesPerTonne, nil
}

// boldWritePoints пишет точки семи рядов одним батчем. Порядок батча детерминирован (сортировка
// по series, date): повторный импорт воспроизводим независимо от порядка обхода источника.
// Сортируется копия: у вызывающего срез должен остаться в том порядке, в котором он его собрал
// (побочный эффект на аргументе — тот класс ошибок, который потом ищут в чужом вызове).
// Abort после ошибки Append не даёт соединению остаться с открытым батчем (следующий шаг DAG-а
// получил бы «уже есть незакрытый батч»); сбой Abort логируем, а возвращаемой ошибкой остаётся
// первопричина Append (прецедент wgc/etf.go).
func boldWritePoints(ctx context.Context, conn driver.Conn, points []point) (int64, error) {
	points = slices.Clone(points)
	sort.Slice(points, func(i, j int) bool {
		if points[i].Series != points[j].Series {
			return points[i].Series < points[j].Series
		}
		return points[i].Date.Before(points[j].Date)
	})

	batch, err := conn.PrepareBatch(ctx, boldMacroSeriesInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldEtfName, macroSeriesTable, err)
	}

	var count int64
	for _, p := range points {
		if err = batch.Append(boldSource, p.Series, p.Date, p.Value); err != nil {
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки append: %v", boldEtfName, aerr)
			}
			return 0, fmt.Errorf("%s: append %s %s: %w", boldEtfName, p.Series, p.Date.Format(boldDateLayout), err)
		}
		count++
	}

	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldEtfName, macroSeriesTable, err)
	}
	return count, nil
}

// boldWriteFunds пишет пер-фондовый срез одним батчем (73 строки за прогон). Порядок — по
// (ticker, name): он совпадает с ключом таблицы, и повторный импорт воспроизводим. Как и у точек,
// сортируется копия — аргумент вызывающего не меняется.
func boldWriteFunds(ctx context.Context, conn driver.Conn, funds []boldFundPoint) error {
	funds = slices.Clone(funds)
	sort.Slice(funds, func(i, j int) bool {
		if funds[i].Ticker != funds[j].Ticker {
			return funds[i].Ticker < funds[j].Ticker
		}
		return funds[i].Name < funds[j].Name
	})

	batch, err := conn.PrepareBatch(ctx, boldFundInsert)
	if err != nil {
		return fmt.Errorf("%s: %s: %w", boldEtfName, boldFundTable, err)
	}

	for _, f := range funds {
		if err = batch.Append(f.Date, f.Ticker, f.Name, f.Units, f.UnitsChange, f.Aum, f.AumChange,
			f.Price, f.PriceChange, f.Shares, f.SharesChange); err != nil {
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч фондов после ошибки append: %v", boldEtfName, aerr)
			}
			return fmt.Errorf("%s: append фонда %s (%s) за %s: %w",
				boldEtfName, f.Ticker, f.Name, f.Date.Format(boldDateLayout), err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("%s: %s: %w", boldEtfName, boldFundTable, err)
	}
	return nil
}

// boldGoldPrice — импортёр bold_gold_price: ежедневная цена золота bold.report в общую таблицу
// gold_prices площадкой venue='bold'. Один запрос за прогон в любом режиме: инкремент читает
// price-latest, полный путь — файл истории price.json. Решение о режиме принимается до запроса
// (см. Import), поэтому «пробного» price-latest, как у ETF-импортёра, здесь нет: он сделал бы
// полный путь двухзапросным.
type boldGoldPrice struct {
	// apiBase — база API с тем же смыслом, что apiBase в boldEtfFlows: пустая означает боевую
	// boldBaseURL, в тестах подставляется httptest-сервер.
	apiBase string
	// now — часы прогона (nil → time.Now): вход решения о режиме. Шов для тестов, как apiBase:
	// с реальными часами выбор «инкремент или полный путь» зависел бы от дня запуска тестов.
	now func() time.Time
}

func (s *boldGoldPrice) Name() string { return boldGoldPriceName }

// base — база API прогона: пустое поле означает боевой адрес (пара к boldEtfFlows.base).
func (s *boldGoldPrice) base() string {
	if s.apiBase == "" {
		return boldBaseURL
	}
	return s.apiBase
}

// clock — время прогона: nil-поле означает прод (time.Now), как у boldEtfFlows.
func (s *boldGoldPrice) clock() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

// Import — один прогон импортёра цены. Порядок шагов: (1) сохранённый максимум площадки bold
// (ошибка чтения — warn и нулевое время: диагностический запрос не повод терять обновление, как
// в bold_etf_flows); (2) выбор режима до запроса; (3) один запрос источника — price-latest
// (инкремент) или price.json (полный путь); (4) на инкременте дата источника не новее
// сохранённого максимума — писать нечего, и соединение дальше чтения максимума не трогается
// (ни DDL, ни батча, ни каталога); (5) DDL — копия канонического DDL gold_prices (на чистой базе
// таблицы ещё нет); (6) батч venue='bold'; (7) каталог — только после успешного Send (описание
// ряда, которого в таблице нет, — обещание данных, которых агент не найдёт; прецедент
// bold_etf_flows).
//
// Решение о режиме принимается по часам прогона, а не по дате ответа: инкремент и полный путь
// стоят по одному запросу, а «пробный» price-latest перед файлом истории сделал бы полный путь
// двухзапросным. Верхняя граница даты источника и без запроса — день прогона, поэтому
// boldNeedsFullFetch (тот же порог 4 календарных дня, что у ETF-импортёра) вызывается с часами:
// сохранённый максимум старше порога значит, что источник мог опубликовать несколько дней,
// которых у нас нет, и инкремент их не доберёт. Пустая площадка уводит на полный путь тем же
// вызовом (нулевой максимум — это всегда полный путь).
//
// Часы приводятся к UTC до сравнения: сохранённый максимум — дата UTC-календаря (boldScanMaxDate
// собирает её как 00:00 UTC), и порог обязан считаться по одной шкале с ней. Локальное время
// процесса (+05:00 у машины разработчика, −07:00 у сборки в другом поясе) дало бы другое число
// часов у того же момента, а решение о режиме не должно зависеть от таймзоны прогона (Ruling 13).
func (s *boldGoldPrice) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	stored, serr := boldPriceStoredMaxDate(ctx, conn)
	if serr != nil {
		log.Warnf("%s: сохранённый максимум площадки %s не прочитан (%v) — беру полную историю",
			boldGoldPriceName, boldPriceVenue, serr)
		stored = time.Time{}
	}

	var points []point
	if boldNeedsFullFetch(s.clock().UTC(), stored) {
		rows, ferr := boldFetchHistory(s.base(), boldPathPrice, boldMetricPrice)
		if ferr != nil {
			return 0, ferr
		}
		if points, err = boldPricePoints(rows); err != nil {
			return 0, err
		}
		log.Infof("%s: полный режим: сохранённый максимум %s, точек истории %d",
			boldGoldPriceName, boldFormatDate(stored), len(points))
	} else {
		latest, lerr := boldFetchLatestRow(s.base(), boldPathPriceLatest, boldMetricPrice)
		if lerr != nil {
			return 0, lerr
		}
		latestDate, derr := parseBoldDate(latest.Date)
		if derr != nil {
			return 0, derr
		}
		// Идемпотентный no-op: дата источника не новее сохранённого максимума (или равна ему) —
		// писать те же точки незачем. На полном пути это условие не проверяется: там максимум
		// старше порога, и цена ошибки обратная — пропущенный день остался бы незакрытым.
		if !latestDate.After(stored) {
			log.Infof("%s: дата источника %s не новее сохранённого максимума %s — нечего обновлять (0 строк)",
				boldGoldPriceName, latestDate.Format(boldDateLayout), stored.Format(boldDateLayout))
			return 0, nil
		}
		if points, err = boldPricePoints([]boldRow{latest}); err != nil {
			return 0, err
		}
		log.Infof("%s: инкремент: сохранённый максимум %s, дата источника %s",
			boldGoldPriceName, boldFormatDate(stored), latestDate.Format(boldDateLayout))
	}

	if err = conn.Exec(ctx, boldGoldPricesDdl); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldGoldPriceName, boldPriceTable, err)
	}

	count, err = boldWritePrices(ctx, conn, points)
	if err != nil {
		return count, err
	}

	if err = util.UpsertSeriesCatalog(ctx, conn, []util.SeriesMeta{gold.BoldPriceSeriesMeta()}); err != nil {
		return count, fmt.Errorf("%s: каталог рядов: %w", boldGoldPriceName, err)
	}

	log.Infof("Imported %d rows of %s (цена %s: площадка %s в %s)",
		count, boldGoldPriceName, boldPathPrice, boldPriceVenue, boldPriceTable)

	return count, nil
}

// boldPriceStoredMaxDate — сохранённый максимум импортёра цены: максимум дат площадки bold в
// gold_prices. Читается через boldScanMaxDate: NULL пустой таблицы и отсутствие самой таблицы
// (чистая база) — это «данных нет» (нулевое время), а не ошибка чтения.
func boldPriceStoredMaxDate(ctx context.Context, conn driver.Conn) (time.Time, error) {
	return boldScanMaxDate(ctx, conn, boldPriceStoredMaxQuery)
}

// boldPricePoints переводит строки цены (файл истории или ответ «последнего дня») в точки
// gold_prices. Series точки — площадка (в gold_prices series == venue), поэтому батч берёт её из
// точки, а не из константы на месте записи. nil в value — ошибка, а не пропуск строки: у метрики
// цены других полей нет, и строка без value означает смену формы ответа, а тихий пропуск дня
// оставил бы в ряду дыру, неотличимую от выходного. Пустой результат — тоже ошибка (fail-fast,
// урок eia): молчаливый ноль оставил бы dagu зелёным, а ряд — без обновления.
func boldPricePoints(rows []boldRow) ([]point, error) {
	points := make([]point, 0, len(rows))
	for i, row := range rows {
		if row.Value == nil {
			return nil, fmt.Errorf("bold: цена, строка %d (%s): нет обязательного поля value", i+1, row.Date)
		}
		date, err := parseBoldDate(row.Date)
		if err != nil {
			return nil, fmt.Errorf("bold: цена, строка %d: %w", i+1, err)
		}
		points = append(points, point{Series: boldPriceVenue, Date: date, Value: *row.Value})
	}
	if len(points) == 0 {
		return nil, errors.New("bold: нет точек цены: в ответе нет ни одной строки")
	}
	return points, nil
}

// boldWritePrices пишет точки цены одним батчем в gold_prices (venue, date, usd). Порядок батча
// детерминирован (сортировка копии по дате): повторный импорт воспроизводим, а срез вызывающего
// не переставляется (побочный эффект на аргументе — тот класс ошибок, который потом ищут в чужом
// вызове). Abort после ошибки Append не оставляет соединению открытый батч; сбой Abort логируем,
// а возвращаемой ошибкой остаётся первопричина Append (прецедент boldWritePoints).
func boldWritePrices(ctx context.Context, conn driver.Conn, points []point) (int64, error) {
	points = slices.Clone(points)
	sort.Slice(points, func(i, j int) bool { return points[i].Date.Before(points[j].Date) })

	batch, err := conn.PrepareBatch(ctx, boldGoldPricesInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldGoldPriceName, boldPriceTable, err)
	}

	var count int64
	for _, p := range points {
		if err = batch.Append(p.Series, p.Date, p.Value); err != nil {
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч цены после ошибки append: %v", boldGoldPriceName, aerr)
			}
			return 0, fmt.Errorf("%s: append %s %s: %w", boldGoldPriceName, p.Series, p.Date.Format(boldDateLayout), err)
		}
		count++
	}

	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", boldGoldPriceName, boldPriceTable, err)
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &boldEtfFlows{}, &boldGoldPrice{})
}
