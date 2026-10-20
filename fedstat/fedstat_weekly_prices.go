package fedstat

// Импортёр ЕМИСС 37426 — «Еженедельные средние потребительские цены (тарифы) на
// отдельные товары и услуги» (https://www.fedstat.ru/indicator/37426). Источник
// отдаёт SDMX XML только через CSRF POST: GET страницы -> token -> POST
// downloadData.do. Шаблон util.ClickHouseImport умеет только GET DataUrl, поэтому
// у импортёра собственный Import() (прецеденты rosstat_ipc_weeks_bulletins,
// cbr_dip_bulletin), а транспорт — util.NewDefaultTLSClient() на прогон:
// кастомный TLS-конфиг util.HttpClient (CERT_FILES) источник отвергает
// пустым/частичным SDMX (живая сверка 2026-10-20; правило 4 AGENTS.md —
// клиент всё равно из util).
//
// v1: 2008–2022 по коду РФ 1688487 (s_OKATO=643) и 2026 недели 39–40 по коду
// 1849012 (s_OKATO=643004.АГ). 2023–2025 в источнике отсутствуют: POST за эти
// годы отдаёт HTML-страницу вместо XML. Один POST на год (~1,8 МБ) с паузой
// 30 с: полный запрос на 27 МБ источник под нагрузкой рвёт EOF/пустым конвертом,
// а серия POST без паузы получает 503. Точки годов сливаются по ключу
// (s_grtov, регион, дата) и пишутся одним батчем.

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// fedstatTable — имя таблицы и Name() импортёра (CLICKHOUSE_IMPORT_STAT и id
	// шага dagu — та же строка, стандарт §7.1–7.2).
	fedstatTable = "fedstat_weekly_prices"

	// fedstatDdl — DDL в %s-форме шаблона. Ключ (product, region, date):
	// регион — часть ключа, потому что 2008–2022 и 2026 — разные определения РФ.
	fedstatDdl = `CREATE TABLE IF NOT EXISTS %s (
    product LowCardinality(String),  -- ряд: товар/услуга, имя объявлено кодом (стандарт §6.8)
    region  LowCardinality(String),  -- 'Российская Федерация' (2008–2022) или 'РФ без учёта новых субъектов' (2026)
    date    Date,                    -- дата регистрации цен (понедельник недели)
    price   Float64                  -- средняя потребительская цена, рублей за единицу из имени ряда
) ENGINE = ReplacingMergeTree ORDER BY (product, region, date);`

	fedstatIndicatorID     = "37426"
	fedstatProductsID      = "58273"
	fedstatRegionsID       = "57831"
	fedstatYearsID         = "3"
	fedstatPeriodsID       = "33560"
	fedstatUnitID          = "30611"
	fedstatUnitRub         = "950351"
	fedstatRegionRF        = "1688487"
	fedstatRegionRFNew     = "1849012"
	fedstatRegionRFCode    = "643"
	fedstatRegionRFNewCode = "643004.АГ"
	fedstatRegionRFName    = "Российская Федерация"
	fedstatRegionRFNewName = "Российская Федерация без учета новых субъектов (с 01.01.2023)"

	// Ожидаемая форма таблицы v1: 158 товаров, 2 региона, 16 лет (2008–2022,
	// 2026), 56 133 уникальные точки (56 201 серия минус 68 точных дублей).
	fedstatExpectedProducts = 158
	fedstatExpectedRegions  = 2
	fedstatExpectedYears    = 16
	fedstatExpectedPoints   = 56133

	fedstatPageTimeout   = 60 * time.Second
	fedstatExportTimeout = 180 * time.Second
	fedstatMaxBodyBytes  = 64 << 20
)

// fedstatYears — годы выгрузки. 2023–2025 в источнике отсутствуют: запрос за них
// возвращает HTML, поэтому в форму они не попадают. 2026 — только недели 39–40 и
// только под кодом 1849012.
var fedstatYears = []string{
	"2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015",
	"2016", "2017", "2018", "2019", "2020", "2021", "2022", "2026",
}

// fedstatRegionByCode — s_OKATO ответа -> стабильное имя региона. Оба кода
// приходят в одном ответе, потому что в POST выбраны оба фильтра 57831.
var fedstatRegionByCode = map[string]string{
	fedstatRegionRFCode:    fedstatRegionRFName,
	fedstatRegionRFNewCode: fedstatRegionRFNewName,
}

// Переменные (а не константы) — тесты подменяют базовый URL и задержки.
var (
	fedstatBaseURL     = fedstatBaseURLConst
	fedstatRetryDelays = []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}
	// fedstatBatchPause — пауза между годовыми POST: серия без паузы получает 503.
	fedstatBatchPause = 30 * time.Second
)

const fedstatBaseURLConst = "https://www.fedstat.ru"

// fedstatTokenRe ищет токен в #downloadTokenHolder. На странице несколько
// name="token"; первый hidden-input принадлежит форме выгрузки (проверено live).
var fedstatTokenRe = regexp.MustCompile(`name="token"\s+value="([^"]+)"`)

// fedstatDateRe — метка с датой: «на 9 января 2008 года», «23 неделя (на 13 июня
// 2016 года)», «37 неделя (на 17сентября 2018 года)» (пробел пропущен),
// «41 неделя (на 14 октября )» (лишние пробелы). После нормализации пробелов.
var fedstatDateRe = regexp.MustCompile(`(?i)на\s+(\d{1,2})\s*([а-яё]+)(?:\s*(\d{4})\s*года)?`)

// fedstatWeekRe — метка только с номером недели: «1 неделя 2021», «39 неделя 2026».
var fedstatWeekRe = regexp.MustCompile(`(?i)^(\d{1,2})\s*неделя(?:\s+(\d{4}))?$`)

// fedstatSDMX — минимальный контракт GenericData: SeriesKey (s_OKATO, s_grtov),
// Attributes (EI, PERIOD), Obs (Time, ObsValue). encoding/xml сверяет локальные
// имена, namespace SDMX не мешает; XMLName пинит корень, поэтому HTML-страница
// вместо XML даёт ошибку разбора, а не нулевую структуру (util.ParseXML).
// Годовой ответ оборачивает Series в DataSet (<DataSet><generic:Series>…),
// а комбинированный ответ и фикстуры кладут их прямо в GenericData — читаются оба.
type fedstatSDMX struct {
	XMLName  xml.Name         `xml:"GenericData"`
	Series   []fedstatSeries  `xml:"Series"`
	DataSets []fedstatDataSet `xml:"DataSet"`
}

type fedstatDataSet struct {
	Series []fedstatSeries `xml:"Series"`
}

type fedstatSeries struct {
	SeriesKey  []fedstatValue `xml:"SeriesKey>Value"`
	Attributes []fedstatValue `xml:"Attributes>Value"`
	Obs        fedstatObs     `xml:"Obs"`
}

type fedstatValue struct {
	Concept string `xml:"concept,attr"`
	Value   string `xml:"value,attr"`
}

type fedstatObs struct {
	Time     string          `xml:"Time"`
	ObsValue fedstatObsValue `xml:"ObsValue"`
}

type fedstatObsValue struct {
	Value string `xml:"value,attr"`
}

type fedstatPoint struct {
	code    string // s_grtov: ключ слияния годовых батчей
	product string
	region  string
	date    time.Time
	price   float64
}

type fedstatKey struct {
	code   string
	region string
	date   time.Time
}

// FedstatWeeklyPricesStat — импортёр; встроенный util.ClickHouseImport даёт
// Name(), DDL, AfterImport и нулевое Publication() (точки контроля нет).
type FedstatWeeklyPricesStat struct {
	util.ClickHouseImport
}

// fedstatWeeklyPricesImporter — регистрация в fedstat/importers.go. Publication
// не объявляется: no-op держит гейт по своей таблице (стандарт §9.3), архива
// версий и source_publications у POST-источника нет (отступление §9.6, спека §10).
var fedstatWeeklyPricesImporter = &FedstatWeeklyPricesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatTable,
	CreateTable: []string{fedstatDdl},
	AfterImport: afterFedstatMacro(fedstatSeriesMeta()),
}}

// Import — DDL -> гейт -> один POST -> разбор -> один батч -> производные.
// На no-op-пути производные публикуются тоже: каталог и витрина идемпотентны и
// лечат сбой публикации прошлого прогона (прецедент rosstat_ipc_weeks_bulletins).
func (s *FedstatWeeklyPricesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatTable, err)
		}
	}

	done, err := fedstatAlreadyImported(ctx, conn)
	if err != nil {
		return 0, err
	}
	if done {
		log.Infof("%s: все %d товаров, %d региона и %d лет на месте", fedstatTable, fedstatExpectedProducts, fedstatExpectedRegions, fedstatExpectedYears)
		if s.AfterImport != nil {
			if err = s.AfterImport(ctx, conn); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	client := util.NewDefaultTLSClient()
	points, err := fedstatDownloadPoints(ctx, client)
	if err != nil {
		return 0, err
	}
	if err = fedstatCheckCoverage(points); err != nil {
		return 0, err
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return 0, err
	}
	for _, point := range points {
		if err = batch.Append(point.product, point.region, point.date, point.price); err != nil {
			fedstatAbortBatch(batch, s.TableName)
			return 0, fmt.Errorf("%s: %s/%s/%s: %w", fedstatTable, point.product, point.region, point.date.Format("2006-01-02"), err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}

	if s.AfterImport != nil {
		if err = s.AfterImport(ctx, conn); err != nil {
			return count, err
		}
	}
	log.Infof("Imported %d rows of %s", count, s.Name())
	return count, nil
}

// fedstatAlreadyImported — гейт по форме своей таблицы: 158 товаров, 2 региона,
// 16 лет и ровно 56 133 точки. Точное равенство, а не >=: v1 — исторический
// срез; новая точка означает, что источник изменился и константу нужно обновить
// осознанно, а не «дозаписать молча». FINAL обязателен (стандарт §9.2).
func fedstatAlreadyImported(ctx context.Context, conn driver.Conn) (bool, error) {
	const query = `SELECT uniqExact(product), uniqExact(region), uniqExact(toYear(date)), count()
FROM fedstat_weekly_prices FINAL`
	var products, regions, years, points uint64
	if err := conn.QueryRow(ctx, query).Scan(&products, &regions, &years, &points); err != nil {
		return false, fmt.Errorf("%s: форма таблицы не прочитана: %w", fedstatTable, err)
	}
	return products == fedstatExpectedProducts &&
		regions == fedstatExpectedRegions &&
		years == fedstatExpectedYears &&
		points == fedstatExpectedPoints, nil
}

// fedstatDownloadPoints — по одному POST на год (16 лет) с паузой между запросами.
// Полный запрос на 27 МБ источник под нагрузкой рвёт EOF или отдаёт пустой конверт,
// а годовой (~1,8 МБ) отдаёт стабильно; серия POST без паузы получает 503. GET
// страницы и токен — на каждый год: сессия и CSRF-токен свежие, cookie jar общий.
func fedstatDownloadPoints(ctx context.Context, client *http.Client) ([]fedstatPoint, error) {
	merged := make(map[fedstatKey]fedstatPoint)
	for i, year := range fedstatYears {
		if i > 0 && fedstatBatchPause > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%s: пауза между годами: %w", fedstatTable, ctx.Err())
			case <-time.After(fedstatBatchPause):
			}
		}
		points, err := fedstatDownloadYear(ctx, client, year)
		if err != nil {
			return nil, fmt.Errorf("%s: год %s: %w", fedstatTable, year, err)
		}
		for _, point := range points {
			key := fedstatKey{code: point.code, region: point.region, date: point.date}
			if previous, exists := merged[key]; exists {
				if previous.price != point.price {
					return nil, fmt.Errorf("%s: %s/%s/%s: дубль с разными значениями %v и %v", fedstatTable, point.product, point.region, point.date.Format("2006-01-02"), previous.price, point.price)
				}
				continue
			}
			merged[key] = point
		}
		log.Infof("%s: год %s: %d точек", fedstatTable, year, len(points))
	}
	result := make([]fedstatPoint, 0, len(merged))
	for _, point := range merged {
		result = append(result, point)
	}
	sortFedstatPoints(result)
	return result, nil
}

func fedstatDownloadYear(ctx context.Context, client *http.Client, year string) ([]fedstatPoint, error) {
	page, err := fedstatGetPage(ctx, client)
	if err != nil {
		return nil, err
	}
	token, err := fedstatToken(page)
	if err != nil {
		return nil, err
	}
	body, err := fedstatPostForm(ctx, client, fedstatFormBody(token, year, fedstatYearRegion(year)))
	if err != nil {
		return nil, err
	}
	return fedstatParse(body)
}

// fedstatYearRegion — 2026 публикуется под новым определением РФ (s_OKATO=643004.АГ),
// 2008–2022 — под 643.
func fedstatYearRegion(year string) string {
	if year == "2026" {
		return fedstatRegionRFNew
	}
	return fedstatRegionRF
}

func fedstatGetPage(ctx context.Context, client *http.Client) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, fedstatPageTimeout)
	defer cancel()
	pageURL := fedstatBaseURL + "/indicator/" + fedstatIndicatorID
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: страница: %w", fedstatTable, err)
	}
	req.Header.Set("User-Agent", util.HttpUA)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: страница %s: %w", fedstatTable, pageURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, fedstatMaxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("%s: страница %s: тело не прочитано: %w", fedstatTable, pageURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: страница %s: http status %d: %s", fedstatTable, pageURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("%s: страница %s: пустое тело", fedstatTable, pageURL)
	}
	return body, nil
}

func fedstatToken(page []byte) (string, error) {
	match := fedstatTokenRe.FindSubmatch(page)
	if len(match) != 2 {
		return "", fmt.Errorf("%s: CSRF-токен (downloadTokenHolder) не найден на странице %s/indicator/%s", fedstatTable, fedstatBaseURL, fedstatIndicatorID)
	}
	return string(match[1]), nil
}

// fedstatField — поле формы в порядке отправки. Порядок ВАЖЕН: сервер принимает
// и multiset, и selectedFilterIds в порядке Task 1, но при алфавитном порядке
// ключей (url.Values.Encode) отвечает пустым GenericData без Series — проверено
// live 2026-10-20. Поэтому тело собирается вручную, как у FGrid/рабочего запроса.
type fedstatField struct {
	key   string
	value string
}

// fedstatFormBody — тело одного годового POST: id, оси, filterObjectIds, token,
// title, indicator, год, регион, unit, товары. Порядок selectedFilterIds важен:
// сервер разбирает значения позиционно; рабочий порядок — как у probe Task 1
// (indicator, year, region, unit, products), товары — в порядке фильтра 58273.
func fedstatFormBody(token, year, region string) []byte {
	fields := []fedstatField{
		{"id", fedstatIndicatorID},
		{"lineObjectIds", fedstatRegionsID},
		{"lineObjectIds", fedstatProductsID},
		{"columnObjectIds", fedstatYearsID},
		{"columnObjectIds", fedstatPeriodsID},
		{"filterObjectIds", fedstatUnitID},
		{"struts.token.name", "token"},
		{"token", token},
		{"title", "fedstat_weekly_prices"},
		{"selectedFilterIds", "0_" + fedstatIndicatorID},
		{"selectedFilterIds", fedstatYearsID + "_" + year},
		{"selectedFilterIds", fedstatRegionsID + "_" + region},
		{"selectedFilterIds", fedstatUnitID + "_" + fedstatUnitRub},
	}
	for _, filterID := range fedstatProductOrder {
		fields = append(fields, fedstatField{"selectedFilterIds", fedstatProductsID + "_" + filterID})
	}
	var body strings.Builder
	for i, field := range fields {
		if i > 0 {
			body.WriteByte('&')
		}
		body.WriteString(url.QueryEscape(field.key))
		body.WriteByte('=')
		body.WriteString(url.QueryEscape(field.value))
	}
	return []byte(body.String())
}

// fedstatPostForm — POST с повтором на 429/5xx. Тело формы кодируется один раз:
// повтор не должен пересобирать запрос. Ошибка транспорта/статуса/HTML — с
// сниппетом тела, чтобы 503 и заглушка WAF читались по причине.
func fedstatPostForm(ctx context.Context, client *http.Client, encoded []byte) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		body, retry, err := fedstatPostOnce(ctx, client, encoded)
		if err == nil {
			return body, nil
		}
		if !retry || attempt >= len(fedstatRetryDelays) {
			return nil, err
		}
		delay := fedstatRetryDelays[attempt]
		log.Warnf("%s: POST не удался (%v), повтор через %s", fedstatTable, err, delay)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: POST отменён: %w", fedstatTable, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func fedstatPostOnce(ctx context.Context, client *http.Client, encoded []byte) (body []byte, retry bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, fedstatExportTimeout)
	defer cancel()
	exportURL := fedstatBaseURL + "/indicator/downloadData.do?format=sdmx"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, exportURL, bytes.NewReader(encoded))
	if err != nil {
		return nil, false, fmt.Errorf("%s: POST: %w", fedstatTable, err)
	}
	req.Header.Set("User-Agent", util.HttpUA)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", fedstatBaseURL+"/indicator/"+fedstatIndicatorID)
	req.Header.Set("Origin", fedstatBaseURL)
	resp, err := client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("%s: POST %s: %w", fedstatTable, exportURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(io.LimitReader(resp.Body, fedstatMaxBodyBytes+1))
	if err != nil {
		return nil, true, fmt.Errorf("%s: POST %s: тело не прочитано: %w", fedstatTable, exportURL, err)
	}
	if len(body) > fedstatMaxBodyBytes {
		return nil, false, fmt.Errorf("%s: POST %s: ответ больше %d байт", fedstatTable, exportURL, fedstatMaxBodyBytes)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("%s: POST %s: http status %d: %s", fedstatTable, exportURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("%s: POST %s: http status %d: %s", fedstatTable, exportURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "html") {
		return nil, false, fmt.Errorf("%s: POST %s: источник вернул HTML вместо SDMX XML (вероятно, за выбранный период данных нет): %s", fedstatTable, exportURL, util.ErrorSnippet(body))
	}
	if !bytes.Contains(body, []byte("<GenericData")) {
		return nil, false, fmt.Errorf("%s: POST %s: в ответе нет GenericData: %s", fedstatTable, exportURL, util.ErrorSnippet(body))
	}
	return body, false, nil
}

// fedstatParse — SDMX XML -> точки. Разбор отделён от сети (стандарт §6.1),
// десятичная запятая, пустые значения пропускаются, дубли (product, region,
// date) схлопываются; расхождение значений в дубле — ошибка.
func fedstatParse(body []byte) ([]fedstatPoint, error) {
	var doc fedstatSDMX
	if err := util.ParseXML(body, &doc); err != nil {
		return nil, fmt.Errorf("%s: SDMX XML не разобран: %w", fedstatTable, err)
	}
	allSeries := append([]fedstatSeries(nil), doc.Series...)
	for _, dataSet := range doc.DataSets {
		allSeries = append(allSeries, dataSet.Series...)
	}
	if len(allSeries) == 0 {
		return nil, fmt.Errorf("%s: в SDMX-ответе нет Series (пустой конверт; вероятен rate limit источника): %s", fedstatTable, util.ErrorSnippet(body))
	}

	points := make(map[fedstatKey]float64, len(allSeries))
	for i, series := range allSeries {
		key := fedstatValues(series.SeriesKey)
		attrs := fedstatValues(series.Attributes)
		code := key["s_grtov"]
		product, ok := fedstatProductByCode[code]
		if !ok {
			return nil, fmt.Errorf("%s: Series %d: неизвестный s_grtov=%q", fedstatTable, i, code)
		}
		region, ok := fedstatRegionByCode[key["s_OKATO"]]
		if !ok {
			return nil, fmt.Errorf("%s: Series %d, товар %q: неизвестный s_OKATO=%q", fedstatTable, i, product.name, key["s_OKATO"])
		}
		year, err := strconv.Atoi(strings.TrimSpace(series.Obs.Time))
		if err != nil {
			return nil, fmt.Errorf("%s: Series %d, товар %q: Time=%q не год: %w", fedstatTable, i, product.name, series.Obs.Time, err)
		}
		date, err := fedstatDate(attrs["PERIOD"], year)
		if err != nil {
			return nil, fmt.Errorf("%s: Series %d, товар %q: %w", fedstatTable, i, product.name, err)
		}
		raw := strings.TrimSpace(series.Obs.ObsValue.Value)
		if raw == "" || raw == "…" || raw == "-" || raw == "–" {
			continue
		}
		price, err := fedstatPrice(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: Series %d, товар %q, %s: %w", fedstatTable, i, product.name, date.Format("2006-01-02"), err)
		}
		pointKey := fedstatKey{code: code, region: region, date: date}
		if previous, exists := points[pointKey]; exists {
			if previous != price {
				return nil, fmt.Errorf("%s: товар %q, регион %q, %s: дубль с разными значениями %v и %v", fedstatTable, product.name, region, date.Format("2006-01-02"), previous, price)
			}
			continue
		}
		points[pointKey] = price
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор дал 0 точек", fedstatTable)
	}

	result := make([]fedstatPoint, 0, len(points))
	for key, price := range points {
		result = append(result, fedstatPoint{
			code:    key.code,
			product: fedstatProductByCode[key.code].name,
			region:  key.region,
			date:    key.date,
			price:   price,
		})
	}
	sortFedstatPoints(result)
	return result, nil
}

func sortFedstatPoints(points []fedstatPoint) {
	sort.Slice(points, func(i, j int) bool {
		if points[i].product != points[j].product {
			return points[i].product < points[j].product
		}
		if points[i].region != points[j].region {
			return points[i].region < points[j].region
		}
		return points[i].date.Before(points[j].date)
	})
}

func fedstatValues(values []fedstatValue) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		out[value.Concept] = value.Value
	}
	return out
}

func fedstatPrice(raw string) (float64, error) {
	price, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	if math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, fmt.Errorf("значение %q не конечное число", raw)
	}
	if price < 0 {
		return 0, fmt.Errorf("значение %q отрицательное", raw)
	}
	return price, nil
}

// fedstatDate — правило даты Task 1 (research §4). Год метки сильнее Time:
// 68 серий начала января имеют Time предыдущего года. Метка только с номером
// недели: неделя 1 — первый понедельник 8 января или позже; проверено на
// 2021/2022/2026 против rosstat_ipc_weeks.
func fedstatDate(label string, timeYear int) (time.Time, error) {
	normalized := strings.Join(strings.Fields(label), " ")
	if match := fedstatDateRe.FindStringSubmatch(normalized); match != nil {
		day, err := strconv.Atoi(match[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("день %q метки %q не число: %w", match[1], label, err)
		}
		month, ok := util.MonthsToNum[strings.ToLower(match[2])]
		if !ok {
			return time.Time{}, fmt.Errorf("месяц %q метки %q не распознан", match[2], label)
		}
		year := timeYear
		if match[3] != "" {
			if year, err = strconv.Atoi(match[3]); err != nil {
				return time.Time{}, fmt.Errorf("год %q метки %q не число: %w", match[3], label, err)
			}
		}
		date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		if date.Day() != day || date.Month() != month || date.Year() != year {
			return time.Time{}, fmt.Errorf("метка %q: невалидная дата %d.%d.%d", label, day, int(month), year)
		}
		return date, nil
	}
	if match := fedstatWeekRe.FindStringSubmatch(normalized); match != nil {
		week, err := strconv.Atoi(match[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("номер недели %q метки %q не число: %w", match[1], label, err)
		}
		if week < 1 || week > 53 {
			return time.Time{}, fmt.Errorf("метка %q: номер недели %d вне 1..53", label, week)
		}
		year := timeYear
		if match[2] != "" {
			if year, err = strconv.Atoi(match[2]); err != nil {
				return time.Time{}, fmt.Errorf("год %q метки %q не число: %w", match[2], label, err)
			}
		}
		return fedstatWeekOne(year).AddDate(0, 0, (week-1)*7), nil
	}
	return time.Time{}, fmt.Errorf("метка периода %q не распознана", label)
}

// fedstatWeekOne — первый понедельник 8 января или позже. Проверено: 2016 ->
// 2016-01-11, 2021 -> 2021-01-11, 2022 -> 2022-01-10, 2026 -> 2026-01-12.
func fedstatWeekOne(year int) time.Time {
	jan8 := time.Date(year, time.January, 8, 0, 0, 0, 0, time.UTC)
	offset := (8 - int(jan8.Weekday())) % 7
	return jan8.AddDate(0, 0, offset)
}

// fedstatCheckCoverage — контракт v1: 158 товаров, 2 региона, 16 лет, 56 133
// уникальные точки. Меньше — источник недодал диапазон; больше — источник
// изменился, и константу/спеку нужно обновить осознанно.
func fedstatCheckCoverage(points []fedstatPoint) error {
	products := make(map[string]bool)
	regions := make(map[string]bool)
	years := make(map[int]bool)
	for _, point := range points {
		products[point.product] = true
		regions[point.region] = true
		years[point.date.Year()] = true
	}
	if len(products) != fedstatExpectedProducts ||
		len(regions) != fedstatExpectedRegions ||
		len(years) != fedstatExpectedYears ||
		len(points) != fedstatExpectedPoints {
		return fmt.Errorf("%s: покрытие не совпало с v1: товаров %d (ожидается %d), регионов %d (%d), лет %d (%d), точек %d (%d)",
			fedstatTable, len(products), fedstatExpectedProducts, len(regions), fedstatExpectedRegions,
			len(years), fedstatExpectedYears, len(points), fedstatExpectedPoints)
	}
	return nil
}

func fedstatAbortBatch(batch driver.Batch, what string) {
	if err := batch.Abort(); err != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки: %v", what, err)
	}
}
