package craw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Транспорт API-импортёра СБКТС (реестр Росстандарта): ручка поиска по библиотеке
// документов отдаёт JSON-страницами, день собирается постранично и режется клиентским
// фильтром по dateofpublication (окно поиска ручки шире одного дня). Здесь только
// транспорт и дневной срез: запись в ClickHouse и обвязка импортёра — в craw/gost.go.
//
// Токен gtn.access.appid приходит снаружи (a.AppID, в проде — из окружения GOST_APP_ID)
// и в тексты ошибок не попадает: подсказка 401/403 называет только переменную окружения.

const (
	// gostPageSize — размер страницы поиска: столько документов отдаёт одна страница
	// (900 — рабочий максимум ручки, больше она молча не отдаёт).
	gostPageSize = 900
	// gostMaxDayCount — предохранитель от «дня-слона»: ручка не отдаёт больше окна поиска
	// (~10 тыс. документов), и день сверх этого — не данные, а сломанный фильтр или
	// сменившийся формат. Постранично такой день всё равно не читается.
	gostMaxDayCount = 10000
	// gostMaxTries — всего попыток запроса (первая и три повтора). Четырёх хватает
	// пережить короткий сбой портала, а дальше ошибка должна дойти до оператора.
	gostMaxTries = 4

	// gostQueryDayLayout — формат query-параметра: ручка ищет по дате публикации.
	gostQueryDayLayout = "02.01.2006"
	// gostPublicationLayout — префикс dateofpublication ("11.10.2026 00:00:00 ZE3"):
	// маркер пояса в строке информации не несёт, дата берётся по календарю.
	gostPublicationLayout = "02.01.2006 15:04:05"
	// gostAppIDHeader — заголовок с токеном из JS-конфига страницы реестра. Здесь имя
	// заголовка, не значение: сам токен живёт только в окружении (правило 1 проекта).
	gostAppIDHeader = "gtn.access.appid"
	// gostAcceptHeader — что просим: без него портал может отдать HTML-страницу портала.
	gostAcceptHeader = "application/json"

	// gostMinYear/gostMaxYear — границы правдоподобного года выпуска: "16", "н/д" и ""
	// в yearofmanufacturing значат «нет данных» и должны стать нулём, а не 16-м годом.
	gostMinYear = 1900
	gostMaxYear = 2100
)

var (
	// gostAPIURL — ручка поиска по библиотеке реестра СБКТС. Переменная, а не константа:
	// тесты подменяют её адресом httptest-сервера.
	gostAPIURL = "https://www.gost.ru/standards/rest-api/libraries/search"
	// gostAPITimeout — дедлайн одной попытки запроса. Переменная (прецедент — util.jsonTimeout):
	// тесты подставляют короткий вместо минутного. Дедлайн нужен именно ради повторов:
	// зависший ответ без него не дал бы ошибки, и день висел бы до ручного разбора.
	gostAPITimeout = 120 * time.Second
	// gostRequestGap — пауза между страницами одного дня: портал не любит шквал запросов
	// (в старом colly-импортёре ту же роль играл backoff между визитами листалки).
	gostRequestGap = 200 * time.Millisecond
	// gostRetryAfterTimeout/gostRetryAfterServer — паузы «подожди и повтори» для дедлайна
	// попытки и для отказов сервера. Переменные, а не литералы 30/60 секунд: иначе тесты
	// повторов ждали бы эти паузы вживую (backoff.RetryAfter важнее политики повторов —
	// он задаёт паузу сам и сбрасывает её счётчик).
	gostRetryAfterTimeout = 30 * time.Second
	gostRetryAfterServer  = 60 * time.Second
	// gostBackoffPolicy — база экспоненциальных пауз между повторами. Копия берётся
	// на запрос: ExponentialBackOff не потокобезопасен, а Retry сам вызывает Reset().
	gostBackoffPolicy = backoff.ExponentialBackOff{
		InitialInterval:     5 * time.Second,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         2 * time.Minute,
	}
)

// gostAPI — клиент ручки поиска: Client — общий util.HttpClient (свой UA, куки портала),
// AppID — токен gtn.access.appid.
type gostAPI struct {
	Client *http.Client
	AppID  string
}

func newGostAPI(client *http.Client, appID string) *gostAPI {
	return &gostAPI{Client: client, AppID: appID}
}

// gostSearchResponse — ответ ручки: count — всего совпадений в окне поиска, Data — страница.
type gostSearchResponse struct {
	Count int       `json:"count"`
	Data  []gostDoc `json:"data"`
}

// gostDoc — документ реестра: идентификатор и поля карточки.
type gostDoc struct {
	ID     int64      `json:"id"`
	Fields gostFields `json:"fields"`
}

// gostFields — поля карточки в написании источника. DateOfIssueOfCertificate — указатель:
// у документов без даты выдачи источник отдаёт null, и ноль-время не должно выглядеть датой.
type gostFields struct {
	Product                  string `json:"product"`
	Type                     string `json:"type"`
	NumberOfCertificate      string `json:"numberofcertificate"`
	DateOfIssueOfCertificate *int64 `json:"dateofissueofcertificate"`
	EcoClass                 string `json:"ecoclass"`
	EngineTrademark          string `json:"enginetrademark"`
	YearOfManufacturing      string `json:"yearofmanufacturing"`
	NameOfManufacturer       string `json:"nameofmanufacturer"`
	UniqueIdentifier         string `json:"uniqueidentifierofdocument"`
	DateOfPublication        string `json:"dateofpublication"`
}

// gostRow — строка витрины: только то, что нужно для записи. Даты — UTC-полночь
// (конвенция сравнения time.Time), год выпуска — число, а не строка источника.
type gostRow struct {
	Mark, Type          string
	Date                time.Time
	CertificateNumber   string
	EcoClass            string
	EngineTrademark     string
	YearOfManufacturing uint16
	NameOfManufacturer  string
	UID                 string
	PublicationDate     time.Time
}

// gostDaySlice — дневной срез: строки дня, всего совпадений в ручке (Count) и сколько
// документов страниц отбросил клиентский фильтр (Skipped). Skipped нужен как датчик:
// резкий рост доли отброшенных — сигнал, что сменился формат dateofpublication. Invalid — из
// отброшенных те, что не разобраны вовсе: по нему вызывающий различает штатный «день без своих
// публикаций» (чужие даты публикации — обычное окно текстового поиска) и поломку формата/данных.
type gostDaySlice struct {
	Rows    []gostRow
	Count   int
	Skipped int
	Invalid int
}

// gostDocReject — почему документ не попал в срез дня.
type gostDocReject int

const (
	// gostDocAccepted — документ дня: попал в срез.
	gostDocAccepted gostDocReject = iota
	// gostDocForeign — дата публикации разбирается, но указывает на другой день: штатный ответ
	// ручки, которая ищет дату в любом поле документа.
	gostDocForeign
	// gostDocInvalid — документ не разобран: нет даты выдачи, пуст номер сертификата или
	// неразбираемая (пустая) дата публикации.
	gostDocInvalid
)

// fetchPage забирает одну страницу дня. Повторы и классификация отказов — здесь:
// transport-сбой и неполный ответ повторяемы, отказ доступа (401/403) и прочие 4xx —
// нет, потому что повтор их не лечит. URL в тексты ошибок не попадает (конвенция
// util.GetJSON): в query бывают ключи, а ошибка уезжает в лог.
func (a *gostAPI) fetchPage(ctx context.Context, day time.Time, page, size int) (gostSearchResponse, error) {
	urlStr := fmt.Sprintf("%s?query=%s&page=%d&size=%d&tmsp=%d",
		gostAPIURL, url.QueryEscape(day.Format(gostQueryDayLayout)), page, size, time.Now().UnixMilli())
	b := gostBackoffPolicy

	op := func() (gostSearchResponse, error) {
		// Дедлайн на каждую попытку, а не на весь Retry: иначе повторы шли бы уже по
		// истёкшему контексту и были бы бессмысленны.
		reqCtx, cancel := context.WithTimeout(ctx, gostAPITimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, urlStr, nil)
		if err != nil {
			// Сломанный URL повтором не лечится. Ошибка разбора приходит из net/http
			// обёрнутой в *url.Error с полным адресом — её снимает gostNoURL.
			return gostSearchResponse{}, backoff.Permanent(gostNoURL(err))
		}
		req.Header.Set(gostAppIDHeader, a.AppID)
		req.Header.Set("Accept", gostAcceptHeader)
		req.Header.Set("User-Agent", util.HttpUA)

		resp, err := a.Client.Do(req)
		if err != nil {
			return gostSearchResponse{}, gostTransportError(err)
		}
		defer func() { _ = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return gostSearchResponse{}, gostTransportError(err)
		}
		if resp.StatusCode != http.StatusOK {
			return gostSearchResponse{}, gostStatusError(resp.StatusCode)
		}
		// Пустое тело (в том числе из одних пробелов) и литерал null неотличимы от
		// «данных нет»: разбор такого ответа дал бы count = 0 и выглядел бы успехом.
		// Повторяем — это сбой ответа, а не день без документов.
		if trimmed := bytes.TrimSpace(body); len(trimmed) == 0 || string(trimmed) == "null" {
			return gostSearchResponse{}, fmt.Errorf("пустое тело ответа")
		}
		var out gostSearchResponse
		if err := json.Unmarshal(body, &out); err != nil {
			return gostSearchResponse{}, fmt.Errorf("разбор JSON: %w", err)
		}
		return out, nil
	}
	return backoff.Retry(ctx, op, backoff.WithBackOff(&b), backoff.WithMaxTries(gostMaxTries))
}

// gostStatusError раскладывает статус по классам повторов.
func gostStatusError(status int) error {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return backoff.Permanent(fmt.Errorf(
			"http status %d — проверьте GOST_APP_ID (токен gtn.access.appid из JS-конфига страницы реестра, standarts.adminsHeaders)",
			status))
	case status == http.StatusTooManyRequests || status >= 500:
		// 429/5xx — предел портала: ждём и повторяем. Причина идёт в текст вместе с
		// паузой: сам RetryAfterError её не несёт, и «retry after 1m0s» без статуса
		// ничего не сказал бы оператору.
		return errors.Join(fmt.Errorf("http status %d", status), gostRetryAfter(gostRetryAfterServer))
	default:
		// Прочие 4xx (404, 400, WAF-отказ) — не тот запрос, повтор не поможет.
		return backoff.Permanent(fmt.Errorf("http status %d", status))
	}
}

// gostTransportError классифицирует сбой транспорта. Дедлайн попытки и таймаут сокета —
// не отказ портала, а перегрузка: повторяем с большой паузой. Прочие обрывы (сброс
// соединения, неудачный DNS) — обычный повтор.
func gostTransportError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return errors.Join(gostNoURL(err), gostRetryAfter(gostRetryAfterTimeout))
	}
	return gostNoURL(err)
}

// gostRetryAfter — ошибка «повторить через d». backoff.RetryAfter принимает только целые
// секунды, а переменные пауз — длительности, поэтому длительность передаётся как есть:
// округление вниз потеряло бы и секундную точность настройки, и короткие тестовые паузы
// (RetryAfterError.Duration — то самое поле, по которому backoff выдерживает паузу).
func gostRetryAfter(d time.Duration) error {
	return &backoff.RetryAfterError{Duration: d}
}

// gostNoURL убирает URL из ошибки транспорта: net/http заворачивает всё в *url.Error и
// печатает адрес вместе с query целиком. URL-контекст добавляет вызывающий — как в util.GetJSON.
func gostNoURL(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}

// day собирает срез одного дня: все страницы окна поиска, клиентский фильтр по дню
// публикации. Частичной записи нет: если страница дня не доехала или уникальных документов
// пришло меньше count, возвращается ошибка — решение «день полный» принимается до батча.
func (a *gostAPI) day(ctx context.Context, day time.Time) (gostDaySlice, error) {
	first, err := a.fetchPage(ctx, day, 1, gostPageSize)
	if err != nil {
		return gostDaySlice{}, err
	}
	if first.Count > gostMaxDayCount {
		return gostDaySlice{}, fmt.Errorf(
			"в дне %s %d документов — больше окна поиска ручки (%d): такой день постранично не читается",
			day.Format(gostQueryDayLayout), first.Count, gostMaxDayCount)
	}

	docs := first.Data
	for page := 2; page <= gostPageCount(first.Count); page++ {
		time.Sleep(gostRequestGap)
		next, err := a.fetchPage(ctx, day, page, gostPageSize)
		if err != nil {
			return gostDaySlice{}, err
		}
		docs = append(docs, next.Data...)
	}

	// Дедупликация — до сравнения с count: окно поиска ручки не снапшот, и при изменении данных
	// между запросами страница может вернуть документ, уже пришедший на предыдущей (сдвиг
	// пагинации). Без неё такой день выглядел бы полным (len(docs) == count) при настоящей
	// недостаче — и check объявил бы ложную дыру либо, наоборот, скрыл реальную.
	unique, dropped := gostUniqueDocs(docs)
	if len(unique) < first.Count {
		return gostDaySlice{}, fmt.Errorf("получено %d документов из %d (дубликатов страниц: %d)",
			len(unique), first.Count, dropped)
	}

	rows, skipped, invalid := gostRowsFromDocs(day, unique)
	return gostDaySlice{Rows: rows, Count: first.Count, Skipped: skipped, Invalid: invalid}, nil
}

// gostUniqueDocs оставляет по одному документу на id, сохраняя порядок появления, и считает
// отброшенные копии: id — идентичность документа в реестре, поэтому повтор id и есть дубликат
// страницы. Нулевой id (сменившаяся форма ответа) схлопывается так же: день не запишется и
// прогон упадёт громко, а это безопасное направление — молчаливая потеря документов хуже.
func gostUniqueDocs(docs []gostDoc) (unique []gostDoc, dropped int) {
	seen := make(map[int64]struct{}, len(docs))
	unique = make([]gostDoc, 0, len(docs))
	for _, d := range docs {
		if _, ok := seen[d.ID]; ok {
			dropped++
			continue
		}
		seen[d.ID] = struct{}{}
		unique = append(unique, d)
	}
	return unique, dropped
}

// dayCount возвращает только число совпадений дня — одним запросом минимальной страницы.
// В check-прогоне это запрос на день вместо выкачивания полного диапазона.
func (a *gostAPI) dayCount(ctx context.Context, day time.Time) (int, error) {
	resp, err := a.fetchPage(ctx, day, 1, 1)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// gostPageCount — сколько страниц нужно, чтобы забрать count документов.
func gostPageCount(count int) int {
	if count <= 0 {
		return 0
	}
	return (count + gostPageSize - 1) / gostPageSize
}

// gostMSKDate переводит миллисекунды источника в дату по Москве: источник кладёт момент
// UTC, а дата выдачи документа — московская (21:00 UTC = полночь следующего дня МСК).
// Время суток обнуляется: дата хранится и сравнивается в UTC-полуночи.
func gostMSKDate(ms int64) time.Time {
	return time.UnixMilli(ms).UTC().Add(3 * time.Hour).Truncate(24 * time.Hour)
}

// gostParsePublicationDay разбирает dateofpublication ("11.10.2026 00:00:00 ZE3") до дня.
// Строка читается только до конца даты и времени: маркер пояса у всех записей одинаков и
// информации не несёт. Короткая или битая строка — ok=false (документ не наш).
func gostParsePublicationDay(s string) (time.Time, bool) {
	if len(s) < len(gostPublicationLayout) {
		return time.Time{}, false
	}
	t, err := time.Parse(gostPublicationLayout, s[:len(gostPublicationLayout)])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// gostYear переводит год выпуска в число. В источнике это строка, причём "16", "н/д" и ""
// значат «нет данных»: год принимается только четырёхзначным и правдоподобным.
func gostYear(raw string) uint16 {
	raw = strings.TrimSpace(raw)
	if len(raw) != 4 {
		return 0
	}
	year, err := strconv.Atoi(raw)
	if err != nil || year < gostMinYear || year > gostMaxYear {
		return 0
	}
	return uint16(year)
}

// gostRowFromDoc собирает строку витрины из документа. Причина отказа различается: чужой день
// публикации — штатный ответ ручки (она ищет дату в любом поле и отдаёт документы соседних дней),
// а невалидный документ — поломка данных или формата (нет даты выдачи, пуст номер сертификата,
// неразбираемая дата публикации). Вызывающий решает по этим причинам по-разному (см. gostRunDays).
func gostRowFromDoc(day time.Time, d gostDoc) (gostRow, gostDocReject) {
	f := d.Fields
	if f.DateOfIssueOfCertificate == nil {
		return gostRow{}, gostDocInvalid
	}
	if strings.TrimSpace(f.NumberOfCertificate) == "" {
		return gostRow{}, gostDocInvalid
	}
	pub, ok := gostParsePublicationDay(f.DateOfPublication)
	if !ok {
		return gostRow{}, gostDocInvalid
	}
	if !gostSameDay(pub, day) {
		return gostRow{}, gostDocForeign
	}
	return gostRow{
		Mark:                f.Product,
		Type:                f.Type,
		Date:                gostMSKDate(*f.DateOfIssueOfCertificate),
		CertificateNumber:   f.NumberOfCertificate,
		EcoClass:            f.EcoClass,
		EngineTrademark:     f.EngineTrademark,
		YearOfManufacturing: gostYear(f.YearOfManufacturing),
		NameOfManufacturer:  f.NameOfManufacturer,
		UID:                 f.UniqueIdentifier,
		PublicationDate:     pub,
	}, gostDocAccepted
}

// gostRowsFromDocs — построчный фильтр дня: строки среза, общее число отброшенных документов и
// из них невалидные. Чужой день публикации и неразобранный документ считаются разными вещами:
// первый — штатное поведение текстового поиска, второй — поломка, и вызывающий решает по ним
// по-разному (см. gostRunDays).
func gostRowsFromDocs(day time.Time, docs []gostDoc) (rows []gostRow, skipped, invalid int) {
	for _, d := range docs {
		row, reject := gostRowFromDoc(day, d)
		switch reject {
		case gostDocAccepted:
			rows = append(rows, row)
		case gostDocInvalid:
			invalid++
			skipped++
		default:
			skipped++
		}
	}
	return rows, skipped, invalid
}

// gostSameDay сравнивает календарные дни: время суток в дате публикации и в срезе не значит
// ничего, а сравнивать моменты нельзя — у источника пояс помечен строкой, а не смещением.
func gostSameDay(a, b time.Time) bool {
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}
