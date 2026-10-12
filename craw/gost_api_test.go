package craw

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Тесты транспорта API СБКТС офлайн: источник — httptest-сервер, тела ответов —
// фикстура craw/testdata/gost_day.json (срез живого ответа) или собранный из неё же
// JSON. Ни сети, ни ClickHouse, ни токена здесь нет: appid подставной.
// Пинятся четыре контракта ручки: форма запроса (путь, query, заголовки), сборка дня
// по страницам, клиентский фильтр и класс повторов (permanent против retryable).

const (
	// gostTestPath — путь ручки внутри подменённого gostAPIURL: слаг живой ручки
	// /standards/rest-api/libraries/search, чтобы форма запроса проверялась целиком.
	gostTestPath = "/standards/rest-api/libraries/search"
	// gostTestAppID — подставной appid: настоящий токен gtn.access.appid тестам не нужен.
	gostTestAppID = "test-appid"
)

// gostTestDay — день среза, для которого фикстура отдаёт ровно одну строку: у остальных
// документов либо другая дата публикации, либо не заполнены дата выдачи и номер.
var gostTestDay = time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)

// gostTestPublication — дата публикации документов фикстуры, попадающих в gostTestDay.
const gostTestPublication = "11.10.2026 00:00:00 ZE3"

// gostDayFixture читает фикстуру дня и разбирает её в gostSearchResponse: разъехавшийся
// json-тег gostDoc/gostFields сломает разбор и уронит тест, а не тихо отдаст пустые поля.
func gostDayFixture(t *testing.T) gostSearchResponse {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "gost_day.json"))
	if err != nil {
		t.Fatalf("чтение фикстуры дня: %v", err)
	}
	var resp gostSearchResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("разбор фикстуры дня: %v", err)
	}
	return resp
}

// gostFixtureDoc — документ фикстуры по номеру в data (порядок фикстуры фиксирован: 0 —
// Toyota дня 11.10.2026, 1 — чужой день публикации, 2 — без даты выдачи, 3 — без номера).
func gostFixtureDoc(t *testing.T, i int) gostDoc {
	t.Helper()
	docs := gostDayFixture(t).Data
	if i < 0 || i >= len(docs) {
		t.Fatalf("в фикстуре нет документа %d (всего %d)", i, len(docs))
	}
	return docs[i]
}

// gostRepeatDoc размножает документ до n штук, меняя id: из одной строки фикстуры
// собирается страница нужного размера (gostPageSize документов) без рукописного JSON.
func gostRepeatDoc(doc gostDoc, n int) []gostDoc {
	return gostRepeatDocFrom(doc, n, 1)
}

// gostRepeatDocFrom — то же, но с начальным id: страницы одного дня несут разные документы, и
// нумерация со сдвигом не даёт разным страницам случайно совпасть id (для day() повтор id —
// дубликат страницы, а не разные документы).
func gostRepeatDocFrom(doc gostDoc, n, startID int) []gostDoc {
	out := make([]gostDoc, n)
	for i := range out {
		doc.ID = int64(startID + i)
		out[i] = doc
	}
	return out
}

// gostTestBody собирает тело ответа ручки: count — «всего совпадений», data — страница.
func gostTestBody(t *testing.T, count int, docs []gostDoc) []byte {
	t.Helper()
	body, err := json.Marshal(gostSearchResponse{Count: count, Data: docs})
	if err != nil {
		t.Fatalf("сборка тела ответа: %v", err)
	}
	return body
}

// gostTestRequest — записанный запрос: по нему проверяется форма обращения к ручке.
type gostTestRequest struct {
	Path   string
	Query  url.Values
	Header http.Header
}

// gostTestAPI — api, направленный на httptest-сервер с handler, и журнал его запросов.
type gostTestAPI struct {
	api    *gostAPI
	server *httptest.Server
	mu     sync.Mutex
	reqs   []gostTestRequest
}

// gostNewTestAPI поднимает httptest-сервер, подменяет gostAPIURL его адресом и сокращает
// паузы транспорта: тест не должен ждать ни gostRequestGap между страницами, ни
// gostRetryAfter* между повторами. Значения переменных восстанавливаются после теста.
func gostNewTestAPI(t *testing.T, handler http.HandlerFunc) *gostTestAPI {
	t.Helper()
	ta := &gostTestAPI{}
	ta.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ta.mu.Lock()
		ta.reqs = append(ta.reqs, gostTestRequest{
			Path:   r.URL.Path,
			Query:  r.URL.Query(),
			Header: r.Header.Clone(),
		})
		ta.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(ta.server.Close)

	oldURL, oldGap := gostAPIURL, gostRequestGap
	oldBackoff, oldRetryTimeout, oldRetryServer := gostBackoffPolicy, gostRetryAfterTimeout, gostRetryAfterServer
	gostAPIURL = ta.server.URL + gostTestPath
	gostRequestGap = 0
	gostBackoffPolicy.InitialInterval = time.Millisecond
	gostBackoffPolicy.MaxInterval = 10 * time.Millisecond
	gostRetryAfterTimeout, gostRetryAfterServer = time.Millisecond, time.Millisecond
	t.Cleanup(func() {
		gostAPIURL, gostRequestGap = oldURL, oldGap
		gostBackoffPolicy = oldBackoff
		gostRetryAfterTimeout, gostRetryAfterServer = oldRetryTimeout, oldRetryServer
	})

	ta.api = newGostAPI(ta.server.Client(), gostTestAppID)
	return ta
}

// requests — запросы в порядке прихода (копия: журнал живёт дольше теста).
func (ta *gostTestAPI) requests() []gostTestRequest {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	return append([]gostTestRequest(nil), ta.reqs...)
}

func (ta *gostTestAPI) requestCount() int {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	return len(ta.reqs)
}

// queryParam — значения одного query-параметра по всем запросам в порядке прихода.
func (ta *gostTestAPI) queryParam(name string) []string {
	reqs := ta.requests()
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Query.Get(name))
	}
	return out
}

// gostPagesHandler отдаёт тела страниц по номеру из pages; неизвестная страница — 404,
// чтобы «прочитал лишнюю страницу» падало ошибкой импорта, а не тихим успехом.
func gostPagesHandler(pages map[int][]byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, ok := pages[page]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}
}

// gostStatusHandler отдаёт один и тот же статус на любой запрос.
func gostStatusHandler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}
}

func TestGostMSKDate(t *testing.T) {
	cases := []struct {
		name string
		ms   int64
		want string
	}{
		// 1791493200000 = 2026-10-08 21:00 UTC, то есть уже 00:00 МСК 09.10:
		// источник кладёт в миллисекунды момент UTC, а дата выдачи — московская.
		{"21:00 UTC — это полночь МСК", 1791493200000, "2026-10-09"},
		{"эпоха", 0, "1970-01-01"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := time.Parse("2006-01-02", c.want)
			if err != nil {
				t.Fatalf("разбор ожидания: %v", err)
			}
			got := gostMSKDate(c.ms)
			if !got.Equal(want) {
				t.Errorf("gostMSKDate(%d) = %s, ожидалось %s", c.ms, got, want)
			}
			// Дата живёт в UTC-полуночи: так сравниваются срезы и пишется колонка Date.
			if got.Location() != time.UTC || got.Hour() != 0 || got.Minute() != 0 || got.Second() != 0 {
				t.Errorf("gostMSKDate(%d) = %s: ожидалась UTC-полночь", c.ms, got)
			}
		})
	}
}

func TestGostParsePublicationDay(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{"живой формат с маркером пояса", "11.10.2026 00:00:00 ZE3", "2026-10-11", true},
		{"форма с временем без маркера пояса", "11.10.2026 00:00:00", "2026-10-11", true},
		// Чистая дата без времени и зоны — вторая наблюдаемая форма: в живом прогоне 15.06.2026
		// ручка отдала документ с dateofpublication = "28.03.2024" среди обычных.
		{"чистая дата", "28.03.2024", "2024-03-28", true},
		{"чистая дата в пробелах", "  28.03.2024  ", "2024-03-28", true},
		{"хвост без пробела — мусор", "28.03.2024xyz", "", false},
		{"мусор", "мусор", "", false},
		{"не дата нужной длины", "abcdefghij", "", false},
		{"пусто", "", "", false},
		{"однозначный день и месяц", "8.3.2024", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := gostParsePublicationDay(c.raw)
			if ok != c.ok {
				t.Fatalf("gostParsePublicationDay(%q) = %s, %v, ожидалось ok=%v", c.raw, got, ok, c.ok)
			}
			if !ok {
				if !got.IsZero() {
					t.Errorf("при ok=false ожидалось нулевое время, получено %s", got)
				}
				return
			}
			if got.Format("2006-01-02") != c.want {
				t.Errorf("gostParsePublicationDay(%q) = %s, ожидалось %s", c.raw, got, c.want)
			}
		})
	}

	// Обе наблюдаемые формы одной даты дают один и тот же день — так их и сравнивает клиентский
	// фильтр дня (gostSameDay): чистая дата не должна становиться «чужой» из-за формы записи.
	plain, okPlain := gostParsePublicationDay("28.03.2024")
	stamped, okStamped := gostParsePublicationDay("28.03.2024 00:00:00 ZE3")
	if !okPlain || !okStamped {
		t.Fatalf("формы одной даты: чистая ok=%v, с временем и поясом ok=%v — обе обязаны разбираться",
			okPlain, okStamped)
	}
	if !plain.Equal(stamped) {
		t.Errorf("формы одной даты разошлись: %s и %s", plain, stamped)
	}
}

func TestGostYear(t *testing.T) {
	cases := []struct {
		raw  string
		want uint16
	}{
		{"2016", 2016},
		{" 2019 ", 2019},
		{"16", 0}, // двузначный год источника — не 16-й год
		{"н/д", 0},
		{"", 0},
		{"0000", 0},
		{"1899", 0}, // вне правдоподобного диапазона
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			if got := gostYear(c.raw); got != c.want {
				t.Errorf("gostYear(%q) = %d, ожидалось %d", c.raw, got, c.want)
			}
		})
	}
}

func TestGostRowsFromDocsFilters(t *testing.T) {
	resp := gostDayFixture(t)
	if resp.Count != 4 || len(resp.Data) != 4 {
		t.Fatalf("фикстура: count=%d, документов=%d, ожидалось 4 и 4", resp.Count, len(resp.Data))
	}
	// День фикстуры совпадает с gostTestDay: на этом держатся все проверки дня.
	if got := resp.Data[0].Fields.DateOfPublication; got != gostTestPublication {
		t.Fatalf("фикстура: дата публикации первого документа = %q, ожидалась %q", got, gostTestPublication)
	}

	// Причина пропуска у каждого документа фикстуры своя: первый — полный и свой, второй —
	// чужой день публикации (дата разбирается), третий и четвёртый — невалидные (нет даты
	// выдачи / пуст номер). Разделение причин — контракт: по нему gostRunDays решает, валить
	// прогон или считать день без своих публикаций.
	want := []gostDocReject{gostDocAccepted, gostDocForeign, gostDocInvalid, gostDocInvalid}
	for i, reject := range want {
		if _, got := gostRowFromDoc(gostTestDay, resp.Data[i]); got != reject {
			t.Errorf("документ %d (%s): решение = %v, ожидалось %v", i, resp.Data[i].Fields.Product, got, reject)
		}
	}

	rows, skipped, invalid := gostRowsFromDocs(gostTestDay, resp.Data)
	if len(rows) != 1 {
		t.Fatalf("строк %d, ожидалась 1: %+v", len(rows), rows)
	}
	if skipped != 3 {
		t.Errorf("skipped = %d, ожидалось 3", skipped)
	}
	if invalid != 2 {
		t.Errorf("invalid = %d, ожидалось 2 (нет даты выдачи и пуст номер): чужая дата невалидной не считается", invalid)
	}

	// Битая дата публикации — тоже «невалидный», а не «чужой»: день по такому документу не
	// разобрать, и на штатное окно текстового поиска это не спишешь.
	brokenPub := resp.Data[0]
	brokenPub.Fields.DateOfPublication = "мусор"
	if row, reject := gostRowFromDoc(gostTestDay, brokenPub); reject != gostDocInvalid {
		t.Errorf("документ с битой датой публикации: решение = %v, ожидался gostDocInvalid (%+v)", reject, row)
	}

	// Проверка «нет даты выдачи» изолируется отдельно: у документа 2 фикстуры пусты и дата,
	// и номер, поэтому его пропуск объясняется и тем и другим, и снятие проверки nil он не
	// поймает. Здесь та же карточка, но с заполненным номером — единственная причина
	// пропуска именно nil-дата, и без этой проверки документ обязан попасть в срез.
	noIssueDate := resp.Data[2]
	noIssueDate.Fields.NumberOfCertificate = "ТС RU А-DE.РА15.99999"
	if row, reject := gostRowFromDoc(gostTestDay, noIssueDate); reject != gostDocInvalid {
		t.Errorf("документ без даты выдачи: решение = %v, ожидался gostDocInvalid (%+v)", reject, row)
	}
	withNoIssueDate := append(append([]gostDoc(nil), resp.Data...), noIssueDate)
	if rows, skipped, invalid = gostRowsFromDocs(gostTestDay, withNoIssueDate); len(rows) != 1 || skipped != 4 || invalid != 3 {
		t.Errorf("с документом без даты выдачи: строк %d, skipped %d, invalid %d, ожидалось 1, 4 и 3",
			len(rows), skipped, invalid)
	}

	// Строка собирается целиком: и сдвиг даты выдачи в МСК, и год, и все поля карточки.
	got := rows[0]
	expected := gostRow{
		Mark:                "Toyota",
		Type:                "ZGE25",
		Date:                time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC),
		CertificateNumber:   "ТС RU А-JP.ОМ80.51298",
		EcoClass:            "4",
		EngineTrademark:     "Toyota, 2ZR,четырехтактный, бензиновый",
		YearOfManufacturing: 2016,
		NameOfManufacturer:  "TOYOTA MOTOR CORPORATION",
		UID:                 "0AFECC5373427BDA43258E8A00457597",
		PublicationDate:     gostTestDay,
	}
	if !got.Date.Equal(expected.Date) || !got.PublicationDate.Equal(expected.PublicationDate) {
		t.Errorf("даты строки: %s / %s, ожидались %s / %s", got.Date, got.PublicationDate, expected.Date, expected.PublicationDate)
	}
	got.Date, got.PublicationDate = time.Time{}, time.Time{}
	expected.Date, expected.PublicationDate = time.Time{}, time.Time{}
	if got != expected {
		t.Errorf("строка = %+v, ожидалась %+v", got, expected)
	}
}

func TestGostDayFetchesAllPages(t *testing.T) {
	toyota := gostFixtureDoc(t, 0)
	// День не влезает в одну страницу: 900 документов на первой, 100 на второй. id сквозные:
	// это разные документы одного дня, и дедупликация страниц их схлопывать не должна.
	pages := map[int][]byte{
		1: gostTestBody(t, 1000, gostRepeatDoc(toyota, gostPageSize)),
		2: gostTestBody(t, 1000, gostRepeatDocFrom(toyota, 100, gostPageSize+1)),
	}
	ta := gostNewTestAPI(t, gostPagesHandler(pages))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err != nil {
		t.Fatalf("day: %v", err)
	}
	if slice.Count != 1000 {
		t.Errorf("Count = %d, ожидалось 1000 (всего совпадений из ответа)", slice.Count)
	}
	if len(slice.Rows) != 1000 {
		t.Errorf("строк %d, ожидалось 1000: день должен читаться целиком", len(slice.Rows))
	}
	if slice.Skipped != 0 {
		t.Errorf("Skipped = %d, ожидалось 0: документы фикстуры попадают в срез", slice.Skipped)
	}
	if got := ta.queryParam("page"); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("страницы = %v, ожидались [1 2]", got)
	}
	if got := ta.queryParam("size"); !slices.Equal(got, []string{strconv.Itoa(gostPageSize), strconv.Itoa(gostPageSize)}) {
		t.Errorf("размеры страниц = %v, ожидался %d", got, gostPageSize)
	}
}

// TestGostDayDedupsRepeatedDocuments: страница вернула документ, уже пришедший на предыдущей
// (сдвиг пагинации при изменении данных). Без дедупликации день выглядел бы собранным «с
// запасом» — с настоящей недостачей внутри, и check либо соврал бы о дыре, либо скрыл её.
func TestGostDayDedupsRepeatedDocuments(t *testing.T) {
	toyota := gostFixtureDoc(t, 0)
	docs := gostRepeatDoc(toyota, gostPageSize)
	docs = append(docs, docs[0]) // одна и та же страница отдала первый документ ещё раз
	ta := gostNewTestAPI(t, gostPagesHandler(map[int][]byte{1: gostTestBody(t, gostPageSize, docs)}))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err != nil {
		t.Fatalf("день с дубликатом страницы: %v", err)
	}
	if slice.Count != gostPageSize {
		t.Errorf("Count = %d, ожидалось %d", slice.Count, gostPageSize)
	}
	if len(slice.Rows) != gostPageSize {
		t.Errorf("строк %d, ожидалось %d: дубликат не должен попасть в срез дня", len(slice.Rows), gostPageSize)
	}
}

// TestGostDayDuplicateShortageFails: страница вернула повторы вместо части документов —
// уникальных меньше count, день не добран, и это ошибка, а не «день полный».
func TestGostDayDuplicateShortageFails(t *testing.T) {
	toyota := gostFixtureDoc(t, 0)
	ta := gostNewTestAPI(t, gostPagesHandler(map[int][]byte{
		1: gostTestBody(t, 1000, gostRepeatDoc(toyota, gostPageSize)),
		2: gostTestBody(t, 1000, gostRepeatDocFrom(toyota, 100, 1)), // те же id: 100 дубликатов
	}))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err == nil {
		t.Fatal("ожидалась ошибка о недостаче после дедупликации")
	}
	if want := "получено 900 документов из 1000 (дубликатов страниц: 100)"; !strings.Contains(err.Error(), want) {
		t.Errorf("ошибка = %q, ожидалась подстрока %q", err, want)
	}
	if len(slice.Rows) != 0 {
		t.Errorf("строк %d: неполный день писать нельзя", len(slice.Rows))
	}
}

func TestGostDayClientFilterCountsSkipped(t *testing.T) {
	// Обе страницы чужого дня: ручка ищет совпадения по своему окну и отдаёт лишнее,
	// отсекает документы уже клиент, и это видно в Skipped.
	docs := gostDayFixture(t).Data[:2]
	for i := range docs {
		docs[i].Fields.DateOfPublication = "10.10.2026 00:00:00 ZE3"
	}
	ta := gostNewTestAPI(t, gostPagesHandler(map[int][]byte{1: gostTestBody(t, len(docs), docs)}))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err != nil {
		t.Fatalf("day: %v", err)
	}
	if slice.Count != 2 {
		t.Errorf("Count = %d, ожидалось 2", slice.Count)
	}
	if len(slice.Rows) != 0 {
		t.Errorf("строк %d, ожидалось 0: все документы чужого дня", len(slice.Rows))
	}
	if slice.Skipped != 2 {
		t.Errorf("Skipped = %d, ожидалось 2", slice.Skipped)
	}
	// Чужие дни публикации — не поломка формата: невалидных документов в дне нет, и день с
	// таким срезом не считается ошибкой (см. gostRunDays).
	if slice.Invalid != 0 {
		t.Errorf("Invalid = %d, ожидалось 0: чужая дата публикации — не невалидный документ", slice.Invalid)
	}
}

// TestGostDayCountsInvalidDocs — день целиком: один свой документ, один чужого дня публикации и
// два невалидных (без даты выдачи, пуст номер). Разделение Skipped/Invalid — контракт с
// gostRunDays: по Invalid решается, валить прогон или считать день без своих публикаций.
func TestGostDayCountsInvalidDocs(t *testing.T) {
	docs := gostDayFixture(t).Data
	ta := gostNewTestAPI(t, gostPagesHandler(map[int][]byte{1: gostTestBody(t, len(docs), docs)}))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err != nil {
		t.Fatalf("day: %v", err)
	}
	if len(slice.Rows) != 1 {
		t.Errorf("строк %d, ожидалась 1", len(slice.Rows))
	}
	if slice.Skipped != 3 {
		t.Errorf("Skipped = %d, ожидалось 3 (все отброшенные)", slice.Skipped)
	}
	if slice.Invalid != 2 {
		t.Errorf("Invalid = %d, ожидалось 2 (нет даты выдачи, пуст номер)", slice.Invalid)
	}
}

func TestGostDayCountMismatchFails(t *testing.T) {
	pages := map[int][]byte{
		1: gostTestBody(t, 1000, gostRepeatDoc(gostFixtureDoc(t, 0), gostPageSize)),
		2: gostTestBody(t, 1000, nil), // вторая страница пришла пустой
	}
	ta := gostNewTestAPI(t, gostPagesHandler(pages))

	slice, err := ta.api.day(context.Background(), gostTestDay)
	if err == nil {
		t.Fatal("ожидалась ошибка о недостаче документов дня")
	}
	if want := "получено 900 документов из 1000"; !strings.Contains(err.Error(), want) {
		t.Errorf("ошибка = %q, ожидалась подстрока %q", err, want)
	}
	if len(slice.Rows) != 0 {
		t.Errorf("строк %d: неполный день писать нельзя", len(slice.Rows))
	}
}

func TestGostDayTooManyDocsFails(t *testing.T) {
	// Порог проверяется на границе: gostMaxDayCount документов — ещё не ошибка,
	// gostMaxDayCount+1 — уже.
	pages := map[int][]byte{
		1: gostTestBody(t, gostMaxDayCount+1, []gostDoc{gostFixtureDoc(t, 0)}),
	}
	ta := gostNewTestAPI(t, gostPagesHandler(pages))

	_, err := ta.api.day(context.Background(), gostTestDay)
	if err == nil {
		t.Fatal("ожидалась ошибка про окно поиска")
	}
	for _, want := range []string{strconv.Itoa(gostMaxDayCount + 1), "окна поиска"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка = %q, ожидалась подстрока %q", err, want)
		}
	}
	if got := ta.requestCount(); got != 1 {
		t.Errorf("запросов %d, ожидался 1: при переполнении страницы 2+ качать незачем", got)
	}
}

func TestGostDayPermanentStatusFails(t *testing.T) {
	// 401/403 — отказ доступа, а не сбой: повтор не поможет и только злит портал.
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			ta := gostNewTestAPI(t, gostStatusHandler(status))

			_, err := ta.api.day(context.Background(), gostTestDay)
			if err == nil {
				t.Fatalf("ожидалась ошибка на статус %d", status)
			}
			if want := "http status " + strconv.Itoa(status); !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка = %q, ожидалась подстрока %q", err, want)
			}
			if !strings.Contains(err.Error(), "GOST_APP_ID") {
				t.Errorf("ошибка = %q: текст должен подсказывать про GOST_APP_ID", err)
			}
			if got := ta.requestCount(); got != 1 {
				t.Errorf("запросов %d, ожидался 1: permanent-ошибка не повторяется", got)
			}
		})
	}
}

func TestGostDayRetriesServerError(t *testing.T) {
	t.Run("500, затем 200", func(t *testing.T) {
		body := gostTestBody(t, 1, []gostDoc{gostFixtureDoc(t, 0)})
		var calls atomic.Int32
		ta := gostNewTestAPI(t, func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(body)
		})

		slice, err := ta.api.day(context.Background(), gostTestDay)
		if err != nil {
			t.Fatalf("500 должен повторяться, получено: %v", err)
		}
		if len(slice.Rows) != 1 {
			t.Errorf("строк %d, ожидалась 1", len(slice.Rows))
		}
		if got := ta.requestCount(); got != 2 {
			t.Errorf("запросов %d, ожидалось 2 (сбой и успех)", got)
		}
	})

	t.Run("500 на всех попытках", func(t *testing.T) {
		ta := gostNewTestAPI(t, gostStatusHandler(http.StatusInternalServerError))

		_, err := ta.api.day(context.Background(), gostTestDay)
		if err == nil {
			t.Fatal("ожидалась ошибка после исчерпания попыток")
		}
		// Причина обязана дожить до текста ошибки: «retry after» без статуса оператору
		// ничего не говорит.
		if !strings.Contains(err.Error(), "http status 500") {
			t.Errorf("ошибка = %q, ожидался статус в тексте", err)
		}
		if got := ta.requestCount(); got != gostMaxTries {
			t.Errorf("попыток %d, ожидалось %d", got, gostMaxTries)
		}
	})
}

func TestGostDayEmptyBodyFails(t *testing.T) {
	// И пустое тело, и пробелы, и литерал null (разбирается без ошибки в нулевой count),
	// и битый JSON — не «день без документов», а сбой ответа: повторяем.
	cases := []struct {
		name string
		body string
		want string
	}{
		{"пустое тело", "", "пустое тело"},
		{"одни пробелы", " \n\t ", "пустое тело"},
		{"литерал null", "null", "пустое тело"},
		{"битый JSON", "{", "разбор JSON"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := []byte(c.body)
			ta := gostNewTestAPI(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(body)
			})

			_, err := ta.api.day(context.Background(), gostTestDay)
			if err == nil {
				t.Fatal("ожидалась ошибка на пустой ответ")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("ошибка = %q, ожидалась подстрока %q", err, c.want)
			}
			if got := ta.requestCount(); got != gostMaxTries {
				t.Errorf("попыток %d, ожидалось %d: пустой ответ повторяем", got, gostMaxTries)
			}
		})
	}
}

// TestGostRetryAfterPauses пинит не факт повтора, а саму паузу: backoff выдерживает ровно
// RetryAfterError.Duration, и она обязана совпадать со значением переменной — иначе
// объявленные 60 с для 5xx и 30 с для дедлайна молча превращаются в ноль.
func TestGostRetryAfterPauses(t *testing.T) {
	if gostRetryAfterTimeout != 30*time.Second || gostRetryAfterServer != 60*time.Second {
		t.Errorf("паузы по умолчанию = %s / %s, ожидались 30s / 60s",
			gostRetryAfterTimeout, gostRetryAfterServer)
	}

	pauses := []struct {
		name string
		err  error
		want time.Duration
	}{
		{"429", gostStatusError(http.StatusTooManyRequests), gostRetryAfterServer},
		{"500", gostStatusError(http.StatusInternalServerError), gostRetryAfterServer},
		{"дедлайн попытки", gostTransportError(context.DeadlineExceeded), gostRetryAfterTimeout},
	}
	for _, c := range pauses {
		t.Run(c.name, func(t *testing.T) {
			var retryAfter *backoff.RetryAfterError
			if !errors.As(c.err, &retryAfter) {
				t.Fatalf("ошибка %q не несёт RetryAfterError: пауза перед повтором не задана", c.err)
			}
			if retryAfter.Duration != c.want {
				t.Errorf("пауза = %s, ожидалась %s", retryAfter.Duration, c.want)
			}
			var permanent *backoff.PermanentError
			if errors.As(c.err, &permanent) {
				t.Errorf("ошибка %q помечена permanent — повтора по паузе не будет", c.err)
			}
		})
	}

	// Та же пауза, но по всему пути до ошибки дня: значение переменной доезжает из
	// fetchPage наружу без округлений (тестовые миллисекунды не должны стать нулём).
	t.Run("пауза доезжает до ошибки дня", func(t *testing.T) {
		ta := gostNewTestAPI(t, gostStatusHandler(http.StatusInternalServerError))
		gostRetryAfterServer = 5 * time.Millisecond // gostNewTestAPI ставил 1 мс

		_, err := ta.api.day(context.Background(), gostTestDay)
		if err == nil {
			t.Fatal("ожидалась ошибка после исчерпания попыток")
		}
		var retryAfter *backoff.RetryAfterError
		if !errors.As(err, &retryAfter) {
			t.Fatalf("ошибка %q не несёт RetryAfterError", err)
		}
		if retryAfter.Duration != gostRetryAfterServer {
			t.Errorf("пауза = %s, ожидалась %s (значение переменной)", retryAfter.Duration, gostRetryAfterServer)
		}
	})
}

// TestGostDayBrokenURLErrorHidesURL: ошибка разбора адреса приходит из net/http обёрнутой
// в *url.Error с полным URL (в query бывают ключи), и в текст ошибки URL попадать не должен.
func TestGostDayBrokenURLErrorHidesURL(t *testing.T) {
	ta := gostNewTestAPI(t, gostPagesHandler(nil))
	gostAPIURL = "http://[::1" // незакрытая скобка: url.Parse падает до запроса

	_, err := ta.api.day(context.Background(), gostTestDay)
	if err == nil {
		t.Fatal("ожидалась ошибка на сломанный адрес ручки")
	}
	for _, bad := range []string{"http", "query="} {
		if strings.Contains(err.Error(), bad) {
			t.Errorf("ошибка = %q: URL не должен попадать в текст (найдена подстрока %q)", err, bad)
		}
	}
	if got := ta.requestCount(); got != 0 {
		t.Errorf("запросов %d, ожидалось 0: сломанный адрес повтором не лечится", got)
	}
}

func TestGostDayRequestShape(t *testing.T) {
	body := gostTestBody(t, 1, []gostDoc{gostFixtureDoc(t, 0)})
	ta := gostNewTestAPI(t, gostPagesHandler(map[int][]byte{1: body}))

	if _, err := ta.api.day(context.Background(), gostTestDay); err != nil {
		t.Fatalf("day: %v", err)
	}
	if _, err := ta.api.dayCount(context.Background(), gostTestDay); err != nil {
		t.Fatalf("dayCount: %v", err)
	}

	reqs := ta.requests()
	if len(reqs) != 2 {
		t.Fatalf("запросов %d, ожидалось 2 (страница дня и count)", len(reqs))
	}
	for i, r := range reqs {
		if r.Path != gostTestPath {
			t.Errorf("запрос %d: путь %q, ожидался %q", i, r.Path, gostTestPath)
		}
		if got := r.Query.Get("query"); got != "11.10.2026" {
			t.Errorf("запрос %d: query = %q, ожидалось 11.10.2026 (день среза)", i, got)
		}
		if got := r.Query.Get("page"); got != "1" {
			t.Errorf("запрос %d: page = %q, ожидалось 1", i, got)
		}
		if got := r.Header.Get(gostAppIDHeader); got != gostTestAppID {
			t.Errorf("запрос %d: заголовок %s = %q, ожидался %q", i, gostAppIDHeader, got, gostTestAppID)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("запрос %d: Accept = %q, ожидался application/json", i, got)
		}
		if got := r.Header.Get("User-Agent"); got != util.HttpUA {
			t.Errorf("запрос %d: User-Agent = %q, ожидался util.HttpUA", i, got)
		}
		if tmsp, err := strconv.ParseInt(r.Query.Get("tmsp"), 10, 64); err != nil || tmsp <= 0 {
			t.Errorf("запрос %d: tmsp = %q, ожидалась метка времени в миллисекундах", i, r.Query.Get("tmsp"))
		}
	}
	// day() тянет полную страницу, dayCount() — минимальную: он нужен ради одного count.
	if got := reqs[0].Query.Get("size"); got != strconv.Itoa(gostPageSize) {
		t.Errorf("day: size = %q, ожидался %d", got, gostPageSize)
	}
	if got := reqs[1].Query.Get("size"); got != "1" {
		t.Errorf("dayCount: size = %q, ожидался 1", got)
	}
}
