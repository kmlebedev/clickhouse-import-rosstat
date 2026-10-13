package util

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// publicationHeadHeaders — заголовки, из которых собирается отпечаток версии артефакта
// (спека §3.4). Пустая строка = заголовка нет: композит остаётся валидным, компонент пустой.
type publicationHeadHeaders struct {
	etag          string
	lastModified  string
	contentLength string
}

// publicationCbrHeaders — фактические заголовки HEAD полного файла mo_br (разведка, спека §2:
// etag "0002694F", last-modified Wed, 02 Sep 2026 12:38:00 GMT, content-length 301574).
// Content-MD5 ЦБ не отдаёт — отпечаток собирается из композита этих трёх заголовков.
var publicationCbrHeaders = publicationHeadHeaders{
	etag:          `"0002694F"`,
	lastModified:  "Wed, 02 Sep 2026 12:38:00 GMT",
	contentLength: "301574",
}

// publicationHeadServer отвечает на HEAD заданными заголовками; тела нет — префлайт обязан
// решать по заголовкам, а не по содержимому (файл на 300 КБ качать незачем). Заодно пинним
// метод и User-Agent: HTTP в проекте идёт только через util.HttpClient и со случайным UA.
func publicationHeadServer(t *testing.T, h publicationHeadHeaders) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("метод %s, ожидался HEAD: префлайт не должен скачивать файл", r.Method)
		}
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent: конвенция GetFile/GetXML/GetJSON — randomUA")
		}
		if h.etag != "" {
			w.Header().Set("ETag", h.etag)
		}
		if h.lastModified != "" {
			w.Header().Set("Last-Modified", h.lastModified)
		}
		if h.contentLength != "" {
			w.Header().Set("Content-Length", h.contentLength)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// publicationFingerprint — отпечаток или фатальный провал теста.
func publicationFingerprint(t *testing.T, url string) string {
	t.Helper()
	fp, err := HeadFingerprint(url)
	if err != nil {
		t.Fatalf("HeadFingerprint(%s): %v", url, err)
	}
	return fp
}

// publicationRequireMd5Hex проверяет формат отпечатка: 32 hex-символа. Длина и алфавит —
// часть контракта: отпечаток пишется в ключ source_publications и сравнивается byte-to-byte.
func publicationRequireMd5Hex(t *testing.T, fingerprint string) {
	t.Helper()
	if len(fingerprint) != 32 {
		t.Fatalf("отпечаток %q длиной %d, ожидались 32 hex-символа", fingerprint, len(fingerprint))
	}
	for _, c := range fingerprint {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("отпечаток %q не hex: символ %q", fingerprint, c)
			return
		}
	}
}

// Ежедневный прогон mo_br решает по отпечатку, скачивать ли файл: два HEAD подряд по
// неизменившемуся файлу обязаны дать один и тот же отпечаток, иначе каждый прогон считался бы
// новой версией и импортёр качал бы 300 КБ и переписывал всю историю ежедневно.
func TestHeadFingerprintStable(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"

	first := publicationFingerprint(t, url)
	second := publicationFingerprint(t, url)
	if first != second {
		t.Errorf("отпечаток не стабилен: %q, затем %q", first, second)
	}
	publicationRequireMd5Hex(t, first)
}

// Композит меняется ровно при смене версии файла: правка любого из трёх заголовков обязана
// дать другой отпечаток, иначе новая волна у ЦБ прошла бы мимо префлайта (no-op навсегда).
func TestHeadFingerprintChangesWithHeaders(t *testing.T) {
	cases := []struct {
		name    string
		changed publicationHeadHeaders
	}{
		{"etag", publicationHeadHeaders{
			etag:          `"00026950"`,
			lastModified:  publicationCbrHeaders.lastModified,
			contentLength: publicationCbrHeaders.contentLength,
		}},
		{"last-modified", publicationHeadHeaders{
			etag:          publicationCbrHeaders.etag,
			lastModified:  "Mon, 05 Oct 2026 10:00:00 GMT",
			contentLength: publicationCbrHeaders.contentLength,
		}},
		{"content-length", publicationHeadHeaders{
			etag:          publicationCbrHeaders.etag,
			lastModified:  publicationCbrHeaders.lastModified,
			contentLength: "312004",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base := publicationFingerprint(t, publicationHeadServer(t, publicationCbrHeaders).URL+"/full.xlsx")
			other := publicationFingerprint(t, publicationHeadServer(t, c.changed).URL+"/full.xlsx")
			if base == other {
				t.Errorf("смена %s не изменила отпечаток (%q): новая версия файла осталась бы незамеченной", c.name, base)
			}
		})
	}
}

// Имя артефакта — часть композита: ЦБ может сменить и файл, и имя (id в URL нестабилен).
// Одинаковые заголовки при разных basename — разные версии в source_publications.
func TestHeadFingerprintChangesWithName(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)

	full := publicationFingerprint(t, server.URL+"/full.xlsx")
	other := publicationFingerprint(t, server.URL+"/other.xlsx")
	if full == other {
		t.Errorf("смена basename URL не изменила отпечаток (%q): переименование файла не было бы замечено", full)
	}
}

// Content-MD5 ЦБ не отдаёт, и ETag может пропасть: отпечаток обязан считаться и без него —
// композит из остальных заголовков (спека §3.4). Важно, что это именно пустой компонент, а не
// пропуск: иначе композит без ETag совпал бы с композитом с ETag.
func TestHeadFingerprintWithoutETag(t *testing.T) {
	without := publicationHeadHeaders{
		lastModified:  publicationCbrHeaders.lastModified,
		contentLength: publicationCbrHeaders.contentLength,
	}
	serverWithout := publicationFingerprint(t, publicationHeadServer(t, without).URL+"/full.xlsx")
	serverWith := publicationFingerprint(t, publicationHeadServer(t, publicationCbrHeaders).URL+"/full.xlsx")

	publicationRequireMd5Hex(t, serverWithout)
	if serverWithout == serverWith {
		t.Error("композит без ETag совпал с композитом с ETag: пустой компонент подменён пропуском")
	}
}

// Предельный случай той же ветки: сервер не отдал ни одного из трёх заголовков. Отпечаток
// всё равно считается (композит "имя|||") — иначе недоступный по заголовкам сервер ломал бы
// префлайт ошибкой вместо деградации до полного импорта без записи версии.
func TestHeadFingerprintWithoutHeaders(t *testing.T) {
	fp := publicationFingerprint(t, publicationHeadServer(t, publicationHeadHeaders{}).URL+"/full.xlsx")
	publicationRequireMd5Hex(t, fp)
}

// Не-2xx — ошибка, а не «отпечаток из пустых заголовков»: иначе 404/500 превратился бы в
// валидную версию и импортёр счёл бы файл уже импортированным. Сообщение обязано называть код:
// вызывающий (mo_br) логирует warn и уходит в полный импорт — ему нужна причина.
func TestHeadFingerprintNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	fp, err := HeadFingerprint(server.URL + "/full.xlsx")
	if err == nil {
		t.Fatalf("HEAD 500 обязан вернуть ошибку, получен отпечаток %q", fp)
	}
	if fp != "" {
		t.Errorf("при ошибке отпечаток обязан быть пустым, получен %q", fp)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("ошибка %q не называет код ответа", err)
	}
}

// Сеть, а не статус: тот же контракт (ошибка и пустой отпечаток) — импортёр деградирует до
// полного импорта без записи версии (спека §3.4, п. 3).
func TestHeadFingerprintNetworkError(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	server.Close()

	fp, err := HeadFingerprint(url)
	if err == nil {
		t.Fatalf("недоступный сервер обязан вернуть ошибку, получен отпечаток %q", fp)
	}
	if fp != "" {
		t.Errorf("при ошибке отпечаток обязан быть пустым, получен %q", fp)
	}
}

// Соединение в тестах не поднимается, но контракт трёх функций БД проверяется фейком: DDL
// литералом уезжает в ARCHITECTURE §6.2 и на живую базу при приёмке, а порядок Append —
// позиционный (INSERT без списка колонок), поэтому обе вещи надо пинить здесь, а не в T6.
type publicationFakeConn struct {
	driver.Conn
	// calls — общий порядок обращений к БД. Разрезы execQueries/rowQueries/batchQueries его не
	// заменяют: префлайт и запись версии живут в одном цикле Import, и контракт «версия
	// последней» (ARCHITECTURE §8 п. 13) проверяется только по общей последовательности.
	calls        []string
	execQueries  []string
	rowQueries   []string
	rowArgs      [][]any
	rows         []driver.Row // очередь ответов QueryRow; опустела — работает row
	row          driver.Row   // nil — строк нет: Scan получает sql.ErrNoRows, как у живого драйвера
	batchQueries []string
	batches      []*publicationFakeBatch
}

func (c *publicationFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.calls = append(c.calls, "Exec")
	c.execQueries = append(c.execQueries, query)
	return nil
}

func (c *publicationFakeConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	c.calls = append(c.calls, "QueryRow")
	c.rowQueries = append(c.rowQueries, query)
	c.rowArgs = append(c.rowArgs, args)
	if len(c.rows) > 0 {
		row := c.rows[0]
		c.rows = c.rows[1:]
		return row
	}
	if c.row != nil {
		return c.row
	}
	return &publicationFakeRow{err: sql.ErrNoRows}
}

func (c *publicationFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.calls = append(c.calls, "PrepareBatch")
	c.batchQueries = append(c.batchQueries, query)
	batch := &publicationFakeBatch{conn: c}
	c.batches = append(c.batches, batch)
	return batch, nil
}

// publicationUntouchedConn — коннект, которого префлайт не должен коснуться. Не запись вызова, а
// паника: при недоступном HEAD решения принимаются без БД (отпечатка нет — нечего и искать в
// source_publications, и записывать), поэтому любое обращение здесь — ошибка логики, а не данных.
type publicationUntouchedConn struct{ driver.Conn }

func (c *publicationUntouchedConn) Exec(context.Context, string, ...any) error {
	panic("префлайт обратился к БД при недоступном HEAD: Exec")
}

func (c *publicationUntouchedConn) QueryRow(context.Context, string, ...any) driver.Row {
	panic("префлайт обратился к БД при недоступном HEAD: QueryRow")
}

func (c *publicationUntouchedConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("префлайт обратился к БД при недоступном HEAD: PrepareBatch")
}

type publicationFakeRow struct {
	driver.Row
	err    error
	points uint64
}

func (r *publicationFakeRow) Err() error { return r.err }

func (r *publicationFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("фейковая строка source_publications: приёмников %d (want 1)", len(dest))
	}
	points, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("фейковая строка source_publications: приёмник points %T (want *uint64)", dest[0])
	}
	*points = r.points
	return nil
}

type publicationFakeBatch struct {
	driver.Batch
	conn    *publicationFakeConn // nil — батч вне коннекта; нужен, чтобы Send попал в общий порядок вызовов
	appends [][]any
	sends   int
}

func (b *publicationFakeBatch) Append(v ...any) error {
	b.appends = append(b.appends, append([]any(nil), v...))
	return nil
}

func (b *publicationFakeBatch) Send() error {
	b.sends++
	if b.conn != nil {
		b.conn.calls = append(b.conn.calls, "Send")
	}
	return nil
}

// DDL — канонический из спеки §3.4 (он же ARCHITECTURE §6.2): колонки и ключ ReplacingMergeTree
// обязаны совпасть с литералом, иначе живая приёмка создаст таблицу, отличную от документации.
func TestEnsurePublicationsDdl(t *testing.T) {
	conn := &publicationFakeConn{}
	if err := EnsurePublications(context.Background(), conn); err != nil {
		t.Fatalf("EnsurePublications: %v", err)
	}
	if len(conn.execQueries) != 1 {
		t.Fatalf("Exec вызван %d раз, ожидался один CREATE TABLE", len(conn.execQueries))
	}
	ddl := conn.execQueries[0]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS " + PublicationsTable,
		"source LowCardinality(String)",
		"name LowCardinality(String)",
		"fingerprint String",
		"first_seen DateTime",
		"points UInt64",
		"url String",
		"ENGINE = ReplacingMergeTree ORDER BY (source, name, fingerprint)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL не содержит %q:\n%s", want, ddl)
		}
	}
	// Порядок колонок — не косметика: SavePublication вставляет позиционно, без списка колонок.
	order := []string{"source LowCardinality", "name LowCardinality", "fingerprint String", "first_seen DateTime", "points UInt64", "url String"}
	prev := -1
	for _, col := range order {
		at := strings.Index(ddl, col)
		if at <= prev {
			t.Errorf("колонка %q стоит не в порядке DDL:\n%s", col, ddl)
		}
		prev = at
	}
}

// Одна увиденная версия — одна строка: батч на одну строку, но всё равно PrepareBatch → Append → Send
// (правило проекта: вставки только батчами). Порядок значений — как в DDL.
func TestSavePublicationOneRow(t *testing.T) {
	conn := &publicationFakeConn{}
	seen := time.Date(2026, time.October, 13, 9, 17, 0, 0, time.UTC)
	p := Publication{
		Source:      "cbr_mo_br",
		Name:        "full.xlsx",
		Fingerprint: "0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		Url:         "https://www.cbr.ru/Content/Document/File/144490/full.xlsx",
		FirstSeen:   seen,
		Points:      9348,
	}
	if err := SavePublication(context.Background(), conn, p); err != nil {
		t.Fatalf("SavePublication: %v", err)
	}
	if len(conn.batchQueries) != 1 || conn.batchQueries[0] != "INSERT INTO "+PublicationsTable {
		t.Fatalf("батч открыт запросом %v, ожидался INSERT INTO %s", conn.batchQueries, PublicationsTable)
	}
	batch := conn.batches[0]
	if len(batch.appends) != 1 {
		t.Fatalf("строк в батче %d, ожидалась одна (одна увиденная версия)", len(batch.appends))
	}
	want := []any{"cbr_mo_br", "full.xlsx", p.Fingerprint, seen, uint64(9348), p.Url}
	if !reflect.DeepEqual(batch.appends[0], want) {
		t.Errorf("строка батча %#v, ожидалась %#v (порядок колонок DDL)", batch.appends[0], want)
	}
	if batch.sends != 1 {
		t.Errorf("Send вызван %d раз, ожидался один", batch.sends)
	}
}

// Версия не найдена — не ошибка: для mo_br это штатный «файл новый», ведущий к полному импорту.
// Ошибку здесь импортёр игнорирует (`points, known, _ :=`), поэтому пустой ответ обязан
// приходить веткой (0, false, nil), а не err — иначе known никогда не стал бы true.
func TestPublicationKnownNoRows(t *testing.T) {
	conn := &publicationFakeConn{}
	points, known, err := PublicationKnown(context.Background(), conn, "cbr_mo_br", "full.xlsx", "0f1e")
	if err != nil {
		t.Fatalf("отсутствие строки — не ошибка, получено %v", err)
	}
	if known {
		t.Error("known = true при пустом ответе запроса")
	}
	if points != 0 {
		t.Errorf("points = %d при пустом ответе, ожидался 0", points)
	}
	if len(conn.rowQueries) != 1 {
		t.Fatalf("QueryRow вызван %d раз, ожидался один", len(conn.rowQueries))
	}
	query := conn.rowQueries[0]
	for _, want := range []string{
		"SELECT points FROM " + PublicationsTable + " FINAL",
		"source = ?",
		"name = ?",
		"fingerprint = ?",
		"LIMIT 1",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("запрос не содержит %q:\n%s", want, query)
		}
	}
	if want := []any{"cbr_mo_br", "full.xlsx", "0f1e"}; !reflect.DeepEqual(conn.rowArgs[0], want) {
		t.Errorf("аргументы запроса %#v, ожидались %#v (source, name, fingerprint)", conn.rowArgs[0], want)
	}
}

// Найденная версия отдаёт сохранённое число точек: им импортёр сверяет фактическое
// содержимое macro_series и решает, деградировать ли в полный импорт (self-heal).
func TestPublicationKnownFinds(t *testing.T) {
	conn := &publicationFakeConn{row: &publicationFakeRow{points: 9348}}
	points, known, err := PublicationKnown(context.Background(), conn, "cbr_mo_br", "full.xlsx", "0f1e")
	if err != nil {
		t.Fatalf("PublicationKnown: %v", err)
	}
	if !known {
		t.Error("known = false при найденной строке версии")
	}
	if points != 9348 {
		t.Errorf("points = %d, ожидалось 9348 (значение из source_publications)", points)
	}
}

// Прочие ошибки БД не глотаются: недоступная таблица/соединение обязаны дойти до вызывающего,
// а не выглядеть как «версия не найдена» (иначе каждый прогон шёл бы в полный импорт молча).
func TestPublicationKnownDbError(t *testing.T) {
	dbErr := fmt.Errorf("соединение потеряно")
	conn := &publicationFakeConn{row: &publicationFakeRow{err: dbErr}}
	points, known, err := PublicationKnown(context.Background(), conn, "cbr_mo_br", "full.xlsx", "0f1e")
	if err == nil {
		t.Fatal("ошибка БД должна вернуться вызывающему")
	}
	if !strings.Contains(err.Error(), "соединение потеряно") {
		t.Errorf("ошибка %v не сохранила причину", err)
	}
	if known || points != 0 {
		t.Errorf("при ошибке ожидалось (0, false), получено (%d, %t)", points, known)
	}
}

// ---- префлайт и цикл шаблона Import (спека §3.1, §3.4) ----

const (
	// publicationTestSource — ключ версии у импортёра, чей Name() не равен TableName (cbr_mo_br
	// пишет в общую macro_series). Проверяется, что ключ из PublicationSource попадает и в
	// source_publications, и в префикс лога.
	publicationTestSource = "cbr_mo_br"
	// publicationTestCountPoints — запрос количественного чека (спека §3.5): литерал, потому что
	// именно он уезжает в БД и он же становится points записываемой версии (спека §3.2).
	publicationTestCountPoints = `SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'`
)

// У табличных импортёров PublicationSource пуст, и ключом версии становится TableName.
// Выводить ключ из Name() нельзя: у cbr_mo_br и cbr_mp_survey Name() совпадает с ключом, но
// таблица у обоих macro_series — по ней версии двух источников смешались бы в одну строку.
func TestPublicationKeyFallsBackToTableName(t *testing.T) {
	tableOnly := &ClickHouseImport{TableName: "macro_series"}
	if got := tableOnly.PublicationKey(); got != "macro_series" {
		t.Errorf("ключ %q при пустом PublicationSource, ожидался TableName macro_series", got)
	}
	named := &ClickHouseImport{TableName: "macro_series", PublicationSource: publicationTestSource}
	if got := named.PublicationKey(); got != publicationTestSource {
		t.Errorf("ключ %q, ожидался %q (PublicationSource сильнее TableName)", got, publicationTestSource)
	}
}

// Пустой PublicationCountPoints — выключенный префлайт: у импортёров, не объявивших точку
// контроля, Publication() обязана быть нулевой — по нулевому значению энфорсер реестра (§3.6)
// отличает «объявление забыли» от «объявление не нужно».
func TestPublicationDeclEmptyWithoutCountPoints(t *testing.T) {
	off := &ClickHouseImport{
		TableName: "macro_series",
		DataUrl:   "https://www.cbr.ru/Content/Document/File/144490/full.xlsx",
	}
	if got := off.Publication(); got != (PublicationDecl{}) {
		t.Errorf("Publication() без PublicationCountPoints = %+v, ожидалось нулевое значение (префлайт выключен)", got)
	}

	on := &ClickHouseImport{
		TableName:              "macro_series",
		DataUrl:                "https://www.cbr.ru/Content/Document/File/144490/full.xlsx",
		PublicationSource:      publicationTestSource,
		PublicationCountPoints: publicationTestCountPoints,
	}
	want := PublicationDecl{
		Source:      publicationTestSource,
		Url:         on.DataUrl,
		CountPoints: publicationTestCountPoints,
	}
	if got := on.Publication(); got != want {
		t.Errorf("Publication() = %+v, ожидалось %+v", got, want)
	}
}

// Недоступный HEAD — деградация, а не сбой: полный импорт без записи версии (иначе следующая
// волна сочла бы файл уже импортированным и данные остались бы устаревшими). БД при этом не
// трогается вовсе — ни EnsurePublications, ни SELECT версии: отпечатка нет, искать нечего.
func TestPublicationPreflightHEADUnavailable(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	server.Close()

	fingerprint, noop, points, err := PublicationPreflight(context.Background(), &publicationUntouchedConn{},
		publicationTestSource, url, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("недоступный HEAD — не ошибка, а деградация к полному импорту, получено %v", err)
	}
	if fingerprint != "" || noop || points != 0 {
		t.Errorf("получено (%q, %t, %d), ожидалось (\"\", false, 0): без отпечатка версия не пишется", fingerprint, noop, points)
	}
}

// Версия не найдена (sql.ErrNoRows) — штатное «файл новый»: полный импорт, отпечаток возвращается
// непустым, чтобы шаблон записал версию после точек. Запрос счёта не выполняется — версия новая,
// сверять её не с чем.
func TestPublicationPreflightVersionUnknown(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	headFingerprint := publicationFingerprint(t, url)

	conn := &publicationFakeConn{} // очередь пуста: SELECT версии получает sql.ErrNoRows
	fingerprint, noop, points, err := PublicationPreflight(context.Background(), conn,
		publicationTestSource, url, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("PublicationPreflight: %v", err)
	}
	if noop {
		t.Error("noop = true при неизвестной версии: файл обязан импортироваться полностью")
	}
	if points != 0 {
		t.Errorf("points = %d при полном импорте, ожидался 0 (число точек даст импорт)", points)
	}
	if fingerprint != headFingerprint {
		t.Errorf("отпечаток %q, ожидался %q (md5 от basename URL и заголовков HEAD)", fingerprint, headFingerprint)
	}
	publicationRequireMd5Hex(t, fingerprint)
	if len(conn.execQueries) != 1 || conn.execQueries[0] != PublicationsDdl {
		t.Fatalf("Exec-запросы %v, ожидался один CREATE TABLE %s: без таблицы SELECT версии упал бы", conn.execQueries, PublicationsTable)
	}
	if len(conn.rowQueries) != 1 {
		t.Fatalf("запросов QueryRow %d (%v), ожидался один SELECT версии", len(conn.rowQueries), conn.rowQueries)
	}
	if conn.rowQueries[0] != publicationsSelectPoints {
		t.Errorf("запрос версии %q, ожидался %q", conn.rowQueries[0], publicationsSelectPoints)
	}
	wantArgs := []any{publicationTestSource, "full.xlsx", headFingerprint}
	if !reflect.DeepEqual(conn.rowArgs[0], wantArgs) {
		t.Errorf("аргументы запроса версии %#v, ожидались %#v (source, name=basename URL, fingerprint)", conn.rowArgs[0], wantArgs)
	}
}

// Знакомая версия и то же число точек — no-op: файл не скачивается, в БД не пишется ничего
// (никаких батчей), а прогон возвращает проверенное число точек.
func TestPublicationPreflightNoop(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	headFingerprint := publicationFingerprint(t, url)

	const recorded = uint64(42)
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: recorded}, // source_publications: версия записала 42 точки
		&publicationFakeRow{points: recorded}, // count() в таблице: те же 42 (один запрос импортёра, §3.2)
	}}
	fingerprint, noop, points, err := PublicationPreflight(context.Background(), conn,
		publicationTestSource, url, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("PublicationPreflight: %v", err)
	}
	if !noop {
		t.Error("noop = false при известной версии и совпавшем числе точек")
	}
	if points != recorded {
		t.Errorf("points = %d, ожидалось %d (число из source_publications)", points, recorded)
	}
	if fingerprint != headFingerprint {
		t.Errorf("отпечаток %q, ожидался %q", fingerprint, headFingerprint)
	}
	if len(conn.rowQueries) != 2 {
		t.Fatalf("запросов QueryRow %d (%v), ожидались два: SELECT версии и count точек", len(conn.rowQueries), conn.rowQueries)
	}
	if conn.rowQueries[1] != publicationTestCountPoints {
		t.Errorf("запрос счёта %q, ожидался объявленный импортёром %q", conn.rowQueries[1], publicationTestCountPoints)
	}
	if len(conn.batchQueries) != 0 {
		t.Errorf("no-op открыл батчи %v: известная версия не перезаписывается", conn.batchQueries)
	}
}

// Версия известна, а точек в таблице меньше (ручная чистка, недоехавший батч) — self-heal полным
// импортом, а не тихий no-op. Отпечаток возвращается: после успешного импорта шаблон перезапишет
// версию, и следующий прогон снова сможет стать no-op.
func TestPublicationPreflightSelfHeal(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	headFingerprint := publicationFingerprint(t, url)

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 42},
		&publicationFakeRow{points: 41},
	}}
	fingerprint, noop, points, err := PublicationPreflight(context.Background(), conn,
		publicationTestSource, url, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("расхождение числа точек — не ошибка прогона, а self-heal: %v", err)
	}
	if noop {
		t.Error("noop = true при расхождении числа точек: механизм молча простоял бы с неполными данными")
	}
	if points != 0 {
		t.Errorf("points = %d при полном импорте, ожидался 0 (число точек даст импорт)", points)
	}
	if fingerprint != headFingerprint {
		t.Errorf("отпечаток %q, ожидался %q", fingerprint, headFingerprint)
	}
}

// Сбой SELECT версии не роняет ежедневный прогон: warn и полный импорт. Отпечаток при этом
// остаётся непустым — HEAD-то удался, — и шаблон запишет версию после импорта, так что следующая
// волна увидит свежую запись, а не старую.
func TestPublicationPreflightKnownCheckError(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	headFingerprint := publicationFingerprint(t, url)

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: fmt.Errorf("соединение потеряно")},
	}}
	fingerprint, noop, points, err := PublicationPreflight(context.Background(), conn,
		publicationTestSource, url, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("сбой проверки версии — деградация к полному импорту, а не ошибка прогона: %v", err)
	}
	if noop {
		t.Error("noop = true при сбое проверки версии: файл обязан импортироваться полностью")
	}
	if points != 0 {
		t.Errorf("points = %d, ожидался 0", points)
	}
	if fingerprint != headFingerprint {
		t.Errorf("отпечаток %q при сбое проверки версии, ожидался %q: без него версия не запишется", fingerprint, headFingerprint)
	}
}

// CountPoints — единственное место, где выполняется объявленный импортёром запрос: им проверяется
// известная версия и им же заполняется points новой записи (спека §3.2), поэтому ошибка Scan
// обязана дойти до вызывающего, а не превратиться в молчаливый ноль точек.
func TestCountPoints(t *testing.T) {
	conn := &publicationFakeConn{rows: []driver.Row{&publicationFakeRow{points: 7}}}
	points, err := CountPoints(context.Background(), conn, publicationTestCountPoints)
	if err != nil {
		t.Fatalf("CountPoints: %v", err)
	}
	if points != 7 {
		t.Errorf("points = %d, ожидалось 7 (значение запроса импортёра)", points)
	}
	if len(conn.rowQueries) != 1 || conn.rowQueries[0] != publicationTestCountPoints {
		t.Errorf("запросы %v, ожидался объявленный импортёром %q", conn.rowQueries, publicationTestCountPoints)
	}

	broken := &publicationFakeConn{rows: []driver.Row{&publicationFakeRow{err: fmt.Errorf("нет колонки points")}}}
	points, err = CountPoints(context.Background(), broken, publicationTestCountPoints)
	if err == nil {
		t.Fatal("ошибка Scan обязана вернуться вызывающему: ноль превратил бы чек в self-heal на каждом прогоне")
	}
	if !strings.Contains(err.Error(), "нет колонки points") {
		t.Errorf("ошибка %v не сохранила причину", err)
	}
	if points != 0 {
		t.Errorf("points = %d при ошибке, ожидался 0", points)
	}
}

// Порядок полного пути (спека §3.1): DDL → BeforeImport → префлайт → точки → AfterImport →
// версия. Версия последней, потому что она утверждает «эти точки записаны и опубликованы»
// (ARCHITECTURE §8 п. 13): сбой AfterImport обязан оставить её незаписанной, иначе следующий
// прогон ушёл бы в no-op с устаревшим каталогом/витриной у агента.
func TestClickHouseImportPublicationOrder(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	headFingerprint := publicationFingerprint(t, url)

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows}, // версия не записана — полный путь
		&publicationFakeRow{points: 7},          // число точек после отправки батча — points версии
	}}
	importer := &ClickHouseImport{
		TableName:              "macro_series",
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                url,
		PublicationSource:      publicationTestSource,
		PublicationCountPoints: publicationTestCountPoints,
	}
	importer.BeforeImport = func(context.Context, driver.Conn) error {
		conn.calls = append(conn.calls, "BeforeImport")
		return nil
	}
	importer.CrawFunc = func(crawUrl string, _ driver.Conn) (int64, error) {
		conn.calls = append(conn.calls, "CrawFunc")
		if crawUrl != url {
			t.Errorf("CrawFunc получил %q, ожидался DataUrl %q", crawUrl, url)
		}
		return 5, nil
	}
	importer.AfterImport = func(context.Context, driver.Conn) error {
		conn.calls = append(conn.calls, "AfterImport")
		return nil
	}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, ожидалось 5 (результат CrawFunc)", count)
	}
	// Два Exec: DDL шаблона и CREATE TABLE общей таблицы версий внутри префлайта.
	wantCalls := []string{"Exec", "BeforeImport", "Exec", "QueryRow", "CrawFunc", "AfterImport", "QueryRow", "PrepareBatch", "Send"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
	if len(conn.batchQueries) != 1 || conn.batchQueries[0] != "INSERT INTO "+PublicationsTable {
		t.Fatalf("батчи %v, ожидался один INSERT INTO %s", conn.batchQueries, PublicationsTable)
	}
	batch := conn.batches[0]
	if len(batch.appends) != 1 {
		t.Fatalf("строк в батче версии %d, ожидалась одна", len(batch.appends))
	}
	row := batch.appends[0]
	if len(row) != 6 {
		t.Fatalf("значений в строке версии %d, ожидалось 6 (порядок колонок DDL): %#v", len(row), row)
	}
	if row[0] != publicationTestSource || row[1] != "full.xlsx" || row[2] != headFingerprint ||
		row[4] != uint64(7) || row[5] != url {
		t.Errorf("строка версии %#v: ожидались source=%q, name=full.xlsx (basename DataUrl), fingerprint=%q, points=7, url=%q",
			row, publicationTestSource, headFingerprint, url)
	}
	if seen, ok := row[3].(time.Time); !ok || time.Since(seen) > time.Minute {
		t.Errorf("first_seen = %#v, ожидалось время этого прогона", row[3])
	}
}

// No-op известной версии в шаблоне: возвращается проверенное число точек, файл не скачивается
// (CrawFunc молчит), производные не публикуются повторно и батчей нет вовсе.
func TestClickHouseImportPublicationNoopSkipsCrawl(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 42},
		&publicationFakeRow{points: 42},
	}}
	importer := &ClickHouseImport{
		TableName:              "macro_series",
		DataUrl:                url,
		PublicationSource:      publicationTestSource,
		PublicationCountPoints: publicationTestCountPoints,
	}
	importer.CrawFunc = func(string, driver.Conn) (int64, error) {
		t.Error("CrawFunc вызван на no-op прогоне: файл скачивать нечего")
		return 0, nil
	}
	importer.AfterImport = func(context.Context, driver.Conn) error {
		t.Error("AfterImport вызван на no-op прогоне: производные опубликованы прошлым импортом")
		return nil
	}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 42 {
		t.Errorf("count = %d, ожидалось 42 (число точек известной версии)", count)
	}
	if len(conn.batchQueries) != 0 {
		t.Errorf("no-op писал в БД: батчи %v", conn.batchQueries)
	}
	if len(conn.rowQueries) != 2 {
		t.Errorf("запросов %d (%v): после no-op новых обращений к БД быть не должно", len(conn.rowQueries), conn.rowQueries)
	}
}

// Импортёр без PublicationCountPoints (все остальные пакеты репозитория): цикл Import не
// меняется — ни HEAD по DataUrl, ни обращений к source_publications. Сервер ловит любой запрос:
// префлайт, включившийся «на всякий случай», стоил бы лишнего сетевого вызова каждому импортёру.
func TestClickHouseImportWithoutPublicationFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("HEAD по DataUrl без PublicationCountPoints: префлайт обязан быть выключен")
	}))
	t.Cleanup(server.Close)

	conn := &publicationFakeConn{}
	importer := &ClickHouseImport{
		TableName:   "cbr_m2",
		CreateTable: []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:     server.URL + "/monetary_agg_SA.xlsx",
		CrawFunc: func(crawUrl string, _ driver.Conn) (int64, error) {
			conn.calls = append(conn.calls, "CrawFunc")
			if crawUrl != server.URL+"/monetary_agg_SA.xlsx" {
				t.Errorf("CrawFunc получил %q, ожидался DataUrl как есть", crawUrl)
			}
			return 3, nil
		},
	}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, ожидалось 3 (результат CrawFunc)", count)
	}
	if !reflect.DeepEqual(conn.calls, []string{"Exec", "CrawFunc"}) {
		t.Errorf("порядок обращений %v, ожидались только DDL шаблона и CrawFunc", conn.calls)
	}
	if len(conn.rowQueries) != 0 || len(conn.batchQueries) != 0 {
		t.Errorf("импортёр без префлайта обратился к source_publications: %v / %v", conn.rowQueries, conn.batchQueries)
	}
}

// Сбой AfterImport отменяет запись версии (спека §3.1, Review Focus #1): AfterImport вызывается
// после Send точек, поэтому точки уже записаны, а строки версии нет — и следующий прогон повторит
// полный путь (идемпотентный), а не уйдёт в no-op с отсутствующим у агента каталогом или витриной.
// Проверяется именно отсутствие INSERT в source_publications, а не только возвращённая ошибка.
func TestClickHouseImportAfterImportErrorKeepsPublicationUnwritten(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows}, // версия не записана — полный путь
		&publicationFakeRow{points: 7},          // остаётся невостребованной: до записи версии дело не дойдёт
	}}
	afterErr := fmt.Errorf("витрина v_cbr_macro не создана")
	importer := &ClickHouseImport{
		TableName:              "macro_series",
		DataUrl:                url,
		PublicationSource:      publicationTestSource,
		PublicationCountPoints: publicationTestCountPoints,
	}
	importer.CrawFunc = func(string, driver.Conn) (int64, error) {
		conn.calls = append(conn.calls, "CrawFunc")
		return 5, nil
	}
	importer.AfterImport = func(context.Context, driver.Conn) error {
		conn.calls = append(conn.calls, "AfterImport")
		return afterErr
	}

	count, err := importer.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("сбой AfterImport обязан вернуться вызывающему: иначе прогон счёл бы производные опубликованными")
	}
	if !strings.Contains(err.Error(), afterErr.Error()) {
		t.Errorf("ошибка %v не сохранила причину сбоя AfterImport", err)
	}
	if count != 5 {
		t.Errorf("count = %d, ожидалось 5: точки отправлены до AfterImport, сбой случился позже", count)
	}
	if len(conn.batchQueries) != 0 || len(conn.batches) != 0 {
		t.Errorf("сбой AfterImport записал версию: батчи %v", conn.batchQueries)
	}
	if len(conn.rowQueries) != 1 {
		t.Errorf("запросов QueryRow %d (%v), ожидался только SELECT версии: число точек для версии после сбоя не запрашивается", len(conn.rowQueries), conn.rowQueries)
	}
	wantCalls := []string{"Exec", "QueryRow", "CrawFunc", "AfterImport"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
}

// Сбой запроса числа точек на записи версии — ошибка прогона (точки уже отправлены, но строка
// версии без points невозможна), и версия при этом тоже не пишется: незаписанная версия — это
// повтор полного пути на следующем прогоне, а не расхождение points с содержимым таблицы.
func TestClickHouseImportCountPointsErrorKeepsPublicationUnwritten(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	url := server.URL + "/full.xlsx"

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows},                    // версия не записана — полный путь
		&publicationFakeRow{err: fmt.Errorf("нет колонки points")}, // запрос числа точек после Send
	}}
	importer := &ClickHouseImport{
		TableName:              "macro_series",
		DataUrl:                url,
		PublicationSource:      publicationTestSource,
		PublicationCountPoints: publicationTestCountPoints,
	}
	importer.CrawFunc = func(string, driver.Conn) (int64, error) {
		conn.calls = append(conn.calls, "CrawFunc")
		return 5, nil
	}
	importer.AfterImport = func(context.Context, driver.Conn) error {
		conn.calls = append(conn.calls, "AfterImport")
		return nil
	}

	count, err := importer.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("сбой запроса числа точек обязан вернуться вызывающему: без points строку версии не собрать")
	}
	for _, want := range []string{publicationTestSource, "count точек", "нет колонки points"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if count != 5 {
		t.Errorf("count = %d, ожидалось 5: точки отправлены до запроса числа точек", count)
	}
	if len(conn.batchQueries) != 0 || len(conn.batches) != 0 {
		t.Errorf("при сбое запроса числа точек версия не пишется, а батчи %v", conn.batchQueries)
	}
	wantCalls := []string{"Exec", "QueryRow", "CrawFunc", "AfterImport", "QueryRow"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
}
