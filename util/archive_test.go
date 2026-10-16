package util

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	"github.com/xuri/excelize/v2"
)

// archiveTestSource — ключ версии тестового импортёра: как у cbr_m2, чей файл качает сам шаблон
// (ImportFunc) — именно такие импортёры охватывает архивация (спека §1.2). Хранилище подставлено
// хук-переменной, поэтому ни одного сетевого вызова в тестах нет.
const (
	archiveTestSource      = "cbr_m2"
	archiveTestName        = "full.xlsx"
	archiveTestCountPoints = `SELECT count() FROM cbr_m2 FINAL`
)

// archiveTestServer — источник, каким его видит шаблон: HEAD отдаёт версионные заголовки
// (префлайт), GET — саму книгу (путь ImportFunc). Заголовки стабильны, тело детерминировано,
// поэтому отпечаток версии, ключ архива и sha256 содержимого сходятся между прогонами.
// GET-ы считаются: этим числом проверяется, что no-op известной версии файл не скачивает вовсе,
// а самолечение скачивает ровно раз (спека §3.3).
type archiveTestServer struct {
	server *httptest.Server
	url    string
	body   []byte
	gets   atomic.Int64
}

func newArchiveTestServer(t *testing.T) *archiveTestServer {
	t.Helper()
	return newArchiveTestServerHead(t, http.StatusOK)
}

// newArchiveTestServerBrokenHead — тот же источник с падающим HEAD (WAF, 500, сеть): отпечатка нет,
// а файл есть. Так выглядит деградация префлайта: версию записать нельзя, и архив версии остаётся
// единственным следом прогона — ради этого случая у ключа и есть sha-фолбэк (спека §3.2).
func newArchiveTestServerBrokenHead(t *testing.T) *archiveTestServer {
	t.Helper()
	return newArchiveTestServerHead(t, http.StatusInternalServerError)
}

func newArchiveTestServerHead(t *testing.T, headStatus int) *archiveTestServer {
	t.Helper()
	s := &archiveTestServer{body: archiveTestBook(t)}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent: конвенция GetFile/HeadFingerprint — randomUA")
		}
		// Content-Length версии — длина тела: у источника он приходит заголовком HEAD, а GET
		// отдаёт тот же файл, поэтому отпечаток и содержимое не расходятся.
		w.Header().Set("Content-Length", strconv.Itoa(len(s.body)))
		switch r.Method {
		case http.MethodHead:
			if headStatus != http.StatusOK {
				w.WriteHeader(headStatus)
				return
			}
			w.Header().Set("ETag", `"archive-0001"`)
			w.Header().Set("Last-Modified", "Tue, 13 Oct 2026 09:00:00 GMT")
		case http.MethodGet:
			s.gets.Add(1)
			_, _ = w.Write(s.body)
		default:
			t.Errorf("метод %s, ожидался HEAD (префлайт) или GET (книга)", r.Method)
		}
	}))
	t.Cleanup(s.server.Close)
	s.url = s.server.URL + "/" + archiveTestName
	return s
}

// archiveTestBook — минимальная валидная книга: ImportFunc её не разбирает, но шаблон обязан
// открыть XLSX из памяти (excelize.OpenReader), иначе архивации не будет по причине сбоя разбора.
func archiveTestBook(t *testing.T) []byte {
	t.Helper()
	book := excelize.NewFile()
	if err := book.SetCellValue("Sheet1", "A1", "точка"); err != nil {
		t.Fatalf("SetCellValue: %v", err)
	}
	buf, err := book.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	if err = book.Close(); err != nil {
		t.Fatalf("Close книги: %v", err)
	}
	return buf.Bytes()
}

// archiveTestRuntime — хранилище теста: LocalStore во временном каталоге, подставленный
// хук-переменной (у прода её нет — там ленивый FromEnv по окружению). t.Cleanup снимает
// подстановку: иначе она утекла бы в соседние тесты пакета.
func archiveTestRuntime(t *testing.T) (*archiveRuntime, string) {
	t.Helper()
	dir := t.TempDir()
	store := archive.NewLocal(dir)
	rt := &archiveRuntime{store: store, registry: archive.NewRegistry(store)}
	archiveRuntimeForTests = rt
	t.Cleanup(func() { archiveRuntimeForTests = nil })
	return rt, dir
}

// archiveTestImporter — прогон шаблона по пути ImportFunc: ImportFunc пишет одну строку батчем,
// after вызывается из AfterImport (считает, дошли ли производные). conn — фейк публикаций
// с очередью ответов QueryRow.
func archiveTestImporter(t *testing.T, url string, after func()) *ClickHouseImport {
	t.Helper()
	importer := &ClickHouseImport{
		TableName:              archiveTestSource,
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                url,
		PublicationCountPoints: archiveTestCountPoints,
	}
	importer.ImportFunc = func(_ *excelize.File, batch driver.Batch) error {
		return batch.Append("точка")
	}
	if after != nil {
		importer.AfterImport = func(context.Context, driver.Conn) error {
			after()
			return nil
		}
	}
	return importer
}

// archiveTestKey — ключ объекта версии, как его называет спека §3.2: «источник/первые 16
// символов отпечатка_имя». Считается здесь по литералу, а не вызовом archive.Key: расхождение
// схемы ключа с документом должен ловить тест, а не совпадать с ним по построению.
func archiveTestKey(fingerprint string) string {
	return archiveTestSource + "/" + fingerprint[:16] + "_" + archiveTestName
}

// archiveTestRegistryEntry — строка реестра с этим ключом. Реестр читается глазами (тем же
// JSONL-файлом, что уезжает в git-зеркало), а не через состояние Registry: строка в хранилище —
// источник истины для будущего восстановления (спека §3.4).
func archiveTestRegistryEntry(t *testing.T, rt *archiveRuntime, key string) archive.Entry {
	t.Helper()
	raw, err := rt.store.Get(context.Background(), archive.RegistryKey)
	if err != nil {
		t.Fatalf("реестр %s: %v", archive.RegistryKey, err)
	}
	var found []archive.Entry
	lines := 0
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		lines++
		var e archive.Entry
		if err = json.Unmarshal(line, &e); err != nil {
			t.Fatalf("строка реестра %q: %v", line, err)
		}
		if e.Key == key {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("строк с ключом %s — %d (всего строк %d): версия должна быть записана ровно один раз", key, len(found), lines)
	}
	return found[0]
}

// archiveTestEntryMatchesContent — инвариант «строка реестра описывает записанные байты»: sha256 и
// size сверяются с фактическим содержимым объекта, а не с тем, что save собирался записать (спека
// §3.2, §3.3). Общий для тестов полного пути и перезаписи устаревшего объекта.
func archiveTestEntryMatchesContent(t *testing.T, entry archive.Entry, body []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); entry.SHA256 != want {
		t.Errorf("sha256 в реестре %q, ожидался %q (содержимое объекта)", entry.SHA256, want)
	}
	if entry.Size != int64(len(body)) {
		t.Errorf("size в реестре %d, ожидался %d", entry.Size, len(body))
	}
}

// archiveUntouchedStore — хранилище, которого префлайт CrawFunc-импортёра касаться не должен:
// такой импортёр архивом не покрыт (файл качает он сам, шаблон его байтов не видит, а запись архива
// живёт в ветке ImportFunc), поэтому любое обращение здесь — ошибка логики, а не данных. Отсюда
// паника, а не учёт вызовов (образец — publicationUntouchedConn из publications_test.go); список
// вызовов всё равно ведётся: зелёный прогон обязан утверждать «ноль обращений» явно, а не только
// отсутствием паники.
type archiveUntouchedStore struct {
	archive.Store
	calls []string
}

func (s *archiveUntouchedStore) Name() string {
	s.calls = append(s.calls, "Name")
	panic("префлайт CrawFunc-импортёра обратился к архиву: Name")
}

func (s *archiveUntouchedStore) Put(context.Context, string, []byte) error {
	s.calls = append(s.calls, "Put")
	panic("префлайт CrawFunc-импортёра обратился к архиву: Put")
}

func (s *archiveUntouchedStore) Get(context.Context, string) ([]byte, error) {
	s.calls = append(s.calls, "Get")
	panic("префлайт CrawFunc-импортёра обратился к архиву: Get")
}

func (s *archiveUntouchedStore) Exists(context.Context, string) (bool, error) {
	s.calls = append(s.calls, "Exists")
	panic("префлайт CrawFunc-импортёра обратился к архиву: Exists")
}

// CrawFunc-импортёр с точкой контроля (bank/sber_fin_rez.go: sber_rpbu) архивом не покрыт, и
// проверка «версия в архиве» на его no-op-пути была бы вечной деградацией: объекта в архиве нет и
// не будет, поэтому no-op не наступил бы ни разу, пока ARCHIVE_BACKEND настроен, — ежедневный
// полный краул и warn в логе у импортёра, который включение архива менять не должно (спека §3.3:
// CrawFunc не трогается). Гейт `s.ImportFunc != nil` в шаблоне держится ровно этим тестом:
// хранилище паникует на любое обращение.
func TestClickHouseImportArchiveSkippedForCrawFunc(t *testing.T) {
	const recorded = uint64(42)
	store := &archiveUntouchedStore{}
	archiveRuntimeForTests = &archiveRuntime{store: store, registry: archive.NewRegistry(store)}
	t.Cleanup(func() { archiveRuntimeForTests = nil })

	srv := newArchiveTestServer(t)
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: recorded}, // source_publications: версия записала 42 точки
		&publicationFakeRow{points: recorded}, // count() в таблице: те же 42 — совпало
	}}
	importer := &ClickHouseImport{
		TableName:              "sber_rpbu",
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                srv.url,
		PublicationCountPoints: archiveTestCountPoints,
	}
	importer.CrawFunc = func(string, driver.Conn) (int64, error) {
		t.Error("CrawFunc вызван: известная версия у CrawFunc-импортёра обязана остаться no-op")
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
	if count != int64(recorded) {
		t.Errorf("count = %d, ожидалось %d (число точек известной версии)", count, recorded)
	}
	if len(store.calls) != 0 {
		t.Errorf("обращения к архиву %v: CrawFunc-импортёр архивом не покрыт — проверки быть не должно", store.calls)
	}
	if srv.gets.Load() != 0 {
		t.Errorf("GET-запросов %d, ожидалось ноль: no-op файл не скачивает", srv.gets.Load())
	}
	if len(conn.batchQueries) != 0 {
		t.Errorf("no-op писал в БД: батчи %v", conn.batchQueries)
	}
	// DDL шаблона → DDL таблицы версий → SELECT версии → count точек. Ни одного обращения к хранилищу
	// между ними: проверка архива у CrawFunc-импортёра отключена целиком.
	wantCalls := []string{"Exec", "Exec", "QueryRow", "QueryRow"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
}

// archiveTestVersionBatch — батч строки версии в source_publications или nil. Батчи различаются
// запросом, а не порядком: у пути ImportFunc батч точек идёт первым, и перепутать их нельзя.
func archiveTestVersionBatch(conn *publicationFakeConn) *publicationFakeBatch {
	for i, query := range conn.batchQueries {
		if query == "INSERT INTO "+PublicationsTable {
			return conn.batches[i]
		}
	}
	return nil
}

// Rows нужен шаблону на пути ImportFunc (count точек = строк в батче), а у прежних тестов его не
// спрашивали: они шли CrawFunc-путём. Метод дописан здесь, а не в publications_test.go: тот файл —
// замороженный контракт «архив выключен = прежнее поведение», и трогать его ради нового пути
// значит ослаблять проверку. Возвращается число строк батча — как у живого драйвера.
func (b *publicationFakeBatch) Rows() int {
	return len(b.appends)
}

// Полный путь с включённым архивом: версия файла ложится объектом в хранилище и строкой в
// реестр, а затем — как и раньше — строкой в source_publications. Объект обязан быть теми же
// байтами, что ушли в разбор и в отправку: архив отвечает на вопрос «что именно импортировали»
// (спека §1), поэтому его байты и sha256 в реестре пинятся против скачанного файла.
func TestClickHouseImportArchivesVersion(t *testing.T) {
	rt, dir := archiveTestRuntime(t)
	srv := newArchiveTestServer(t)
	fingerprint := publicationFingerprint(t, srv.url)
	key := archiveTestKey(fingerprint)

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows}, // версия неизвестна — полный путь
		&publicationFakeRow{points: 7},          // число точек для строки версии
	}}
	importer := archiveTestImporter(t, srv.url, nil)

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	if srv.gets.Load() != 1 {
		t.Errorf("GET-запросов %d, ожидался один: файл скачивается ровно раз на импорт", srv.gets.Load())
	}
	stored, err := rt.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("объект %s в архиве: %v", key, err)
	}
	if !bytes.Equal(stored, srv.body) {
		t.Errorf("байты объекта %s (%d байт) не совпали с отданным файлом (%d байт)", key, len(stored), len(srv.body))
	}
	if _, err = os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
		t.Errorf("объекта %s нет в каталоге архива: %v", key, err)
	}
	if _, err = os.Stat(filepath.Join(dir, archive.RegistryKey)); err != nil {
		t.Errorf("реестра %s нет в каталоге архива: %v", archive.RegistryKey, err)
	}
	entry := archiveTestRegistryEntry(t, rt, key)
	archiveTestEntryMatchesContent(t, entry, srv.body)
	if entry.Source != archiveTestSource || entry.Name != archiveTestName || entry.Fingerprint != fingerprint ||
		entry.URL != srv.url || entry.Key != key {
		t.Errorf("строка реестра %+v: ожидались source=%q, name=%q, fingerprint=%q, url=%q, key=%q",
			entry, archiveTestSource, archiveTestName, fingerprint, srv.url, key)
	}
	if entry.FirstSeen.IsZero() {
		t.Error("first_seen пуст: реестр обязан фиксировать, когда версия увидена")
	}
	// Строка версии пишется по тому же отпечатку, что и ключ архива: source_publications и архив
	// должны называть версию одинаково (спека §3.2).
	if batch := archiveTestVersionBatch(conn); batch == nil {
		t.Errorf("батчи %v: строки версии в %s нет", conn.batchQueries, PublicationsTable)
	} else if len(batch.appends) != 1 || batch.appends[0][2] != fingerprint {
		t.Errorf("строка версии %#v: ожидался fingerprint %q", batch.appends, fingerprint)
	}
}

// Отпечаток HEAD не изменился, а содержимое файла — да (источник перезаписал выгрузку под теми же
// заголовками), и полный импорт ушёл в self-heal по числу точек. Под ожидаемым ключом уже лежат
// байты прежней версии: объект обязан быть перезаписан. Иначе строка реестра описывала бы
// содержимое, которого в архиве нет («файл + реестр» перестают быть самодостаточными), а будущая
// проверка целостности (шаг 1.2 роадмапа) сверяла бы sha256 строки с чужими байтами объекта.
func TestClickHouseImportArchiveOverwritesStaleObject(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	srv := newArchiveTestServer(t)
	fingerprint := publicationFingerprint(t, srv.url)
	key := archiveTestKey(fingerprint)

	// Состояние прошлого прогона: тот же ключ (заголовки те же), другое тело — пишет тот же save,
	// что и у прода, поэтому предпосылка теста не расходится с кодом.
	stale := []byte("прежняя версия того же файла: источник перезаписал выгрузку под тем же ETag")
	if err := rt.save(context.Background(), archiveTestSource, srv.url, stale, fingerprint); err != nil {
		t.Fatalf("подготовка объекта прежней версии: %v", err)
	}

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 42}, // source_publications: версия записала 42 точки
		&publicationFakeRow{points: 7},  // count() в таблице: 7 — чек не сошёлся, полный импорт
		&publicationFakeRow{points: 7},  // CountPoints записи новой версии
	}}
	importer := archiveTestImporter(t, srv.url, nil)

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	stored, err := rt.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("объект %s в архиве: %v", key, err)
	}
	if !bytes.Equal(stored, srv.body) {
		t.Errorf("объект %s (%d байт) не совпал с отданным файлом (%d байт): запись объекта пропущена", key, len(stored), len(srv.body))
	}
	entry := archiveTestRegistryEntry(t, rt, key)
	archiveTestEntryMatchesContent(t, entry, srv.body)
}

// Версия известна и лежит в архиве — no-op: файл не скачивается (GET-ов ноль), в БД не пишется
// ничего. Проверка архива стоит один Store.Exists и обходится без содержимого файла (спека §3.2).
func TestClickHouseImportArchiveKnownNoop(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	srv := newArchiveTestServer(t)
	fingerprint := publicationFingerprint(t, srv.url)
	// Объект кладётся тем же кодом, что и у прода (save): расхождение ключей save и versionExists
	// тест поймает лишним GET, а не только отсутствием объекта.
	if err := rt.save(context.Background(), archiveTestSource, srv.url, srv.body, fingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 42}, // source_publications: версия записала 42 точки
		&publicationFakeRow{points: 42}, // count() в таблице: те же 42 — совпало
	}}
	importer := archiveTestImporter(t, srv.url, func() {
		t.Error("AfterImport вызван на no-op прогоне: производные опубликованы прошлым импортом")
	})
	importer.ImportFunc = func(*excelize.File, driver.Batch) error {
		t.Error("ImportFunc вызван на no-op прогоне: файл скачивать нечего")
		return nil
	}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 42 {
		t.Errorf("count = %d, ожидалось 42 (число точек известной версии)", count)
	}
	if srv.gets.Load() != 0 {
		t.Errorf("GET-запросов %d, ожидалось ноль: версия в архиве, качать нечего", srv.gets.Load())
	}
	if len(conn.batchQueries) != 0 {
		t.Errorf("no-op писал в БД: батчи %v", conn.batchQueries)
	}
}

// Самолечение: версия известна и точки на месте, а объекта в архиве нет (первое включение
// архива, ручная чистка, потеря строки реестра) — полный импорт вместо no-op, и объект
// восстанавливается. Без этой проверки версия не попала бы в архив никогда: прогон считал бы её
// импортированной (спека §3.2).
func TestClickHouseImportArchiveSelfHeal(t *testing.T) {
	rt, dir := archiveTestRuntime(t)
	srv := newArchiveTestServer(t)
	fingerprint := publicationFingerprint(t, srv.url)
	key := archiveTestKey(fingerprint)
	if err := rt.save(context.Background(), archiveTestSource, srv.url, srv.body, fingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
		t.Fatalf("удаление объекта %s: %v", key, err)
	}

	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 42}, // версия известна, точки сошлись — кандидат в no-op
		&publicationFakeRow{points: 42}, // count() в таблице: те же 42
		&publicationFakeRow{points: 42}, // CountPoints записи восстановленной версии
	}}
	after := 0
	importer := archiveTestImporter(t, srv.url, func() { after++ })

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	if after != 1 {
		t.Errorf("AfterImport вызван %d раз, ожидался один: отсутствие объекта в архиве — полный путь", after)
	}
	if srv.gets.Load() != 1 {
		t.Errorf("GET-запросов %d, ожидался один: самолечение качает файл заново", srv.gets.Load())
	}
	stored, err := rt.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("объект %s не восстановлен: %v", key, err)
	}
	if !bytes.Equal(stored, srv.body) {
		t.Errorf("восстановленный объект %s (%d байт) не совпал с файлом (%d байт)", key, len(stored), len(srv.body))
	}
	if len(conn.rowQueries) != 3 {
		t.Errorf("запросов QueryRow %d (%v), ожидались три: версия, её чек и points перезаписи", len(conn.rowQueries), conn.rowQueries)
	}
	if batch := archiveTestVersionBatch(conn); batch == nil {
		t.Errorf("батчи %v: версия не перезаписана после самолечения", conn.batchQueries)
	} else if len(batch.appends) != 1 || batch.appends[0][2] != fingerprint {
		t.Errorf("строка версии %#v: ожидался fingerprint %q", batch.appends, fingerprint)
	}
}

// archiveFailingPutStore — хранилище, у которого падает запись объекта: сбой архива как он есть
// у дежурного (Диск недоступен, токен отозван, квота). Exists и Get делегируются настоящему
// LocalStore — падать должна именно запись.
type archiveFailingPutStore struct {
	archive.Store
	putErr error
}

func (s archiveFailingPutStore) Put(context.Context, string, []byte) error { return s.putErr }

// Сбой архива не роняет прогон (данные важнее, спека §3.1), но и не оставляет версию
// «импортированной»: без строки в source_publications следующий прогон повторит полный путь и
// допишет архив, а дыры «версия считается импортированной, а в архиве её нет» не возникает.
// Производные при этом публикуются: их сбой отменил бы и данные, уже отправленные батчем.
func TestClickHouseImportArchiveFailureKeepsVersionUnwritten(t *testing.T) {
	dir := t.TempDir()
	store := archiveFailingPutStore{
		Store:  archive.NewLocal(dir),
		putErr: errors.New("хранилище недоступно"),
	}
	archiveRuntimeForTests = &archiveRuntime{store: store, registry: archive.NewRegistry(store)}
	t.Cleanup(func() { archiveRuntimeForTests = nil })

	srv := newArchiveTestServer(t)
	fingerprint := publicationFingerprint(t, srv.url)
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows}, // версия не записана — полный путь
		&publicationFakeRow{points: 7},          // остаётся невостребованной: до записи версии дело не дойдёт
	}}
	after := 0
	importer := archiveTestImporter(t, srv.url, func() { after++ })

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("сбой архива — не сбой прогона, получено %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	if after != 1 {
		t.Errorf("AfterImport вызван %d раз, ожидался один: сбой архива производные не отменяет", after)
	}
	if len(conn.batchQueries) != 1 || conn.batchQueries[0] != "INSERT INTO "+archiveTestSource {
		t.Errorf("батчи %v, ожидался только батч точек: при сбое архива строка версии не пишется", conn.batchQueries)
	}
	if len(conn.rowQueries) != 1 {
		t.Errorf("запросов QueryRow %d (%v), ожидался только SELECT версии: points неписанной версии не спрашиваются", len(conn.rowQueries), conn.rowQueries)
	}
	if batch := archiveTestVersionBatch(conn); batch != nil {
		t.Errorf("версия записана (%#v) при сбое архива: строка утверждала бы импорт, которого нет", batch.appends)
	}
	if _, err = os.Stat(filepath.Join(dir, filepath.FromSlash(archiveTestKey(fingerprint)))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("объект версии в архиве: %v, ожидалось отсутствие (запись падает)", err)
	}
}

// Архив выключен (пустой ARCHIVE_BACKEND): поведение ровно прежнее — файл скачивается один раз,
// порядок обращений к БД не меняется, никаких обращений к хранилищу. Это контракт Review Focus
// №6: включение механизма не должно ничего стоить импортёрам, у которых архива нет.
func TestClickHouseImportArchiveOffNoCalls(t *testing.T) {
	archiveRuntimeForTests = nil // хук снят: идёт прод-путь activeArchive()
	// Синглтон прод-пути кэширует хранилище: без сброса тест мог бы взять результат соседнего
	// теста, уже прошедшего через activeArchive() с включённым бэкендом, и проверял бы не то.
	prevOnce, prevRT := archiveOnce, archiveRT
	archiveOnce, archiveRT = sync.Once{}, nil
	t.Cleanup(func() { archiveOnce, archiveRT = prevOnce, prevRT })
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(archive.EnvLocalDir, "")

	srv := newArchiveTestServer(t)
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{err: sql.ErrNoRows}, // версия неизвестна — полный путь
		&publicationFakeRow{points: 7},
	}}
	importer := archiveTestImporter(t, srv.url, nil)

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	if srv.gets.Load() != 1 {
		t.Errorf("GET-запросов %d, ожидался один: без архива файл качается по-прежнему раз", srv.gets.Load())
	}
	// DDL → DDL таблицы версий → префлайт → батч точек → Send → points версии → строка версии.
	wantCalls := []string{"Exec", "Exec", "QueryRow", "PrepareBatch", "Send", "QueryRow", "PrepareBatch", "Send"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
	if archiveTestVersionBatch(conn) == nil {
		t.Errorf("батчи %v: строка версии не записана — без архива путь обязан остаться прежним", conn.batchQueries)
	}
}

// HEAD недоступен, а файл есть: отпечатка нет — версию записать нельзя, и строка в
// source_publications не пишется (как и до архивации). Архив при этом обязан сохранить версию по
// sha-ключу содержимого: иначе именно тот случай, когда версия особенно нужна (заголовков версии
// у источника нет), остался бы без следа — а ключ по отпечатку схлопнул бы разные версии одного
// файла в один объект (спека §3.2).
func TestClickHouseImportArchiveWithoutFingerprint(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	srv := newArchiveTestServerBrokenHead(t)
	sum := sha256.Sum256(srv.body)
	key := archiveTestSource + "/sha-" + hex.EncodeToString(sum[:])[:16] + "_" + archiveTestName

	conn := &publicationFakeConn{}
	importer := archiveTestImporter(t, srv.url, nil)

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка точек", count)
	}
	if srv.gets.Load() != 1 {
		t.Errorf("GET-запросов %d, ожидался один: недоступный HEAD не отменяет импорт", srv.gets.Load())
	}
	stored, err := rt.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("объект %s в архиве: %v", key, err)
	}
	if !bytes.Equal(stored, srv.body) {
		t.Errorf("байты объекта %s (%d байт) не совпали с отданным файлом (%d байт)", key, len(stored), len(srv.body))
	}
	entry := archiveTestRegistryEntry(t, rt, key)
	if entry.Fingerprint != "" {
		t.Errorf("fingerprint в реестре %q, ожидался пустым: HEAD не удался", entry.Fingerprint)
	}
	if want := hex.EncodeToString(sum[:]); entry.SHA256 != want || entry.Size != int64(len(srv.body)) {
		t.Errorf("строка реестра %+v: ожидались sha256=%q, size=%d", entry, want, len(srv.body))
	}
	// Ни одной строки версии: без отпечатка её запись утверждала бы версию, которой префлайт не
	// видел, — и следующий прогон считал бы файл уже импортированным (спека §3.1).
	if len(conn.batchQueries) != 1 || conn.batchQueries[0] != "INSERT INTO "+archiveTestSource {
		t.Errorf("батчи %v, ожидался только батч точек: без отпечатка строка версии не пишется", conn.batchQueries)
	}
	if len(conn.rowQueries) != 0 {
		t.Errorf("запросов QueryRow %d (%v), ожидалось ноль: без отпечатка префлайт в БД не ходит", len(conn.rowQueries), conn.rowQueries)
	}
}
