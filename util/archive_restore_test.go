package util

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	"github.com/xuri/excelize/v2"
)

// restoreTestURL — заведомо недоступный адрес источника: на порту 1 не слушает никто, и любой
// сетевой вызов (HEAD-префлайт или GET) падал бы отказом соединения. Прогон восстановления обязан
// пройти с таким DataUrl целиком — это и есть проверка «restore не ходит в сеть».
const (
	restoreTestURL = "https://127.0.0.1:1/x"
	// restoreTestFingerprint — отпечаток версии в строке реестра: при восстановлении он не считается
	// (HEAD не делается), а читается из строки — ровно его и подставляет тест при save. Литерал и
	// 32 символа, как у настоящего md5-отпечатка: им проверяется и запись строки версии в
	// source_publications, и префикс ключа объекта.
	restoreTestFingerprint = "abababababababababababababababab"
)

// restoreTestKey — ключ объекта восстановления: «<источник>/<префикс отпечатка>_<имя>», имя — это
// basename недоступного URL («x»). Литералом, а не вызовом archive.Key: расхождение схемы ключа с
// документом должен ловить тест, а не совпадать с кодом по построению.
func restoreTestKey() string {
	return archiveTestSource + "/" + restoreTestFingerprint[:16] + "_x"
}

// Режим читается в момент вызова, а не в init(): тест подставляет значение через t.Setenv уже
// после старта процесса (ARCHITECTURE §8.6 — конфигурация и сеть в init() запрещены).
func TestArchiveRestoreModeReadsEnvAtCallTime(t *testing.T) {
	t.Setenv(EnvArchiveAction, "")
	if ArchiveRestoreMode() {
		t.Error("пустое значение включило режим восстановления")
	}
	t.Setenv(EnvArchiveAction, ArchiveActionRegistry)
	if ArchiveRestoreMode() {
		t.Errorf("%s=%s включил режим восстановления", EnvArchiveAction, ArchiveActionRegistry)
	}
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)
	if !ArchiveRestoreMode() {
		t.Errorf("%s=%s не включил режим восстановления", EnvArchiveAction, ArchiveActionRestore)
	}
}

// Успех восстановления: байты версии и её строка реестра приходят из хранилища (сеть не нужна —
// версия уже лежит в архиве), sha256 объекта сходится со строкой, то есть пара «строка ↔ объект»
// самодостаточна и версию можно импортировать.
func TestRestoreVersionSuccess(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	ctx := context.Background()
	body := archiveTestBook(t)
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, entry, err := RestoreVersion(ctx, archiveTestSource, restoreTestURL)
	if err != nil {
		t.Fatalf("RestoreVersion: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("RestoreVersion вернула %d байт, ожидались байты архива (%d байт)", len(got), len(body))
	}
	if entry.Key != restoreTestKey() {
		t.Errorf("ключ версии %q, ожидался %q", entry.Key, restoreTestKey())
	}
	if entry.Fingerprint != restoreTestFingerprint {
		t.Errorf("отпечаток версии %q, ожидался %q: строку версии в source_publications шаблон берёт из реестра", entry.Fingerprint, restoreTestFingerprint)
	}
	sum := sha256.Sum256(body)
	if entry.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("sha256 строки %q не описал архивные байты", entry.SHA256)
	}
}

// Версии под запрошенным URL в архиве нет — ошибка с URL (fail-fast): восстановление не должно ни
// молча импортировать чужую версию, ни возвращать пустые байты, которые разбор принял бы за файл.
func TestRestoreVersionMissing(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	ctx := context.Background()
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, []byte("тело"), restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	const missing = "https://127.0.0.1:1/нет-такого.htm"
	_, entry, err := RestoreVersion(ctx, archiveTestSource, missing)
	if err == nil {
		t.Fatal("RestoreVersion для отсутствующей версии не дала ошибки")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("в ошибке нет URL: %v", err)
	}
	if entry.Key != "" {
		t.Errorf("вместе с ошибкой вернулась версия %q", entry.Key)
	}
}

// Битый sha256 — ошибка: тело объекта подменено (или объект усечён), и реестр с объектом перестали
// быть самодостаточной парой. Молчаливое восстановление записало бы чужое содержимое под именем
// версии реестра — ровно то, от чего проверка целостности и стоит.
func TestRestoreVersionBrokenSHA(t *testing.T) {
	rt, dir := archiveTestRuntime(t)
	ctx := context.Background()
	body := archiveTestBook(t)
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}
	path := filepath.Join(dir, filepath.FromSlash(restoreTestKey()))
	if err := os.WriteFile(path, []byte("подменённое тело"), 0o644); err != nil {
		t.Fatalf("подмена объекта %s: %v", path, err)
	}

	_, _, err := RestoreVersion(ctx, archiveTestSource, restoreTestURL)
	if err == nil {
		t.Fatal("RestoreVersion с подменённым телом не дала ошибки")
	}
	if !strings.Contains(err.Error(), "sha256") {
		t.Errorf("ошибка не называет расхождение sha256: %v", err)
	}
}

// Архив не настроен (пустой ARCHIVE_BACKEND) — ошибка с именем переменной: восстановление без
// архива невозможно, и это отказ, а не тихий полный импорт с сетью.
func TestRestoreVersionNoArchive(t *testing.T) {
	prevRT := archiveRuntimeForTests
	archiveRuntimeForTests = nil
	t.Cleanup(func() { archiveRuntimeForTests = prevRT })
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(archive.EnvLocalDir, "")

	_, _, err := RestoreVersion(context.Background(), archiveTestSource, restoreTestURL)
	if err == nil {
		t.Fatal("RestoreVersion без архива не дала ошибки")
	}
	if !strings.Contains(err.Error(), archive.EnvBackend) {
		t.Errorf("в ошибке нет имени переменной %s: %v", archive.EnvBackend, err)
	}
}

// Несколько строк на одну пару (source, url) — дубли содержимого (один файл под sha- и
// fingerprint-ключом): выбирается последняя по first_seen, а при равном времени — последняя
// строка файла, потому что файл упорядочен по first_seen и его порядок значим (спека §3.1, §7).
func TestRestoreVersionPicksLatest(t *testing.T) {
	seen := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	fps := [2]string{strings.Repeat("a", 32), strings.Repeat("b", 32)}
	bodies := [2][]byte{[]byte("прежняя версия"), []byte("свежая версия")}
	for _, tt := range []struct {
		name       string
		secondSeen time.Duration
	}{
		{name: "последняя по first_seen", secondSeen: time.Hour},
		{name: "при равном first_seen — последняя строка файла"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt, _ := archiveTestRuntime(t)
			ctx := context.Background()
			for i, body := range bodies {
				sum := sha256.Sum256(body)
				key := archive.Key(archiveTestSource, archiveTestName, fps[i], [sha256.Size]byte{})
				if err := rt.store.Put(ctx, key, body); err != nil {
					t.Fatalf("Put %s: %v", key, err)
				}
				if err := rt.registry.Ensure(ctx, archive.Entry{
					Source:      archiveTestSource,
					Name:        archiveTestName,
					Fingerprint: fps[i],
					SHA256:      hex.EncodeToString(sum[:]),
					Size:        int64(len(body)),
					URL:         restoreTestURL,
					Key:         key,
					FirstSeen:   seen.Add(time.Duration(i) * tt.secondSeen),
				}); err != nil {
					t.Fatalf("Ensure %s: %v", key, err)
				}
			}

			got, entry, err := RestoreVersion(ctx, archiveTestSource, restoreTestURL)
			if err != nil {
				t.Fatalf("RestoreVersion: %v", err)
			}
			if !bytes.Equal(got, bodies[1]) {
				t.Errorf("RestoreVersion вернула %q, ожидалась свежая версия %q", got, bodies[1])
			}
			if entry.Fingerprint != fps[1] {
				t.Errorf("выбрана версия %q, ожидалась %q", entry.Fingerprint, fps[1])
			}
		})
	}
}

// Восстановление шаблона на пути BodyFunc: байты приходят из архива, а не из сети. DataUrl —
// заведомо недоступный адрес: любой сетевой вызов (HEAD-префлайт или GET) уронил бы прогон, так
// что его успех целиком и есть проверка «restore не ходит в сеть». Строка версии при этом пишется
// с отпечатком ИЗ СТРОКИ РЕЕСТРА — источник не спрашивали, и вычислять отпечаток нечем.
func TestClickHouseImportRestoreBodyFromArchive(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)
	ctx := context.Background()
	body := []byte("<html>бюллетень 2021-08-09</html>")
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	var gotBody []byte
	conn := &publicationFakeConn{rows: []driver.Row{&publicationFakeRow{points: 7}}}
	importer := &ClickHouseImport{
		TableName:              archiveTestSource,
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                restoreTestURL,
		PublicationCountPoints: archiveTestCountPoints,
	}
	importer.BodyFunc = func(b []byte, batch driver.Batch) error {
		gotBody = b
		return batch.Append("точка")
	}

	count, err := importer.Import(ctx, conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка", count)
	}
	if !bytes.Equal(gotBody, body) {
		t.Errorf("BodyFunc получил %q, ожидались байты из архива %q", gotBody, body)
	}
	// Префлайта нет: единственный QueryRow — count() для строки версии; HEAD и SELECT версии в
	// source_publications не выполняются вовсе.
	if !reflect.DeepEqual(conn.rowQueries, []string{archiveTestCountPoints}) {
		t.Errorf("запросы QueryRow %v, ожидался только count точек: в режиме восстановления префлайт не выполняется", conn.rowQueries)
	}
	if batch := archiveTestVersionBatch(conn); batch == nil {
		t.Errorf("батчи %v: строки версии в %s нет", conn.batchQueries, PublicationsTable)
	} else if len(batch.appends) != 1 || batch.appends[0][2] != restoreTestFingerprint {
		t.Errorf("строка версии %#v: ожидался fingerprint из реестра %q", batch.appends, restoreTestFingerprint)
	}
	// DDL → точки → (повторной архивации нет) → EnsurePublications → строка версии. Порядок пинится
	// целиком: лишний Exec означал бы повторную архивацию, лишний QueryRow — префлайт.
	wantCalls := []string{"Exec", "PrepareBatch", "Send", "Exec", "QueryRow", "PrepareBatch", "Send"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
}

// Путь ImportFunc восстанавливается тем же кодом: книга открывается из архивных байтов
// (excelize.OpenReader — тоже без сети), и прогон доходит до отправки точек.
func TestClickHouseImportRestoreXlsFromArchive(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)
	ctx := context.Background()
	body := archiveTestBook(t)
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	parsed := false
	conn := &publicationFakeConn{rows: []driver.Row{&publicationFakeRow{points: 3}}}
	importer := &ClickHouseImport{
		TableName:              archiveTestSource,
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                restoreTestURL,
		PublicationCountPoints: archiveTestCountPoints,
	}
	importer.ImportFunc = func(_ *excelize.File, batch driver.Batch) error {
		parsed = true
		return batch.Append("точка")
	}

	count, err := importer.Import(ctx, conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if !parsed {
		t.Error("ImportFunc не вызван: книга из архивных байтов не разобрана")
	}
	if count != 1 {
		t.Errorf("count = %d, ожидалась одна отправленная строка", count)
	}
}

// Пустой отпечаток в строке реестра — версия записана по sha-ключу (HEAD в момент архивации был
// недоступен): строка версии не пишется, как и в обычном пути, — отпечатка, которым её называть, у
// шаблона нет. Обращений к таблице версий при этом не происходит вовсе.
func TestClickHouseImportRestoreWithoutFingerprint(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)
	ctx := context.Background()
	body := []byte("<html>версия без отпечатка</html>")
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, ""); err != nil {
		t.Fatalf("save: %v", err)
	}

	conn := &publicationFakeConn{}
	importer := &ClickHouseImport{
		TableName:              archiveTestSource,
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                restoreTestURL,
		PublicationCountPoints: archiveTestCountPoints,
	}
	importer.BodyFunc = func(_ []byte, batch driver.Batch) error { return batch.Append("точка") }

	if _, err := importer.Import(ctx, conn); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if len(conn.rowQueries) != 0 {
		t.Errorf("запросы QueryRow %v: без отпечатка строка версии не пишется, и points не спрашиваются", conn.rowQueries)
	}
	if len(conn.batchQueries) != 1 || conn.batchQueries[0] != "INSERT INTO "+archiveTestSource {
		t.Errorf("батчи %v, ожидался только батч точек", conn.batchQueries)
	}
	wantCalls := []string{"Exec", "PrepareBatch", "Send"}
	if !reflect.DeepEqual(conn.calls, wantCalls) {
		t.Errorf("порядок обращений\n получили %v\n ожидали %v", conn.calls, wantCalls)
	}
}

// CrawFunc-импортёр в режиме восстановления — ошибка с явным текстом, до DDL, до БД и до сети: его
// файл качает и разбирает сам импортёр, шаблон его байтов не видит, и в архиве объекта нет и не
// будет — граница шага 1.1 (спека §3.1).
func TestClickHouseImportRestoreRejectsCrawFunc(t *testing.T) {
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)

	conn := &publicationFakeConn{}
	importer := &ClickHouseImport{
		TableName:   "sber_rpbu",
		CreateTable: []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:     restoreTestURL,
	}
	importer.CrawFunc = func(string, driver.Conn) (int64, error) {
		t.Error("CrawFunc вызван в режиме восстановления")
		return 0, nil
	}

	_, err := importer.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("CrawFunc + restore не дали ошибки")
	}
	if !strings.Contains(err.Error(), "CrawFunc") {
		t.Errorf("ошибка не называет CrawFunc: %v", err)
	}
	if len(conn.calls) != 0 {
		t.Errorf("обращения к БД %v, ожидалось ноль: отказ стоит до DDL", conn.calls)
	}
}

// BeforeImport в режиме восстановления не вызывается: его смысл — вычислить DataUrl из страницы
// источника, а restore источник не спрашивает вовсе (ветка restore в Import и комментарий там же).
// Проба красная: взведённый здесь BeforeImport означал бы сетевой обход, которого в restore быть
// не должно.
func TestClickHouseImportRestoreSkipsBeforeImport(t *testing.T) {
	rt, _ := archiveTestRuntime(t)
	t.Setenv(EnvArchiveAction, ArchiveActionRestore)
	ctx := context.Background()
	body := []byte("<html>бюллетень</html>")
	if err := rt.save(ctx, archiveTestSource, restoreTestURL, body, restoreTestFingerprint); err != nil {
		t.Fatalf("save: %v", err)
	}

	beforeCalled := false
	conn := &publicationFakeConn{rows: []driver.Row{&publicationFakeRow{points: 1}}}
	importer := &ClickHouseImport{
		TableName:              archiveTestSource,
		CreateTable:            []string{"CREATE TABLE IF NOT EXISTS %s (source String) ENGINE = Memory"},
		DataUrl:                restoreTestURL,
		PublicationCountPoints: archiveTestCountPoints,
		BeforeImport: func(context.Context, driver.Conn) error {
			beforeCalled = true
			return nil
		},
	}
	importer.BodyFunc = func(_ []byte, batch driver.Batch) error { return batch.Append("точка") }

	if _, err := importer.Import(ctx, conn); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if beforeCalled {
		t.Error("BeforeImport вызван в режиме восстановления: источник не должен спрашиваться вовсе")
	}
}
