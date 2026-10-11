package wgc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// Имя импортёра — контракт расписания (dagu/wgc.yaml), ручного запуска
// (make import STAT=wgc_etf_flows) и source строк в macro_series/series_catalog.
func TestEtfFlowsStatName(t *testing.T) {
	if got := (&etfFlowsStat{}).Name(); got != "wgc_etf_flows" {
		t.Fatalf("Name() = %q, ожидалось wgc_etf_flows", got)
	}
}

// apiDown — сервер, отвечающий 500: тесты, которым нужен файловый путь, не должны
// ходить в реальный fscdn (и не должны зависеть от сети вовсе). Возвращается база в
// том же виде, что etfAPIBaseURL: путями эндпоинтов её дополняет etfAPIURL.
func apiDown(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/"
}

// Пустая папка данных — не ошибка (warn + 0 строк): dagu не должен краснеть до
// первого скачивания выгрузки. API при этом обязан быть недоступен (apiDown): иначе
// тест ушёл бы в сеть за настоящими рядами и закрепил бы не фолбэк. conn не трогается
// вовсе — тест зовёт Import с nil и упал бы паникой уже на первом обращении к соединению.
func TestEtfImportEmptyDirIsNoop(t *testing.T) {
	stat := etfFlowsStat{dir: t.TempDir(), apiBase: apiDown(t)}
	count, err := stat.Import(context.Background(), nil)
	if err != nil {
		t.Fatalf("Import пустого каталога: %v", err)
	}
	if count != 0 {
		t.Errorf("Import пустого каталога = %d строк, ожидалось 0", count)
	}
}

// Битый файл — ошибка импорта (fail-fast, урок eia): тихий ноль оставил бы dagu
// зелёным, а ряды — без обновления. Ошибка обязана называть файл: по каталогу
// wgc/data лежат выгрузки разных задач, и «что-то не открылось» неотличимо.
// API здесь недоступен (apiDown), поэтому путь к файлу — закрепление фолбэка.
func TestEtfImportBrokenFileFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ETF_Flows_broken.xlsx")
	if err := os.WriteFile(path, []byte("не xlsx"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	stat := etfFlowsStat{dir: dir, apiBase: apiDown(t)}
	_, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("битый файл должен давать ошибку, а не тихий ноль")
	}
	if !strings.Contains(err.Error(), "ETF_Flows_broken.xlsx") {
		t.Errorf("ошибка не называет файл: %v", err)
	}
}

// newHeaderOnlyEtfWorkbook собирает воркбук со всеми четырьмя листами и шапками,
// но без строк данных: так выглядит выгрузка, если данные не попали в файл.
// Колонка B листов фондов (справочные итоги, парсер её не читает) заполнена в
// строках 1..7, чтобы GetRows вернул непрерывный диапазон и размер листа дошёл до
// проверки «данные с 7-й строки»; с дырой в шапке парсер упал бы раньше guard-а,
// и тест закреплял бы не тот fail-fast путь.
func newHeaderOnlyEtfWorkbook(t *testing.T) *excelize.File {
	t.Helper()

	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })

	sheets := []string{etfFlowsSheet, etfDemandSheet, etfHoldingsSheet, etfChartsSheet}
	if err := f.SetSheetName("Sheet1", sheets[0]); err != nil {
		t.Fatalf("SetSheetName(%s): %v", sheets[0], err)
	}
	for _, sheet := range sheets[1:] {
		if _, err := f.NewSheet(sheet); err != nil {
			t.Fatalf("NewSheet(%s): %v", sheet, err)
		}
	}

	for _, sheet := range sheets[:3] {
		refs := map[string]string{
			"F4": "North America", "G4": "Europe", "H4": "Asia", "I4": "Other",
			"B1": "0", "B2": "0", "B3": "0", "B4": "0", "B5": "0", "B6": "0", "B7": "0",
		}
		for ref, value := range refs {
			setEtfCell(t, f, sheet, ref, value)
		}
	}

	for ref, value := range map[string]string{
		"A1": etfAumChartsHeader,
		"A2": etfDateHeader, "B2": "North America", "C2": "Europe", "D2": "Asia", "E2": "Other",
	} {
		setEtfCell(t, f, etfChartsSheet, ref, value)
	}

	return f
}

// Воркбук с шапками, но без точек — ошибка до PrepareBatch: пустой срез прошёл бы
// в Send, а каталог описал бы ряды, которых в таблице нет. Тест зовёт Import с nil
// conn: guard срабатывает раньше соединения, и обращение к conn упало бы паникой.
// API недоступен (apiDown): разбор файла на фолбэк-пути обязан остаться строгим —
// «0 точек» не должно становиться тихим нулём только потому, что канал запасной.
func TestEtfImportWorkbookWithoutDataFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ETF_Flows_2026-10-02_1401.xlsx")
	if err := newHeaderOnlyEtfWorkbook(t).SaveAs(path); err != nil {
		t.Fatalf("SaveAs: %v", err)
	}

	stat := etfFlowsStat{dir: dir, apiBase: apiDown(t)}
	count, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("воркбук без строк данных должен давать ошибку, а не тихий ноль с публикацией каталога")
	}
	if count != 0 {
		t.Errorf("Import = %d строк, ожидалось 0", count)
	}
	if !strings.Contains(err.Error(), "без точек") {
		t.Errorf("ошибка не фиксирует причину (выгрузка без точек): %v", err)
	}
}

// TestEtfLoadPointsFallsBackToFile: API отвечает 500, в каталоге — реальная выгрузка:
// транспорт обязан уйти на файл, и это не «ноль строк»: в день недоступности CDN ряды
// обязаны обновиться из выгрузки, а не остаться без обновления под зелёным dagu.
// Выгрузка берётся настоящая (testdata/ETF_Flows_2026-10-02_1401.xlsx — та же
// фикстура, что у xlsx-парсера), а не синтетический воркбук: фолбэк должен работать
// на боевом формате файла. Путь выбранного файла возвращается наружу — им Import
// инструментирует лог и называет источник в guard-е устаревшей выгрузки, поэтому тест
// пинит и его: без пути предупреждение об устаревшей выгрузке не назвало бы файл.
func TestEtfLoadPointsFallsBackToFile(t *testing.T) {
	dir := t.TempDir()
	name := "ETF_Flows_2026-10-02_1401.xlsx"
	if err := os.WriteFile(filepath.Join(dir, name), etfAPITestdata(t, name), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", name, err)
	}

	points, fromAPI, file, err := etfLoadPoints(dir, apiDown(t), etfAPITestNow)
	if err != nil {
		t.Fatalf("etfLoadPoints: %v", err)
	}
	if fromAPI {
		t.Error("API ответил 500 — источником обязан стать файл (fromAPI = true)")
	}
	if len(points) == 0 {
		t.Error("выгрузка разобрана в 0 точек — фолбэк не сработал")
	}
	if want := filepath.Join(dir, name); file != want {
		t.Errorf("etfLoadPoints вернул путь %q, ожидался %q", file, want)
	}
}

// TestEtfLoadPointsPrefersAPIOverFile: API отвечает фикстурами, а в каталоге лежит битый
// файл, на котором разбор обязан упасть. Успешный результат доказывает, что источником
// стал именно API: победи файл — тест упал бы на его разборе. Смешивать транспорты нельзя
// (двойные точки и разный состав рядов на одном месяце), поэтому источник ровно один,
// и путь файла при API-транспорте пуст — иначе Import назвал бы в логе не тот источник.
func TestEtfLoadPointsPrefersAPIOverFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ETF_Flows_broken.xlsx"), []byte("не xlsx"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	srv := etfAPITestServer(t, nil, nil)

	points, fromAPI, file, err := etfLoadPoints(dir, srv.URL+"/", etfAPITestNow)
	if err != nil {
		t.Fatalf("etfLoadPoints: %v", err)
	}
	if !fromAPI {
		t.Error("API ответил фикстурами — источником обязан стать API (fromAPI = false)")
	}
	if file != "" {
		t.Errorf("etfLoadPoints вернул путь файла %q при API-транспорте — источником стал не API", file)
	}
	if len(points) == 0 {
		t.Error("фикстуры API разобраны в 0 точек")
	}
}

// TestEtfCacheLooksStale — табличный: решение guard-а устаревшего ответа CDN проверяется без
// соединения (сама выборка max(date) тестом не покрывается — соединения в тестах нет, а
// сравнение дат покрывается). Пустая таблица приходит сюда нулевой датой (etfStoredMaxDate так
// её нормализует) — сравнивать не с чем, значит false; равные даты — false: ответ того же месяца
// не «устаревание», и warn на каждом прогоне обесценил бы сигнал; true — только строго меньший
// максимум API, но не наоборот.
func TestEtfCacheLooksStale(t *testing.T) {
	aug := time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
	sep := time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)
	oct := time.Date(2026, time.October, 31, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		apiMax    time.Time
		storedMax time.Time
		want      bool
	}{
		{"пустая таблица", sep, time.Time{}, false},
		{"пустая таблица и пустой ответ API", time.Time{}, time.Time{}, false},
		{"даты равны", sep, sep, false},
		{"API отстаёт на месяц", aug, sep, true},
		{"API свежее сохранённого", oct, sep, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := etfCacheLooksStale(c.apiMax, c.storedMax); got != c.want {
				t.Errorf("etfCacheLooksStale(%s, %s) = %t, ожидалось %t",
					c.apiMax.Format("2006-01-02"), c.storedMax.Format("2006-01-02"), got, c.want)
			}
		})
	}
}

// etfFakeConn — фейковое соединение для проверок порядка обращений и разбора NULL: достаточно
// встроить driver.Conn и переопределить три используемых метода. Остальные остаются nil и
// паникуют при вызове — поэтому тесты обязаны останавливать Import на PrepareBatch, что и
// делает фейк (возвращает ошибку).
type etfFakeConn struct {
	driver.Conn
	calls []string
}

func (c *etfFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.calls = append(c.calls, "exec: "+query)
	return nil
}

func (c *etfFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.calls = append(c.calls, "queryrow: "+query)
	return etfFakeRow{}
}

func (c *etfFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.calls = append(c.calls, "preparebatch: "+query)
	return nil, errors.New("фейковое соединение: батч не поддержан")
}

// etfFakeRow — ответ maxOrNull(date) по пустой таблице: NULL. Scan проверяет тип приёмника —
// NULL приходит в **time.Time (указатель), поэтому возврат к var maxDate time.Time вместе с
// прежней эвристикой «пустая таблица — год 1970» роняет тест, а не тихо отдаёт 1970-01-01.
type etfFakeRow struct{}

func (etfFakeRow) Err() error { return nil }

func (etfFakeRow) ScanStruct(any) error {
	return errors.New("фейковая строка: ScanStruct не используется")
}

func (etfFakeRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("фейковая строка: ожидался один приёмник")
	}
	maxDate, ok := dest[0].(**time.Time)
	if !ok {
		return errors.New("фейковая строка: NULL сканируется только в **time.Time")
	}
	*maxDate = nil
	return nil
}

// TestEtfStoredMaxDateEmptyTableIsZero: «сохранённых точек нет» — факт ответа maxOrNull (NULL),
// а не вывод из года. Живого ClickHouse в тестах нет, поэтому используется фейковое соединение
// (прецедент — текстовые проверки запросов в dcf), и именно здесь лежала ошибка: эвристика
// max(date).Year() <= 1970 объявляла пустой и таблицу с настоящими точками 1970-01.
func TestEtfStoredMaxDateEmptyTableIsZero(t *testing.T) {
	conn := &etfFakeConn{}

	got, err := etfStoredMaxDate(context.Background(), conn)
	if err != nil {
		t.Fatalf("etfStoredMaxDate по пустой таблице: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("etfStoredMaxDate по пустой таблице = %s, ожидалась нулевая дата (данных для сравнения нет)", got)
	}
	if len(conn.calls) != 1 || !strings.Contains(conn.calls[0], "maxOrNull") {
		t.Errorf("обращения к соединению = %v, ожидался один запрос maxOrNull(date)", conn.calls)
	}
}

// TestEtfImportCreatesTableBeforeStaleGuard: DDL идёт до guard-а устаревания. На чистой базе
// macro_series ещё нет, и прежний порядок (guard, затем DDL внутри записи) давал на первом
// прогоне ложный warn «сохранённый максимум дат не прочитан» при полностью здоровом источнике.
// Фейковое соединение записывает порядок обращений: DDL, затем запрос guard-а, затем батч.
func TestEtfImportCreatesTableBeforeStaleGuard(t *testing.T) {
	srv := etfAPITestServer(t, nil, nil)
	conn := &etfFakeConn{}
	stat := etfFlowsStat{dir: t.TempDir(), apiBase: srv.URL + "/"}

	if _, err := stat.Import(context.Background(), conn); err == nil {
		t.Fatal("Import обязан вернуть ошибку фейкового PrepareBatch")
	}

	if len(conn.calls) != 3 {
		t.Fatalf("обращения к соединению = %v, ожидались три: DDL, guard, батч", conn.calls)
	}
	if !strings.Contains(conn.calls[0], "CREATE TABLE IF NOT EXISTS "+macroSeriesTable) {
		t.Errorf("первым обращением к соединению было %q, ожидался DDL macro_series до guard-а", conn.calls[0])
	}
	if !strings.Contains(conn.calls[1], "maxOrNull") {
		t.Errorf("вторым обращением к соединению было %q, ожидался запрос guard-а maxOrNull(date)", conn.calls[1])
	}
	if !strings.HasPrefix(conn.calls[2], "preparebatch: ") {
		t.Errorf("третьим обращением к соединению было %q, ожидался батч вставки", conn.calls[2])
	}
}
