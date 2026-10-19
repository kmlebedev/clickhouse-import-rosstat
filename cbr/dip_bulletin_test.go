package cbr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// Тесты импортёра cbr_dip_bulletin (cbr/dip_bulletin.go): гейт прогона, отбор новых выпусков и
// запись точек одного выпуска. Import() целиком здесь не вызывается — он ходит в сеть за
// страницей выпусков (util.GetPage), а сетевых вызовов в тестах пакета нет (стандарт §11.3):
// порядок «DDL → страница → гейт → цикл по выпускам», агрегация ошибок errors.Join и no-op
// повторного прогона — предмет живой приёмки (спека §3.5; план пункта, Task 7). Офлайн
// проверяются звенья цикла: страница — dip_page_test.go, книга — dip_parse_test.go, гейт
// (cbrDipKnownReleases, cbrDipNewReleases) и запись выпуска (cbrDipAppendPoints) — здесь.
// Ни сети, ни БД: гейт отвечает фейковый driver.Conn, точки принимает харнесс appendRecorder
// (стандарт §11.6).

// dipAppendColumns — колонки строки батча cbr_dip_bulletin в порядке DDL (спека §3.1):
// series, date, release_id, release, publication_date, value. Литералы, а не ссылка на DDL:
// перестановка соседних строковых колонок (release_id ↔ release) записала бы id выпуска в номер
// выпуска и наоборот, а распознать это в таблице было бы нечем — обе LowCardinality(String).
var dipAppendColumns = []string{"series", "date", "release_id", "release", "publication_date", "value"}

// dipVintageBatch — приёмник строк выпуска: строка cbr_dip_bulletin широкая (шесть значений, из
// которых три — винтажные колонки выпуска), а appendedRow харнесса знает только формы (date,
// value) и (name, date, value). Приёмник раскладывает строку надвое: точка (series, date, value)
// уходит харнессу — на ней работает общий двусторонний контракт каталога (checkCbrSeriesMeta,
// стандарт §11.4), — а полная строка остаётся у теста: порядок колонок DDL и винтажные значения
// сверяются с ней. Расхождение формы — ошибка Append, а не паника (прецедент разложения широкой
// строки — ruoniaNamedBatch, cbr/ruonia_test.go).
type dipVintageBatch struct {
	driver.Batch
	rec  *appendRecorder
	rows [][]any // принятые строки: порядок значений — порядок колонок DDL
}

// Append проверяет форму строки до записи и запоминает её только после успеха харнесса:
// отвергнутая строка записанной не считается.
func (b *dipVintageBatch) Append(v ...any) error {
	if len(v) != len(dipAppendColumns) {
		return fmt.Errorf("строка выпуска из %d значений, ожидалось %d (%v)",
			len(v), len(dipAppendColumns), dipAppendColumns)
	}
	series, ok := v[0].(string)
	if !ok {
		return fmt.Errorf("имя ряда пришло как %T, ожидалась строка (колонка series)", v[0])
	}
	date, ok := v[1].(time.Time)
	if !ok {
		return fmt.Errorf("дата пришла как %T, ожидался time.Time (колонка date)", v[1])
	}
	if err := b.rec.Append(series, date, v[5]); err != nil {
		return err
	}
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

// dipBatchRow — принятая строка батча по (ряд, дата): ряд ищется в позиции 0, дата — в позиции 1
// (тот же порядок колонок DDL); отсутствие строки фатально — спот, которого нет, ничего не
// проверяет.
func dipBatchRow(t *testing.T, rows [][]any, series string, date time.Time) []any {
	t.Helper()
	for _, row := range rows {
		at, ok := row[1].(time.Time)
		if ok && row[0] == series && at.Equal(date) {
			return row
		}
	}
	t.Fatalf("в батче нет строки (%q, %s)", series, date.Format("2006-01-02"))
	return nil
}

// dipCheckBatchPoint — сверка записанной строки с разбором книги (cbrDipParseBook): значение
// строки (колонка value, позиция 5) — то же число, что дал разбор. Сверка с разбором, а не с
// литералом: литералы значений и дат уже пинит dip_parse_test.go (TestDipSpotValues), здесь
// проверяется, что батч донёс разобранное до строки без потерь.
func dipCheckBatchPoint(t *testing.T, rows [][]any, points []cbrDipPoint, series string, date time.Time) {
	t.Helper()
	row := dipBatchRow(t, rows, series, date)
	want := dipSpotValue(t, points, series, date)
	got, ok := row[5].(float64)
	if !ok {
		t.Fatalf("строка (%q, %s): значение записано как %T, ожидался float64 (колонка value — Float64)",
			series, date.Format("2006-01-02"), row[5])
	}
	if !cbrSpotEqualFloat64(got, want) {
		t.Errorf("строка (%q, %s): значение %v, разбор книги даёт %v",
			series, date.Format("2006-01-02"), got, want)
	}
}

// TestDipAppendPoints: обёртка записи одного выпуска (ImportFunc шаблона) пишет по строке на точку
// разбора, значения — в порядке колонок DDL, а к точке подставляется винтаж выпуска: release_id,
// release и publication_date — свойства файла выпуска, а не книги внутри него (cbrDipAppendPoints,
// спека §3.5). Фикстура 26-06 разбирается в 544 точки (TestDipFixtureRows): потерянная при
// записи строка или пропущенная колонка винтажа оставила бы выпуски неразличимыми — ключ строки
// (series, date, release_id) собирается именно здесь.
func TestDipAppendPoints(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	f, err := excelize.OpenFile(dipFixture2606)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", dipFixture2606, err)
	}
	defer func() { _ = f.Close() }()

	batch := &dipVintageBatch{rec: newCbrRecorder()}
	if err = cbrDipAppendPoints(release)(f, batch); err != nil {
		t.Fatalf("запись выпуска %s: %v", release.Release, err)
	}
	if batch.rec.err != nil {
		t.Fatalf("схема строк не совпала с разбором: %v", batch.rec.err)
	}
	if len(batch.rows) != 544 {
		t.Errorf("строк батча %d, want 544 (точки выпуска 26-06)", len(batch.rows))
	}
	for i, row := range batch.rows {
		if _, ok := row[0].(string); !ok {
			t.Fatalf("строка %d: имя ряда пришло как %T (позиция 0 — колонка series)", i, row[0])
		}
		if _, ok := row[1].(time.Time); !ok {
			t.Fatalf("строка %d: дата пришла как %T (позиция 1 — колонка date)", i, row[1])
		}
		if _, ok := row[5].(float64); !ok {
			t.Fatalf("строка %d: значение пришло как %T, ожидался float64 (колонка value — Float64)", i, row[5])
		}
		// Винтажные колонки — свойства выпуска: у всех 544 строк они одни и те же.
		if row[2] != release.ID || row[3] != release.Release {
			t.Fatalf("строка %d: выпуск записан как (%v, %v), ожидался (%q, %q) (release_id — ключ винтажа)",
				i, row[2], row[3], release.ID, release.Release)
		}
		if published, ok := row[4].(time.Time); !ok || !published.Equal(release.PublicationDate) {
			t.Fatalf("строка %d: publication_date = %v, ожидалась %s",
				i, row[4], release.PublicationDate.Format("2006-01-02"))
		}
	}

	// Выборочно — три точки из разных листов выпуска (SAAR и оба ряда безработицы): записанное
	// значение обязано совпасть с разбором той же книги.
	points := dipParseFixture(t, dipFixture2606)
	dipCheckBatchPoint(t, batch.rows, points, "saar_all_goods", dipDate(2021, time.January, 31))
	dipCheckBatchPoint(t, batch.rows, points, "unemployment_nsa", dipDate(2015, time.January, 31))
	dipCheckBatchPoint(t, batch.rows, points, "unemployment_sa", dipDate(2026, time.June, 30))
}

// TestDipAppendPointsParseError: разбор книги не удался — обёртка возвращает ошибку и не пишет ни
// одной строки: частично записанный выпуск хуже неписанного (стандарт §6.4). Книга — копия живой
// фикстуры 26-06 с нечисловым значением в объявленной колонке (первая точка SAAR), файл на диске
// не меняется (dipOpenFixture, стандарт §11.1).
func TestDipAppendPointsParseError(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	batch := &dipVintageBatch{rec: newCbrRecorder()}
	f := dipOpenFixture(t, dipFixture2606, func(t *testing.T, f *excelize.File) {
		dipSetCell(t, f, "2", "B4", "н/д")
	})
	err := cbrDipAppendPoints(release)(f, batch)
	if err == nil {
		t.Fatalf("ожидалась ошибка разбора, записано строк %d", len(batch.rows))
	}
	for _, want := range []string{"cbr_dip_bulletin", "26-06", "62343", `значение "н/д"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if len(batch.rows) != 0 {
		t.Errorf("при ошибке разбора записано %d строк, want 0", len(batch.rows))
	}
	if len(batch.rec.raw) != 0 {
		t.Errorf("при ошибке разбора харнесс записал %d строк, want 0", len(batch.rec.raw))
	}
}

// TestDipAppendPointsAppendError: сбой Append не проглатывается — без проверки err строка выпуска
// терялась бы молча, — а ошибка называет ряд и дату точки (cbrDipAppendPoints). Роль «батч
// отверг строку» играет харнесс со схемой из одной колонки: тройку (series, date, value) он
// отвергает так же, как настоящий батч — негодную строку (прецедент —
// TestCbrIntRateLoansAppendErrorNamesRow).
func TestDipAppendPointsAppendError(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	batch := &dipVintageBatch{rec: newCbrRecorder("series")}
	f, err := excelize.OpenFile(dipFixture2606)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", dipFixture2606, err)
	}
	defer func() { _ = f.Close() }()

	err = cbrDipAppendPoints(release)(f, batch)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, строк принято %d", len(batch.rows))
	}
	for _, want := range []string{"cbr_dip_bulletin", "26-06", "62343", "saar_all_goods", "2021-01-31"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if len(batch.rows) != 0 {
		t.Errorf("при ошибке Append принято строк %d, want 0", len(batch.rows))
	}
}

// dipFakeConn — фейковое соединение гейта и проверки выпуска: тест не ходит в БД (стандарт
// §11.3) и подменяет драйвер минимальным типом. Встраивание driver.Conn оставляет необъявленные
// методы nil — вызов одного из них паникует, и это признак того, что сценарий дошёл до
// обращения, которого фейк не обещал (прецедент — etfFakeConn, wgc/etf_test.go).
type dipFakeConn struct {
	driver.Conn
	rows     driver.Rows // ответ Query, когда ошибки нет
	queryErr error       // ошибка Query: сценарий «гейт не прочитан»
	queries  []string    // тексты запросов Query и QueryRow: тест пинит сами запросы
	execs    []string    // тексты запросов Exec: тест пинит создание витрин point-in-time слоя
	execErr  error       // ошибка Exec: сценарий сбоя витрин слоя
	row      driver.Row  // ответ QueryRow, когда ошибки нет
	rowArgs  [][]any     // аргументы QueryRow: тест пинит, что id выпуска уходит плейсхолдером
}

func (c *dipFakeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}

// QueryRow — ответ проверки «строки выпуска в таблице есть» (cbrDipReleaseWritten). Ошибка
// запроса у clickhouse-go приходит из Scan, а не из QueryRow: сценарий «запрос не выполнен»
// моделируется полем scanErr у dipFakeRow. row == nil — сценарий до проверки не доходил, и
// паника на Scan здесь означает обращение, которого фейк не обещал.
func (c *dipFakeConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	c.queries = append(c.queries, query)
	c.rowArgs = append(c.rowArgs, args)
	return c.row
}

// Exec — приёмник CREATE OR REPLACE VIEW и ALTER TABLE витрин point-in-time слоя
// (ensurePitViews): тексты запросов копятся в execs, а сбой моделируется полем execErr
// (TestEnsurePitViewsError). Необъявленный Exec унаследовал бы nil-заглушку.
func (c *dipFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return c.execErr
}

// dipFakeRows — ответ гейта: строки release_id. Объявлены только методы, которые вызывает
// cbrDipKnownReleases, — Next, Scan, Err и Close; остальные унаследованы nil-заглушками (вызов —
// паника: фейк их не обещал).
type dipFakeRows struct {
	driver.Rows
	ids     []string // id известных выпусков в порядке ответа
	pos     int      // номер выданной строки (1-based): Next выдал — Scan читает
	scanErr error    // ошибка чтения строки
	err     error    // ошибка чтения после строк (rows.Err)
	closed  bool     // Close вызван: строки не текут
}

func (r *dipFakeRows) Next() bool {
	if r.pos >= len(r.ids) {
		return false
	}
	r.pos++
	return true
}

func (r *dipFakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos == 0 {
		return errors.New("фейковая строка гейта: Scan без Next")
	}
	if len(dest) != 1 {
		return fmt.Errorf("фейковая строка гейта: приёмников %d, ожидался один (release_id)", len(dest))
	}
	id, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка гейта: приёмник %T, ожидался *string", dest[0])
	}
	*id = r.ids[r.pos-1]
	return nil
}

func (r *dipFakeRows) Err() error { return r.err }

func (r *dipFakeRows) Close() error {
	r.closed = true
	return nil
}

// dipFakeRow — ответ QueryRow проверки «строки выпуска в таблице есть»: число строк выпуска.
// Объявлен только Scan — единственный метод, который вызывает cbrDipReleaseWritten; Err и
// ScanStruct унаследованы nil-заглушками (вызов — паника: фейк их не обещал). Сбой запроса у
// clickhouse-go приходит именно из Scan, поэтому сценарий «запрос не выполнен» — scanErr, а не
// ошибка QueryRow. Тип приёмника проверяется: count() сканируется только в *uint64, и ответ
// другого типа означал бы расхождение фейка с драйвером (прецедент — etfFakeRow, wgc/etf_test.go).
type dipFakeRow struct {
	driver.Row
	written uint64 // число строк выпуска в ответе
	scanErr error  // ошибка чтения ответа: сценарий «запрос не выполнен»
}

func (r *dipFakeRow) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != 1 {
		return fmt.Errorf("фейковый ответ: приёмников %d, ожидался один (число строк)", len(dest))
	}
	written, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("фейковый ответ: приёмник %T, ожидался *uint64", dest[0])
	}
	*written = r.written
	return nil
}

// TestDipKnownReleases: множество release_id выпусков, уже лежащих в таблице, — ответ гейта
// (спека §3.5 шаг 3). Запрос пиннится литералом целиком: DISTINCT не даёт множеству раздуться
// дублями строк одного выпуска, FINAL читает смёрженное состояние (стандарт §9.2), а имя таблицы —
// то же, что у импортёра. Строки обязаны закрыться: на каждом прогоне гейт спрашивает таблицу.
func TestDipKnownReleases(t *testing.T) {
	rows := &dipFakeRows{ids: []string{"46493", "46646", "62343"}}
	conn := &dipFakeConn{rows: rows}
	known, err := cbrDipKnownReleases(context.Background(), conn)
	if err != nil {
		t.Fatalf("cbrDipKnownReleases: %v", err)
	}
	if len(conn.queries) != 1 || conn.queries[0] != "SELECT DISTINCT release_id FROM cbr_dip_bulletin FINAL" {
		t.Errorf("запросы гейта = %v, ожидался один SELECT DISTINCT release_id FROM cbr_dip_bulletin FINAL", conn.queries)
	}
	if len(known) != len(rows.ids) {
		t.Errorf("известных выпусков %d, want %d", len(known), len(rows.ids))
	}
	for _, id := range rows.ids {
		if !known[id] {
			t.Errorf("id %q не попал в множество известных выпусков", id)
		}
	}
	if !rows.closed {
		t.Error("строки гейта не закрыты: соединение течёт на каждом прогоне")
	}
}

// TestDipKnownReleasesErrors: сбой чтения гейта — ошибка прогона, а не «считаем, что известных
// нет»: пустое множество означает «все выпуски новые» и пере-импортировало бы все 49 файлов —
// сбой диагностики обернулся бы полным прогоном (cbr/dip_bulletin.go). Множество при ошибке —
// nil: вызывающий, даже проигнорировав ошибку, не примет «известных нет» за ответ.
func TestDipKnownReleasesErrors(t *testing.T) {
	queryErr := errors.New("фейковое соединение: запрос не выполнен")
	scanErr := errors.New("фейковая строка: release_id не прочитан")
	readErr := errors.New("фейковое соединение: строки не дочитаны")
	for name, tc := range map[string]struct {
		conn *dipFakeConn
		want error
	}{
		"ошибка Query": {conn: &dipFakeConn{queryErr: queryErr}, want: queryErr},
		"ошибка Scan":  {conn: &dipFakeConn{rows: &dipFakeRows{ids: []string{"62343"}, scanErr: scanErr}}, want: scanErr},
		"ошибка Err":   {conn: &dipFakeConn{rows: &dipFakeRows{ids: []string{"62343"}, err: readErr}}, want: readErr},
	} {
		t.Run(name, func(t *testing.T) {
			known, err := cbrDipKnownReleases(context.Background(), tc.conn)
			if err == nil {
				t.Fatalf("ожидалась ошибка, множество известных: %v", known)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("ошибка %v не оборачивает %v: причина сбоя потеряна", err, tc.want)
			}
			if !strings.Contains(err.Error(), "cbr_dip_bulletin") {
				t.Errorf("ошибка %q не называет таблицу", err)
			}
			if known != nil {
				t.Errorf("при ошибке возвращено множество из %d id: пустое множество пере-импортировало бы все выпуски", len(known))
			}
		})
	}
}

// TestDipReleaseWritten: успешный импорт выпуска подтверждён его строками в таблице (спека §3.5
// шаг 4): число строк по release_id больше нуля — nil. Запрос пиннится литералом целиком: FINAL
// читает смёрженное состояние (стандарт §9.2), имя таблицы — то же, что у импортёра, а id выпуска
// уходит плейсхолдером драйвера, а не подстановкой в текст (release_id — данные страницы, и
// литерал в запросе разошёлся бы с ними молча).
func TestDipReleaseWritten(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	conn := &dipFakeConn{row: &dipFakeRow{written: 544}}
	if err := cbrDipReleaseWritten(context.Background(), conn, release); err != nil {
		t.Fatalf("cbrDipReleaseWritten: %v", err)
	}
	if len(conn.queries) != 1 || conn.queries[0] != "SELECT count() FROM cbr_dip_bulletin FINAL WHERE release_id = ?" {
		t.Errorf("запросы проверки = %v, ожидался один SELECT count() FROM cbr_dip_bulletin FINAL WHERE release_id = ?", conn.queries)
	}
	if len(conn.rowArgs) != 1 || len(conn.rowArgs[0]) != 1 || conn.rowArgs[0][0] != release.ID {
		t.Errorf("аргументы проверки = %v, ожидался id выпуска %q плейсхолдером", conn.rowArgs, release.ID)
	}
}

// TestDipReleaseWrittenMissing: шаблон вернул no-op (версия известна), но строк выпуска в таблице
// нет — ошибка с именем выпуска (release + id) и подсказкой про source_publications: молча
// пропущенный выпуск следующий прогон снова счёл бы новым (спека §3.5 шаг 4). Состояние
// практически недостижимо (число точек всей таблицы растёт с каждым выпуском), но тихая дыра
// обязана стать громкой ошибкой.
func TestDipReleaseWrittenMissing(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	conn := &dipFakeConn{row: &dipFakeRow{}}
	err := cbrDipReleaseWritten(context.Background(), conn, release)
	if err == nil {
		t.Fatal("ожидалась ошибка: успех шаблона без строк выпуска — ровно та тихая дыра, ради которой проверка написана")
	}
	for _, want := range []string{"cbr_dip_bulletin", "26-06", "62343", "no-op", "source_publications", "повторите прогон"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestDipReleaseWrittenError: сбой чтения — ошибка прогона, а не «считаем, что записано»:
// проглоченный сбой проверки пропустил бы ровно ту дыру, ради которой она написана. Ошибка
// оборачивает причину (errors.Is) и называет выпуск. Сбой запроса у clickhouse-go приходит из
// Scan — им и моделируется «запрос не выполнен».
func TestDipReleaseWrittenError(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	scanErr := errors.New("фейковое соединение: число строк выпуска не прочитано")
	err := cbrDipReleaseWritten(context.Background(), &dipFakeConn{row: &dipFakeRow{scanErr: scanErr}}, release)
	if err == nil {
		t.Fatal("ожидалась ошибка чтения")
	}
	if !errors.Is(err, scanErr) {
		t.Errorf("ошибка %v не оборачивает %v: причина сбоя потеряна", err, scanErr)
	}
	for _, want := range []string{"cbr_dip_bulletin", "26-06", "62343"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q: без имени выпуска сбой не привязать к шагу", err, want)
		}
	}
}

// TestEnsurePitViewsCreatesAll — контракт ensurePitViews по текстам исполненных запросов:
// витрины слоя создаются все три и именно через CREATE OR REPLACE VIEW, as-of несёт параметр
// {as_of:Date}, а колонковые комментарии есть только у непараметризованных витрин —
// 7 (v_point_in_time) + 6 (v_point_in_time_version) = 13. У v_point_in_time_asof колонковых
// комментариев нет: у параметризованной витрины ClickHouse нет схемы без аргументов, и
// COMMENT COLUMN возвращает NOT_FOUND_COLUMN_IN_BLOCK (спека §2.6, ограничение CH 26.10).
func TestEnsurePitViewsCreatesAll(t *testing.T) {
	conn := &dipFakeConn{row: &dipFakeRow{written: 1}}
	if err := ensurePitViews(context.Background(), conn); err != nil {
		t.Fatalf("ensurePitViews: %v", err)
	}
	joined := strings.Join(conn.execs, "\n")
	for _, name := range []string{"v_point_in_time", "v_point_in_time_asof", "v_point_in_time_version"} {
		if !strings.Contains(joined, "CREATE OR REPLACE VIEW "+name+" ") {
			t.Errorf("нет CREATE OR REPLACE VIEW для %s", name)
		}
	}
	if !strings.Contains(joined, "{as_of:Date}") {
		t.Error("в определении as-of витрины нет параметра {as_of:Date}")
	}
	if strings.Count(joined, "CREATE OR REPLACE VIEW ") != 3 {
		t.Errorf("CREATE OR REPLACE VIEW: %d, want 3", strings.Count(joined, "CREATE OR REPLACE VIEW "))
	}
	// У параметризованной витрины колонковых комментариев быть не может: 7 (base) + 6 (version).
	if got := strings.Count(joined, "COMMENT COLUMN"); got != 13 {
		t.Errorf("COMMENT COLUMN: %d, want 13 (7 base + 6 version)", got)
	}
}

// TestEnsurePitViewsError — сбой CreateView не проглатывается и не теряет имя витрины: прогон
// обязан увидеть ошибку слоя (Import присоединяет её к errors.Join), а диагноз — назвать
// витрину и причину. util.CreateView уже оборачивает ошибку Exec как «<view>: …», поэтому
// ensurePitViews не должен дублировать префикс.
func TestEnsurePitViewsError(t *testing.T) {
	t.Run("exec", func(t *testing.T) {
		conn := &dipFakeConn{row: &dipFakeRow{written: 1}, execErr: errors.New("boom")}
		err := ensurePitViews(context.Background(), conn)
		if err == nil {
			t.Fatal("ожидалась ошибка: сбой CreateView не должен проглатываться")
		}
		for _, want := range []string{"v_point_in_time", "boom"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не содержит %q", err, want)
			}
		}
		if got := strings.Count(err.Error(), "v_point_in_time"); got != 1 {
			t.Errorf("имя витрины встречается %d раз, want 1 (двойной префикс): %q", got, err)
		}
	})
	t.Run("metadata", func(t *testing.T) {
		// Ошибка проверки system.tables приходит из Scan и не обёрнута util.CreateView:
		// имя витрины обязан добавить ensurePitViews.
		conn := &dipFakeConn{row: &dipFakeRow{scanErr: errors.New("meta boom")}}
		err := ensurePitViews(context.Background(), conn)
		if err == nil {
			t.Fatal("ожидалась ошибка: сбой проверки таблицы не должен проглатываться")
		}
		for _, want := range []string{"v_point_in_time", "meta boom"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не содержит %q", err, want)
			}
		}
	})
}

// dipReleaseIDs — id выпусков в порядке среза: проверяется порядок и состав выпусков, а поля
// выпусков (url, дата публикации, номер) пинит разбор страницы — dip_page_test.go.
func dipReleaseIDs(releases []dipRelease) []string {
	ids := make([]string, 0, len(releases))
	for _, release := range releases {
		ids = append(ids, release.ID)
	}
	return ids
}

// TestDipNewReleases: отбор выпусков, которых нет в таблице (cbrDipNewReleases, спека §3.5 шаг 4).
// Выпуски страницы приходят свежими первыми, а в цикл обязаны попасть старые первыми: первый
// прогон наполняет историю детерминированным префиксом. Порядок и состав — литералами, даты и id —
// настоящие со страницы выпусков (dip_page_test.go).
func TestDipNewReleases(t *testing.T) {
	// Свежие первыми — как на странице.
	page := []dipRelease{
		{ID: "62444", URL: dipUrlPrefix + "/Collection/Collection/File/62444/bulletin_26-07.xlsx",
			PublicationDate: dipDate(2026, time.October, 14), Release: "26-07"},
		{ID: "62343", URL: dipUrlPrefix + "/Collection/Collection/File/62343/bulletin_26-06.xlsx",
			PublicationDate: dipDate(2026, time.September, 1), Release: "26-06"},
		{ID: "36593", URL: dipUrlPrefix + "/Collection/Collection/File/36593/stat_bulletin_21-06.xlsx",
			PublicationDate: dipDate(2021, time.September, 1), Release: "21-06"},
	}
	// Пара с одним именем файла: stat_bulletin_23-07.xlsx лежит у двух выпусков с разными id и
	// разными датами публикации (спека §2) — гейт обязан смотреть на id, а сортировка на дату.
	sameFile := []dipRelease{
		{ID: "46646", URL: dipUrlPrefix + "/Collection/Collection/File/46646/stat_bulletin_23-07.xlsx",
			PublicationDate: dipDate(2023, time.December, 5), Release: "23-08"},
		{ID: "46493", URL: dipUrlPrefix + "/Collection/Collection/File/46493/stat_bulletin_23-07.xlsx",
			PublicationDate: dipDate(2023, time.October, 17), Release: "23-07"},
	}
	for name, tc := range map[string]struct {
		releases []dipRelease
		known    map[string]bool
		want     []string
	}{
		"все выпуски новые": {
			releases: page,
			want:     []string{"36593", "62343", "62444"},
		},
		"часть выпусков известна": {
			releases: page,
			known:    map[string]bool{"62343": true},
			want:     []string{"36593", "62444"},
		},
		"все выпуски известны": {
			releases: page,
			known:    map[string]bool{"36593": true, "62343": true, "62444": true},
		},
		"гейт по id, а не по имени файла": {
			releases: sameFile,
			known:    map[string]bool{"46493": true},
			want:     []string{"46646"},
		},
		"сортировка по дате публикации, а не по порядку страницы": {
			releases: sameFile,
			want:     []string{"46493", "46646"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := dipReleaseIDs(cbrDipNewReleases(tc.releases, tc.known))
			if !slices.Equal(got, tc.want) {
				t.Errorf("выпуски гейта = %v, want %v", got, tc.want)
			}
		})
	}

	// Устойчивость: выпуски одной даты идут в порядке страницы (свежие первыми), а не в
	// произвольном. Даты и id здесь синтетические — у живой страницы повтор даты один (пара
	// 46493/46646, спека §2), — а проверка идёт на восьми парах: на одном повторе перестановку
	// равных ключей неустойчивая сортировка не показывает, на нескольких — показывает, и порядок
	// страницы был бы потерян (sort.Slice вместо sort.SliceStable эту проверку роняет). Порядок
	// детерминирован и без уникальности дат — это часть контракта отбора (§3.5 шаг 4).
	t.Run("выпуски одной даты держат порядок страницы", func(t *testing.T) {
		const pairs = 8
		page := make([]dipRelease, 0, 2*pairs)
		for i := 0; i < pairs; i++ {
			published := dipDate(2026, time.October-time.Month(i), 14) // свежие пары первыми
			for part := 0; part < 2; part++ {
				id := fmt.Sprintf("9%04d", 2*i+part)
				release := fmt.Sprintf("26-%02d", 2*(pairs-i)-part)
				page = append(page, dipRelease{
					ID:              id,
					URL:             fmt.Sprintf("%s/Collection/Collection/File/%s/bulletin_%s.xlsx", dipUrlPrefix, id, release),
					PublicationDate: published,
					Release:         release,
				})
			}
		}
		got := cbrDipNewReleases(page, nil)
		if len(got) != len(page) {
			t.Fatalf("выпусков гейта %d, want %d (все выпуски новые)", len(got), len(page))
		}
		pageOrder := make(map[string]int, len(page))
		for i, release := range page {
			pageOrder[release.ID] = i
		}
		for i, release := range got {
			if i == 0 {
				continue
			}
			before := got[i-1]
			if before.PublicationDate.After(release.PublicationDate) {
				t.Errorf("выпуск %s (%s) стоит после %s (%s): хронологический порядок (старые → новые) потерян",
					release.ID, release.PublicationDate.Format("2006-01-02"),
					before.ID, before.PublicationDate.Format("2006-01-02"))
			}
			if before.PublicationDate.Equal(release.PublicationDate) && pageOrder[release.ID] < pageOrder[before.ID] {
				t.Fatalf("выпуски одной даты %s переставлены: %s идёт после %s, хотя на странице был раньше — порядок страницы сохраняет только устойчивая сортировка",
					release.PublicationDate.Format("2006-01-02"), before.ID, release.ID)
			}
		}
	})
}

// TestDipSeriesMetaCoversParsedVintages: двусторонний контракт каталога (стандарт §11.4: имена
// рядов объявлены кодом, §6.8) — объединение разобранных рядов трёх живых выпусков и
// cbrDipSeriesMeta совпадают в обе стороны, с сообщением о конкретном лишнем или забытом ряде.
// Объединение, а не один выпуск: составы разные — безработица (26-06 и 2021-06) и занятость
// (26-07), — и только вместе они дают все восемь объявленных рядов спеки §3.2.
func TestDipSeriesMetaCoversParsedVintages(t *testing.T) {
	union := make(map[string]bool, 8)
	checkCbrSeriesMeta(t, "cbr_dip_bulletin", cbrDipSeriesMeta, func(rec *appendRecorder) error {
		for _, file := range []string{dipFixture2606, dipFixture2607, dipFixture2106} {
			f, err := excelize.OpenFile(file)
			if err != nil {
				return fmt.Errorf("открытие фикстуры %s: %w", file, err)
			}
			release := dipFixtureReleases[file]
			batch := &dipVintageBatch{rec: rec}
			err = cbrDipAppendPoints(release)(f, batch)
			_ = f.Close()
			if err != nil {
				return fmt.Errorf("выпуск %s: %w", release.Release, err)
			}
			for _, row := range batch.rows {
				name, ok := row[0].(string)
				if !ok {
					return fmt.Errorf("строка батча выпуска %s: имя ряда пришло как %T", release.Release, row[0])
				}
				union[name] = true
			}
		}
		return nil
	})
	if len(cbrDipSeriesMeta) != 8 {
		t.Errorf("каталог описывает %d рядов, want 8 (спека §3.2)", len(cbrDipSeriesMeta))
	}
	if len(union) != 8 {
		t.Errorf("три выпуска разобрали %d рядов, want 8 (спека §3.2)", len(union))
	}
}

// TestDipSeriesMetaFields: поля каталога, объявленные спекой §3.6 общими для всех восьми рядов, —
// литералами: единица «%», частота «M» (ряды месячные, значения — проценты), source — имя таблицы
// импортёра, origin называет страницу выпусков и лист источника, description — привязку даты
// (последний день отчётного месяца, спека §3.3) и винтажность значения. Сверка идёт со спекой, а
// не с каталогом: описание ряда — единственное место, откуда агент MCP узнаёт, что он читает.
func TestDipSeriesMetaFields(t *testing.T) {
	for _, meta := range cbrDipSeriesMeta {
		if meta.Source != "cbr_dip_bulletin" {
			t.Errorf("ряд %q: source %q, want cbr_dip_bulletin (источник каталога = имя таблицы)", meta.Series, meta.Source)
		}
		if meta.Unit != "%" {
			t.Errorf("ряд %q: unit %q, want %%", meta.Series, meta.Unit)
		}
		if meta.Frequency != "M" {
			t.Errorf("ряд %q: frequency %q, want M", meta.Series, meta.Frequency)
		}
		for _, want := range []string{"cbr.ru/ec_research/mb/", "лист «"} {
			if !strings.Contains(meta.Origin, want) {
				t.Errorf("ряд %q: origin %q не содержит %q", meta.Series, meta.Origin, want)
			}
		}
		for _, want := range []string{"последний день", "выпуск"} {
			if !strings.Contains(meta.Description, want) {
				t.Errorf("ряд %q: description %q не содержит %q (привязка даты и винтажность значения)", meta.Series, meta.Description, want)
			}
		}
	}
}
