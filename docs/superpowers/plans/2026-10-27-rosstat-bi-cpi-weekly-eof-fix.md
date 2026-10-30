# План: `rosstat_bi_cpi_weekly` — ретраи экспорта и гейт версии (issue #338)

**Спека:** [2026-10-27-rosstat-bi-cpi-weekly-eof-fix-design.md](../specs/2026-10-27-rosstat-bi-cpi-weekly-eof-fix-design.md).
**Пункт:** `key-rate/phase-2-source-fixes`, issue [#338](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/338).
**Ветка:** `fix/rosstat-bi-cpi-weekly`.

## Шаг 1. Тесты-контракты (до кода)

1. `rosstat/bi_cpi_weekly_test.go`: `TestBICpiWeeklyExportRetriesOnEOF` — сервер рвёт соединение на первой попытке, отвечает `//OK` на второй; `TestBICpiWeeklyExportFailsAfterAttempts` — рвёт всегда, ошибка после третьей; в обоих `biCpiWeeklySleep = func(context.Context, time.Duration) error { return nil }` и `biCpiWeeklyAttempts` по умолчанию.
2. `TestBICpiWeeklyFingerprint` — стабилен на повторе, не зависит от порядка входа, меняется от значения.
3. `TestBICpiWeeklyShouldSkip` — 4 случая (`known=false`; `known=true` и точки совпали; `known=true`, точек меньше; больше).
4. `TestBICpiWeeklyExportPathFixture` — на живой фикстуре `testdata/bi_cpi_weekly_rpc_ok.txt`.

## Шаг 2. Транспорт (`rosstat/bi_cpi_weekly.go`)

1. `biCpiWeeklyAttempts = 3`, `biCpiWeeklySleep` (переменная, по умолчанию `time.Sleep` с `ctx`).
2. `biCpiWeeklyRetry[T any](what string, fn func() (T, error)) (T, error)` — 3 попытки, паузы 2 с/5 с, `ctx`-отмена; повтор только транспортных ошибок (`PostFileBytes`/`GetFileBytes`), не ошибок разбора.
3. `biCpiWeeklyCSVURL` → через ретрай; итоговая ошибка: `export RPC: <причина> (N попыток)`.
4. Скачивание CSV — через тот же ретрай.

## Шаг 3. Гейт версии и полный цикл `Import()`

1. Константы: `biCpiWeeklyVersionName`, `biCpiWeeklyCountPoints = "SELECT count() FROM rosstat_bi_cpi_weekly FINAL"`.
2. `biCpiWeeklyFingerprint(points []biCpiWeeklyPoint) string` — `sha256` по `2006-01-02\t<strconv 'g' -1>`; каноническая строка закреплена пином в тесте.
3. `biCpiWeeklyShouldSkip(known bool, knownPoints, dbPoints uint64) bool`.
4. `Import()` = отказ `ARCHIVE_ACTION=restore` (архива нет) → DDL → URL (ретраи) → тело (`util.GetFileBytesChecked`: статус в ошибке и в ретрае) → `biCpiWeeklyParseCSV` → отпечаток → `EnsurePublications` → `PublicationKnown` → no-op (`AfterImport` + `0, nil`) или полный путь (батч по `s.TableName`, `Abort` на ошибке, `AfterImport`, `SavePublication`, `count`).
5. Инвариант: полный путь с `count == 0` → ошибка.
6. Тесты `Import()` на фейковом `driver.Conn`: no-op без INSERT, пустой батч → ошибка, `Abort` на сбое `Append`, отказ в режиме restore.

## Шаг 3.1. `util.GetFileBytesChecked`

Статус-проверяющий GET для одноразового `tmp/exp<uuid>`: `GetFile`/`GetFileBytes` статус не проверяют
(их потребители качают XLSX по стабильным ссылкам), а здесь 5xx-заглушка выглядела бы «успешными»
байтами и падала бы уже в разборе, вне ретрая. Тест — `util/xls_test.go`.

## Шаг 4. Фикстуры

- `rosstat/testdata/bi_cpi_weekly_rpc_ok.txt` — живой ответ RPC.
- CSV-фикстура: добавить строку `19.10.26 … 100.05` (spot) и ожидания в тестах парсера.

## Шаг 5. Реестр и комментарии

- `rosstat/importers.go`: комментарий про BI — версия пишется по хешу содержимого (литерал `countPoints` остаётся пустым).
- `rosstat/importers_test.go`: переписать комментарии «у BI нет строки версии».
- `weeklysa/importer.go`: оговорка «published_at может быть NULL» → у BI теперь есть строка версии.

## Шаг 6. Документация

- `docs/ROADMAP_KEY_RATE.md` — протокол в пункт `phase-2-source-fixes` (строка 220).
- `ARCHITECTURE.md` §6.1 (строка `rosstat_bi_cpi_weekly`), §8 п. 13, §7 (шаг `dagu/rosstat.yaml`).
- `docs/IMPORTERS.md` (описание импортёра).
- `dagu/rosstat.yaml` — комментарий шага.

## Шаг 7. Приёмка

1. `make all`.
2. `make import STAT=rosstat_bi_cpi_weekly` → `Imported N rows`; MCP: `count() FINAL`, `min/max(date)`, spot `19.10.2026`.
3. Повтор → `Imported 0 rows`, `count() FINAL` без изменений.
4. `make mcp-check`, `dagu validate dagu/rosstat.yaml`, `make docs-check`.
5. Протокол — комментарием в PR, `Closes #338`.
