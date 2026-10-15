# Task 3 — импортёр `keyrate_futures`: отчёт

Дата: 2026-10-13. Ветка `key-rate-forecast`, база `4b521da`, коммит задачи `08f2782`.
Инструменты: `mcp__jetbrains__*` (проверка правок) и `mcp__clickhouse__*` (диагностика SQL) —
оба доступны и использованы.

## 1. Что сделано

### `moex/keyrate.go`

* **DDL двух таблиц** (`keyrateDdl`, `keyrateContractDdl`) — дословно из брифа (без inline-комментариев,
  как принято в Go-копиях DDL этого репозитория: `ofz_curve`, `news_events`). Схема сверена со
  спекой §«Схема» программно (скрипт снимает `--`-комментарии из блока спеки): **совпадение
  колонка-в-колонку, включая `ENGINE = ReplacingMergeTree ORDER BY`**.
* **Витрина `v_keyrate_expectations`** через `util.CreateView`; `Tables` = `keyrate_futures`,
  `keyrate_contract`, `cbr_key_rate` (последняя — чтобы витрина не публиковалась без ряда ставки).
  10 колонок, у каждой — комментарий (правило 11б), комментарий таблицы тоже.
* **`Import`** — порядок шагов контракта: (1) два `conn.Exec(DDL)` до сети; (2) `fetchKeyrateSecids` →
  `validSecid` → `fetchKeyrateContract` по каждой серии; (3) один батч паспортов
  (`secid, series_name, first_trade, last_trade, now()`); (4) по каждой серии
  `keyrateStoredDate` → `keyrateSince` → `fetchKeyrateHistory` → строки в батч дней;
  (5) `Send` обоих батчей и `log.Infof("Собрано %d дней по %d сериям KEYRATE", count, len(contracts))`;
  (6) `util.UpsertSeriesCatalog` **после** успешного `Send`; (7) `util.CreateView`.
  Вставка — только `PrepareBatch → Append` (каждый `Append` с проверкой ошибки) `→ Send`.
* **`keyrateSince(c, stored)`** — `stored` (zero) → `c.firstTrade`; иначе `stored.AddDate(0, 0, 1)`.
* **`keyrateSeriesMeta(contracts)`** — одна запись `util.SeriesMeta` на серию: `source='moex'`,
  `series=secid`, `unit="п.п. ключевой ставки"`, `frequency="D"`, в `Origin` — дата экспирации
  (`LSTDELDATE`) и `FRSTTRADE`, в `Description` — мнемоника из спеки §«Мнемоника перевода в ожидания»
  (вменённый уровень на дату экспирации; `Δ = (settle − ставка) × 100 б.п.` к последней известной
  `cbr_key_rate`; шаг между заседаниями — разность `settle` соседних серий; вероятностная
  интерпретация `P(−25) ≈ (ставка − settle) / 0.25` с оговоркой, что полного распределения из одного
  числа не получить), множитель 1 п.п. = 1 000 ₽, низкая ликвидность и «`LAST` не используется».
* **`type keyrateFutures struct{}`** + `Name()` = `keyrate_futures` + `init()` → `chimport.Stats`.
* **Обработка ошибок по сериям** (как ленты в `news_watch`): сбой серии → `log.Warnf` с `secid` и
  продолжение, ошибка накапливается в `seriesErrs` и возвращается `errors.Join` в конце прогона;
  если паспорта не получены ни по одной серии, или ISS не отдал ни одной серии — ошибка импорта
  до публикации каталога и витрины. Каталог/витрина публикуются, когда хотя бы одна серия собрана.

### Решения контроллера, применённые дословно

1. Спот — **скалярный подзапрос** (`round(f.settle - (SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 4)`),
   `CROSS JOIN` в витрине нет; `cbr_key_rate` остался в `util.View.Tables`. Тест
   `TestKeyrateViewContract` фиксирует и наличие скалярного подзапроса, и отсутствие `CROSS JOIN`.
2. `minstep`/`stepprice` в схему не заводятся: множитель 1 п.п. = 1 000 ₽ (шаг 0,01 п.п. = 10 ₽)
   живёт в комментариях витрины (колонка `secid`) и в `Description` каталога. Тест
   `TestKeyrateDdlHasNoMinStepColumns`.

### Паркованная находка Task 1–2 (исправлена)

`secid` форматировался в путь ISS без проверки, а `validSecid` использовался только тестами.

* `fetchKeyrateSecids` теперь **фильтрует** коды, не проходящие `validSecid`, с `log.Warnf`
  по каждому отброшенному (молчаливая потеря серии недопустима) — закрывает вход, из которого
  код шёл в URL и в SQL-условие инкремента;
* в `Import` остался явный барьер `!validSecid(secid) → warn + ошибка серии` (шаг 2 брифа);
* введён `keyrateStoredDate(ctx, conn, c)` — единственное место, где `secid` подставляется в
  **SQL**-условие `lastDate`: проверка `validSecid` выполняется **до** обращения к базе;
* дополнительно `fetchKeyrateContract` и `fetchKeyrateHistory` отвергают недопустимый `secid`
  (оба тоже подставляют его в путь запроса) — маленькое расширение сверх буквы задания,
  потому что это ровно те строки, на которые указало ревью.

Тесты: `TestFetchKeyrateSecidsFiltersInvalid` (коды `KKV6; DROP TABLE …--` и `KK V6` отсеиваются,
валидные остаются) и `TestKeyrateSecidValidatedBeforeRequestAndSQL` (недопустимый код не доходит
ни до сети — httptest-обработчик валит тест при любом запросе, — ни до SQL: `keyrateStoredDate`
с `nil`-conn возвращает ошибку, не касаясь базы).

## 2. TDD

### RED (до реализации)

```
$ go test ./moex/ -run 'KeyrateSince|KeyrateSeriesMeta|KeyrateName|KeyrateViewContract|KeyrateDdl|FetchKeyrateSecidsFilters|KeyrateSecidValidated' -v
# github.com/kmlebedev/clickhouse-import-rosstat/moex [github.com/kmlebedev/clickhouse-import-rosstat/moex.test]
moex/keyrate_test.go:227:14: undefined: keyrateSince
moex/keyrate_test.go:240:10: undefined: keyrateSeriesMeta
moex/keyrate_test.go:274:14: undefined: keyrateFutures
moex/keyrate_test.go:274:47: undefined: keyrateTable
moex/keyrate_test.go:275:41: undefined: keyrateTable
moex/keyrate_test.go:284:9: undefined: keyrateView
… (too many errors)
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/moex [build failed]
```

### GREEN (после реализации)

```
$ go test ./moex/ -run 'Keyrate' -v -count=1     # фрагмент
--- PASS: TestKeyrateSince (0.00s)        (подтесты: «день после сохранённого», «пустая таблица — с начала обращения»)
--- PASS: TestKeyrateSeriesMetaPerContract (0.00s)
--- PASS: TestKeyrateName (0.00s)
--- PASS: TestKeyrateFuturesIsRegistered (0.00s)
--- PASS: TestKeyrateViewContract (0.00s)
--- PASS: TestKeyrateDdlHasNoMinStepColumns (0.00s)
--- PASS: TestFetchKeyrateSecidsFiltersInvalid (0.00s)
--- PASS: TestKeyrateSecidValidatedBeforeRequestAndSQL (0.00s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	0.412s
```

Проверки на финальном коммите:

| Команда | Результат |
|---|---|
| `go test ./moex/... -race -count=1` | `ok github.com/kmlebedev/clickhouse-import-rosstat/moex 1.579s` |
| `gofmt -l moex/` | пусто |
| `go vet ./moex/` | без замечаний |
| `golangci-lint run ./moex/...` | `0 issues.` |
| `go build ./...` | OK (импортёр удовлетворяет `chimport.ImportStat`) |
| `mcp__jetbrains__get_file_problems` (`keyrate.go`, `keyrate_test.go`) | ошибок нет; только три IDE-WARNING «SQL dialect is not configured» на SQL-строках (DDL/Select) |

`TestKeyrateName`, `TestKeyrateFuturesIsRegistered` и `TestKeyrateDdlHasNoMinStepColumns` — добавлены
сверх обязательного минимума брифа (первые два — по образцу `bold/price_test.go`), они прошли с
первого запуска и RED-фазы не имели.

## 3. Живая проверка SQL (без записи в базу)

Писать в ClickHouse я не пытался (ни `CREATE TABLE`, ни `CREATE VIEW`), но read-only MCP-пользователь
позволяет прогнать `DESCRIBE (<запрос>)` — это разбор и анализ запроса без его выполнения.
Выяснилось важное:

1. **Буквальная SQL-витрина из брифа синтаксически неверна.** Форма
   `FROM keyrate_futures FINAL f` (и `INNER JOIN keyrate_contract FINAL c`) — это
   `SYNTAX_ERROR` в ClickHouse 26.10.1:

   ```
   DESCRIBE (SELECT f.date FROM keyrate_futures FINAL f LIMIT 1)
   Code: 62. DB::Exception: Syntax error: failed at position 52 (f) …
   Expected one of: SAMPLE, STREAM, table, … JOIN, …, alias, AS. (SYNTAX_ERROR)
   ```

   Рабочая форма — алиас **до** `FINAL`. Поэтому в коде витрина записана как
   `FROM keyrate_futures AS f FINAL` / `INNER JOIN keyrate_contract AS c FINAL ON …`.
   Это единственное отклонение от буквального текста брифа.
2. **Полная строка витрины разбирается.** Взятый из `moex/keyrate.go` `keyrateView.Select`
   прогнан через `DESCRIBE` — ошибка только семантическая (`UNKNOWN_TABLE: keyrate_futures`,
   таблиц ещё нет), синтаксических ошибок нет.
3. **Семантика формы проверена на живых отношениях-заглушках** (16 колонок/типов не важны, важен
   разбор): `FROM <view> AS f FINAL INNER JOIN <view> AS c FINAL ON …` плюс скалярный подзапрос
   с `FINAL` и `round(...)` — `DESCRIBE` вернул ровно 10 ожидаемых колонок; типы:
   `Date`, `LowCardinality(String)`, `String`, `Date`, `Float64`, `Float64`, `UInt64`, `UInt64`,
   `Nullable(Float64)`, `Nullable(Float64)`.
4. **Что делает пустая `cbr_key_rate`** (проверено тем же способом): `argMax` по пустому множеству
   возвращает `0`, а не `NULL` (тип колонки при этом `Nullable(Float64)` — артефакт скалярного
   подзапроса; NULL в значении не появляется). То есть при не собранном `cbr_key_rate` значения
   вырождаются в `settle × 100`. Это зафиксировано в комментариях колонок `expected_change_pp/bp`
   («при пустой cbr_key_rate подзапрос отдаёт 0 и значение вырождается»).
5. **DDL разбирается:** `EXPLAIN AST CREATE TABLE …` для обоих DDL не даёт `SYNTAX_ERROR`
   (контрольный запрос с намеренной ошибкой в DDL даёт `SYNTAX_ERROR`, то есть проверка различает
   случаи). Вывод `EXPLAIN AST` сам не декодируется клиентом MCP (`Unrecognized ClickHouse type
   base: ColumnDeclaration`) — это особенность сериализации AST, не ошибка DDL.

## 4. Изменённые файлы

| Файл | Что |
|---|---|
| `moex/keyrate.go` | `validSecid`-барьеры в транспорте; DDL, витрина, `keyrateFutures` (`Name`/`Import`), `keyrateSince`, `keyrateStoredDate`, `keyrateSeriesMeta`, `init()` |
| `moex/keyrate_test.go` | 8 новых тестов: `TestKeyrateSince`, `TestKeyrateSeriesMetaPerContract`, `TestKeyrateName`, `TestKeyrateFuturesIsRegistered`, `TestKeyrateViewContract`, `TestKeyrateDdlHasNoMinStepColumns`, `TestFetchKeyrateSecidsFiltersInvalid`, `TestKeyrateSecidValidatedBeforeRequestAndSQL` |

`deploy/staging/dagu.service` остался незастейдженным (предсуществующая правка, в коммит не попала:
`git status --short` после коммита показывает только его).

## 5. Самопроверка диффа

* Схема/`Import`/каталог/витрина — только в `moex/`, новых зависимостей нет, сетевых вызовов в
  `init()` нет, `fmt.Print` нет, построчных `conn.Exec` в цикле нет.
* Порядок шагов `Import` совпадает с контрактом брифа (включая «DDL до сети» и «каталог после `Send`»).
* `log.Infof("Собрано %d дней по %d сериям KEYRATE", …)` — дословно из брифа; предупреждения по
  сериям — с `secid`, ошибки не проглатываются (`errors.Join` в конце прогона).
* Ошибка `batch.Append` прерывает импорт (не «проглатывается»), оба батча открыты одновременно
  (`lastDate` при открытом батче — тот же приём, что в `ofz_curve`).
* Тесты не используют сеть, кроме `httptest`; `keyrateStoredDate` в тесте вызывается с `nil`-conn
  (проверка `validSecid` стоит до обращения к базе) — это осознанно и прокомментировано в тесте.

## 6. Замечания и риски

1. **Витрина не выполнена на живой базе** (`CREATE VIEW` и `SELECT` не запускались — нет прав на
   запись и нет самих таблиц). Всё, что доступно без записи, проверено `DESCRIBE`/`EXPLAIN AST`
   (см. §3). Остаточный риск — только ошибки времени выполнения; форма (join двух `FINAL`-отношений
   + скалярный подзапрос + `round`) на заглушках анализируется и исполняется.
2. **Правка брифа наружу:** в `docs/superpowers/plans/2026-10-13-moex-keyrate.md` (Task 3, Step 3) и,
   вероятно, в спеке §«Схема» текст витрины содержит неисправную форму `FROM … FINAL f`. В Task 4
   при переносе витрины в `ARCHITECTURE.md` §6.3 нужно брать форму из кода (`AS f FINAL`) — иначе
   копия в документации разойдётся с рабочей витриной.
3. **Пустая `cbr_key_rate`** даёт вырожденные значения (`settle × 100`) — задокументировано в
   комментариях колонок, но не защищено кодом (можно усилить до `if(count() = 0, NULL, …)`;
   в объём Task 3 не входит).
4. **Частичный сбой серий** возвращает ошибку прогона, хотя успешные серии уже записаны, а каталог и
   витрина опубликованы (поведение `news_watch`). Это осознанно: dagu перезапустит прогон, повторная
   запись тех же строк идемпотентна по ключу `(secid, date)`.
5. **Серия без паспорта** (ошибка `fetchKeyrateContract`) не попадает ни в `keyrate_contract`, ни в
   каталог в этом прогоне — запись появится при следующем прогоне; в логе и в ошибке серия названа.
6. **DDL сверен со спекой как текст** (совпадение колонка-в-колонку) и разобран `EXPLAIN AST`;
   `CREATE TABLE` на живом стенде не выполнялся по условию задания.
7. Живая приёмка (`make import STAT=keyrate_futures`, идемпотентность, `≈ −7 б.п.` по `KKV6`,
   `make mcp-check`, `dagu validate`) — Task 5, не выполнялась: секреты и запуск импортёров мне
   недоступны по условиям задания.

---

# Fix report (ревью Task 3, коммит `5f5b95c`)

Исходная задача — `08f2782`. Ревью: **Needs fixes**, два Important, одно ⚠️ и три minor.
Ниже — что изменено по каждому пункту, покрывающие тесты и их вывод. Стенд: ClickHouse 26.10.1
через read-only MCP (запись не выполнялась), GoLand MCP — проверка правок.

## Important 1 — публикация до проверки результата (исправлено)

`moex/keyrate.go` (`Import`): введён счётчик `okSeries` — серия считается собранной, когда
`keyrateStoredDate` и `fetchKeyrateHistory` по ней отработали без ошибки (пустой срез дней — тоже
успех: серия без сделок — нормальное состояние `KKG7`). Если `okSeries == 0`

```go
	if okSeries == 0 {
		// Сбой всех серий — ошибка импорта, и она возвращается ДО публикации: пустой прогон
		// не должен отправлять батчи, писать каталог и пересоздавать витрину.
		return count, errors.Join(
			fmt.Errorf("keyrate_futures: дни не собраны ни по одной из %d серий", len(contracts)),
			errors.Join(seriesErrs...))
	}
```

возврат происходит **до** `contractBatch.Send`, `dayBatch.Send`, `UpsertSeriesCatalog` и
`CreateView`. Толерантность к отдельной серии сохранена: сбой серии — `log.Warnf` с `secid`,
продолжение по остальным, ошибка накапливается в `seriesErrs` и возвращается в конце прогона.

## Important 2 / Ruling 4 — комментарий описывает то, что делает код (исправлено, поведение не менялось)

Комментарий у `keyrateSeriesMeta(contracts)` переписан по факту:

```go
	// Каталог — после успешного Send и на КАЖДУЮ обнаруженную серию (её паспорт), в том числе
	// на серию без дневных итогов: инструмент существует, его ряд должен быть виден агенту ещё
	// до первых сделок — так требует план («одна запись на обнаруженную серию»).
```

`keyrateSeriesMeta(contracts)` по-прежнему получает все паспорта — как предписывает план
(«одна запись на обнаруженную серию»); предложение ревью передавать только успешные серии
отклонено решением контроллера 4. Это поведение теперь закреплено тестом
`TestKeyrateImportPartialFailurePublishes`: при сбое одной из двух серий в каталоге ровно 2 записи
(`KKV6` и `KKZ6`), хотя дни есть только по `KKV6`.

## ⚠️ / Ruling 5 — пустая `cbr_key_rate` даёт NULL, а не `settle × 100` (исправлено)

В `keyrateView.Select` спот обёрнут в `nullIf(…, 0)` в обеих вычисляемых колонках:

```
round(f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0), 4) AS expected_change_pp,
round((f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)) * 100, 1) AS expected_change_bp
```

Комментарии переписаны: `expected_change_pp` — «NULL означает, что ставка ещё не загружена
(cbr_key_rate пуста): вменённое значение неизвестно, нулём его считать нельзя»;
`expected_change_bp` — «NULL (cbr_key_rate пуста, ставка ещё не загружена) значит «неизвестно»,
а не нулевое изменение»; в комментарии витрины добавлена фраза, что `expected_change_pp/bp` равны
NULL, пока `cbr_key_rate` пуста. Поведение при **нулевой** ставке (ставка РФ никогда не была 0)
не меняется — 0 используется как сигнал «ряд ещё не загружен».

## Minors (все три взяты)

* `log.Infof("Собрано %d дней по %d сериям KEYRATE", count, okSeries)` — счётчик серий теперь
  считает успешные серии, а не все паспорта (в логе теста: «Собрано 3 дней по 1 сериям KEYRATE»).
* `type keyrateFutures struct{}` — однострочная запись.
* Три сообщения `"keyrate: invalid secid %q"` → `"keyrate: недопустимый код серии %q"`
  (`fetchKeyrateContract`, `fetchKeyrateHistory`, `keyrateStoredDate`); `grep -n "invalid secid" moex/*.go`
  — пусто.

## Покрывающие тесты

Добавлены два теста на весь путь `Import` поверх фейкового соединения (образец — `bold/bold_test.go`,
`news/news_test.go`: встраивание `driver.Conn` + переопределение `Exec`/`QueryRow`/`PrepareBatch`):
`keyrateFakeRow` (пустой `max(date)` = epoch, как у настоящего драйвера, и счётчик существующих
таблиц для `util.CreateView`), `keyrateFakeBatch` (rows + sent), `keyrateFakeConn`
(calls, batches). ISS-стенд — `useKeyrateIssServer(t, secids, failing)`: две серии, паспорт по
шаблону, сбой истории по указанным secid.

* `TestKeyrateImportAllSeriesFailedBeforePublication` — сбой истории по обеим сериям: ошибка
  содержит «ни по одной», `count == 0`, среди вызовов нет `series_catalog` и
  `v_keyrate_expectations`, открыто ровно 2 батча и ни один не отправлен.
* `TestKeyrateImportPartialFailurePublishes` — сбой истории по `KKZ6`: ошибка называет `KKZ6`,
  `count == 3` (дни `KKV6` из фикстуры), батч паспортов отправлен с 2 строками, батч дней — с 3,
  каталог описывает `[KKV6 KKZ6]`, витрина создана (`CREATE OR REPLACE VIEW v_keyrate_expectations`).
* `TestKeyrateViewContract` расширен: `nullIf`-обёртка встречается ровно 2 раза, и комментарии
  `expected_change_pp/bp` упоминают NULL.

### RED: новые тесты ловят старое поведение

Старый `moex/keyrate.go` (`git show HEAD:moex/keyrate.go`) + новые тесты:

```
--- FAIL: TestKeyrateViewContract (0.00s)
    keyrate_test.go:323: nullIf-обёртка спота встречается 0 раз, want 2 (expected_change_pp и expected_change_bp)
    keyrate_test.go:328: комментарий expected_change_bp не объясняет NULL при пустой cbr_key_rate: …
--- FAIL: TestKeyrateImportAllSeriesFailedBeforePublication (0.00s)
    keyrate_test.go:587: ошибка не называет причину: KKV6: /iss/history/…/KKV6.json?…: http status 500
    keyrate_test.go:594: до публикации дело дошло: exec: CREATE TABLE IF NOT EXISTS series_catalog (…
    keyrate_test.go:594: до публикации дело дошло: preparebatch: INSERT INTO series_catalog
    keyrate_test.go:594: до публикации дело дошло: exec: CREATE OR REPLACE VIEW v_keyrate_expectations …
FAIL
```

То есть на старом коде пустой прогон действительно создавал каталог и витрину — ровно то, на что
указало ревью.

### GREEN: покрывающие тесты

```
$ go test ./moex/ -run 'TestKeyrateImportAllSeriesFailedBeforePublication|TestKeyrateImportPartialFailurePublishes|TestKeyrateViewContract|TestKeyrateSince|TestKeyrateSeriesMetaPerContract|TestFetchKeyrateSecidsFiltersInvalid|TestKeyrateSecidValidatedBeforeRequestAndSQL|TestKeyrateName|TestKeyrateFuturesIsRegistered|TestKeyrateDdlHasNoMinStepColumns' -v -count=1
--- PASS: TestKeyrateSince (0.00s)
--- PASS: TestKeyrateSeriesMetaPerContract (0.00s)
--- PASS: TestKeyrateName (0.00s)
--- PASS: TestKeyrateFuturesIsRegistered (0.00s)
--- PASS: TestKeyrateViewContract (0.00s)
--- PASS: TestKeyrateDdlHasNoMinStepColumns (0.00s)
--- PASS: TestFetchKeyrateSecidsFiltersInvalid (0.00s)
--- PASS: TestKeyrateSecidValidatedBeforeRequestAndSQL (0.00s)
--- PASS: TestKeyrateImportAllSeriesFailedBeforePublication (0.00s)
--- PASS: TestKeyrateImportPartialFailurePublishes (0.00s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	0.284s
```

Полный прогон пакета и линтеры (один раз перед коммитом):

```
$ go test ./moex/... -race -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	1.321s
$ golangci-lint run ./moex/...
0 issues.
$ gofmt -l moex/        # пусто
$ go vet ./moex/        # пусто
```

Проверка правок в IDE (`get_file_problems`): `keyrate_test.go` — без замечаний; `keyrate.go` —
только три IDE-WARNING «SQL dialect is not configured» на SQL-строках.

## Живая проверка изменённой витрины (read-only, без создания таблиц)

Техника: `DESCRIBE (<запрос>)` для тела витрины. Ошибка `UNKNOWN_TABLE` = парсер запрос принял,
`SYNTAX_ERROR` (код 62) = нет.

**До правки** (текст `Select` из `08f2782`):

```
DESCRIBE (SELECT f.date AS date, …, round(f.settle - (SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 4) AS expected_change_pp, … FROM keyrate_futures AS f FINAL INNER JOIN keyrate_contract AS c FINAL ON f.secid = c.secid)
→ Code: 60. DB::Exception: Unknown table expression identifier 'keyrate_futures'. … (UNKNOWN_TABLE)
```

**После правки** (текст `Select` из `5f5b95c`, извлечён из файла скриптом, `nullIf` — 2 вхождения):

```
DESCRIBE (SELECT f.date AS date, f.secid AS secid, c.series_name AS series_name,
       c.last_trade AS decision_date, f.settle AS settle, f.close AS close,
       f.volume AS volume, f.openposition AS openposition,
       round(f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0), 4) AS expected_change_pp,
       round((f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)) * 100, 1) AS expected_change_bp
FROM keyrate_futures AS f FINAL
INNER JOIN keyrate_contract AS c FINAL ON f.secid = c.secid)
→ Code: 60. DB::Exception: Unknown table expression identifier 'keyrate_futures'. … (UNKNOWN_TABLE)
```

**Работоспособность `nullIf`-обёртки на живом отношении-заглушке** (спот «загружен» и «пуст»):

```sql
SELECT
  round(toFloat64(13.94) - nullIf((SELECT argMax(value, date) FROM v_bea_pce FINAL), 0), 4) AS pp_loaded, isNull(pp_loaded),
  round(toFloat64(13.94) - nullIf((SELECT argMax(value, date) FROM v_bea_pce FINAL WHERE date < toDate('1900-01-01')), 0), 4) AS pp_empty, isNull(pp_empty),
  round(toFloat64(13.94) - (SELECT argMax(value, date) FROM v_bea_pce FINAL WHERE date < toDate('1900-01-01')), 4) AS pp_empty_unguarded, isNull(pp_empty_unguarded)
```

```
pp_loaded | pp_loaded_is_null | pp_empty | pp_empty_is_null | pp_empty_type     | pp_empty_unguarded | …_is_null
-117.639  | 0                 | NULL     | 1                | Nullable(Float64) | 13.94              | 0
```

Вывод: при загруженной ставке значение считается как раньше; при пустой — `nullIf` даёт NULL
(`isNull = 1`), тогда как необёрнутый вариант отдавал бы `13.94` (то самое `settle − 0`,
«≈ +1390 б.п.»). Это и есть цель решения 5.

## Остаток

* Живые `CREATE VIEW`/`SELECT` по-прежнему не выполнялись (нет таблиц и прав на запись) — витрина
  проверена анализом запроса и прогоном эквивалентной формы на заглушках; полноценная приёмка — Task 5.
* Правка брифа наружу из первого отчёта остаётся в силе: в плане (Task 3, Step 3) и, вероятно, в
  спеке §«Схема» витрина записана в неисправной форме `FROM … FINAL f`; актуальный текст — в коде
  (`AS f FINAL` + `nullIf`-обёртка), по нему же надо обновлять `ARCHITECTURE.md` §6.3 в Task 4.
