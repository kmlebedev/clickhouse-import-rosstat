# Task 2 report — импортёр `bold_etf_flows` (инкремент вместо ежедневной перезаписи)

- **Статус:** DONE_WITH_CONCERNS (после fix round 1 — §7; контракт брифа выполнен целиком: все 13 названных тестов есть, плюс 11 своих); открыто одно унаследованное сомнение — §6.1 (единицы `boldStoredUnits`: тонны базы → унции); §6.3 (семантика «дубликата даты между файлами») и §6.4 (8 запросов на полном прогоне) закрыты подтверждением ревью и правкой плана/спеки контроллером.
- **Ветка:** `feat/bold-t1-gold`, BASE = `ba6bcf4`.
- **Коммиты:**
  - `5ee7bb0c58c2b00d931218cf21f820795937aaf0` — `add bold_etf_flows importer with incremental fetch` (4 файла, +1731: `bold/bold.go` 660, `bold/bold_test.go` 968, `bold/series_meta.go` 102, `main.go` +1). Промежуточный `feff038` (правка плана контроллером) был HEAD'ом на старте задачи и не мой;
  - `89894ae033189f8ccbf5ef44f113aa4599f6f3e6` — `bold etf flows: full fetch when series and funds diverge` (fix round 1: Ruling 8 + три Minor; 2 файла, +221/−33).
- **Тесты:** `go test ./bold/... -v -count=1` → **PASS, 41 top-level + 18 подтестов, 0 FAIL** (`ok … 0.282s`); `gofmt -l bold/ main.go` пусто; `golangci-lint run ./bold/...` → `0 issues.`; `make all` **зелёный** (`0 issues.` → `go vet ./...` → `go test -race ./...` (в т.ч. `bold` 1.747 s) → `CGO_ENABLED=0 GOOS=linux` сборка + `build/ingest`).
- **Сеть/БД в тестах:** нет — `httptest` + фикстуры `bold/testdata/` + фейковое соединение в памяти. Живой ClickHouse и живой API не трогались (живая приёмка — Task 5); `make import` не запускался.

## 1. Что сделано

### `bold/bold.go`

- Константы: `boldSource = "bold"`, `boldEtfName = "bold_etf_flows"`, `boldFundTable = "bold_fund_snapshot"`, `macroSeriesTable = "macro_series"`, `boldFullFetchGapHours = 96`; DDL `boldMacroSeriesDdl` (копия канона ARCHITECTURE §6.2 / `wgc/etf.go`, с комментарием «копии обязаны совпадать»), `boldFundDdl` (копия канона спеки §4.3, ключ `ORDER BY (date, ticker, name)` — Ruling 3), два INSERT'а.
- Гвардейские SQL — ровно как в брифе: `boldStoredSeriesMaxQuery` = `SELECT maxOrNull(date) FROM macro_series FINAL WHERE source = 'bold' AND series LIKE 'etf\_bold\_%'` (в Go-строке двойной бэкслеш), `boldStoredFundMaxQuery` = `SELECT maxOrNull(date) FROM bold_fund_snapshot FINAL`, запрос за стоком — `SELECT maxOrNull(value) FROM macro_series FINAL WHERE source = 'bold' AND series = 'etf_bold_holdings_tonnes' AND date = '<YYYY-MM-DD>'` (собирается функцией, пинится тестом целиком, вместе с датой).
- `boldEtfFlows{apiBase, now}`, `Name()`, `base()`, `clock()`, `Import(ctx, conn)` — порядок ровно из плана: (1) `boldStoredMaxDate` (ошибка чтения → `log.Warnf` + нулевое время); (2) `boldEtfFetchLatest` (4 запроса); (3) no-op при «дата источника не новее сохранённого максимума» → `log.Infof` + `return 0, nil` **до любого Exec/батча**; (4) режим: `boldEtfFullFetch` + `boldETFSets(rows, nil, time.Time{})` либо `boldStoredUnits` + `boldETFSets([]boldRow{latest}, &prevUnits, stored)`; (5) `boldFundPoints(funds, latestDate)` **до** батчей (Ruling 4 — двухзначный вызов); (6) `Exec(boldFundDdl)`, затем `Exec(boldMacroSeriesDdl)`; (7) батч `macro_series` (`PrepareBatch → Append (err) → Send`) и батч `bold_fund_snapshot` (73 строки) со своим `Abort` и проверкой каждого `err`; (8) `util.UpsertSeriesCatalog(ctx, conn, boldETFSeriesMeta())` после обоих `Send`; `log.Infof("Imported %d rows of %s (bold ETF: %d точек, %d фондов)", …)`. `count` = число точек `macro_series`.
- `boldStoredMaxDate` (Ruling 7) — **два запроса + минимум** (`boldMinDate`): ноль в любой половине уводит на полный путь. `UNKNOWN_TABLE` («таблицы ещё нет») → нулевое время, а не ошибка (см. §6.2).
- `boldStoredUnits` — `maxOrNull(value)` по ряду стока; `NULL` → ошибка `нет запасов (etf_bold_holdings_tonnes) за <date>`, а не ноль.
- `boldNeedsFullFetch(latest, storedMax)`: `storedMax.IsZero()` или `latest.Sub(storedMax).Hours() > 96`.
- `boldSourceLooksStale(latest, now)` + `clock()` — единственный потребитель поля `now`: warn T+1-застоя (> 96 ч), не ошибка (см. §6.5).
- `boldEtfFetchLatest` / `boldMergeLatest` — три ответа `*-latest` + `summary`; даты обязаны совпасть, `price` обязан быть во всех трёх и совпасть (иначе ошибка с датами/ценой и эндпоинтами).
- `boldEtfFullFetch` / `boldEtfHistory` / `boldMergeHistory` — три файла истории + `summary`; слияние по дате с проверками Ruling 5 (§6.3), строки по дате, порядок — по разобранной дате.
- Запись: `boldWritePoints` (сортировка `series, date`), `boldWriteFunds` (сортировка `ticker, name`).

### `bold/series_meta.go`

`boldETFSeriesMeta()` — 7 записей, `source='bold'`, `frequency="D"`, единицы из §4.1 (тонны / млн USD / млрд USD). Общий хвост `boldETFScope` (повторяется в каждом описании, т.к. агент читает каталог построчно) несёт обязательные оговорки: «73 фонда», «≈83 % мировой величины WGC», «не складывать и не сравнивать напрямую с `etf_holdings_tonnes_world`/`etf_flow_tonnes_world` (там все фонды мира и месячная частота)», «Окна — календарные (7/30/90 дней): сумма 5 торговых дней ≠ culm7day», «Источник — bold.report (ByteTree), ключей нет, режим щадящий».

### `main.go`

Пустой импорт `_ ".../bold"` — **в алфавитном порядке** (между `bls` и `calendar`), а не «рядом с `wgc`»: импортный блок main.go отсортирован, и вставка «рядом с wgc» его бы сломала (см. §6.7).

## 2. Тесты (`bold/bold_test.go`, 22 функции)

Названные брифом (13): `TestBoldEtfFlowsName`, `TestBoldNeedsFullFetch` (пусто→true, +1→false, +3 (Fri→Mon)→false, +4 (ровно порог)→false, +5→true, совпадение→false), `TestBoldEtfFetchLatestMismatchedDatesFails`, `TestBoldEtfFullFetchUsesHistoryPaths`, `TestBoldEtfFetchCountsRequests` (4 и 4), `TestBoldEtfHistoryMergesByDate`, `TestBoldEtfFullFetchRejectsPartialDay`, `TestBoldEtfImportFullWritesAllSeries`, `TestBoldEtfImportIncrementalWritesOneDay`, `TestBoldEtfImportNoopWhenUpToDate`, `TestBoldEtfImportMissingUnitsAtMaxFails`, `TestBoldEtfImportEmptyResponseFails`, `TestBoldEtfCatalogDescribesEverySeries`.

Свои (9): `TestBoldEtfFlowsIsRegistered` (реестр `chimport.Stats` + тип `*boldEtfFlows`, прецедент `polyus/registry_test.go`), `TestBoldSourceLooksStaleAndClock`, `TestBoldEtfFetchLatestMismatchedPricesFails`, `TestBoldEtfFullFetchRejectsConflictingBetweenFiles` (Ruling 5, вторая половина), `TestBoldStoredMaxDateIsMinOfSeriesAndFunds` (Ruling 7, таблица из 5 случаев: ряды свежее среза / срез свежее рядов / половины пусты), `TestBoldStoredMaxDateMissingFundsTableIsZero`, `TestBoldStoredMaxDateReadErrorIsError`, `TestBoldStoredQueriesPinSql`, `TestBoldStoredUnitsPinsQueryAndConverts`.

Что именно пинится, кроме значений:

- **тексты SQL** (`TestBoldStoredQueriesPinSql`, `TestBoldStoredUnitsPinsQueryAndConverts`) — у `kimi_reader` нет доступа к `macro_series`/`bold_fund_snapshot`, проверить иначе нечем; пин держит и экранирование `\_` в LIKE;
- **пути и число запросов**: инкремент = `units-latest, aum-latest, flows/all-latest, summary`, полный = `units, aum, flows/all, summary`, ни одного `-latest` на полном пути;
- **порядок обращений к соединению**: оба DDL до первого `PrepareBatch`, каталог `series_catalog` — после батчей; каталог различается по тексту INSERT (за прогон батчей три);
- **состав батчей**: 12 строк/7 рядов на полном пути и 7 строк/один день на инкременте, `source='bold'`, 73 строки фондов, три `GLD` и `units_oz = 33 552 628` у SPDR (Ruling 3), пины Task 1 (`3520.318`, `+4.233`, `573.75`);
- **форма ответа фейковой строки**: `NULL` сканируется только в указатель (`**time.Time`, `**float64`) — как `etfFakeRow` в wgc.

## 3. Саботаж-проверки (9, файл правился и восстанавливался; `md5` до и после — `b52d0de71173e19c7b89dcea3c96ee3a`)

| # | саботаж | упавший тест |
|---|---|---|
| A | `boldNeedsFullFetch` без порога (всегда `true`) | `TestBoldNeedsFullFetch` (4 подтеста) |
| B | база Δ берётся из ответа `units-latest` вместо ClickHouse (`prevUnits = *latest.Units`, `prevDate = latestDate`) | `TestBoldEtfImportIncrementalWritesOneDay`, `TestBoldEtfImportMissingUnitsAtMaxFails` |
| C | снята сверка дат трёх ответов «последнего дня» | `TestBoldEtfFetchLatestMismatchedDatesFails` |
| D | `boldMinDate` → максимум | `TestBoldStoredMaxDateIsMinOfSeriesAndFunds` (2 подтеста) |
| E | не читается максимум `bold_fund_snapshot` (нет второй половины Ruling 7) | `TestBoldStoredMaxDateIsMinOfSeriesAndFunds` (2 подтеста) |
| F | снята конверсия тонны → унции в `boldStoredUnits` | `TestBoldStoredUnitsPinsQueryAndConverts`, `TestBoldEtfImportIncrementalWritesOneDay` (поток `3520.208` вместо `4.233`) |
| G | снята проверка «день неполон» (Ruling 5) | `TestBoldEtfFullFetchRejectsPartialDay` |
| H | снят no-op (`if false && …`) | `TestBoldEtfImportNoopWhenUpToDate` |
| I | отсутствие таблицы снова ошибка (снят `boldTableMissing`) | `TestBoldStoredMaxDateMissingFundsTableIsZero` |

## 4. Команды и результаты

```
$ gofmt -l bold/ main.go                 # пусто
$ go test ./bold/... -v -count=1         # PASS 39 top-level + 17 подтестов, 0 FAIL, ok … 0.259s
$ golangci-lint run ./bold/...           # 0 issues.
$ make all                               # 0 issues. → go vet ./... → go test -race ./... → две сборки; ok …/bold 1.500s
$ git commit …                           # 5ee7bb0c58c2b00d931218cf21f820795937aaf0 (4 файла, +1731)
```

MCP:

- `mcp__jetbrains__get_file_problems` — `bold/bold.go`, `bold/bold_test.go`, `bold/series_meta.go`: **errors: []**; только три+три+ноль WARNING «SQL dialect is not configured» на строковых литералах с SQL (тот же класс инспекции, что у `wgc/etf.go`).
- `mcp__clickhouse__run_query` / `list_tables`:
  - `list_tables default like 'bold%'` → **0 таблиц**: `bold_fund_snapshot` действительно новая;
  - `SELECT source, count() FROM v_series_catalog WHERE source='bold'` → **0 строк**: каталог `bold` ещё не засеян (Task 4 ждёт 7);
  - `SELECT 'etf_bold_holdings_tonnes' LIKE 'etf\_bold\_%'` → **1**: экранирование из пиннутого запроса работает;
  - `v_series_catalog` по `etf_holdings_tonnes_world`/`etf_flow_tonnes_world` → `frequency = M`, `unit = тонны` — формулировка «не складывать … (там все фонды мира и месячная частота)» в описаниях соответствует соседним рядам.

Границы: `go.mod`/`go.sum` не тронуты, новых зависимостей нет; HTTP только `util.GetJSON`; запись только батчами с проверкой каждого `err`; секреты не читались; логирование — `logrus`.

## 5. Что Task 3–5 получают как есть

- `boldSource`, `boldFundTable`, `boldFundDdl`, `boldNeedsFullFetch`, `boldStoredMaxDate`/`boldScanMaxDate` (шаблон для `boldPriceStoredMaxDate` в Task 3), `boldTableMissing`, `boldFormatDate`, `boldFetchBody`/`boldFetchLatestRow`/`boldFetchHistory` (Task 3 переиспользует на `gold/price*` и `boldMetricPrice`).
- Task 3: `boldPathPrice`/`boldPathPriceLatest` из Task 1 в Task 2 не используются (ожидаемо); копия DDL `gold_prices` — по образцу `boldMacroSeriesDdl`.
- Task 4: каталог `bold` = 7 записей; витрина фондов обязана сортировать `date DESC, ticker, name`; сырая `bold_fund_snapshot` агенту не выдаётся.
- Task 5: `make import STAT=bold_etf_flows` на пустой базе → полный путь: в логе `полный режим: сохранённый максимум (таблицы пусты), дата источника 2026-10-09, дней истории …`, затем `Imported N rows of bold_etf_flows (bold ETF: M точек, 73 фондов)`.
- Ручная правка для Task 5 (вне моего объёма): список `source` в комментарии DDL `macro_series` (ARCHITECTURE §6.2 и копия в `ingest/schema.go`) стоит дополнить `bold`.

## 6. Сомнения и отклонения от буквы брифа

### 6.1 `boldStoredUnits` возвращает **унции**, конверсия из тонн — внутри функции (важно для ревью)

Ряд `etf_bold_holdings_tonnes` в базе в тоннах, а `boldETFSets` (Task 1, заморожен) считает Δ в унциях источника. Если отдать ему тонны как есть, Δ будет в 32150 раз меньше унции: поток за 09.10 стал бы `3520.208` т вместо `+4.233` (это и показал саботаж F). Поэтому функция читает тонны и умножает на `ouncesPerTonne`. В брифе тест описан как «conn возвращает … units 113044763» — это унции **источника**, поэтому фейковое соединение отдаёт `ozToTonnes(113044763) = 3516,0815…` т (то есть то, что реально лежит в `macro_series`), а пины теста остались брифовыми: `+4.233` т и `573.75` млн $. Цена ошибки выбранного чтения: нет; цена обратного чтения («унции базы») — молча неверный знак и масштаб потока.

### 6.2 «Дубликат даты между файлами» (Ruling 5) — по значению, а не по факту повтора поля

Формально один и тот же день приходит из всех трёх файлов (это и есть слияние), а `price` есть во всех трёх, поэтому «поле из двух файлов = ошибка» без оговорки про цену сломало бы штатный путь. Реализовано так: поле, пришедшее из двух файлов **с разными значениями**, — ошибка с датой, обоими файлами и обоими значениями; совпавшее значение дублем не считается (запись идемпотентна по ключу). Тот же принцип, что у сверки цены на инкременте. Цена ошибки: если источник начнёт штатно отдавать расходящиеся значения в двух файлах, импорт станет красным — это и задумано (заметно в логе).

### 6.3 Дни «только с ценой» отбрасываются в `boldEtfHistory`

Строки 2015-01-01…2015-03-31 (64 дня) несут только `price`; такой день не даёт точек ни одному из 7 рядов (цену ведёт `bold_gold_price`, Task 3). Если оставить его в слиянии, проверка «день обязан иметь и `units`, и `aum`» — а у него нет ни того, ни другого — либо падала бы на живых фикстурах, либо требовала бы исключения. Поэтому «день без ETF-полей» не считается днём этого импортёра; тест `TestBoldEtfHistoryMergesByDate` ждёт ровно 2 строки (08.10, 09.10) и 12 точек.

### 6.4 **Полный прогон делает 8 запросов**, а не 4

План (и бриф) ставят `boldEtfFetchLatest` шагом 2 **всегда**: без него не узнать дату источника для решения no-op, а `boldFundPoints` датируется именно ею. Значит, полный прогон = 4 запроса «последнего дня» + 4 запроса истории; в тесте это зафиксировано как 8, а контракт «не больше 4» пинится на уровне режима (`TestBoldEtfFetchCountsRequests`: 4 и 4). Дополнительно `summary` на полном пути скачивается дважды (`boldEtfFetchLatest` и `boldEtfFullFetch`) — так велит пиннутый интерфейс `boldEtfFullFetch(baseURL) ([]boldRow, []boldFundRow, error)` (его потребляют Task 3–5) и самодостаточность полного пути. Если контроллер считает восемь запросов нарушением глобального ограничения, дешёвое лечение — не обновлять `funds` из `boldEtfFullFetch`, а брать уже полученный срез: тогда полный прогон = 7 запросов; тест на 8 поменяется в одну строку.

### 6.5 Поле `now` получило потребителя

Бриф требует поле `now func() time.Time`, но ни один шаг плана его не читает. Чтобы оно не осталось мёртвым, добавлен чисто информационный warn `boldSourceLooksStale` (> 96 ч между датой данных и днём прогона) — как guard устаревшего CDN в `wgc_etf_flows`: не отказ, перезапись тех же точек идемпотентна. Поведение данных не меняется; тест `TestBoldSourceLooksStaleAndClock` пинит и решение, и шов часов (nil → `time.Now`).

### 6.6 Полный путь не фильтрует точки по сохранённому максимуму

План (шаг 3) буквально: `boldETFSets(rows, nil, time.Time{})` — все дни истории; спека §5 говорит «пишется всё новее сохранённого максимума». Следовал плану: (а) так считает Task 5-приёмка («21 054 точки» на первом прогоне), (б) запись идемпотентна по `ReplacingMergeTree`. Побочный плюс: любой полный прогон (в т.ч. лечащий дыры, §6.7) переписывает всю историю заново. Фильтр цены (Task 3) — отдельное решение плана и на этот путь не влияет.

### 6.7 Инкремент с разрывом 1–3 дня не гарантирует отсутствие дыр

Порог 96 ч из Review Focus 3 означает: при сбое джобы на один торговый день (пример: записано за вторник, следующий прогон — четверг) импорт уйдёт на инкремент и тихо не запишет среду; поток четверга станет двухдневной дельтой. Это свойство выбранного правила «≤ 4 календарных дня», а не ошибка реализации; лечится полным прогоном (он переписывает всю историю и залечивает все прошлые дыры), который срабатывает сам при любом более длинном разрыве. Если это важно, отдельное решение контроллера: например, считать инкрементом только разрыв ≤ 1 дня. Я оставил правило брифа как есть.

### 6.8 Прочее

- `boldTableMissing` — строковая проверка `UNKNOWN_TABLE` / `does not exist` вместо `errors.As(*clickhouse.Exception)`: пакетам источников корневой пакет драйвера не нужен (как и `wgc`), тест подставляет ту же строку.
- Пустой импорт в `main.go` — в алфавитном порядке (между `bls` и `calendar`), а не рядом с `wgc`: блок отсортирован.
- «Дубликата ключа (date, ticker, name) нет» (Ruling 6) — не проверяю ни на разборе, ни при записи, как и решено в плане.
- Дублирующий лог `Imported %d rows of %s` (мой + main.go) — ровно как требует план.

---

## 7. Fix round 1 (коммит `89894ae`)

Ревью: 1 Important (Ruling 8) + 3 Minor. Пины, имена рядов, батчи, каталог, no-op-семантика не тронуты (подтверждены) — изменены только четыре названные вещи.

### 7.1 Important — Ruling 8: база дельты инкремента

`boldStoredBounds(ctx, conn) (seriesMax, fundMax time.Time, err error)` — новый хелпер: читает обе половины сохранённого максимума (два запроса, оба через `boldScanMaxDate`); `boldStoredMaxDate` осталась обёрткой над ним и по-прежнему отдаёт минимум (Ruling 7 сохранён, её тесты не менялись). `Import` берёт обе даты, `stored = boldMinDate(seriesMax, fundMax)`, а условие полного пути стало

```go
if boldNeedsFullFetch(latestDate, stored) || diverged {   // diverged = !seriesMax.Equal(fundMax)
```

Обоснование в коде: инкремент посчитал бы Δ от ряда, отставшего на торговый день (срез 08.10, ряды 09.10), — то есть многодневный поток, выданный за дневной, и молча, потому что база `stored` выглядит «предыдущим торговым днём». Полный путь пишет и ряды, и срез за дату источника (при обычном совпадении максимума истории с датой источника это S = F = L), после чего предусловие инкремента снова выполнено. Отдельная ветка лога: «ряд и срез разошлись … — выравниваю по дате источника»; и warn для редкого случая «расхождение есть, но источник не новее обоих» (тогда догонять нечего и прогон остаётся no-op).

Новый тест `TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge`: фейковое соединение отдаёт `seriesMax = 2026-10-09`, `fundMax = 2026-10-08`, третий ответ (за стоком) не предусмотрен вовсе — если прогон уйдёт инкрементом, импорт упадёт. Проверяется: 12 строк в батче `macro_series` (оба дня истории, а не один), 73 строки среза с датой источника `09.10`, оба запроса максимумов, обе DDL, оба `preparebatch`, **ни одного** запроса за предыдущим стоком, все четыре пути истории запрошены по разу. Существующий `TestBoldEtfImportIncrementalWritesOneDay` (S = F = 08.10) не изменён по существу — только комментарии (в них теперь описаны обе ветки).

**Саботаж (требование ревью):** возврат условия без `|| diverged` →

```
$ python3 -c '… s.replace("if boldNeedsFullFetch(latestDate, stored) || diverged {", "if boldNeedsFullFetch(latestDate, stored) {") …'
$ go test ./bold/... -count=1 -run TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge
--- FAIL: TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge (0.00s)
    bold_test.go:894: Import: bold_etf_flows: нет запасов (etf_bold_holdings_tonnes) за 2026-10-08 — дневной поток (Δ) посчитать нечем
FAIL
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/bold	0.369s
```

(Без расхождения импорт уходит в инкрементную ветку и читает сток за `stored` — тест это и ловит: в списке вызовов появляется запрос за стоком, а батч `macro_series` содержал бы 7 строк вместо 12.)

### 7.2 Minor — no-op больше не обещает, а проверяет отсутствие DDL

`TestBoldEtfImportNoopWhenUpToDate` дополнительно требует, что обращений к соединению ровно два (оба — чтения максимумов) и что среди них нет ни одного `exec: ` (раньше проверялись только `preparebatch: `). Саботаж «выполнить DDL до no-op-возврата» (`_ = conn.Exec(ctx, boldFundDdl)` перед `log.Infof`) валит тест:

```
--- FAIL: TestBoldEtfImportNoopWhenUpToDate (0.00s)
    bold_test.go:…: DDL выполнялась на no-op прогоне: [queryrow: … queryrow: … exec: CREATE TABLE IF NOT EXISTS bold_fund_snapshot ( … ]
FAIL
```

### 7.3 Minor — сортировка больше не меняет аргумент

`boldWritePoints`/`boldWriteFunds` сортируют `slices.Clone(points)` / `slices.Clone(funds)`: у вызывающего срез остаётся в том порядке, в котором он его собрал. Новый тест `TestBoldWriteKeepsInputOrder` проверяет и сохранность входного порядка, и что сам батч отсортирован (первая строка `macro_series` — `etf_bold_aum_usd_bn` 08.10, первая строка фондов — `GLD`). Саботаж «убрать клон» (обе строки и импорт `slices`) валит тест:

```
--- FAIL: TestBoldWriteKeepsInputOrder (0.00s)
    bold_test.go:1048: срез точек переставлен: [{etf_bold_aum_usd_bn 2026-10-08 …} {etf_bold_aum_usd_bn 2026-10-09 …} {etf_bold_holdings_tonnes 2026-10-09 …}] (позиция 0 = etf_bold_aum_usd_bn)
FAIL
```

### 7.4 Minor — тест часов детерминирован

Добавлены `boldTestNow` (`2026-10-10T09:00Z` — сутки после даты фикстур, меньше порога застоя) и `boldTestClock()`; все пять тестов `Import` теперь конструируют импортёра через `boldTestStat(server)` с фиксированными часами, поэтому лог (и потенциальное поведение warn-а) не зависят от дня запуска. `TestBoldSourceLooksStaleAndClock` читает реальные часы **нигде**: таблица решений — фиксированные даты (`2026-10-10`, `2026-10-12`, `2026-10-13` — ровно порог, `2026-10-16`, zero), а шов проверяется подставленным временем; ветка `now == nil → time.Now` намеренно не проверяется (её проверка читала бы реальные часы), в коде это отмечено комментарием.

### 7.5 Проверки перед коммитом (вывод)

```
$ gofmt -l bold/ main.go                 # пусто
$ go test ./bold/... -v -count=1         # PASS: 41 top-level + 18 подтестов, 0 FAIL, ok … 0.282s
$ golangci-lint run ./bold/...           # 0 issues.
$ make all                               # 0 issues. → go vet ./... → go test -race ./...
                                         #   ok …/bold 1.747s → CGO_ENABLED=0 GOOS=linux go build … → go build ./cmd/ingest
```

Новые тесты fix round 1: `TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge`, `TestBoldWriteKeepsInputOrder` (было 39 → стало 41 top-level); саботажей в этом раунде три (§7.1, §7.2, §7.3), файл после каждого восстанавливался из бэкапа — `md5 bold/bold.go` до и после саботажей `4b3e1461926f1873a76e1f91d8963ad5`.

Границы: тронуты только `bold/bold.go` и `bold/bold_test.go`. Незакоммиченные правки `docs/superpowers/plans/2026-10-12-bold-t1-gold.md` и `docs/superpowers/specs/2026-10-12-bold-t1-gold-design.md` (контроллерские: 8 запросов на полном прогоне, «пишутся все точки» полного пути) я **не коммитил** — они остались в рабочем дереве как есть, потому что их автор не я.

Открытые сомнения после fix round 1: §6.1 (единицы `boldStoredUnits` — выбранное чтение «тонны → унции»; цена обратного чтения — неверный масштаб и знак потока) и два остатка, которые ревью не просило менять: §6.7 (инкремент с разрывом 1–3 дня может оставить дыру — свойство порога 96 ч, полный путь её лечит) и §6.4 (полный прогон = 8 запросов, `summary` дважды — так требует пиннутый интерфейс `boldEtfFullFetch`; контроллер это подтвердил правкой плана/спеки).
