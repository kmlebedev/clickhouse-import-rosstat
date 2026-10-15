# Task 2 report — `craw/gost.go`: импортёр (схема, конфиг, режимы `since`/`full`/`check`, запись)

Ветка: `feat/gost-sbkts-api-importer` (основной чекаут). База: `0c0ee7c` (Task 1 — `craw/gost_api.go`).

## Что сделано

`craw/gost.go` переписан целиком: colly-код, рекурсивный обход, построчные `conn.Exec`, мёртвый
DDL `gost_vehicle_safety_certificate_type` и вписанное в комментарии живое значение токена
`gtn.access.appid` удалены. `craw/gost_api.go` не менялся (ревью Task 1 пройдено); проблем,
блокирующих Task 2, в нём не нашёл.

Создан `craw/gost_test.go` (пакет `craw`, офлайн: `httptest` + фейковое соединение).

Реализованные сущности (ровно интерфейсы брифа):

- `type gostImporter struct{}`, `Name() == gost_vehicle_safety_certificate`, `Import(ctx, conn)`;
  `init()` регистрирует импортёр в `chimport.Stats`.
- `gostConfig{Mode, From, To, SettleDays, AppID}` + `gostConfigFromEnv()`: `GOST_MODE` пусто → `since`,
  точные значения `since`/`full`/`check`, иначе ошибка; `GOST_APP_ID` через `strings.TrimSpace`
  (пусто → ошибка с подсказкой `standarts.adminsHeaders`); `GOST_SETTLE_DAYS` — `Atoi`, `< 0`/не число
  → ошибка; `GOST_FROM`/`GOST_TO` — `time.Parse("2006-01-02")`; дефолты диапазона только для
  `full`/`check`: FROM = 2016-01-01, TO = вчера МСК.
- `gostSchemaStatements()` — CREATE (копия §6.2) + 6 × `ADD COLUMN IF NOT EXISTS … AFTER …`
  (ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer, uid, publication_date).
- `gostDaysBetween` — включительно, `from > to` → ошибка, > `gostMaxRangeDays = 4000` → ошибка.
- `gostSinceDays` — якорь одним `QueryRow`: запрос из резолюции 1 (дословно), `nullIf(maxIf(…), toDate('1970-01-01'))`;
  оба NULL → `tableEmpty = true` и окно последних 7 дней; иначе `anchor = max(max_issue, max_pub)`,
  `from = anchor − settle`, `to = вчера МСК`, якорь из будущего прижат к `to`.
- `gostWriteDay` — один `PrepareBatch` на день, порядок `Append` = порядок колонок DDL,
  `Abort` только в ветке ошибки `Append`, счётчик — `batch.Rows()`.
- `gostRunDays` — ошибка дня в агрегат (обход продолжается), `Count == 0` → `log.Infof` без батча,
  `Skipped > 0` → `log.Warnf`, `time.Sleep(gostRequestGap)` между днями, итог —
  `не прочитаны дни (N): …`.
- `gostCheckDays` — счётчики таблицы одним запросом (дословно из резолюции 4), затем по дню
  `api.dayCount`; `count > gostMaxDayCount` → ошибка дня; расхождение → `api.day` и сравнение
  `len(Rows)`: совпало — «шум: N → M», не совпало — дыра; сводка «проверено N дней, дыр M»
  (`check` не открывает батчей и не публикует витрин).
- `gostLegacyRows` — `SELECT count() … WHERE publication_date = toDate('1970-01-01')`, ошибка
  фатальна; `count > 0` → `log.Warnf` с призывом `GOST_MODE=full`.
- `var gostClock = time.Now` (подмена «сейчас»; в тестах подменяется).

Порядок `Import`: разбор конфига (ошибка — до любого обращения к `conn`) → DDL-стейтменты (Exec) →
предупреждение о легаси-строках → ветка режима (`check` → сверка; `since`/`full` → дневной обход с
батчами). Витрины не публикуются — их добавит Task 3 (`gost_views.go` не создавался).

## Тесты: RED → GREEN

RED (перед реализацией), `go test ./craw/` — компиляция падает (`undefined`):

```
craw/gost_test.go:44:32: undefined: gostModeEnv
craw/gost_test.go:44:45: undefined: gostFromEnv
… FAIL github.com/kmlebedev/clickhouse-import-rosstat/craw [build failed]
```

GREEN, `go test ./craw/ -v -run TestGost`:

```
--- PASS: TestGostConfigDefaults (0.00s)
--- PASS: TestGostConfigRejectsUnknownMode (0.00s)
--- PASS: TestGostConfigRejectsBadDate (0.00s)
--- PASS: TestGostConfigRejectsBadSettle (0.00s)
--- PASS: TestGostConfigRequiresAppID (0.00s)
--- PASS: TestGostConfigRejectsEmptyRange (0.00s)
--- PASS: TestGostSchemaStatementsShape (0.00s)
--- PASS: TestGostDaysBetween (0.00s)
--- PASS: TestGostSinceDaysAnchors (0.00s)
--- PASS: TestGostWriteDayOneBatch (0.00s)
--- PASS: TestGostRunDaysSkipsEmptyAndAggregatesErrors (0.00s)
--- PASS: TestGostCheckDaysMatchNoError (0.00s)
--- PASS: TestGostCheckDaysReportsGap (0.00s)
--- PASS: TestGostCheckDaysTextNoiseNotGap (0.00s)
--- PASS: TestGostCheckDaysSkippedDayFails (0.00s)
--- PASS: TestGostCheckDaysLegacyWarnOnly (0.00s)
--- PASS: TestGostImportCheckDoesNotWrite (0.00s)
--- PASS: TestGostImportSinceEmptyTableWarns (0.00s)
--- PASS: TestGostImportUnknownModeFailsBeforeDB (0.00s)
--- PASS: TestGostImportLegacyQueryFails (0.00s)
… PASS; ok github.com/kmlebedev/clickhouse-import-rosstat/craw 0.345s
```

Плюс 14 тестов транспорта Task 1 в том же прогоне — всего 34 `--- PASS`, 0 FAIL. Добавлен один
тест сверх списка брифа: `TestGostImportLegacyQueryFails` (пинит резолюцию 2: ошибка счёта
легаси-строк фатальна и до батчей).

Команды и результаты:

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/ -timeout 300s` | `ok … 1.683s` |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| `go build ./...` | OK |
| GoLand MCP `get_file_problems` (`craw/gost.go`, `craw/gost_test.go`) | ошибок нет; в `gost.go` — только IDE-warning «SQL dialect is not configured» на двух строках SQL-констант |

## Саботаж-проверки (обе из брифа)

1. **Убрать `AddDate(0,0,-1)` из дефолта TO** (`to = gostMSKDate(gostClock().UnixMilli())`):
   `--- FAIL: TestGostConfigDefaults/full:_вся_история_по_умолчанию` —
   `gost_test.go:124: To = 2026-10-12, ожидалось 2026-10-11 (вчера МСК при часах 2026-10-12T12:00:00Z)`.
   Откат — тест снова зелёный.
2. **Вернуть `conn.Exec` в цикле вместо батча** в `gostWriteDay`:
   `--- FAIL: TestGostWriteDayOneBatch` —
   `батчей 0, ожидался один на день: [exec: INSERT INTO gost_vehicle_safety_certificate (…) exec: INSERT …]`.
   Откат — тест снова зелёный.

Отдельно всплыл и исправлен баг в тестовом фейке (полезен как доказательство офлайн-режима): `Scan`
фейковой строки `QueryRow` не возвращал подложенную ошибку, `TestGostImportLegacyQueryFails` проскочил
проверку и ушёл **в живую сеть** (тест упал по `-timeout`, в стеке — TLS-соединение к боевому
`gostAPIURL`). Исправлено: `Scan` отдаёт заданную ошибку первой. После этого весь `./craw/` проходит за
0.6 с без сети — то есть тесты офлайн не только по замыслу, но и по факту (а живой прогон проваливаться
будет громко, а не «тихо зелёным»).

## Изменённые файлы

- `craw/gost.go` — переписан целиком (124 строки colly-кода → 556 строк импортёра).
- `craw/gost_test.go` — новый (1040 строк: тесты + фейковое соединение/батч/строки и httptest-хендлеры).

Коммит: `920ff02 craw: rewrite gost importer on api day slices` (только эти два файла; без push).
- `deploy/staging/dagu.service` остаётся изменённым в рабочем дереве **до** начала работы — не моё,
  не индексировалось.

Новых зависимостей нет (`go.mod`/`go.sum` не тронуты); сетевых вызовов в `init()` нет; HTTP-кода в
`gost.go` нет (только `newGostAPI` + `util.HttpClient`).

## Само-ревью

- Резолюции 1–8 выполнены дословно, включая точные тексты запросов якоря и счётчиков check.
- Порядок колонок `Append` совпадает с DDL (проверяется `gostFakeBatch.inserted`, который разбирает
  строку по именам колонок и падает на перестановке).
- Токен: в коде и тестах нет значения, только имя заголовка/переменной; `grep -rn "gtn.access.appid"`
  находит лишь упоминания имени заголовка и подсказку «где взять».
- Идемпотентность: все DDL — `IF NOT EXISTS`; миграция живой таблицы выполняется каждый прогон.
- Комментарии и логи — по-русски, объясняют «почему» (нормализация 1970-01-01, различение
  «дыра/шум», пауза между днями, фатальность счёта легаси-строк), а не пересказывают код.
- Отличия от формулировки брифа в тестовом фейке (обоснованы сценариями): фейк хранит `batches
  []*gostFakeBatch` (а не один батч) — тесты считают батчи по дням; реализованы и `Query`
  (счётчики check), и `QueryRow` (якорь since, легаси-счёт) — одиночное значение читается
  `QueryRow`, как в живом драйвере и в `bold/bold.go`; `gostFakeRow.Scan` отдаёт подложенную ошибку.

## Сомнения / на что смотреть ревьюеру

1. **Долгое окно `since` на непробэкфиленной таблице.** Якорь включает `max(date)` (резолюция 1).
   Если у живой legacy-таблицы максимум даты выдачи — 2016 год, окно `since` станет ~3930 дней: под
   пределом `4000` (ошибки не будет) и под предупреждением о легаси-строках формально тоже (оно
   срабатывает только при `publication_date = 1970-01-01`). То есть первый плановый прогон dagu после
   деплоя может уйти в многолетний обход вместо быстрой недели. Бриф/резолюции такого гейта не
   предусматривают, и я его не добавлял (вне объёма); если это реальный риск — стоит follow-up:
   warn/лимит «якорь старше N дней → запустите `GOST_MODE=full`».
2. **`checked` в check.** Считаю дни, у которых удалось прочитать `count` ручки (день с дырой тоже
   входит, день с ошибкой ручки — нет). В брифе число `checked` пинится только для совпавшего дня.
3. **Возврат `checked` из `Import` в check-режиме**: main печатает `Imported N rows of …` и в check
   (формулировка «`Imported %d rows` делает main» из брифа — так и оставлено, ничего не пишется).
4. **Пауза `gostRequestGap` есть и в `check`** между днями (в брифе сказано только про `gostRunDays`):
   check по 4000 дней — это +~13 минут, зато ровный щадящий ритм обращений к порталу.
5. `Import` пока не публикует витрины и ничего не знает про `gostPublishViews` — это Task 3
   (в `gost_views.go` вызов нужно вставить **до** обхода дней, в пишущих режимах).

---

# Fix-раунд (ревью Task 2): три findings

Коммит: `craw: fail loud on filtered days, dedup day pages, cap since window`.
Правки — только `craw/gost.go`, `craw/gost_test.go`, `craw/gost_api.go` (дедупликация) и
`craw/gost_api_test.go` (тесты дедупликации); прод-поведение изменилось ровно по трём пунктам.

## Finding 1 — день с `Count > 0` и пустым срезом теперь ошибка прогона

`craw/gost.go`, `gostRunDays`: ветка `len(slice.Rows) == 0` больше не «warn и пропуск», а ошибка:

```go
errText := fmt.Sprintf("день %s: все %d документов отброшены фильтром публикации (Skipped %d) — проверьте формат даты публикации/ручку",
    label, slice.Count, slice.Skipped)
log.Errorf("%s: %s", gostTable, errText)
failed = append(failed, errText)
continue
```

День попадает в агрегат `не прочитаны дни (N): …` — прогон краснеет, dagu ретраит, а пропущенный
день больше не выглядит успехом. Комментарий приведён в соответствие: это не пустой день
(`Count == 0` — по-прежнему `log.Infof` без батча), а смена формата `dateofpublication`; обход при
этом продолжается (резюмируемость сохранена).

Покрывающий тест: `TestGostRunDaysFilteredDayFails` — день из одного текстового шума
(`Count: 4, Noise: 4` → `Rows == 0`) плюс нормальный день: ошибка содержит день, «все 4 документов
отброшены фильтром публикации» и «не прочитаны дни (1)», `count == 1`, батч ровно один (сбойный день
не пишется, следующий пишется).

## Finding 2 — `day()` дедуплицирует документы по `id`

`craw/gost_api.go`: новый `gostUniqueDocs(docs) (unique, dropped)` (порядок появления сохраняется),
и `day()` сравнивает с `count` **уникальные** документы; в срез дня идут они же. Текст ошибки
недостачи сохранён совместимым и теперь называет отброшенные копии:
`получено 900 документов из 1000 (дубликатов страниц: 100)`.

Покрывающие тесты (в `craw/gost_api_test.go`):
- `TestGostDayDedupsRepeatedDocuments` — страница вернула 900 документов + копию первого
  (901 документ при `count = 900`): день собирается, `Rows == 900`.
- `TestGostDayDuplicateShortageFails` — вторая страница целиком состоит из повторов первой
  (`count = 1000`, уникальных 900): ошибка с текстом
  `получено 900 документов из 1000 (дубликатов страниц: 100)`, срез пуст.
- `TestGostDayFetchesAllPages` приведён к реальности: страницы одного дня несут **разные**
  документы (id второй страницы — со сдвигом `gostPageSize+1`); заодно появился хелпер
  `gostRepeatDocFrom(doc, n, startID)`. Прежние данные теста случайно моделировали 100 повторов
  id между страницами — то есть именно тот дефект, который теперь ловится.

## Finding 3 — окно `since` ограничено 30 днями

`craw/gost.go`: `const gostMaxSinceDays = 30`. В `gostSinceDays` после выбора якоря:

```go
from := anchor.AddDate(0, 0, -settle)
if limit := to.AddDate(0, 0, -(gostMaxSinceDays - 1)); from.Before(limit) {
    log.Warnf("%s: таблица отстала: якорь %s, окно since ограничено %d днями (с %s) — запустите GOST_MODE=full для истории",
        gostTable, anchor.Format(gostDateLayout), gostMaxSinceDays, limit.Format(gostDateLayout))
    from = limit
}
```

Пустая таблица (окно 7 дней) и свежий якорь не затронуты; док-комментарий функции дополнен.

Покрывающий тест: `TestGostSinceDaysCapsStaleAnchor` — «свежий якорь» (2026-10-09, settle 3 →
6 дней 06.10…11.10) и «старый якорь» (2026-01-01 → ровно 30 дней 2026-09-12…2026-10-11 + warn с
якорем и `GOST_MODE=full`).

## Мутационные проверки каждого фикса (тест падает без правки)

| Мутация | Результат |
|---|---|
| 1: не добавлять день в `failed` (только `log.Errorf`) | `--- FAIL: TestGostRunDaysFilteredDayFails` — «день, отфильтрованный целиком, не попал в ошибку прогона» |
| 2: сравнение с `count` и срез — по сырым `docs`, без дедупликации | `--- FAIL: TestGostDayDedupsRepeatedDocuments` («строк 901, ожидалось 900») и `--- FAIL: TestGostDayDuplicateShortageFails` («ожидалась ошибка о недостаче после дедупликации»); `TestGostDayCountMismatchFails`/`TestGostDayFetchesAllPages` остались зелёными (они не о дедупликации) |
| 3: не обрезать окно (`from = limit` убран) | `--- FAIL: TestGostSinceDaysCapsStaleAnchor/старый_якорь` — «дней 287, ожидалось ровно 30» |

Все три мутации откачены, в дереве нет следов (`grep -rn "МУТАЦИ" craw/` пусто).

## Команды и вывод (после отката мутаций)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/... -timeout 300s` | `ok github.com/kmlebedev/clickhouse-import-rosstat/craw 1.546s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 38 `--- PASS`, 0 FAIL (было 34: +2 дедупликации, +1 отфильтрованный день, +1 предел окна) |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| GoLand MCP `get_file_problems` (`craw/gost_api.go`) | ошибок нет |

Тесты по-прежнему офлайн (весь пакет `./craw/` проходит за ~0.35 с без сети и БД), батчи и запреты
брифа соблюдены; `craw/gost_api.go` изменён только в части `day()`/`gostUniqueDocs`.

---

# Fix-раунд 2 (живая приёмка): «день без своих публикаций» ≠ «день с невалидными документами»

Коммит: `craw: treat foreign-date-only days as empty, not failed`.
Правки — только `craw/gost_api.go`, `craw/gost_api_test.go`, `craw/gost.go`, `craw/gost_test.go`.

## Причина (живой прогон 2026-10-13)

```
день 2026-10-05: отброшено документов чужой даты публикации: 47 из 47
день 2026-10-05: все 47 документов отброшены фильтром публикации (Skipped 47) … [error]
```

Текстовый `query=DD.MM.YYYY` матчит дату в **любом** поле документа, поэтому `count` дня бывает
ненулевым из-за документов, чья `dateofpublication` — другая дата. День без своих публикаций —
штатный ответ источника, и валить на нём часовой DAG нельзя (он краснел бы на каждом таком дне,
пока окно его не проедет). При этом неразобранный документ (поломка формата/данных) по-прежнему
обязан валить прогон — это и есть уточнение предыдущего «fail loud».

## Что изменено

`craw/gost_api.go`:
- `gostDaySlice` дополнен полем `Invalid int` (Skipped остался общим числом отброшенных).
- Введён `gostDocReject` (`gostDocAccepted` / `gostDocForeign` / `gostDocInvalid`);
  `gostRowFromDoc` возвращает причину вместо `bool`. «Чужой» — дата публикации разбирается, но
  указывает на другой день; «невалидный» — нет `dateofissueofcertificate`, пуст
  `numberofcertificate` или неразбираемая/пустая `dateofpublication`.
- `gostRowsFromDocs` возвращает `(rows, skipped, invalid)`; `day()` кладёт `Invalid` в срез дня.

`craw/gost.go` (`gostRunDays`), ветка `Count > 0` и пустого среза:
- `slice.Invalid > 0` → ошибка дня (fail loud), текст: «день %s: %d документов не разобраны
  (отброшено %d из %d) — проверьте поля dateofissueofcertificate/numberofcertificate/dateofpublication»;
- иначе (все отброшенные — чужие даты) → `log.Warnf` «день %s: публикаций нет — %d документов
  ручки относятся к другим датам публикации», день считается успешно обработанным: без батча,
  без ошибки, в `failed` не попадает.
Warn о `Skipped` (общий датчик доли отброшенных) оставлен на месте — прод-поведение изменилось
ровно в описанной ветке.

## Покрывающие тесты

`craw/gost_test.go`:
- `TestGostRunDaysForeignDateOnlyDayWarns` — день из одних чужих дат (`Count: 4, Noise: 4`) плюс
  нормальный день: ошибки нет, `count == 1`, батч один, в логе
  «публикаций нет — 4 документов ручки относятся к другим датам публикации», и в логе нет
  «не разобраны» (то есть день не в списке failed и не помечен поломкой).
- `TestGostRunDaysInvalidDocsFail` — день `Count: 3, Invalid: 1, Noise: 2` (один документ без даты
  выдачи, два чужих): ошибка содержит день, «1 документов не разобраны» и «не прочитаны дни (1)»;
  следующий день всё равно записан (резюмируемость).
- `TestGostRunDaysFilteredDayFails` (старый кейс «все отброшены») заменён этими двумя: он больше
  не соответствует правилу.

`craw/gost_api_test.go`:
- `TestGostRowsFromDocsFilters` переведён на `gostDocReject`: фикстура даёт
  `accepted/foreign/invalid/invalid`; `skipped == 3`, `invalid == 2`; битая дата публикации —
  `gostDocInvalid`; «нет даты выдачи с заполненным номером» — `gostDocInvalid` (`invalid == 3` при
  добавленном документе).
- `TestGostDayCountsInvalidDocs` (новый) — `day()` по всей фикстуре: строк 1, `Skipped == 3`,
  `Invalid == 2`.
- `TestGostDayClientFilterCountsSkipped` дополнен проверкой `Invalid == 0` у дня из одних чужих дат.

## Мутационные проверки правки

| Мутация | Результат |
|---|---|
| A: любой пустой срез при `Count > 0` — снова ошибка (старое поведение) | `--- FAIL: TestGostRunDaysForeignDateOnlyDayWarns` — «день из одних чужих дат публикации уронил прогон»; `TestGostRunDaysInvalidDocsFail` остался зелёным |
| B: чужой день публикации считать невалидным | падают четыре теста: `TestGostRowsFromDocsFilters` («invalid = 3, ожидалось 2»), `TestGostDayClientFilterCountsSkipped` («Invalid = 2, ожидалось 0»), `TestGostDayCountsInvalidDocs` («Invalid = 3, ожидалось 2»), `TestGostRunDaysForeignDateOnlyDayWarns` |

Обе мутации откачены (`grep -rn "МУТАЦИ" craw/` пусто).

## Команды и вывод (после отката)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/... -timeout 300s` | `ok … 1.729s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 43 `--- PASS` (включая подтесты), 0 FAIL |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| GoLand MCP `get_file_problems` (`craw/gost.go`) | ошибок нет |

Тесты по-прежнему офлайн (~0.36 с на пакет без сети и БД), комментарии и логи по-русски, токен —
только из env, запись — батчами.

---

# Fix-раунд 3 (живая приёмка, июньский `full`): формы `dateofpublication` и наблюдаемость счётчиков

Коммит: `craw: accept plain-date publication form and surface invalid counts`.
Правки — только `craw/gost_api.go`, `craw/gost_api_test.go`, `craw/gost.go`, `craw/gost_test.go`.

## F3 (баг транспорта) — принимаются обе наблюдаемые формы `dateofpublication`

Живой прогон: в дне 15.06.2026 из 36 документов 35 были с чужими датами вида
`'11.06.2026 00:00:00 ZE3'`, а один — с `dateofpublication = '28.03.2024'` (чистая дата). Старый
парсер читал ровно `"02.01.2006 15:04:05"` и считал такой документ невалидным, а правило fail-loud
роняло день и весь июньский прогон.

`craw/gost_api.go`:
- `gostPublicationLayout` = `"02.01.2006"` (разбирается только календарный день; время и маркер
  пояса не разбираются вовсе — они у всех записей одинаковы и информации не несут);
- `gostParsePublicationDay`: `TrimSpace` → строка короче 10 символов невалидна → первые 10
  символов парсятся как дата → хвост допускается только начиная с пробела (`rest != "" &&
  !strings.HasPrefix(rest, " ")` → невалидно). Так `"28.03.2024xyz"` остаётся невалидным, а
  `"11.10.2026 00:00:00 ZE3"`, `"11.10.2026 00:00:00"`, `"28.03.2024"` и `"  28.03.2024  "` —
  валидными.

Тесты (`craw/gost_api_test.go`, `TestGostParsePublicationDay`): таблица форм (с маркером пояса, без
маркера, чистая дата, чистая дата в пробелах — валидны; `"28.03.2024xyz"`, `"мусор"`,
`"abcdefghij"`, `""`, `"8.3.2024"` — невалидны) и явная проверка «обе формы одной даты дают один и
тот же день» (`28.03.2024` == `28.03.2024 00:00:00 ZE3`).

Дополнительно в `craw/gost_test.go` тестовая константа `gostForeignPublication` переведена в чистую
форму `"01.01.2020"`: теперь живая июньская форма документа проходит через весь путь обвязки
(`day()` → `gostRunDays`), а не только через тест парсера.

## F4 (наблюдаемость) — разбивка отброшенных в warn дня

`craw/gost.go`, `gostRunDays`:
- warn дня с отброшенными: «день %s: отброшено %d документов из %d (чужой даты: %d, невалидных: %d)»
  — невалидные документы видны и у дня с непустым срезом, не только общим `Skipped`;
- warn дня без публикаций: «день %s: публикаций нет — %d документов ручки относятся к другим датам
  публикации (чужой даты: %d, невалидных: %d)»;
- текст ошибки дня с невалидными тоже несёт разбивку: «… не разобраны (отброшено %d из %d: чужой
  даты %d, невалидных %d)».

Резолюция по неоднозначности брифа: в описании теста F4 сказано «день пустой с 1 невалидным и 35
чужими — предупреждение с обеими цифрами, ошибки нет», но в «Требованиях» того же задания
сказано «правило fail-loud НЕ ослаблять (ошибка остаётся при «срез пуст и Invalid>0»)».
Выполнено требование: такой день остаётся ошибкой прогона (день попадает в `failed`), а warn и
текст ошибки несут обе цифры — различие «чужие / невалидные» стало видимым, правило не ослаблено.
Если контроллер имел в виду, что при `0 < Invalid < Count` день должен проходить с warn-ом,
это правка одной строки (`slice.Invalid > 0` → `slice.Invalid == slice.Count`) — скажите, и внесу.

Тесты (`craw/gost_test.go`):
- `TestGostRunDaysWarnsInvalidBreakdown` — день с непустым срезом и 1 невалидным документом
  (`Count: 3, Invalid: 1, Noise: 1`): строки пишутся, ошибки нет, в логе «отброшено 2 документов
  из 3 (чужой даты: 1, невалидных: 1)»;
- `TestGostRunDaysEmptyDayWarnShowsBreakdown` — день 36 документов (1 невалидный, 35 чужих): в логе
  «чужой даты: 35, невалидных: 1», ошибка дня содержит день и «чужой даты 35, невалидных 1», батчей
  нет (fail-loud сохранён);
- `TestGostRunDaysForeignDateOnlyDayWarns` дополнен проверкой разбивки в сообщении «публикаций нет»
  («чужой даты: 4, невалидных: 0»).

## Мутационные проверки

| Мутация | Результат |
|---|---|
| F3a: хвост обязателен (старое поведение парсера — живой июньский баг) | падают 4 теста: `TestGostParsePublicationDay` («gostParsePublicationDay("28.03.2024") = …, false»), `TestGostRunDaysForeignDateOnlyDayWarns` («день из одних чужих дат публикации уронил прогон … 4 документов не разобраны»), `TestGostRunDaysInvalidDocsFail`, `TestGostRunDaysEmptyDayWarnShowsBreakdown` — то есть июньский сценарий воспроизводится и ловится на уровне обвязки |
| F3b: хвост любой (проверка пробела снята) | `--- FAIL: TestGostParsePublicationDay` — «gostParsePublicationDay("28.03.2024xyz") = …, true, ожидалось ok=false» |
| F4: разбивка убрана из warn дня | падают `TestGostRunDaysWarnsInvalidBreakdown` и `TestGostRunDaysEmptyDayWarnShowsBreakdown` |

Все мутации откачены (`grep -rn "МУТАЦИ" craw/` пусто).

## Команды и вывод (после отката)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/... -timeout 300s` | `ok … 1.710s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 45 `--- PASS` (включая подтесты), 0 FAIL |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |

Тесты офлайн, комментарии и логи по-русски, токен — только из env, запись — батчами, правило
fail-loud не ослаблено.

---

# Fix-раунд 4 (решение контроллера): ошибка дня — только полная невалидность дня

Коммит: `6535c25 craw: fail only when a day is entirely invalid`. Правки — только `craw/gost.go`
и `craw/gost_test.go`.

## Изменение правила

`gostRunDays` (ветка `len(slice.Rows) == 0` при `Count > 0`): условие ошибки дня заменено с
`slice.Invalid > 0` на `slice.Invalid == slice.Count`.

- `Invalid == Count` (не разобрался ни один документ дня — нет даты выдачи, пуст номер, битая
  дата публикации) — **ошибка дня и прогона**, текст: «день %s: все N документов дня невалидны —
  проверьте поля dateofissueofcertificate/numberofcertificate/dateofpublication». Именно здесь
  сохранён «fail loud»: систематическая поломка формата по-прежнему краснеет.
- `0 < Invalid < Count` — день **успешен с предупреждением**: «день %s: публикаций нет —
  отброшено N из M (чужой даты: F, невалидных: I)», батча нет, в `failed` день не попадает.
  Одиночный мусорный документ в шумовом дне больше не валит весь прогон.
- `Invalid == 0` (день из одних чужих дат) — поведение не изменилось, warn тот же с новой
  формулировкой счётчиков.
- Непустой день с частью невалидных (`Rows > 0`) не тронут: строки пишутся, warn с разбивкой
  («отброшено … (чужой даты: …, невалидных: …)») на месте.

Прод-поведение изменилось ровно в этой ветке.

## Тесты

- `TestGostRunDaysPartlyInvalidNoisyDayWarns` (заменил `TestGostRunDaysEmptyDayWarnShowsBreakdown`) —
  день из 36 документов (1 невалидный, 35 чужих): прогон не падает, `count == 0`, батчей нет, в
  логе «публикаций нет — отброшено 36 из 36», «чужой даты: 35, невалидных: 1», и нет «документов
  дня невалидны» (день не в `failed`).
- `TestGostRunDaysAllInvalidDocsFail` (заменил `TestGostRunDaysInvalidDocsFail`) — день
  `Count: 3, Invalid: 3` плюс нормальный день: ошибка содержит день, «все 3 документов дня
  невалидны» и «не прочитаны дни (1)»; следующий день всё равно записан (резюмируемость).
- `TestGostRunDaysForeignDateOnlyDayWarns` — ожидаемая строка warn приведена к новой формулировке
  («публикаций нет — отброшено 4 из 4 (чужой даты: 4, невалидных: 0)»).
- `TestGostRunDaysWarnsInvalidBreakdown` (непустой день, `Rows > 0`) и
  `TestGostRunDaysSkipsEmptyAndAggregatesErrors` не менялись и остаются зелёными.

## Мутационные проверки

| Мутация | Результат |
|---|---|
| 1: вернуть `slice.Invalid > 0` (старое правило) | `--- FAIL: TestGostRunDaysPartlyInvalidNoisyDayWarns` — «невалидность части дня уронила прогон: … все 36 документов дня невалидны …»; `TestGostRunDaysAllInvalidDocsFail` зелёный |
| 2: убрать ошибку при `Invalid == Count` (`if false`) | `--- FAIL: TestGostRunDaysAllInvalidDocsFail` — «день, не разобранный целиком, не попал в ошибку прогона»; `TestGostRunDaysPartlyInvalidNoisyDayWarns` зелёный |

Обе мутации откачены (`grep -rn "МУТАЦИ" craw/` пусто).

## Команды и вывод (после отката)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/... -timeout 300s` | `ok github.com/kmlebedev/clickhouse-import-rosstat/craw 1.535s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 45 `--- PASS`, 0 FAIL |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| `go build ./...` | OK |
| GoLand MCP `get_file_problems` (`craw/gost.go`, `craw/gost_test.go`) | ошибок нет |

Тесты офлайн (~0.4 с на пакет без сети и БД), токен — только из env, комментарии и логи по-русски,
запись — батчами. `git push` не делался; `deploy/staging/dagu.service` остаётся изменённым в дереве
до начала работы (не моё, не индексировалось).

---

# Fix-раунд 5 (живая приёмка, июньский `check`): перепубликация ≠ дыра

Коммит: `5a0f4d2 craw: tell republication drift from real check gaps`. Правки — только
`craw/gost.go` и `craw/gost_test.go`.

## Причина

Живой `check` за июнь 2026 нашёл 2 «дыры»: 2026-06-03 (таблица 2912 против 2934 в дне источника)
и 2026-06-04 (2729 против 2760). Разбор: источник отдал документы с `dateofpublication` этого
дня, но 22 из них имеют дубликат-публикацию позже (тот же `certificate_number`), и строка
таблицы помечена `publication_date = 2026-06-05` — ключ ReplacingMergeTree
`(mark, type, date, certificate_number)` дату публикации не хранит, побеждает последняя запись.
Каждый сертификат в таблице есть; разошлась метрика дня (per-publication_date), а не данные.
`check` же объявлял такие дни дырами и возвращал exit ≠ 0 — отчётом нельзя было принимать работу.

## Изменение (`craw/gost.go`, только `gostCheckDays`)

После существующего fallback-сравнения длины среза (совпало — текстовый шум) при расхождении:

1. `gostStoredDayNumbers` — номера таблицы этого дня публикации одним запросом
   `SELECT certificate_number FROM gost_vehicle_safety_certificate FINAL WHERE publication_date = ?`;
2. `gostMissingNumbers` — номера дневного среза, которых в этом множестве нет (в Go; повторы
   схлопываются, порядок среза сохраняется);
3. `missing` пусто — расхождения нет: «… все номера дневного среза есть в таблице, дыр нет»;
4. `gostKnownNumbers` — какие из `missing` есть в таблице вообще, одним запросом
   `SELECT certificate_number FROM gost_vehicle_safety_certificate FINAL WHERE certificate_number IN (?)`
   (список — только `missing`; `[]string` разворачивает драйвер, синтаксис `IN (['a','b'])`
   проверен на живой ClickHouse через MCP: возвращает 1);
5. `gostSplitDrift` делит `missing` на перепубликованные (номер есть под другой датой) и реальные
   пропуски:
   - есть перепубликованные — warn «день %s: N документов перепубликованы под другой датой
     публикации (например, %s), реальных пропусков M», **не ошибка** при M == 0;
   - M > 0 — день идёт в ошибку с перечислением реально отсутствующих номеров
     (`gostNumbersHint`: до 5 номеров + «и ещё N»);
6. Сводка: `проверено N дней, дыр M, перепубликаций K` — дни с одними перепубликациями в `gaps`
   не попадают.

Новые константы — `gostDayNumbersQuery`, `gostNumbersQuery`. Режимы записи (`since`/`full`),
правило fail-loud и запросы якоря/счётчиков/легаси не тронуты; токен — по-прежнему только из env.

Толкование счётчика: `перепубликаций K` — документы (номера), а не дни: так K сходится с текстом
строки дня «N документов перепубликованы». Если контроллер имел в виду дни, это правка одной
строки.

## Тесты (`craw/gost_test.go`)

Фейковое соединение расширено по образцу: `dayNums map[string][]string` (номера таблицы по дню
публикации) и `numbers map[string]bool` (номера таблицы вообще); `Query` различает два новых
запроса по тексту (`WHERE publication_date = ?`, `certificate_number IN (?)`), проверяет тип
аргументов и падает на неизвестном запросе; сеттеры — `gostNumberSetters` / `gostKnownNumberSetters`.

Новые тесты:
- `TestGostCheckDaysRepublicationNotGap` — день из 5 документов, в таблице этого дня 3, два
  отсутствующих номера найдены под другой датой: ошибки нет, в логе «2 документов
  перепубликованы под другой датой публикации», «реальных пропусков 0», сводка
  «дыр 0, перепубликаций 2»;
- `TestGostCheckDaysRealGapAfterRepublication` — один отсутствующий номер перепубликован, второго
  нет вовсе: ошибка дня называет день и только настоящий пропуск («реальных пропусков 1»),
  перепубликованный номер в списке пропусков не звучит, сводка «дыр 1, перепубликаций 1»;
- `TestGostCheckDaysAllNumbersStoredNoGap` — все номера среза есть в таблице этого дня (счётчик
  таблицы больше среза): расхождения нет, запрос номеров таблицы вообще не выполняется.

Существующие тесты check (`ReportsGap`, `TextNoiseNotGap`, `SkippedDayFails`, `MatchNoError`,
`LegacyWarnOnly`, `ImportCheckDoesNotWrite`) не менялись и остаются зелёными.

## Саботаж-проверки (обе из задания, откачены)

| Мутация | Результат |
|---|---|
| 1: шаги 4–5 убраны — каждый `missing` объявляется дырой | `--- FAIL: TestGostCheckDaysRepublicationNotGap` — «перепубликация объявлена дырой: … реальных пропусков 2: ТС RU …00004, …00005»; `TestGostCheckDaysRealGapAfterRepublication` — «перепубликованный номер объявлен пропуском», нет «перепубликаций» в сводке |
| 2: настоящие пропуски игнорируются (`continue` до `gaps++`) | `--- FAIL: TestGostCheckDaysRealGapAfterRepublication` — «настоящий пропуск не стал ошибкой check»; `--- FAIL: TestGostCheckDaysReportsGap` — «расхождение счётчиков не стало ошибкой check» |

`grep -rn "МУТАЦИ" craw/` пусто — маркеры убраны вместе с мутациями.

## Команды и вывод (после отката)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/... -timeout 300s` | `ok github.com/kmlebedev/clickhouse-import-rosstat/craw 1.629s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 48 `--- PASS` (было 45: +3), 0 FAIL |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| `go build ./...` | OK |
| GoLand MCP `get_file_problems` (`craw/gost.go`, `craw/gost_test.go`) | ошибок нет |

Тесты офлайн, токен — только из env, комментарии и логи по-русски, запись — батчами. `git push`
не делался; `deploy/staging/dagu.service` остаётся изменённым в дереве до начала работы (не моё,
не индексировалось).

---

## Финальная фикс-волна (финальное ревью ветки, 2026-10-13)

Пять пунктов финального ревью — одним коммитом
`c327b40` `craw: today upper bound, abort on auth failure, minor hardening`
(8 файлов, +251/−53).

### F-A. Верхняя граница `since` — «сегодня МСК» (возврат к утверждённой спеке)

Спека §«Три режима» задавала окно `[anchor − GOST_SETTLE_DAYS, сегодня МСК]`, а реализация
закрывала его вчерашним днём. Возвращено «сегодня»: в `gostSinceDays` верхняя граница —
`gostTodayMSK(now)` (новая функция; `gostYesterdayMSK` остался дефолтом `GOST_TO` диапазонов
`full`/`check` и выражен через неё). Часовой прогон снова ловит и дневной батч текущего дня.
Дефолты `full`/`check` («вчера МСК») не тронуты. Выровнены все упоминания: `README.md` (раздел
про `craw/`), `ARCHITECTURE.md` (§5 — строка `GOST_MODE`, §6.1 — строка импортёра, §7 —
таблица расписаний), комментарий `dagu/craw.yaml`, пункт роадмапа
`docs/GOLD_THESIS_DATA_ROADMAP.md`.

Тесты: таблица `TestGostSinceDaysAnchors` (все 4 кейса — окна теперь кончаются 2026-10-12),
`TestGostSinceDaysCapsStaleAnchor` (свежий якорь — 2026-10-06…2026-10-12; отставший — окно
30 дней 2026-09-13…2026-10-12). Пины `TestGostConfigDefaults` (дефолт `full` = вчера) не менялись.

### F-B. Fail-fast на систематическом отказе авторизации; список дней ограничен

- `craw/gost_api.go`: тип-сентинел `gostAuthError` (`Error()` + `Unwrap()`), которым обёрнута
  401/403-ветка `gostStatusError` — внутри по-прежнему `backoff.Permanent` и подсказка про
  `GOST_APP_ID`. `backoff.Retry` на permanent-ошибке возвращает развёрнутую ошибку, поэтому
  `errors.As` достаёт тип из ошибки дня; это закреплено в `TestGostDayPermanentStatusFails`.
- `craw/gost.go`: `gostRunDays` при ошибке дня проверяет `errors.As(err, &authErr)` и прерывает
  обход: «авторизация отклонена (проверьте GOST_APP_ID): прервано на дне %s, дней не прочитано %d»
  (+ `%w` первопричины — статус). При мёртвом токене `full` больше не жжёт часы по тысячам дней и
  не прячет подсказку в мегабайтном агрегате; 500-е и прочие точечные сбои прерывания не дают.
- Список неудачных дней в агрегате усечён: `gostDaysHint` — первые 10 + «и ещё N» (по образцу
  `gostNumbersHint`).

Тесты: `TestGostRunDaysAbortsOnAuthError` (401 на первом дне → один запрос, обход прерван,
в сообщении день и «дней не прочитано 3», запись/батчи отсутствуют, прерывание в логе),
`TestGostRunDaysCapsFailedDayList` (12 сбойных дней: «не прочитаны дни (12)», первые десять
перечислены, «и ещё 2», дни 11–12 не звучат; запрошены все 12 × 4 попытки — 500 не прерывает).

### F-C. `{}` — не «день без публикаций»

`gostSearchResponse.Count` — `*int`; `fetchPage` считает отсутствие ключа сбоем ответа
(retryable, как пустое тело). `{"count":0,"data":[]}` — валидный пустой день без повторов.
Тесты: в `TestGostDayEmptyBodyFails` добавлены кейсы `{}` и `{"data":[]}` (ошибка «ответ без
count», `gostMaxTries` попыток); `TestGostDayZeroCountIsValid` — легальный пустой день
(Count 0, один запрос, `dayCount` = 0).

### F-D. Guard диапазона: 4000 → 5000 дней

`gostMaxRangeDays = 5000` — 4000 дней от `2016-01-01` исчерпывались к 2026-12-15 (дефолтный
`full` до «вчера»), 5000 — запас до ~2029-09 (2016-01-01 + 5000 дней = 2029-09-08). Комментарий
обновлён; граничный тест `TestGostDaysBetween` написан по константе и обновился сам.

### F-E. Диапазон легаси-строк в роадмапе

Проверено по живой dev-БД (read-only MCP, `v_gost_vehicle_safety_certificate`):
легаси (`publication_date = 1970-01-01`) — **212 537** строк с датами выдачи
**2026-06-16…2026-10-02**; размеченные — **80 684** строки с датами выдачи
**2021-02-01…2026-10-09**. Пункт приёмки follow-up 6 в `docs/GOLD_THESIS_DATA_ROADMAP.md`
исправлен: диапазоны названы раздельно, с пояснением, что 2021-02…2026-10-09 — у размеченных
строк (перепубликации старых сертификатов, которые `check` и показывает как «перепубликации»).

### Проверки (после отката саботажей)

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./craw/...` | `ok github.com/kmlebedev/clickhouse-import-rosstat/craw 1.650s` |
| `go test -count=1 ./craw/ -v -run TestGost` | 99 `--- PASS` (51 тест верхнего уровня + 48 подтестов; было 48/45), 0 FAIL |
| `go vet ./craw/` | OK (0 замечаний) |
| `gofmt -l craw/` | пусто |
| `golangci-lint run ./craw/...` | `0 issues.` |
| `uv run --with pyyaml python3` — парсинг `dagu/craw.yaml` (PyYAML локально нет) | OK: `chain`, `17 * * * *`, один шаг `gost_vehicle_safety_certificate`, `GOST_MODE=since`, retry 3 × 300 с |
| `go build ./...` | OK |
| GoLand MCP `get_file_problems` (`craw/gost.go`, `gost_api.go`, `gost_test.go`, `gost_api_test.go`) | ошибок нет |

### Саботаж-проверки (все обнаружены, откачены)

| Мутация | Результат |
|---|---|
| F-A: `to := gostYesterdayMSK(now)` в `gostSinceDays` | FAIL `TestGostSinceDaysAnchors` (4 подтеста) и `TestGostSinceDaysCapsStaleAnchor` (2 подтеста) |
| F-B: sentinel-аборт выключен (`errors.As(...) && false`) | FAIL `TestGostRunDaysAbortsOnAuthError`: нет «авторизация отклонена (проверьте GOST_APP_ID)», «запросов 3, ожидался 1» |
| F-C: отсутствие `count` снова трактуется как ноль | FAIL `TestGostDayEmptyBodyFails/нет_ключа_count` и `/нет_ключа_count,_есть_data`; `TestGostDayZeroCountIsValid` проходит |

Маркеров мутаций не осталось (`grep -rn "МУТАЦИЯ" craw/` пусто). Токен — только из env; тесты
офлайн; батчи и fail-loud-правила (`Invalid == Count`, кап `since` 30 дней, `count > 10000`) не
менялись. `git push` не делался; после коммита в дереве остаётся только чужое изменение
`deploy/staging/dagu.service` (не индексировалось).
