# Task 3 report — витрины `v_gost_*`: стейтменты, гранты, mcp-check

**Коммит:** `bb7de52` `craw: publish gost views and mcp checks` (без push; в коммит не попал
предсуществующий незакоммиченный `deploy/staging/dagu.service` — он был грязным до начала работы и Task 4).

## Что сделано

1. **`craw/gost_views.go`** (новый) — `gostRowsView`/`gostDailyView`, `var gostViewStatements []string`
   (16 стейтментов), `func gostPublishViews(ctx, conn) error`.
   - Строковая: `CREATE OR REPLACE VIEW v_gost_vehicle_safety_certificate DEFINER = default SQL SECURITY
     DEFINER AS SELECT date, publication_date, mark, type, certificate_number, ecoclass, enginetrademark,
     yearofmanufacturing, nameofmanufacturer FROM gost_vehicle_safety_certificate FINAL` — без `uid` и
     **без `ORDER BY`** (3.4 млн строк; в комментарии — «фильтруйте по `date` — витрина не сортирована»).
   - Дневная: `… SELECT date, mark, uniqExact(certificate_number) AS certificates … FINAL GROUP BY date, mark
     ORDER BY date DESC, mark`.
   - Комментарии: 1 × `ALTER TABLE … MODIFY COMMENT` + 9 × `COMMENT COLUMN` (строковая), 1 + 3 (дневная);
     русские, одной строкой, с оговорками (лаг публикации ~2 суток, amendments → вторая строка при том же
     номере, `certificates` = `uniqExact`, а не число строк, год выпуска `0` = не указан / до бэкфилла,
     пустой день отсутствует).
2. **`craw/gost.go`** — единственная правка: вызов `gostPublishViews` в `Import` **после** DDL и **до**
   первого обращения к API, только в пишущих режимах (`cfg.Mode != gostModeCheck`); в `check` витрины не
   публикуются. Обновлён doc-комментарий порядка шагов (пункт 3 — публикация витрин).
3. **`sql/mcp_kimi_reader.sql`** — две строки `GRANT SELECT` в конец блока витрин
   (`…v_gost_vehicle_safety_certificate`, затем `…v_gost_vehicle_safety_daily`).
4. **`scripts/mcp_check.py`** — блок проверок в конце `check()` по образцу bold-блока: для обеих витрин
   `SELECT count() FROM v_gost_*` + разбор `result_rows` (существование и право чтения доказывает сам
   запрос, а не поиск имени в `list_tables`; значение count не проверяется — витрина может быть пустой);
   `SELECT count() FROM gost_vehicle_safety_certificate` → ожидание `ACCESS_DENIED` / `Not enough privileges`
   (497).
5. **`craw/gost_views_test.go`** (новый) — три теста из брифа.

## Тесты: RED → GREEN

**RED** (тест написан до реализации):

```
$ go test -run 'TestGostView|TestGostImportWritesPublishViews' ./craw/
craw/gost_views_test.go:62:11: undefined: gostViewStatements
craw/gost_views_test.go:69:31: undefined: gostRowsView
craw/gost_views_test.go:89:31: undefined: gostDailyView
... too many errors
FAIL github.com/kmlebedev/clickhouse-import-rosstat/craw [build failed]
```

**GREEN** (после реализации `craw/gost_views.go` + вызова в `Import`):

```
$ go test -race -count=1 ./craw/...
ok  github.com/kmlebedev/clickhouse-import-rosstat/craw  1.534s
$ go vet ./craw/            # VET_OK, без замечаний
$ gofmt -l craw/ scripts/   # пусто
$ go build ./...            # BUILD_OK
$ python3 -m py_compile scripts/mcp_check.py   # PY_OK
$ golangci-lint run ./craw/...                 # 0 issues (v2.13.2, если установлен)
```

Что пинят тесты:

- `TestGostViewStatementsShape` — `len == 16` и состав: `[0]`/`[1]` — определения обеих витрин с
  `DEFINER = default SQL SECURITY DEFINER`; строковая — точный список колонок SELECT, `FROM … FINAL`,
  отсутствие `uid` и `ORDER BY`; дневная — `uniqExact(certificate_number)`, `GROUP BY date, mark`,
  `ORDER BY date DESC, mark`, отсутствие `count()`; далее ровно 1 `MODIFY COMMENT` и по одному
  `COMMENT COLUMN` на каждую колонку каждой витрины (9 и 3) — и ни одного лишнего стейтмента.
- `TestGostViewsDocumentSemantics` — в текстах комментариев (регистронезависимо) есть: «реестр СБКТС
  Росстандарта», «REST API», «gost.ru», «дата выдачи СБКТС (МСК)», «дата публикации среза», «лаг к выдаче
  ~2 суток», «изменения сертификата дают вторую строку», «ReplacingMergeTree FINAL», «фильтруйте по
  `date`», «витрина не сортирована», «год выпуска», «`0` — не указан», «до бэкфилла», «uniqExact»,
  «уникальных номеров», «пустой день отсутствует»; и ни в одном комментарии нет переноса строки.
- `TestGostImportWritesPublishViews` — `Import(GOST_MODE=since, пустая таблица)`: все 16 стейтментов
  витрин пришли в `Exec` в порядке среза, и индекс последнего из них **меньше** индекса первого
  `PrepareBatch`.

## Саботаж-проверки (каждая — с бэкапом и откатом; после отката пакет зелёный)

| Саботаж | Результат |
| --- | --- |
| `uniqExact(certificate_number)` → `count(certificate_number)` в дневной витрине (саботаж из брифа) | FAIL `TestGostViewStatementsShape`: «дневная витрина без "uniqExact(certificate_number)"» |
| Удалён вызов `gostPublishViews` из пишущего режима | FAIL `TestGostImportWritesPublishViews`: «Exec стейтментов витрин 0, ожидалось 16» + дамп вызовов (DDL → queryrow легаси/якорь → preparebatch) |
| Удалён стейтмент `COMMENT COLUMN yearofmanufacturing` | FAIL `TestGostViewsDocumentSemantics`: нет «год выпуска»/«`0` — не указан» + `gostColumnComment` «комментариев колонки…: 0, ожидался один» |

После откатов: `diff -q` с бэкапами совпал, `go test -count=1 ./craw/` — ok.

## Изменённые файлы

```
 craw/gost.go            |  19 ++--   (только вызов публикации + doc-комментарий порядка)
 craw/gost_views.go      |  80 ++     (новый)
 craw/gost_views_test.go | 225 ++     (новый)
 scripts/mcp_check.py    |  30 ++
 sql/mcp_kimi_reader.sql |   2 +
```

`craw/gost_api.go` не тронут; батчи, правила `since`/`full`/`check` и тексты ошибок Task 2 не менялись;
`ARCHITECTURE.md` §6.2/§6.3 и `dagu/craw.yaml` — не мои (Task 4). Токен `gtn.access.appid` в коде,
тестах и комментариях не появляется (правило 1) — в текстах ошибок и комментариях только имя `GOST_APP_ID`
(как было в Task 2).

## Само-ревью

- **Порядок публикации.** Публикация — после DDL (витрина не создаётся до таблицы) и до первого обращения
  к API; в `check` шаг пропущен — существующий `TestGostImportCheckDoesNotWrite` пиннит, что Exec-ов в
  check ровно `len(gostSchemaStatements())` и ни одного «VIEW», и он зелёный.
- **Сбой публикации фатален** (без проглатывания): прогон, отчитавшийся успехом с витриной от старого
  определения, — это чтение агента по чужой схеме.
- **Идемпотентность.** `CREATE OR REPLACE VIEW` + `MODIFY COMMENT`/`COMMENT COLUMN` — повторный прогон
  (раз в час) не ломает витрину и не копит объекты.
- **Витрина = контракт чтения.** Сырая таблица агенту закрыта (проверка 497 в mcp-check), поэтому
  оговорки (лаг публикации, amendments/`uniqExact`, `0` в годе) лежат именно в комментариях витрин.
- **`ORDER BY` только в дневной.** В строковой сортировка убрана осознанно (3.4 млн строк, страница
  запроса агента почти всегда сужена `date`); тест пиннит отсутствие `ORDER BY`, а не «на глаз».
- **Дневная витрина содержит `date`, `mark`, `certificates`** — без `uid` и без лишних колонок: смысл
  каждой описан комментарием, и `certificates` не спутать с числом строк.

## Сомнения / на что смотреть ревьюеру

1. **Формулировки комментариев** взяты из решений контроллера; три места сознательно согласованы с
   брифами/репозиторием, смысл сохранён:
   - «дата выдачи СБКТС **(МСК)**» в комментарии таблицы (в решении — «по МСК»); та же формулировка уже
     стоит в DDL-комментарии колонки `date` в `craw/gost.go` — и именно её требует токен теста из брифа.
   - «**дата** публикации среза у источника» в комментарии колонки `publication_date` (в решении —
     «день публикации среза»); так выполняется токен брифа «дата публикации среза».
   - «строка **до бэкфилла**» у `yearofmanufacturing` (в решении — «не бэкфилена»); так же сказано в
     DDL-комментарии `craw/gost.go`, и это токен брифа.
   Если ревью хочет буквальные формулировки решений — правки текстов тривиальны (тест устоит: токены
   «(МСК)»/«не бэкфилена» придётся подвинуть вместе с текстом).
2. **Тесты семантики комментариев ищут подстроки, а не точный текст.** Намеренно: проверяется наличие
   оговорки, а не редакция; дословные формулировки (длинные, с «×», «…», backtick-ами) фиксированы только
   в определении витрин. Если нужен пиннинг дословных строк — скажите, добавлю.
3. **`TestGostViewsDocumentSemantics` сравнивает регистронезависимо** — комментарий таблицы начинается с
   «Реестр …», а токен брифа с «реестр …».
4. **MCP-проверки и гранты на живой БД не прогонялись** (сети и ClickHouse в моём окружении нет, и по
   задаче они проверяются на приёмке в Task 5). `python3 -m py_compile` проходит; блок скопирован по
   структуре bold-блока и использует те же `text()`/`result_rows()`/`expect()`.
5. **`list_tables`-проверки для витрин сознательно не добавлены** — по решению 6 существование
   подтверждается запросом (урок bold: имя отсутствующей витрины попадало в текст `list_tables` из
   COMMENT соседки).
6. **JetBrains MCP**: `get_file_problems` по всем трём Go-файлам — без ошибок (в `craw/gost.go` остались
   два предсуществующих WARNING инспекции «SQL dialect is not configured» на строковых константах — они
   есть и в неизменённых строках Task 2). ClickHouse MCP не вызывался (по задаче ходить в базу не нужно).

---

# Fix-раунд (ревью Task 3): два findings

**Коммит:** `9617860` `craw: document legacy rows in gost publication_date comment`
(правки только в `craw/gost_views.go` и `craw/gost_views_test.go`; состав из 16 стейтментов не изменился —
`TestGostViewStatementsShape` зелёный без правок).

## Finding 1 (Important) — оговорка про строки до бэкфилла в комментарии `publication_date`

План (§Task 3, пункт про комментарии) требует оговорку про легаси-строки в комментариях витрин; в колонке
`publication_date` её не было.

Что сделано:

- `craw/gost_views.go`, комментарий колонки `publication_date`:
  `'`publication_date` — дата публикации среза у источника (лаг к выдаче ~2 суток); у строк до бэкфилла — 1970-01-01'`
  — 4-й стейтмент (`COMMENT COLUMN publication_date`), порядок и число стейтментов не тронуты.
- `craw/gost_views_test.go`, `TestGostViewsDocumentSemantics`: в список проверяемых подстрок добавлен
  `"1970-01-01"` **плюс** адресная проверка — оговорка обязана жить именно в комментарии `publication_date`
  (через `gostColumnComment`): комментарий должен содержать и `1970-01-01`, и `до бэкфилла`. Общий токен-лист
  ловит потерю оговорки в любом месте файла, адресная проверка — её перенос в чужую колонку/таблицу.
- В doc-комментарии `gostViewStatements` список оговорок дополнен «строки до бэкфилла» (комментарий
  остался в русском языке и не разъехался с содержимым среза).

## Finding 2 (Global Constraint) — английская фраза в русском комментарии

`craw/gost_views.go`, шапка файла: `(before first API call)` → `(до первого обращения к API)`.
Других англоязычных фрагментов в русских комментариях файла нет (`FINAL`, `uniqExact`, `since/full/check`,
`ReplacingMergeTree`, `gost.ru`, `REST API` — имена объектов/режимов/SQL, как и в остальном репозитории).

## Саботаж (с бэкапом и откатом)

Снята подстрока `; у строк до бэкфилла — 1970-01-01` из комментария колонки:

```
$ perl -0pi -e 's/; у строк до бэкфилла — 1970-01-01//' craw/gost_views.go   # grep -c '1970-01-01' → 0
$ go test -run TestGostViewsDocumentSemantics ./craw/
    gost_views_test.go:164: в комментариях витрин нет "1970-01-01":
    gost_views_test.go:182: комментарий publication_date без оговорки о строках до бэкфилла:
      "ALTER TABLE v_gost_vehicle_safety_certificate COMMENT COLUMN publication_date '`publication_date` — дата публикации среза у источника (лаг к выдаче ~2 суток)'"
--- FAIL: TestGostViewsDocumentSemantics (0.00s)
FAIL
```

Оба замечания валят обе проверки (общую и адресную) — как и требуется.

## Команды и вывод (после отката саботажа, `diff -q` с бэкапом совпал)

```
$ gofmt -l craw/                              # пусто
$ go test -race -count=1 ./craw/...
ok  github.com/kmlebedev/clickhouse-import-rosstat/craw  1.555s
$ go vet ./craw/                              # VET_OK, без замечаний
$ get_file_problems craw/gost_views.go, craw/gost_views_test.go   # errors: []
```

Сомнений по фиксу нет: обе правки локальны, состав стейтментов (16) и их порядок не менялись,
живые гранты и mcp-check по-прежнему проверяются на приёмке Task 5.
