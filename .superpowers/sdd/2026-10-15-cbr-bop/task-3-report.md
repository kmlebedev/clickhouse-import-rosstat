# Task 3 report: реестр, расписание, MCP (платёжный баланс ЦБ)

**Ветка:** `key-rate-4-2-14-bop`. **Бриф:** `task-3-brief.md`.
**Коммиты:** `a1543f9`, `f73e868` (оба на ветке, рабочее дерево чистое).
**Статус:** DONE (сомнения — в конце, ничего блокирующего).

Код Task 1/Task 2 (`cbr/bop_est.go`, `cbr/bop_standard.go`, `cbr/bop_meta.go`, `cbr/views.go`,
фикстуры, тесты импортёров) не трогал — только подключил.

---

## Что сделано

### 1. `cbr/importers.go` (+3/−1) — реестр

Две записи **в хвост** `cbrImporters`, литералами таблицы и запроса чека (стандарт §8.2):

```go
{mpSummaryImporter, "cbr_mp_summary", ""},
{cbrBopEstImporter, "cbr_bop_est", "SELECT count() FROM cbr_bop_est FINAL"},
{cbrBopStandardImporter, "cbr_bop_standard", "SELECT count() FROM cbr_bop_standard FINAL"},
```

Порядок = порядок шагов `dagu/cbr.yaml` (17-й, 18-й). Счётная формулировка комментария файла
обновлена: «16 записей в одном месте» → «18 записей в одном месте». Вторая цифра в той же фразе
(«вместо 13 прежних») — **исторический факт** о 13 персональных `init()`-append, которые реестр
заменил (см. `git show a3ed11a:cbr/importers.go`: «13 точек стали одной»), поэтому оставлена
13; точно так же её не трогал предыдущий пункт (`git show 49b7cb8 -- cbr/importers.go` менял
только «15 записей» → «16 записей»).

### 2. `cbr/importers_test.go` (+3/−1) — тест-энфорсер

`expectedPublicationCountPoints` получил два имени с дословно теми же запросами:

```go
"cbr_bop_est":      "SELECT count() FROM cbr_bop_est FINAL",
"cbr_bop_standard": "SELECT count() FROM cbr_bop_standard FINAL",
```

Комментарий проверки (д) — «у ожидаемых **одиннадцати**» → «у ожидаемых **тринадцати**».
Других «одиннадцати»/«одиннадцать» в файле нет (единственное совпадение — `bop_standard_test.go:490`
«одиннадцатая строка списка», оно не про число импортёров и не трогалось).

Дословное совпадение запросов подтверждено сверкой всех четырёх копий утверждения:

```
$ grep -rn "SELECT count() FROM cbr_bop" cbr/*.go
cbr/bop_est.go:31:    cbrBopEstCountPoints = "SELECT count() FROM cbr_bop_est FINAL"          # константа импортёра
cbr/bop_standard.go:30: cbrBopStandardCountPoints = "SELECT count() FROM cbr_bop_standard FINAL"
cbr/importers.go:70:  {cbrBopEstImporter, "cbr_bop_est", "…cbr_bop_est FINAL"}               # реестр
cbr/importers.go:71:  {cbrBopStandardImporter, "cbr_bop_standard", "…cbr_bop_standard FINAL"}
cbr/importers_test.go:60-61: то же                                                          # ожидание теста
```

### 3. `dagu/cbr.yaml` (+16) — расписание

Два шага **в конец** цепочки (17-й, 18-й), `run` / `env: CLICKHOUSE_IMPORT_STAT=<Name()>` /
`retry_policy: limit 3, interval_sec 300` — копия соседнего блока. В середину цепочки не лез:
`type: chain`, падение шага останавливает последующие (стандарт §12.1), и хвост гарантирует, что
новый шаг не лишит данных шестнадцать проверенных.

```
$ grep -c "CLICKHOUSE_IMPORT_STAT=" dagu/cbr.yaml
18
$ grep -o "CLICKHOUSE_IMPORT_STAT=[a-z_]*" dagu/cbr.yaml | nl | tail -2
    17	CLICKHOUSE_IMPORT_STAT=cbr_bop_est
    18	CLICKHOUSE_IMPORT_STAT=cbr_bop_standard
$ tail -c 30 dagu/cbr.yaml | xxd | tail -1        # файл по-прежнему кончается переводом строки
00000010: 7276 616c 5f73 6563 3a20 3330 300a       rval_sec: 300.
```

### 4. `sql/key_rate_coverage.sql` (+8) — снимок покрытия

Две ветки `UNION ALL` по образцу `cbr_int_rate_loans` — `name, count(), min(date), max(date)` с
`GROUP BY name` и `FINAL`, с комментарием в стиле соседнего сберовского блока:

```sql
UNION ALL SELECT 'cbr_bop_est', name, count(), min(date), max(date) FROM cbr_bop_est FINAL GROUP BY name

UNION ALL SELECT 'cbr_bop_standard', name, count(), min(date), max(date) FROM cbr_bop_standard FINAL GROUP BY name
```

Поставлены в конец списка `UNION ALL` (после сберовских веток) — как и предыдущие новые наборы;
`sorted by tbl, series` в конце разложит их рядом с `cbr_bank_int_rate`. Комментарий называет,
почему ветки две, а не одна (единицы различаются в 1000 раз — млрд и млн долларов США; вида
периода два набора не пересекают) и почему `FINAL` обязателен (оценка ЦБ ревизуется новым
выпуском по тому же ключу `(name, date)`).

### 5. `sql/mcp_kimi_reader.sql` (+1)

`GRANT SELECT ON default.v_cbr_bop TO kimi_reader;` — в конец раздела грантов на витрины
(список пополняется хронологически). Сырые `cbr_bop_est`/`cbr_bop_standard` агенту **не** выданы —
гранта на них нет (стандарт §10.4, спека §3.7).

### 6. `scripts/mcp_check.py` (+63) — проверки контура

Блок вставлен сразу после блока `cbr_int_rate_loans` (соседний по контуру и по образцу), перед
блоком `cbr_mp_summary`. Существующие проверки не менялись — только добавление.

(а) **витрина `v_cbr_bop` существует и непуста** — `SELECT count() FROM v_cbr_bop` разобранным
ответом `run_query` (существование витрины подтверждается запросом к ней, а не поиском имени в
тексте `list_tables` — урок bold), значение проверяется `> 0`, потому что строки публикуют
импортёры `cbr/`, и ноль означает ни разу не отработавший контур. Оба сообщения подсказывают
`make import`: пустой ответ — «на пустой БД её создаёт прогон импортёра
(`make import STAT=cbr_bop_est`)», ноль строк — «прогнать импортёры контура: `make import
STAT=cbr_bop_est` и `STAT=cbr_bop_standard` (витрину пересоздаёт `AfterImport` каждого прогона)».

(б), (в) **каталоги полны** — точным равенством (`51`, `34`), а не «> 0»:

```python
for bop_source, bop_want in (("cbr_bop_est", 51), ("cbr_bop_standard", 34)):
    … f"SELECT count() AS n FROM v_series_catalog WHERE source = '{bop_source}'"
```

Меньшее число значит ни разу не отработавший шаг (числа агент прочитает, а что измеряет ряд,
в каких единицах и как привязана дата — нет), большее — ряд, которому в листе источника не
соответствует строка-агрегат. Выбрана выборка по `source` (а не по `origin LIKE`, как в образце
`cbr_int_rate_loans`), потому что у этих наборов источник каталога равен имени таблицы
(спека §3.7) — `source='cbr_bop_est'`/`'cbr_bop_standard'`, а `origin` дополнительно называет лист
и подпись строки; бриф допускает оба варианта. Расшифровка чисел в комментарии: 51 = 17 агрегатов
× 3 вида периода, 34 = 17 агрегатов × 2 листа.

(г) **обе сырые таблицы закрыты** — цикл по `cbr_bop_est` (подпись «оценка ключевых агрегатов») и
`cbr_bop_standard` («стандартная презентация»), отказ распознаётся по `ACCESS_DENIED` /
`Not enough privileges`; при прочих ответах печатается фрагмент, как в соседних блоках.

---

## Проверки (команды и результат)

Все — офлайн (на живой БД не ходил: это Task 5).

| Команда | Результат |
|---|---|
| `gofmt -l cbr` | пусто (exit 0) |
| `python3 -m py_compile scripts/mcp_check.py` | OK (exit 0) |
| `dagu validate dagu/cbr.yaml` | exit 0 (только WARN про `auth.mode`) |
| `go test -race -count=1 ./cbr/` | `ok … /cbr 51.744s` |
| `go test -race -count=1 -run TestCbrImportersPrefixedAndRegistered -v ./cbr/` | `--- PASS: TestCbrImportersPrefixedAndRegistered (0.00s)` |
| `make all` (lint → vet → `go test -race ./…` → оба билда) | exit 0; `0 issues.` (golangci-lint), `ok …/cbr 52.610s`, остальные пакеты `(cached)`, оба `go build` прошли |
| `grep -c "CLICKHOUSE_IMPORT_STAT=" dagu/cbr.yaml` | 18 |
| GoLand MCP `get_file_problems --errorsOnly true` | `{}` (без ошибок) для `cbr/importers.go`, `cbr/importers_test.go`, `dagu/cbr.yaml`, `sql/key_rate_coverage.sql`, `scripts/mcp_check.py` |

**Негативный контроль тест-энфорсера** (доказательство, что новые записи реестра
«несущие», а не декоративные). Временно закомментировал строку реестра `cbr_bop_est`
и прогнал только энфорсер:

```
--- FAIL: TestCbrImportersPrefixedAndRegistered (0.00s)
    importers_test.go:205: в реестре cbrImporters нет импортёра "cbr_bop_est", для которого спека §3.5 ожидает объявление префлайта
    importers_test.go:236: шаг dagu/cbr.yaml CLICKHOUSE_IMPORT_STAT=cbr_bop_est не найден в реестре cbrImporters: расписание позовёт импортёр, которого нет
```

После чего правка восстановлена (`git diff cbr/importers.go` — ровно `+3/−1`), тест снова зелёный,
`gofmt -l cbr` пуст. Затем `make all` прогнан ещё раз **на закоммиченном состоянии** (`git status`
пуст, оба коммита на ветке) — exit 0, обе сборки прошли.

Чего проверить не смог: живой `make mcp-check` (нужна БД и `kimi_reader`; по заданию — Task 5).
Синтаксис скрипта проверен (`py_compile` + проблемы GoLand), ветки разбора ответа
(`result_rows is None` / `== 0` / `> 0`) — копия проверенных соседних блоков.

---

## Изменённые файлы

```
a1543f9 cbr: register bop importers and schedule their steps
 cbr/importers.go      |  4 +++-
 cbr/importers_test.go |  4 +++-
 dagu/cbr.yaml         | 16 ++++++++++++++++
f73e868 sql: grant v_cbr_bop and check the payment balance contour in mcp_check
 scripts/mcp_check.py      | 63 +++++++++++++++++++++++++++++++++++++++++++++++
 sql/key_rate_coverage.sql |  8 ++++++
 sql/mcp_kimi_reader.sql   |  1 +
```

Больше ничего не тронуто (`git status` чистый; `cbr/bop_*.go`, `cbr/views.go`, фикстуры, тесты
импортёров, другие `sql/` и `dagu/*.yaml`, ARCHITECTURE/README/роадмап — без правок).

---

## Self-review

1. **Порядок реестра = порядок расписания.** Обе таблицы-источника проверены глазами: реестр
   заканчивается `… cbr_mp_summary, cbr_bop_est, cbr_bop_standard`, `dagu/cbr.yaml` — теми же
   `CLICKHOUSE_IMPORT_STAT` 16-м, 17-м, 18-м (нумерация по `grep -o … | nl`).
2. **Литералы, а не константы** (§8.2) — в реестре и в тесте строки литералами; сверены с
   константами импортёров (`cbrBopEstCountPoints`, `cbrBopStandardCountPoints`) вне теста.
3. **`Name() == table`** у обеих записей: `cbrBopEstImporter`/`cbrBopStandardImporter` — это
   `&util.ClickHouseImport` с `TableName` `cbr_bop_est`/`cbr_bop_standard`, поэтому проверка (б)
   теста проходит не по случайности; `Publication().Source` пуст → ключ версии = `TableName`
   (проверка (в) теста).
4. **Ничего не «сверх».** Не добавлял в `mcp_check.py` проверку «витрина объединяет оба набора»
   (её нет в списке Task 3, хотя у Сбера такая есть): состав двух наборов косвенно закрыт двумя
   проверками каталогов 51 и 34. Если ревью сочтёт нужным `uniqExact(source) = 2` — добавлю одним
   фикс-раундом.
5. **Формулировка счётного комментария в тесте.** «Одиннадцати» → «тринадцати» — это ровно число
   ключей `expectedPublicationCountPoints` (11 + 2 = 13), совпадает с числом импортёров
   с префлайтом в реестре: 13 в `cbr/` (11 прежних + два bop), 5 вне (`rosstat` ×3, `bank` ×1,
   `fred`/`bls`/… — вне пакета, см. `util/publications.go`).
6. **`FINAL` во всех трёх местах**, где это важно: запросы чека (реестр/тест — из объявления
   импортёров), снимок покрытия, и нигде не использован `count()` без `FINAL` по ReplacingMergeTree.
7. **Стиль комментариев `mcp_check.py`** — как у соседей: что доказывает проверка, что означает
   меньшее число, почему существование витрины подтверждается запросом (урок bold), кто создаёт
   таблицу (поэтому `UNKNOWN_TABLE` = провал).

---

## Проблемы и сомнения

1. **`util/publications.go:22` — устаревший счётный комментарий** (вне объёма Task 3):
   `// (шестнадцать импортёров: одиннадцать в cbr/ и пять вне него)`. С двумя bop-объявлениями
   верно «восемнадцать импортёров: тринадцать в `cbr/` и пять вне него». Тот же инвариант
   продублирован в `ARCHITECTURE.md` (стр. 297 «у 16 импортёров», стр. 409 «шестнадцать
   импортёров: одиннадцать в `cbr/` и пять вне него») и в `README.md` — это файлы Task 4
   (документация), и прецедент такой же: в пункте 4.2 №8 (`cbr_int_rate_loans`) эти две копии
   правил доковый шаг Task 5, а не шаг реестра/расписания. **Не трогал, чтобы не выйти за объём;
   прошу Task 4 не забыть именно `util/publications.go` (комментарий кода, не только `.md`).**
2. **`source` вместо `origin LIKE`** в проверках каталога (бриф допускал оба). Плюс `source`:
   ровно спека §3.7 и точно 51/34 без риска, что `origin` набора оценки когда-нибудь перестанет
   содержать `bop_est.xlsx` (имя листа там префиксное). Минус: у этих наборов `source` — не
   литерал `'cbr'`, и проверка «непустота витрины + два каталога» не доказывает самостоятельно,
   что обе ветки витрины живы (см. self-review п. 4).
3. **Ветка `bop_view_rows is None`** трактует и `UNKNOWN_TABLE`, и `ACCESS_DENIED` как одну
   ошибку с подсказкой про `make import`. У соседей ровно так же (например, сберовский блок);
   «нет прав» при верном гранте практически не встречается, а отдельного разбора MCP-ошибок
   в скрипте нет.
4. **Порядок веток в `key_rate_coverage.sql`** — в хвосте списка `UNION ALL`, а не сразу после
   `cbr_int_rate_loans`. На вывод не влияет (`ORDER BY tbl, series`), но если ревью предпочтёт
   группировку cbr-веток подряд — перенесу одной правкой.
5. Живых прогонов не делал осознанно (Task 5 контроллера): возможные расхождения с живой БД
   (например, фактическое число рядов каталога) этой задачей не проверяются — числа 51/34 взяты
   из спеки §3.3/§3.7 и совпадают с офлайн-тестами каталога (`bop_est_test.go`,
   `bop_standard_test.go`).

---

## Fix round 1 (ревью Task 3: один Important — `scripts/mcp_check.py`)

### Решение ревьюера и правка

**Important:** в проверках каталога (`scripts/mcp_check.py:551-552`) выборка шла только по
`source`, тогда как бриф и спека §4 (`docs/superpowers/specs/2026-10-15-cbr-bop-design.md:251`)
называют `origin LIKE`. Функционально эквивалентно (`source` каталога равен имени таблицы), но это
отступление от плана, и проверка доказывала только счёт, а не источник ряда. Ревьюер выбрал более
сильную формулировку — выборку по **обоим** полям:

```python
bop_catalogs = (
    ("cbr_bop_est", "bop_est.xlsx", "%bop\\_est.xlsx%", 51),
    ("cbr_bop_standard", "bal_of_payments_standart.xlsx",
     "%bal\\_of\\_payments\\_standart.xlsx%", 34),
)
for bop_source, bop_file, bop_origin_like, bop_want in bop_catalogs:
    … f"SELECT count() AS n FROM v_series_catalog WHERE source = '{bop_source}' AND origin LIKE '{bop_origin_like}'"
```

Экранирование подчёркиваний — ровно как у принятой проверки `cbr_int_rate_loans`
(`scripts/mcp_check.py:489`, `origin LIKE '%loans\\_nonfin.xlsx%'`). Комментарий переписан: теперь
он объясняет, что даёт выборка по обоим полям (`source` — ряд принадлежит этому импортёру,
`origin` — ряд разобран именно из файла этого импортёра, спека §3.7), почему одного счёта по
`source` мало (51/34 записи с верным `source`, но `origin` чужого файла означали бы каталог,
описывающий не тот артефакт, чьи точки лежат в витрине), что означает меньшее число (шаг не
отработал — прогнать `make import STAT=<имя шага>`; числа агент прочитает, а что измеряет ряд,
в каких единицах и как привязана дата — нет) и зачем экранирован `_` (в LIKE это «любой один
символ», без экранирования шаблон поймал бы постороннее имя файла). Сообщения проверки теперь
называют файл: «рядов cbr_bop_est из bop_est.xlsx: 51».

Больше ничего не менялось: реестр, расписание, SQL и остальные проверки — как принято ревью
(правка касается только блока контура платёжного баланса в `scripts/mcp_check.py`).

### Проверки

| Команда | Результат |
|---|---|
| `python3 -W error::SyntaxWarning -m py_compile scripts/mcp_check.py` | OK (без SyntaxWarning — важно: `\\_` в литерале не даёт «invalid escape sequence») |
| Пробный рендер f-строки тем же кодом | `… WHERE source = 'cbr_bop_est' AND origin LIKE '%bop\_est.xlsx%'`, `… source = 'cbr_bop_standard' AND origin LIKE '%bal\_of\_payments\_standart.xlsx%'` — SQL дословно как просило ревью |
| `go test -race -count=1 ./cbr/` | `ok … /cbr 53.168s` (Go не тронут) |
| `make all` (дважды: после правки и на закоммиченном состоянии) | exit 0, `0 issues.` (golangci-lint), обе сборки прошли |
| GoLand MCP `get_file_problems --errorsOnly true scripts/mcp_check.py` | `{}` (без ошибок) |
| `git status --short` после коммита | пусто |

**Чтение живой БД через `mcp__clickhouse__run_query`** (только SELECT; `kimi_reader`) — две
проверки, что экранирование и шаблоны рабочие, а не декоративные:

```
SELECT countIf(source='cbr_bop_est' AND origin LIKE '%bop\_est.xlsx%')      AS bop_est_rows,
       countIf(source='cbr_bop_standard' AND origin LIKE '%bal\_of\_payments\_standart.xlsx%') AS bop_standard_rows,
       countIf(source='cbr' AND origin LIKE '%loans\_nonfin.xlsx%')          AS loans_rows
FROM v_series_catalog FINAL
→ 0 | 0 | 16
```

(принятая проверка `cbr_int_rate_loans` тем же экранированием видит свои 16 рядов; новые наборы
пока 0 — их шаги ещё не гоняли, это Task 5, и проверка сейчас обязана честно падать с подсказкой
`make import STAT=cbr_bop_est`).

```
SELECT 'bopXest.xlsx' LIKE '%bop\_est.xlsx%' AS escaped_no_match,
       'bopXest.xlsx' LIKE '%bop_est.xlsx%'  AS unescaped_matches,
       'bop_est.xlsx' LIKE '%bop\_est.xlsx%' AS escaped_real_match
→ 0 | 1 | 1
```

— сервер ClickHouse экранирование `\_` действительно соблюдает (без него шаблон совпал бы с
посторонним именем файла, отличающимся одним символом).

### Коммит

```
d8eef22 scripts: pin the bop catalog checks to both source and origin
 scripts/mcp_check.py | 38 ++++++++++++++++++++++++++------------
 1 file changed, 26 insertions(+), 12 deletions(-)
```

