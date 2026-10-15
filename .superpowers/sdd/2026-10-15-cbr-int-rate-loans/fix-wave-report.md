# Fix-wave по финальному ревью ветки `key-rate-4-2-8-loan-rates` — отчёт

**Дата:** 2026-10-15. **Ветка:** `key-rate-4-2-8-loan-rates` (до правки `17496de`).
**Коммит правки:** `e0f330f622116994f9f05e3b3acc2a3592adade0` — `fix cbr_int_rate_loans:
anchor-based column base, zero as no-data, review minors` (один коммит, 8 файлов,
+393/−56).

**Статус: DONE_WITH_CONCERNS.** Все пункты A–I закрыты; «concerns» — три объявленных
отклонения/дополнения к брифу (перечислены в конце, п. «Замечания»), а не незакрытые
находки. Живые прогоны (`make import`, `make mcp-check`) не выполнялись, данные в БД не
менялись; SQL к ClickHouse — только `SELECT` через MCP.

Секреты (`~/.config/rosstat/env`) не читались и не печатались. MCP-инструменты в наборе
были: `mcp__jetbrains__execute_tool` (использован для `get_file_problems`) и
`mcp__clickhouse__run_query`.

## A. База колонок данных — из шапки, а не `i+1` (Important, §6.3)

**Сделано** (`cbr/int_rate_loans.go`):

- `cbrIntRateLoansColumns` теперь возвращает базу первой колонки данных:
  `func cbrIntRateLoansColumns(rows [][]string) (names []string, base, firstDataRow int, err error)`,
  `base = starts[0]` (позиция подписи первой группы) — все ветки ошибок возвращают
  `nil, 0, 0, …`;
- в `cbrIntRateLoansImport` запись идёт по `column := base + i`
  (`names, base, firstDataRow, err := cbrIntRateLoansColumns(rows)`);
- добавлена явная проверка инварианта `base+len(names) <= len(rows[headerRow])` с
  комментарием «это не повтор проверок границ групп, а вывод из них». **Место проверки —
  внутри `cbrIntRateLoansColumns`, а не в импортёре:** `rows[headerRow]` есть только там
  (`headerRow = firstDataRow-1` в импортёре восстанавливать не нужно), и проверка стережёт
  именно возвращаемую базу — то есть гарантирует инвариант потребителю;
- комментарий функции обновлён: «Якоря, а не магические индексы (стандарт §6.3)»
  осталось правдой, добавлено, что база тоже выводится из шапки и почему (`base+i`).

**Результат разбора не изменился:** `TestCbrIntRateLoansFixtureRows` — 16 × 152 = 2 432
точки, `TestCbrIntRateLoansSpotValues` — 7.35 / 19.06 / 14.22, `TestCbrIntRateLoansAppendsFloat64`
— все точки float64 (см. вывод тестов ниже).

## B. Литеральный `0` — пропуск точки, а не 0 % годовых (Important, §6.7)

- `cbr/int_rate_loans.go`: после `ParseFloat` литеральный ноль — `continue` (та же ветка,
  что пустая ячейка) с комментарием в духе §6.7 («литеральный 0 в источнике означает
  „данных не имеется“») — точкой ряда не становится; комментарий `cbrIntRateLoansImport`
  и спека приведены к этому же правилу;
- `cbr/series_meta.go`: в `description` рядов добавлено, что **и пустая ячейка, и
  литеральный ноль** источника — пропуск точки, а не 0 % годовых
  (`cbrIntRateLoansDescription`); текст «в текущем файле пропусков нет» заменяет устаревавшую
  формулировку «пустых нет»;
- `docs/superpowers/specs/2026-10-15-cbr-int-rate-loans-design.md` §3.4: формулировка
  приведена к стандарту — «Пропуск точки — пустая ячейка **или** литеральный ноль: по §6.7
  ноль означает в источнике „данных не имеется“»; §5 «Отступлений нет» теперь правда.
- Дополнительно (объявленное расширение брифа, см. «Замечания»): в `ARCHITECTURE.md`
  (шапка «Последнее обновление», запись про `cbr_int_rate_loans`) фраза «пустая ячейка
  источника — не точка ряда» заменена на «пустая ячейка источника и литеральный ноль — не
  точка ряда» плюс уточнено, что из якоря выводятся и база колонок данных (правило 13.1
  AGENTS.md: изменение пакета отражается в ARCHITECTURE.md тем же PR).

## C. Обёртка ошибки `batch.Append` (Minor, §6.4)

Код ровно по брифу:

```go
if err = batch.Append(name, date, rate); err != nil {
    return fmt.Errorf("%s: метка %q, ряд %q, %s: %w",
        cbrIntRateLoansTable, label, name, date.Format("2006-01-02"), err)
}
```

## D. `NaN`/`Inf` отсекаются (Minor)

После `ParseFloat` и проверки `err`:

```go
if math.IsNaN(rate) || math.IsInf(rate, 0) {
    return fmt.Errorf("%s: лист «%s», метка %q, ряд «%s»: значение %q не конечно (NaN/Inf)",
        cbrIntRateLoansTable, cbrIntRateLoansSheet, label, name, cell)
}
```

Обвязка та же, что у соседней ветки `ParseFloat` (таблица/лист/метка/ряд/значение);
добавлен импорт `math`.

## E. Тесты на непокрытые ветки разбора (Minor, §11.1/§11.5)

Наборный тест на **живой фикстуре** (мутации в памяти через `extra/xlsx`, как в
`mp_survey_parse_test.go`; харнесс — `appendRecorder`, БД не нужна):

| Подтест | Мутация живой фикстуры | Проверка |
|---|---|---|
| `TestCbrIntRateLoansStructureFailsFastOnFixture/группа_из_7_колонок_вместо_8` | подпись второй группы сдвинута на колонку влево (`J4` → `I4`) | ошибка содержит «источник изменил структуру листа», есть имя таблицы, записано 0 строк (живой текст: `… группа «Всего»: колонок 7, а сроков 8 — источник изменил структуру листа`) |
| `TestCbrIntRateLoansStructureFailsFastOnFixture/шапка_без_строк_данных` | `RemoveRow` строк 6…159 | ошибка содержит «не дал ни одной точки» и `cbr_int_rate_loans` |
| `TestCbrIntRateLoansSkipsNonPointsOnFixture/пустая_ячейка` | `B6` = `""` | точек 2 431 (ровно −1), точка `(Всего: до 30 дней…, 2014-01-30)` отсутствует, февраль и сосед на месте |
| `TestCbrIntRateLoansSkipsNonPointsOnFixture/литеральный_ноль` | `B6` = `0` | то же (новая семантика B) |
| `TestCbrIntRateLoansSkipsNonPointsOnFixture/строка_данных_короче_шапки` | опустошены `J6:Q6` (группа «СМП») | точек 2 424 (−8), восемь СМП-рядов за 2014-01-30 отсутствуют, «Всего» и февраль на месте |

Плюс два теста на ветки, которые иначе остались бы непокрытыми (C и G):

- `TestCbrIntRateLoansFailFastBranches/значение_NaN` — `rows[5][1] = "NaN"` → ошибка (D);
- `TestCbrIntRateLoansAppendErrorNamesRow` — приёмник объявлен схемой из одной колонки,
  поэтому `Append` отвергает каждую строку: в ошибке есть `cbr_int_rate_loans`,
  «Январь 2014», имя ряда и `2014-01-30` (C);
- `TestCbrIntRateLoansOriginNamesGroupAndTerm` — origin всех 16 записей содержит
  «лист «ставки_руб»», «группа «…»» и «срок «…»» раздельно и не содержит имя ряда целиком
  в кавычках (G).

**Найденная при написании тестов особенность excelize** (задокументирована в тесте):
`SetCellValue` направляет запись в **объединённый** диапазон в его левую верхнюю ячейку —
запись в `I4` легла бы в `B4` и стёрла «Всего», поэтому мутация сначала делает
`UnmergeCell("B4", "J4")` (разбор объединённые диапазоны и так читает как обычные ячейки —
значение несёт только первая). Вторая особенность — дата февраля 2014 по правилу разбора
(`time.Date(y, m+1, -1)`) это **27-е**, а не 28-е; в тесте стоит литерал с пояснением.

## F. Точный LIKE в `scripts/mcp_check.py` (Minor)

```python
{"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr' AND origin LIKE '%loans\\_nonfin.xlsx%'"}
```

Ничего другого в файле не менялось. Проверено живым `SELECT` через MCP, что шаблон точен и
по-прежнему находит наши ряды:

```sql
SELECT 'a_b' LIKE 'a\_b' AS esc_match, 'axb' LIKE 'a\_b' AS esc_nomatch,
       count() FROM v_series_catalog WHERE source='cbr' AND origin LIKE '%loans\_nonfin.xlsx%'
-- esc_match=1, esc_nomatch=0, count()=16
```

## G. `origin` — группа и срок раздельно; полнота комментария `series` (Minor, правило 11)

**`cbr/series_meta.go`:** `cbrIntRateLoansSeriesMeta` собирается своим циклом
(`cbrIntRateLoansMeta`) вместо `namedSeriesMeta`/`seriesTemplate`; подпись шаблона
«группа и колонка срока «<имя целиком>»» больше не используется. В структуру групп
(`cbr/int_rate_loans.go`) добавлено поле `SourceLabel` — полный текст подписи группы
(у «СМП» якорь `Label` обрезан: «в том числе субъектам малого»), чтобы в каталоге группа
называлась целиком. Фактический origin (выгружен временным тестом, удалён):

```
Всего: от 1 года до 3 лет →
cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx, лист «ставки_руб», группа «Всего», срок «от 1 года до 3 лет»

СМП: от 1 года до 3 лет →
…, лист «ставки_руб», группа «в том числе субъектам малого и среднего предпринимательства», срок «от 1 года до 3 лет»
```

`description` рядов не менялся, кроме правки B.

**`cbr/views.go`** (колонка `series` витрины `v_cbr_macro`): восстановлена полнота
формулировки — имена берутся из источника у кредитов физлицам по регионам, кредитов
юрлицам по видам деятельности (ОКВЭД), ставок по вкладам, ИПЦ, инфляционных ожиданий и
балансов домохозяйств; отдельно сказано, что у ставок по кредитам юрлицам имя — комбинация
двух ячеек источника (группа заёмщиков и срок), собираемая кодом (стандарт §6.8). Прочие
комментарии витрины не тронуты.

**Ниша, объявленная в «Замечаниях»:** у шести сроков текст содержит собственные кавычки
источника («до 30 дней, включая «до востребования»»), поэтому в origin они видны внутри
кавычек срока: `срок «до 30 дней, включая «до востребования»»`. Это цитата текста
источника (как в заголовке листа), а не вложение составного имени: группа и срок всё равно
названы раздельно и различимы.

## H. Комментарий про размер фикстуры (Minor)

`cbr/int_rate_loans_test.go`: «живой файл источника целиком: книга ~124 КБ (124 288 байт,
8 листов) коммитится как есть — стандарт §11.2, zip-хирургия нужна только крупным книгам».
Суть нормы §11.2 сохранена.

## I. Документы

**`docs/superpowers/plans/2026-10-15-cbr-int-rate-loans.md`, Task 6:**

- «**Результат приёмки (2026-10-15, воспроизведено дословно)**» → «**Результат приёмки
  (2026-10-15)**» + пояснение, что в командах и URL сохранены многоточия (там опущены части,
  не влияющие на результат); «— дословная строка ниже» → «— строка ниже»; «Строка версии в
  `source_publications` (дословно, …)» → «(разделители — вертикальные черты; URL сокращён)»;
- имя приведено к фактическому: `SELECT count() FROM series_catalog FINAL …` →
  `SELECT count() FROM v_series_catalog …` (+ пояснение, что каталог агент читает через
  витрину);
- `make ch-sql` → `make ch-sql FILE=sql/key_rate_coverage.sql` (как в шаге 7 той же задачи).

Дополнительно (объявленное расширение брифа) в Task 2 той же задачи убрано расхождение
с кодом после правки G: «через `namedSeriesMeta` с `seriesTemplate`» → «собирается своим
циклом по группам и срокам, а не `namedSeriesMeta`», «группа …, колонка срока «…»» →
«группа «…», срок «…»», «пустая ячейка источника — пропуск точки» → «пустая ячейка и
литеральный ноль источника — пропуск точки».

**`docs/superpowers/specs/2026-10-15-cbr-int-rate-loans-design.md`:**

- §3.4 — правило §6.7 приведено к стандарту (см. B);
- §3.4 — база колонок данных тоже выводится из якоря (правка A, чтобы спека не расходилась
  с кодом по §6.3);
- §3.7 — добавлен абзац: шаг вставлен в середину цепочки `dagu/cbr.yaml` (10-й из 16, сразу
  после `cbr_bank_int_rate`), поэтому структурный сбой источника останавливает и последующие
  шаги цепочки — осознанное решение (стандарт §12.1), а не побочный эффект.

## Проверки (обязательный набор, вывод)

```
$ gofmt -l cbr/ scripts/
(пусто)

$ go vet ./cbr/
OK

$ go test -race -count=1 ./cbr/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	19.197s

$ go test -race -count=1 -v -run 'TestCbrIntRateLoans' ./cbr/
--- PASS: TestCbrIntRateLoansNames (0.00s)
--- PASS: TestCbrIntRateLoansImporterName (0.00s)
--- PASS: TestCbrIntRateLoansDdl (0.00s)
--- PASS: TestCbrIntRateLoansSeriesMetaCoversParsedRows (0.07s)
--- PASS: TestCbrIntRateLoansFixtureRows (0.06s)          # 2 432 точки = 16 × 152
--- PASS: TestCbrIntRateLoansSpotValues (0.06s)           # 7.35 / 19.06 / 14.22
--- PASS: TestCbrIntRateLoansAppendsFloat64 (0.06s)
--- PASS: TestCbrIntRateLoansMissingSheetFails (0.00s)
--- PASS: TestCbrIntRateLoansShortSheetFails (0.01s)      # 3 подтеста
--- PASS: TestCbrIntRateLoansFailFastBranches (0.02s)     # + новый подтест «значение NaN»
--- PASS: TestCbrIntRateLoansFootnoteEndsParsing (0.00s)
--- PASS: TestCbrIntRateLoansStructureFailsFastOnFixture (0.63s)   # 2 подтеста
--- PASS: TestCbrIntRateLoansSkipsNonPointsOnFixture (0.36s)       # 3 подтеста
--- PASS: TestCbrIntRateLoansAppendErrorNamesRow (0.06s)
--- PASS: TestCbrIntRateLoansOriginNamesGroupAndTerm (0.00s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	2.732s

$ python3 -m py_compile scripts/mcp_check.py
OK

$ make all
0 issues.            # golangci-lint
go vet ./...
go test -race ./...  # все пакеты ok, cbr 19.4s
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build … -o build/clickhouse-import-rosstat .
go build -o build/ingest ./cmd/ingest

$ git log --oneline -2
e0f330f fix cbr_int_rate_loans: anchor-based column base, zero as no-data, review minors
17496de docs: record live acceptance for cbr_int_rate_loans

$ git status --short
(пусто)
```

Дополнительно (не заменяя обязательных проверок):

```
$ mcp__jetbrains__execute_tool get_file_problems --filePath cbr/int_rate_loans.go
{"filePath":"cbr/int_rate_loans.go","errors":[]}
$ … cbr/int_rate_loans_test.go   → {"errors":[]}
$ … cbr/series_meta.go           → {"errors":[]}
```

Живые прогоны не запускались; к ClickHouse ушли только `SELECT`-запросы (шаблон LIKE,
`v_cbr_macro FINAL` = 48 942 — запись приёмки сходится, каталог по экранированному шаблону
= 16 строк).

## Замечания (объявленные отклонения/дополнения к брифу)

1. **Место проверки инварианта (A).** Проверка `base+len(names) <= len(rows[headerRow])`
   стоит в `cbrIntRateLoansColumns`, а не в `cbrIntRateLoansImport`: `rows[headerRow]` есть
   только в первой функции, и она же возвращает `base` — так инвариант гарантируется на
   выходе, без восстановления `headerRow = firstDataRow-1` в импортёре.
2. **Вложенные кавычки в origin у шести сроков (G).** `срок «до 30 дней, включая «до
   востребования»»` — внутренние кавычки принадлежат тексту источника (как в заголовке
   листа). Группа и срок названы раздельно и различимы; составное имя целиком в кавычки
   больше не попадает (проверяется тестом `TestCbrIntRateLoansOriginNamesGroupAndTerm`).
3. **Две правки вне списка файлов брифа — обе ради «док не расходится с кодом»:**
   `ARCHITECTURE.md` (запись про `cbr_int_rate_loans`: литеральный ноль + база колонок
   из якоря; правило 13.1 AGENTS.md) и Task 2 плана (убрано «через `namedSeriesMeta`»,
   «колонка срока» → «срок», ноль тоже пропуск точки). Правка G меняла именно это
   поведение/текст, а план и ARCHITECTURE.md его описывали; оставленные как есть, они
   стали бы новой находкой того же класса («документ сузил правило молча»).

Не закрыто и вне объёма правки: pre-existing опечатка-ключ «авугст» в `util/xls.go`
(отмечена ещё в ревью Task 1) — отдельный пункт.
