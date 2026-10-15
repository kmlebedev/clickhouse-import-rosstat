# Task 9 — Живая приёмка и миграция данных: отчёт

**Дата:** 2026-10-14
**Ветка:** `feat/rosstat-refactor`, HEAD на старте — `53e8443` (`rosstat: truthful series comment; mcp_check hints use real step names`)
**БД:** local ClickHouse, порт 8123 (не поднимался и не останавливался — `make ch-up`/`ch-down` не запускались)
**Пользователи:** `kimi_reader` (MCP, только витрины) и `default` (только через `make ch-sql`, сырые таблицы и `system.*`)

Порядок шагов — стандарт §15.3: заполнить новые таблицы → сверить → переключить витрины → удалить старые таблицы.
Ничего не удалялось до того, как новые таблицы сверены и витрины пересозданы.

Итог: **приёмка пройдена**, все четыре ожидаемых числа совпали, **одно расхождение найдено и названо**
(каталог 145 вместо 141 из-за четырёх осиротевших строк прежних имён ИПЦ; устранено шагом 8).

---

## Шаг 1. `make all` и свежий прогон тестов

```
$ make all
0 issues.
go vet ./...
go test -race ./...
?   	github.com/kmlebedev/clickhouse-import-rosstat	[no test files]
?   	github.com/kmlebedev/clickhouse-import-rosstat/bank	[no test files]
ok  	github.com/kmlebedev/clickhouse-import-rosstat/bea	2.373s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/bls	1.551s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/bold	2.134s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/calendar	2.718s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	18.890s
?   	github.com/kmlebedev/clickhouse-import-rosstat/chimport	[no test files]
?   	github.com/kmlebedev/clickhouse-import-rosstat/cmd/ingest	[no test files]
ok  	github.com/kmlebedev/clickhouse-import-rosstat/craw	2.187s
?   	github.com/kmlebedev/clickhouse-import-rosstat/customs	[no test files]
ok  	github.com/kmlebedev/clickhouse-import-rosstat/dcf	2.183s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/eia	3.095s
?   	github.com/kmlebedev/clickhouse-import-rosstat/fao	[no test files]
?   	github.com/kmlebedev/clickhouse-import-rosstat/financial	[no test files]
?   	github.com/kmlebedev/clickhouse-import-rosstat/financial/exporter	[no test files]
ok  	github.com/kmlebedev/clickhouse-import-rosstat/forecast	(cached)
ok  	github.com/kmlebedev/clickhouse-import-rosstat/fred	2.510s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/gold	3.320s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/ingest	(cached)
ok  	github.com/kmlebedev/clickhouse-import-rosstat/minfin	3.780s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	3.399s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/news	4.415s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/polyus	(cached)
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	6.322s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	3.016s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/views	3.488s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/wgc	48.180s
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o build/clickhouse-import-rosstat .
go build -o build/ingest ./cmd/ingest
```

`rc=0`. `0 issues.` (gofmt + golangci-lint), `go vet` без замечаний, все пакеты `ok`, обе сборки прошли.

```
$ go test -race -count=1 ./rosstat/ ./cbr/ ./util/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	4.872s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	20.425s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	1.925s
```

## Шаг 2. Grep-чистота пакета

```
$ grep -rn "gocolly\|fmt\.Print" --include=*.go rosstat/
$ echo "grep exit code=$?"
grep exit code=1 (1 = no matches)
```

Пусто, как и требовалось.

## Шаг 3. Прогон четырёх импортёров по одному

```
$ make import STAT=rosstat_ipc_mes
go build -o build/clickhouse-import-rosstat-native .
CLICKHOUSE_IMPORT_STAT=rosstat_ipc_mes ./build/clickhouse-import-rosstat-native
time="2026-10-14T13:02:53+05:00" level=info msg="Connected to clickhouse"
time="2026-10-14T13:02:55+05:00" level=info msg="Get xlsx file "
time="2026-10-14T13:02:55+05:00" level=info msg="Imported 1716 rows of rosstat_ipc_mes"
EXIT=0
```

```
$ make import STAT=rosstat_ipc_weeks
time="2026-10-14T13:02:58+05:00" level=info msg="Connected to clickhouse"
time="2026-10-14T13:02:59+05:00" level=info msg="Get xlsx file "
time="2026-10-14T13:02:59+05:00" level=info msg="Imported 26611 rows of rosstat_ipc_weeks"
EXIT=0
```

```
$ make import STAT=rosstat_salaries_mes
time="2026-10-14T13:03:02+05:00" level=info msg="Connected to clickhouse"
time="2026-10-14T13:03:03+05:00" level=info msg="Get xlsx file "
time="2026-10-14T13:03:03+05:00" level=info msg="Imported 421 rows of rosstat_salaries_mes"
EXIT=0
```

```
$ make import STAT=rosstat_vvp_kvartal
time="2026-10-14T13:03:06+05:00" level=info msg="Connected to clickhouse"
time="2026-10-14T13:03:08+05:00" level=info msg="Get xlsx file "
time="2026-10-14T13:03:08+05:00" level=warning msg="vvpKvartal: rosstat_vvp_kvartal: лист \"14\", колонка 57: метка года \"20252)\" не больше текущего 2025 — год выведен как 2026"
time="2026-10-14T13:03:08+05:00" level=info msg="Imported 302 rows of rosstat_vvp_kvartal"
EXIT=0
```

Все четыре `rc=0`. Предупреждение по листу «14» — ожидаемое и единственное.
Новые таблицы созданы рядом со старыми; старые продолжали питать витрины до шага 7 (`v_model_inputs`
в это время читала `default.ipc_mes`/`default.ipc_weeks` — см. шаг 7). Побочно: `AfterImport`
последнего импортёра пересоздал `v_rosstat_macro` на `rosstat_*` — витрина отдавала 29 050 строк / 141 ряд
уже здесь, потому что все четыре новые таблицы к этому моменту существовали.

## Шаг 4. Сверка новых таблиц (admin, `make ch-sql`)

Временный файл `sql/tmp_t9_check.sql`, выполнен, **удалён** (в git не попадал).

Ожидаемое vs наблюдённое:

| Таблица | Рядов (ожид.) | Рядов (набл.) | Строк (ожид.) | Строк (набл.) | `min(date)` | `max(date)` |
|---|---|---|---|---|---|---|
| `rosstat_ipc_mes` | 4 | **4** | 1 716 | **1 716** | 1991-01-31 | 2026-09-30 |
| `rosstat_ipc_weeks` | 131 | **131** | 26 611 | **26 611** | 2022-01-10 | 2026-10-05 |
| `rosstat_salaries_mes` | 1 | **1** | 421 | **421** | 1991-08-01 | 2026-08-01 |
| `rosstat_vvp_kvartal` | 5 | **5** | 302 | **302** | 2011-04-01 | 2026-07-01 |

Дополнительно: `countIf(salary = 0)` по `rosstat_salaries_mes` = **0** (нулей нет).

Четыре имени ряда ИПЦ (дословно, `GROUP BY name ORDER BY name`):

```
Индексы потребительских цен на непродовольственные товары по Российской Федерации
Индексы потребительских цен на продовольственные товары по Российской Федерации
Индексы потребительских цен на товары и услуги по Российской Федерации
Индексы потребительских цен на услуги по Российской Федерации
```

Ни в одном нет ни подстроки `1991-2026`, ни `)` — имена новые и стабильные.

Строки версий (`source_publications`): `rosstat_ipc_mes` = `ipc_mes_09-2026.xlsx` 1716,
`rosstat_salaries_mes` = `tab1-zpl_07-2026.xlsx` 421, `rosstat_vvp_kvartal` = `VVP_kvartal_s-1995-2026.xlsx` 302.
У `rosstat_ipc_weeks` точки контроля публикации нет по устройству.

## Шаг 5. Spot-значения против источника

Живые артефакты скачаны заново со страниц источника (`https://rosstat.gov.ru/storage/mediabank/…`)
и **побайтово совпали с фикстурами пакета**:

| Живой файл | sha256 | Фикстура пакета |
|---|---|---|
| `ipc_mes_09-2026.xlsx` | `540553b4fce35ed2c57e5497f1f1fa1011c3da4dcf39d7191185760568f97d3e` | `rosstat/testdata/ipc_mes.xlsx` (тот же) |
| `Nedel_ipc.xlsx` | `dcccf65de3c196bb4464c4d15ed17ba7a3c9edc5ad427e21ff8d4bd6abba17d4` | `rosstat/testdata/ipc_weeks.xlsx` (тот же) |
| `tab1-zpl_07-2026.xlsx` | `1228595982e9d03bfca4db6a6d22a4150db9d688ee5c0e00e39c64de6cdd2adf` | `rosstat/testdata/salaries_mes.xlsx` (тот же) |
| `VVP_kvartal_s-1995-2026.xlsx` | `66eaab3f7b687fa09b427a020d038698e85665a669bf1e64bb6fa37a006086b0` | `rosstat/testdata/vvp_kvartal.xlsx` (тот же) |

Значения (ячейка источника → значение в витрине `v_rosstat_macro` через MCP):

| Что | Ячейка источника | Значение источника | Дата в таблице | Значение в витрине | Пин теста |
|---|---|---|---|---|---|
| зарплата, июнь 2026 | `tab1-zpl_07-2026.xlsx`, `Лист1!L43` | 114743 | 2026-07-01 | **114743** | `TestSalariesMesSpotValues`: 2026-07-01 = 114743 |
| зарплата, июль 2026 (последняя точка) | `Лист1!M43` | 109524 | 2026-08-01 | **109524** | `TestSalariesMesReadsFixture`: последняя точка 2026-08-01 |
| ВВП в текущих ценах, 2026-I | `VVP_kvartal_s-1995-2026.xlsx`, лист «2» `BI5` | 49869.532405343656 | 2026-04-01 | **49869.53125** | Float32-колонка (см. расхождение 1) |
| ВВП в текущих ценах, 2026-II | лист «2» `BJ5` | 56166.30179396068 | 2026-07-01 | **56166.30078125** | `TestVvpKvartalReadsFixture`: последняя точка ряда «2» = 2026-07-01 |
| ИФО ВВП, 2026-I | лист «12» `BE5` | 99.84838569242305 | 2026-04-01 | **99.848388671875** | — |
| ИФО ВВП, 2026-II | лист «12» `BF5` | 101.31028730944105 | 2026-07-01 | **101.31028747558594** | `TestVvpKvartalReadsFixture`: последняя точка ряда «12» = 2026-07-01 |
| недельный индекс, «Говядина (кроме бескостного мяса), кг» | `Nedel_ipc.xlsx`, лист «2026» `A5` (товар) / `B4` («на 12 января\*\*») / `B5` | 100.65 | 2026-01-12 | **100.6500015258789** | `TestIpcWeeksSpotValue`: 100.65 (raw-разбор, `float32`) |

Значения совпали с пинами тестов. Разница между raw-ячейкой и витриной — только Float32-представление
колонок таблиц (названное расхождение 1).

Контекст ВВП-листа «2» (проверка вывода года): строка годов `BI3` = `'20262)'` при `BJ3`/`BK3`/`BL3` = пусто;
лист «12»: `BE3` = `'20262)'`. На листе «14» вместо `2026` источник повторяет `20252)` — отсюда
предупреждение шага 3 и 58 различных дат вместо 56.

## Шаг 6. Повторные прогоны

```
$ make import STAT=rosstat_ipc_mes
time="2026-10-14T13:05:15+05:00" level=info msg="rosstat_ipc_mes: no-op, версия d22cce339e3d63fbbec658ffe6bef2d7 уже импортирована, 1716 точек на месте"
time="2026-10-14T13:05:15+05:00" level=info msg="Imported 1716 rows of rosstat_ipc_mes"
EXIT=0

$ make import STAT=rosstat_salaries_mes
time="2026-10-14T13:05:19+05:00" level=info msg="rosstat_salaries_mes: no-op, версия 67c128442873c91e66e69a93fc1177f1 уже импортирована, 421 точек на месте"
time="2026-10-14T13:05:19+05:00" level=info msg="Imported 421 rows of rosstat_salaries_mes"
EXIT=0

$ make import STAT=rosstat_vvp_kvartal
time="2026-10-14T13:05:23+05:00" level=info msg="rosstat_vvp_kvartal: no-op, версия f3513c572113995116592d4b3dfef400 уже импортирована, 302 точек на месте"
time="2026-10-14T13:05:23+05:00" level=info msg="Imported 302 rows of rosstat_vvp_kvartal"
EXIT=0

$ make import STAT=rosstat_ipc_weeks
time="2026-10-14T13:05:27+05:00" level=info msg="Get xlsx file "
time="2026-10-14T13:05:27+05:00" level=info msg="Imported 26611 rows of rosstat_ipc_weeks"
EXIT=0
```

Три импортёра ушли в no-op по известной версии **без скачивания** (сообщения `Get xlsx file` нет).
Версия `ipc_mes` = `d22cce339e3d63fbbec658ffe6bef2d7` совпадает с версией приёмки роллаута точек
контроля публикаций от 2026-10-14 — файл-артефакт тот же.
`rosstat_ipc_weeks` переимпортирован (точки контроля по устройству нет) и оставил счётчики прежними —
проверено admin-запросом: 1716 / 26 611 / 421 / 302 (см. шаг 7, блок `counts_after_rerun`).

## Шаг 7. Переключение витрин

```
$ make import STAT=gold_views
time="2026-10-14T13:05:30+05:00" level=info msg="View v_model_inputs created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_gold_dashboard created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_forecast_accuracy created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_dcf_assumptions created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_imf_macro created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_gold_forecasts created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_gold_etf_flows created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_cb_gold_dynamics created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_gold_etf_flows_daily created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="View v_gold_etf_funds_daily created: true"
time="2026-10-14T13:05:30+05:00" level=info msg="Imported 0 rows of gold_views"
EXIT=0
```

### Admin-доказательство ссылок на таблицы

`make ch-sql` по `SELECT name, arrayDistinct(extractAll(create_table_query, 'default\\.[a-zA-Z_0-9]+')), position(...) FROM system.tables WHERE name IN ('v_rosstat_macro','v_model_inputs')`:

```
Row 1:
name:      v_model_inputs
refs:      default.v_model_inputs, default.gold_prices, default.cbr_currency_usd, default.cbr_key_rate,
           default.ofz_curve, default.rosstat_ipc_mes, default.rosstat_ipc_weeks, default.macro_series,
           default.stock_prices, default.model_runs
new_mes:   1
new_weeks: 1
new_sal:   0
new_vvp:   0
old_mes:   0
old_weeks: 0
old_sal:   0
old_vvp:   0

Row 2:
name:      v_rosstat_macro
refs:      default.v_rosstat_macro, default.rosstat_ipc_mes, default.rosstat_ipc_weeks,
           default.rosstat_vvp_kvartal, default.rosstat_salaries_mes
new_mes:   1
new_weeks: 1
new_sal:   1
new_vvp:   1
old_mes:   0
old_weeks: 0
old_sal:   0
old_vvp:   0
```

То есть старых имён (`default.ipc_mes`, `default.ipc_weeks`, `default.salaries_mes`, `default.vvp_kvartal`)
нет ни в одной из двух витрин; `v_rosstat_macro` читает все четыре `rosstat_*`-таблицы,
`v_model_inputs` — две из них (`rosstat_ipc_mes`, `rosstat_ipc_weeks`) плюс свои не-росстатовские.

До переключения (тот же admin-запрос) `v_model_inputs` читала `default.ipc_mes` и `default.ipc_weeks` —
то есть переключение было настоящим, а не совпадением.

### MCP-проверка витрин (пользователь `kimi_reader`)

```
SELECT count() AS rows, uniqExact(series) AS series, toString(min(date)) AS d0, toString(max(date)) AS d1 FROM v_rosstat_macro;
{"columns": ["rows","series","d0","d1"], "rows": [[29050, 141, "1991-01-31", "2026-10-05"]]}

SELECT ipc_mes_last, ipc_mes_last_date, ipc_week_ytd, ipc_week_ytd_date FROM v_model_inputs;
{"columns": ["ipc_mes_last","ipc_mes_last_date","ipc_week_ytd","ipc_week_ytd_date"],
 "rows": [[100.3499984741211, "2026-09-30", 6.594052874301837, "2026-10-05"]]}

-- все четыре поля не NULL:
SELECT ..., ipc_mes_last IS NOT NULL AND ipc_week_ytd IS NOT NULL AND ipc_mes_last_date IS NOT NULL AND ipc_week_ytd_date IS NOT NULL AS all_present FROM v_model_inputs;
→ 100.3499984741211 | 2026-09-30 | 6.594052874301837 | 2026-10-05 | 1
```

29 050 = 1 716 + 26 611 + 302 + 421, рядов 141 = 4 + 131 + 5 + 1 — совпадает с ожиданием дословно.

## Шаг 8. Удаление старых таблиц и осиротевших строк

### Проверка, что старые имена нигде не остались

```
$ grep -rn "\bipc_mes\b\|\bipc_weeks\b\|\bsalaries_mes\b\|\bvvp_kvartal\b" --include=*.go --include=*.sql . | grep -v rosstat_
./rosstat/ipc_mes_test.go:27,34      — имя фикстуры testdata/ipc_mes.xlsx в комментарии и OpenFile
./rosstat/salaries_mes_test.go:14,19 — то же для testdata/salaries_mes.xlsx
./rosstat/ipc_weeks_test.go:16,23    — то же для testdata/ipc_weeks.xlsx
./rosstat/ipc_weeks_test.go:46       — комментарий «От прежней таблицы ipc_weeks …»
./rosstat/vvp_kvartal_test.go:15,22  — то же для testdata/vvp_kvartal.xlsx
```

Без тестов (`| grep -v _test.go`) — **пусто** (`exit 1`), то есть ни одного боевого `.go`/`.sql`
с прежними именами. Отдельно: в `dashboard/rosstat.json` и корневом `grafana_dashboard_rosstat`
(18 вхождений `ipc_mes`) старые имена остались — это legacy-экспорты, не провижинятся, по заданию
не трогались. Каталоги `.worktrees/*` — другие рабочие копии репозитория, не этот чекаут.

Объекты БД, читающие старые таблицы (`SELECT … FROM system.tables WHERE position(create_table_query, 'default.ipc_mes') > 0` и т. д.):
совпадения — только сами четыре удаляемые таблицы, **ни одной витрины или другой таблицы**;
после шага 7 обе живые витрины контура уже смотрели на `rosstat_*`.

Перед дропом четыре старые таблицы выгружены в `/tmp/t9_old_tables_backup.tsv`
(27 574 строки, sha256 `48a015f2e80498a9404220b1220615a99acfb7ba9a9de74ac5a5de9538763c64`) — страховка
вне репозитория; файл в git не попадает.

### Миграция

Временный `sql/tmp_t9_migrate.sql` (удалён после прогона, не коммитился), `SET mutations_sync = 2`:

```
$ make ch-sql FILE=sql/tmp_t9_migrate.sql
before_stale_catalog_rows	4
before_old_publications	1
after_stale_catalog_rows	0
after_old_publications	0
after_old_tables	0
catalog_rosstat_total	141
EXIT=0
```

Что сделано файлом: `DROP TABLE IF EXISTS ipc_mes / ipc_weeks / salaries_mes / vvp_kvartal`;
`ALTER TABLE source_publications DELETE WHERE source IN ('ipc_mes','ipc_weeks','salaries_mes','vvp_kvartal')`
(осиротевшая версия была ровно одна — `ipc_mes`); `ALTER TABLE series_catalog DELETE WHERE source='rosstat' AND series LIKE '%1991-2026%'`
— четыре осиротевшие строки каталога прежних имён ИПЦ (см. расхождение 3).

### Проверка после дропа (MCP)

```
v_rosstat_macro: 29050 строк / 141 ряд
v_model_inputs:  100.3499984741211 | 2026-09-30 | 6.594052874301837 | 2026-10-05
```

Деградации нет; контур золота не потерял входы ни на одном шаге миграции
(`ipc_mes_last`/`ipc_week_ytd` и обеих даты актуальности всегда были не NULL).

## Шаг 9. `make mcp-check` и `dagu validate`

```
$ make mcp-check
...
OK   витрина v_rosstat_macro видна: list_tables default
OK   витрина v_rosstat_macro непуста: строк: 29050, рядов: 141
OK   каталог rosstat полон: рядов rosstat: 141 (4 ИПЦ + 131 недельный + 5 ВВП + 1 зарплата)
OK   сырая rosstat_ipc_mes закрыта (ИПЦ помесячно): отказ ClickHouse 497 на rosstat_ipc_mes (ожидаемо)
OK   сырая rosstat_ipc_weeks закрыта (недельные цены): отказ ClickHouse 497 на rosstat_ipc_weeks (ожидаемо)
OK   сырая rosstat_salaries_mes закрыта (зарплата помесячно): отказ ClickHouse 497 на rosstat_salaries_mes (ожидаемо)
OK   сырая rosstat_vvp_kvartal закрыта (ВВП поквартально): отказ ClickHouse 497 на rosstat_vvp_kvartal (ожидаемо)
ПРОВЕРКА ПРОЙДЕНА
EXIT=0
```

Полный лог — 60+ строк, все `OK`; четыре новые строки контура Росстата выше.
Новых грантов не потребовалось: `make mcp-user` не запускался (витрины те же, новые витрины не появлялись;
гранты на витрины сохранились при `CREATE OR REPLACE VIEW`).

```
$ dagu validate dagu/rosstat.yaml
time=2026-10-14T13:06:28.176+05:00 level=WARN msg="No auth.mode configured — defaulting to 'builtin'. ..."
EXIT=0
```

Единственное сообщение — общее для всех DAG-файлов предупреждение `auth.mode`.

## Шаг 10. Обновление роадмапа

`docs/ROADMAP_KEY_RATE.md`:

- **§4.1** — строки `salaries_mes`/`vvp_kvartal` больше не «пусто»: `rosstat_salaries_mes` 1991-08-01 … 2026-08-01 (421), `rosstat_vvp_kvartal` 2011-04-01 … 2026-07-01 (302); строки `ipc_mes`/`ipc_weeks` переименованы в `rosstat_ipc_mes` (1 716) / `rosstat_ipc_weeks` (26 611, 131 позиция); вводная строка объясняет, что строки контура пересняты 2026-10-14, а прежние 1 712 / 2026-08-31 и 25 691 / 128 позиций — числа прежнего разбора; пункт 1 списка дефектов помечен закрытым шагом 1 фазы 2.
- **§5** — список входов контура: `ipc_mes`/`ipc_weeks`/`salaries_mes`/`vvp_kvartal` → `rosstat_*` (иначе документ ссылался бы на удалённые таблицы).
- **§6 фаза 2** — первый шаг закрыт ссылкой на новый блок приёмки.
- **§6, новый блок** — «Живая приёмка 2026-10-14 — фаза 2, шаг 1 …», по образцу блока фазы 3: девять
  пунктов «команда → результат» плюс семь названных расхождений (raw-точность vs Float32, четыре
  переименованных ряда ИПЦ, каталог 145→141, недельные −28/+948/+3, отставшая строка §4.1,
  изменение `ipc_week_ytd` при переключении, отсутствовавшая строка риска §8).
- **§8** — добавлена строка риска про технический долг `rosstat/` (в тексте её не было; долг устранён,
  приёмка выполнена); вводная фраза протокола приёмки расширена на «шаг 1 фазы 2».

## Шаг 11. Коммит

Коммит содержит только `docs/ROADMAP_KEY_RATE.md` (см. ниже). Временные SQL-файлы
(`sql/tmp_t9_check.sql`, `sql/tmp_t9_backup.sql`, `sql/tmp_t9_migrate.sql`) удалены и не коммитились;
`git status` до коммита показывает только `docs/ROADMAP_KEY_RATE.md` и предсуществующее
владельческое изменение `deploy/staging/dagu.service` (не трогалось).

---

## Итоговое состояние БД

| Объект | Состояние |
|---|---|
| `rosstat_ipc_mes` | существует, 1 716 точек / 4 ряда, 1991-01-31 … 2026-09-30 |
| `rosstat_ipc_weeks` | существует, 26 611 / 131, 2022-01-10 … 2026-10-05 |
| `rosstat_salaries_mes` | существует, 421 / 1, 1991-08-01 … 2026-08-01, нулей 0 |
| `rosstat_vvp_kvartal` | существует, 302 / 5, 2011-04-01 … 2026-07-01 |
| `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal` | **удалены** |
| `source_publications` (старые ключи) | **удалены** (была одна строка — `ipc_mes`) |
| `series_catalog` (`source='rosstat'`) | 141 (4 + 131 + 5 + 1) |
| `v_rosstat_macro` | читает только `rosstat_*`, 29 050 строк / 141 ряд |
| `v_model_inputs` | читает `rosstat_ipc_mes`/`rosstat_ipc_weeks`, четыре IPC-поля не NULL |

## Названные расхождения

1. **Каталог 145 вместо 141 (единственное расхождение с леджером; устранено миграцией).**
   Все четыре таблицы совпали с ожиданием дословно, но `v_series_catalog WHERE source='rosstat'`
   после четырёх импортёров давал **145**, а не 141: девять новых описаний (4 переименованных ИПЦ + 5 ВВП
   + 1 зарплата) записались поверх, но четыре прежних описания ИПЦ под старыми именами
   (`… в 1991-2026*)гг.`) остались — `series_catalog` дедуплицируется по `(source, series)`, а `series`
   сменился. Это арифметическое упущение в леджере (он сам называет переименование рядов отклонением),
   а не дефект импортёров: подтверждено запросом `count() = 4` по предикату `series LIKE '%1991-2026%'`.
   После удаления старых таблиц эти строки описывают ряды, которых нет ни в одной таблице, и удалены
   вместе с ними в шаге 8 → каталог **141**, как требует и леджер, и `scripts/mcp_check.py`.
2. **Raw-точность против `Float32`-колонок.** Квартальные и недельные значения в витрине —
   Float32-представление сырой ячейки (`49869.532405343656` → `49869.53125`, `100.65` → `100.6500015258789`,
   `101.31028730944105` → `101.31028747558594`). Разбор отдаёт сырое значение (стандарт §6.2), колонка
   таблицы — `Float32`; тесты spot-значений это учитывают (проверяют разбор, не таблицу).
3. **Четыре ряда ИПЦ переименованы** (прежние имена несли сноски и годы выгрузки) — сопоставление по
   старому `series` в `v_series_catalog` больше не работает.
4. **Недельные ряды −28 / +948 / +3** (25 691 → 26 611 точек, 128 → 131 ряд): −28 нулей из ячеек «…»,
   +948 числовых ячеек отрезанных обрывом строк (+66 в самих строках с «…» и +882 в строках ниже),
   +3 товара, которых в прежней таблице не было (`Поездки в отдельные страны Юго-Восточной Азии, поездка`,
   `Поездка на отдых на Черноморское побережье России, поездка`, `Поездка на отдых в Турцию, поездка` —
   получено сравнением имён из MCP-витрины с именами старой таблицы в `/tmp`-выгрузке).
5. **Строка `ipc_mes` в §4.1 отставала от данных**: прежний снимок 1 712 / 2026-08-31, живая таблица
   1 716 / 2026-09-30.
6. **`ipc_week_ytd` в `v_model_inputs` изменился при переключении витрины**: `6.504149308567153`
   (старые `ipc_weeks`, 128 товаров) → `6.594052874301837` (`rosstat_ipc_weeks`, 131 товар) — следствие
   исправленной выборки недельных товаров, а не деградация. `ipc_mes_last` (100.3499984741211),
   `ipc_mes_last_date` (2026-09-30) и `ipc_week_ytd_date` (2026-10-05) не менялись.
7. **§8 не содержал строки риска про технический долг `rosstat/`** (хотя план фазы 2 говорил об её
   «обновлении»): строка добавлена по образцу строки про `cbr/`.
8. **Правки роадмапа сверх буквы задания** (для неразрывности документа): §5 (имена таблиц во входе
   контура), вводная фраза протокола приёмки в §8 и пометка «закрыто» у пункта 1 списка дефектов §4.1.
