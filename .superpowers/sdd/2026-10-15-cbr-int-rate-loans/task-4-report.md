# Task 4 — расписание, снимок покрытия, MCP-проверки для `cbr_int_rate_loans`

Статус: **DONE**. Ветка `key-rate-4-2-8-loan-rates`. Импортёры не запускались, против ClickHouse
не выполнялось ничего (ни `make import`, ни `make mcp-check`, ни SQL через MCP) — это Task 6.

## Что изменено (3 файла, +45 строк, ничего не удалено)

### 1. `dagu/cbr.yaml`
Шаг `cbr_int_rate_loans` добавлен СРАЗУ ПОСЛЕ `cbr_bank_int_rate` (перед `cbr_credit_m2x`) —
порядок шагов совпадает с порядком записей реестра `cbr/importers.go`
(`cbr_bank_int_rate`, `cbr_int_rate_loans`, `cbr_credit_m2x`). Форма шага скопирована у соседей
дословно: `run: "$ROSSTAT_IMPORT_BIN"`, `env: CLICKHOUSE_IMPORT_STAT=cbr_int_rate_loans`,
`retry_policy: limit 3, interval_sec 300`. Другие шаги не тронуты; цепочка остаётся `type: chain`.

### 2. `sql/key_rate_coverage.sql`
Ветка добавлена после `cbr_bank_int_rate` (перед `cbr_m2`), дословно по форме соседей:

```
UNION ALL SELECT 'cbr_int_rate_loans', name, count(), min(date), max(date) FROM cbr_int_rate_loans FINAL GROUP BY name
```

Разделительные строки — как у соседей, 4 пробела (проверено `sed | cat -et`), файл остаётся
read-only: только `SELECT`, `FINAL` на месте (требование шапки файла про ReplacingMergeTree).
Другие ветки не менялись.

### 3. `scripts/mcp_check.py`
Две проверки добавлены в блок контура ЦБ — сразу после проверок витрины `v_cbr_macro`
(строка «сырая `cbr_households_b_mes` закрыта»), до контура `cbr_mp_summary`. Комментарии
по-русски, в стиле файла:

* (а) **каталог полон**: `SELECT count() FROM v_series_catalog WHERE source = 'cbr' AND origin LIKE '%loans_nonfin.xlsx%'`,
  ожидание ровно 16 (точное равенство, как у `survey_catalog`/`rosstat_catalog`/`sber_*`, не «> 0»);
  детализация печатает наблюдённое число и подсказку `make import STAT=cbr_int_rate_loans`
  (каталог перепубликовывается прогоном). 16 = 8 сроков кредита × 2 группы («Всего»/«СМП»),
  имена рядов собирает код (`cbrIntRateLoansNames`), а `origin` выбран потому, что
  `source='cbr'` — общий для всех импортёров ЦБ. Разбор ответа — через тот же
  `result_rows(...)`, ключ `count()`.
* (б) **сырая таблица закрыта**: `SELECT count() FROM cbr_int_rate_loans`, ожидается отказ
  ClickHouse 497 — приём скопирован у `cbr_households_b_mes`:
  `denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw`, детализация
  «отказ ClickHouse 497 на cbr_int_rate_loans (ожидаемо)» либо первые 200 символов ответа.

Ни одна существующая проверка не изменена и не удалена (diff — только вставки).

## Команды и результаты

```
$ go build ./...
BUILD_OK

$ go test -race -count=1 -run TestCbrImportersPrefixedAndRegistered ./cbr/
ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  1.591s      # PASS: шаг dagu теперь виден проверке (г)

$ go test -race -count=1 ./cbr/           # весь пакет, для страховки
ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  18.333s

$ python3 -m py_compile scripts/mcp_check.py
PY_COMPILE_OK

$ gofmt -l cbr/
(пусто)
```

Проверки правок через GoLand MCP (`get_file_problems`):
* `dagu/cbr.yaml` — `errors: []` (проблем нет);
* `sql/key_rate_coverage.sql` — только `WARNING: SQL dialect is not configured` на строке 1
  (штатное для файла без настроенного диалекта, было и до правки);
* `scripts/mcp_check.py` — только те же `SQL dialect is not configured` на каждой строке с SQL
  (предсуществующий шум файла, ни одного error/noinspection; новая строка 489 — та же категория).

`dagu validate` не запускался (живая проверка — Task 6); YAML сверен глазами по образцу соседей
(отступы 2/4/6, `- id:` на уровне списка шагов), IDE-парсер претензий не имеет.

## Коммит

`5b4716d` — `schedule cbr_int_rate_loans, extend coverage snapshot and mcp checks`
(`dagu/cbr.yaml`, `sql/key_rate_coverage.sql`, `scripts/mcp_check.py`; 3 files changed, 45 insertions(+)).
Побочных артефактов нет: `scripts/__pycache__` от `py_compile` удалён, после коммита `git status`
пуст (отчёт лежит в `.superpowers/sdd/`, закрытом локальным `.gitignore`).

## Замечания

* Живые проверки (реальные `count()` каталога = 16, отказ 497 на сырой таблице, форма снимка
  покрытия) остаются за Task 6: в текущей БД ряды ещё не импортированы, поэтому значения
  проверок не подтверждены фактом — подтверждён только офлайн-каркас.
* Числа в проверках (16 рядов, коды отказа 497) взяты из брифа и кода импортёра
  (`cbrIntRateLoansNames` = 8 сроков × 2 группы), а не из ответа ClickHouse.
