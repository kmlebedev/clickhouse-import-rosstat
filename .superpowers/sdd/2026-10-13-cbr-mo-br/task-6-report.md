# Task 6 — живая приёмка шага 1.5 (`cbr_mo_br`) — Status: **DONE**

Ветка `feat/cbr-mo-br`, BASE = `e92c5e4`, коммит приёмки — **`583e82d`** `roadmap key rate: mark 1.5 done (cbr_mo_br consensus acceptance)` (2 файла: `docs/ROADMAP_KEY_RATE.md`, `README.md`).

Все команды с env/ClickHouse — только через `make`; `~/.config/rosstat/env` не читался, секреты не печатались. `make ch-up`/`make ch-down` не запускались. `deploy/staging/dagu.service` (незакоммиченная правка владельца) не тронут и в индекс не добавлялся. Временные SQL-файлы лежали в `/tmp` и удалены.

## 1. `make all` — зелёный (exit 0)

```
0 issues.
go vet ./...
go test -race ./...
?   	github.com/kmlebedev/clickhouse-import-rosstat	[no test files]
...
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	18.900s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/wgc	48.099s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/views	2.806s
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o build/clickhouse-import-rosstat .
go build -o build/ingest ./cmd/ingest
```

`0 issues.` — от golangci-lint (gofmt-проверка прошла молча), `go vet` без замечаний, все тестовые пакеты `ok` (в т.ч. `cbr` 18,9 с, `wgc` 48,1 с), обе сборки прошли. Полный лог — `/tmp/task6_make_all.log` (вне репозитория).

## 2. Первый `make import STAT=cbr_mo_br` — полный импорт (rc=0)

```
go build -o build/clickhouse-import-rosstat-native .
CLICKHOUSE_IMPORT_STAT=cbr_mo_br ./build/clickhouse-import-rosstat-native
time="2026-10-13T23:33:15+05:00" level=info msg="Connected to clickhouse"
time="2026-10-13T23:33:16+05:00" level=info msg="Get xlsx file "
time="2026-10-13T23:33:16+05:00" level=info msg="cbr_mo_br: собрано 9348 точек"
time="2026-10-13T23:33:16+05:00" level=info msg="Imported 9348 rows of cbr_mo_br"
time="2026-10-13T23:33:16+05:00" level=info msg="Imported 9348 rows of cbr_mo_br"
```

- строка про собранные точки: **`cbr_mo_br: собрано 9348 точек`**;
- строка про импорт: **`Imported 9348 rows of cbr_mo_br`** (печатается дважды: внутри `util.ClickHouseImport.Import` и в `cbr/mo_br.go` — как у прочих импортёров, не дефект);
- **N = 9 348 точек**, что совпадает со спекой §3.1 (9 348 точек / 642 ряда): октябрьская волна на 2026-10-13 ЦБ ещё не опубликована, версия файла — сентябрьская (`max(date) = 2026-09-01`).

Наблюдение: в строке `Get xlsx file ` поле пути пустое (`util/xls.go:163` логирует `xlsx.Path`, который у этой копии структуры не выставлен — данные берутся из `DataUrl`). Косметика лога, на приёмку не влияет; в отчёт вынесено как сомнение.

## 3. Админ-чек `source_publications` (`/tmp/mo_br_pub.sql` → `make ch-sql` → файл удалён)

SQL:

```sql
SELECT source, name, fingerprint, first_seen, points, url FROM source_publications FINAL WHERE source = 'cbr_mo_br';
SELECT count() AS all_publications FROM source_publications;
```

Фактический вывод (`make ch-sql FILE=/tmp/mo_br_pub.sql`, rc=0):

```
cbr_mo_br	full.xlsx	4fef4f25716d388e9048149fa296039c	2026-10-13 23:33:16	9348	https://www.cbr.ru/Content/Document/File/144490/full.xlsx
1
```

- ровно одна строка по `source = 'cbr_mo_br'`, `points = 9348 = N`;
- fingerprint непустой и равен ожидаемому `4fef4f25716d388e9048149fa296039c` (он же появился в no-op-строке повтора, шаг 4);
- `SELECT count() FROM source_publications` → **1** — таблица создана этим PR, других версий по всем источникам нет.

Файл `/tmp/mo_br_pub.sql` удалён (`ls` → No such file or directory), в git не попадал.

## 4. Повторный `make import STAT=cbr_mo_br` — no-op (rc=0)

```
go build -o build/clickhouse-import-rosstat-native .
CLICKHOUSE_IMPORT_STAT=cbr_mo_br ./build/clickhouse-import-rosstat-native
time="2026-10-13T23:33:50+05:00" level=info msg="Connected to clickhouse"
time="2026-10-13T23:33:51+05:00" level=info msg="cbr_mo_br: no-op, версия 4fef4f25716d388e9048149fa296039c уже импортирована, 9348 точек на месте"
time="2026-10-13T23:33:51+05:00" level=info msg="Imported 9348 rows of cbr_mo_br"
```

- no-op-строка дословно: **`cbr_mo_br: no-op, версия 4fef4f25716d388e9048149fa296039c уже импортирована, 9348 точек на месте`**;
- в логе нет ни `Get xlsx file` (скачивания), ни `собрано … точек` (разбора/батча) — прогон ограничился страницей + HEAD;
- повторный админ-чек (`/tmp/mo_br_pub2.sql`, тоже удалён) — **та же единственная строка** с тем же `first_seen = 2026-10-13 23:33:16` и `count() = 1`, новых строк нет;
- счётчики витрины не изменились: `SELECT count() FROM v_cbr_mo_br` → 9348 до и после повтора (см. шаг 6).

## 5. `make mcp-user` — гранты применены (rc=0)

Вывод маскирован (`IDENTIFIED BY '***'`), пароль не печатался. Последние строки вывода (всего 33 строки, все `OK`):

```
OK   GRANT SELECT ON default.v_keyrate_expectations TO kimi_reader
OK   GRANT SELECT ON default.v_cbr_survey TO kimi_reader
OK   GRANT SELECT ON default.v_cbr_mo_br TO kimi_reader
```

Последняя строка — **`OK GRANT SELECT ON default.v_cbr_mo_br TO kimi_reader`**. Выполнено ДО MCP-проверок (ruling контроллера): без гранта витрина `v_cbr_mo_br` под `kimi_reader` не читается.

## 6. MCP-проверки под `kimi_reader` (`mcp__clickhouse__run_query`)

| Запрос | Результат |
|---|---|
| `SELECT count() AS points, uniqExact(series) AS series, min(date) AS d0, max(date) AS d1 FROM v_cbr_mo_br` | **9348 / 642 / 2021-05-01 / 2026-09-01** |
| `SELECT count() AS catalog_rows FROM v_series_catalog WHERE source = 'cbr_mo_br'` | **642** (= числу рядов из первой строки) |
| `SELECT uniqExact(date) AS waves, min(date), max(date) FROM v_cbr_mo_br` | 43 волны, 2021-05-01 … 2026-09-01 |
| `SELECT series, date, value FROM v_cbr_mo_br WHERE date = '2026-09-01' AND series IN ('key_rate_avg_2026_median','cpi_dec_2026_median','neutral_rate_median') ORDER BY series` | `cpi_dec_2026_median` 2026-09-01 = **6.6**; `key_rate_avg_2026_median` 2026-09-01 = **14.5**; `neutral_rate_median` 2026-09-01 = **8.0** |
| `SELECT count(), uniqExact(series), min(date), max(date) FROM v_cbr_mo_br WHERE series LIKE '%2026%'` | 1650 / 84 / 2023-09-01 / 2026-09-01 (прогнозные годы 2026+ появились с волны 2023-09 — согласуется с разбором) |

Критерии: точек 9 348 ≥ 9 348 ✅, рядов 642 ≥ 642 ✅, `min(date) = 2021-05-01` ≤ 2021-05-01 ✅, `max(date) = 2026-09-01` ≥ 2026-09-01 ✅, каталог = числу рядов ✅, три спота сентябрьской волны совпали с ожиданием (14.5 / 6.6 / 8.0) ✅.

## 7. `make mcp-check` (rc=0) и `dagu validate`

`make mcp-check` → последняя строка **`ПРОВЕРКА ПРОЙДЕНА`**, exit 0. Хвост вывода (новые строки контура — предпоследние):

```
OK   витрина v_cbr_survey существует и читается: строк: 2084
OK   каталог cbr_survey описывает 13 рядов: записей: 13
OK   витрина v_cbr_mo_br существует и читается: строк: 9348
OK   каталог cbr_mo_br непуст: записей: 642
OK   витрина v_cbr_macro существует и читается: строк: 46510
OK   сырая cbr_households_b_mes закрыта: отказ ClickHouse 497 на cbr_households_b_mes (ожидаемо)
ПРОВЕРКА ПРОЙДЕНА
```

(Строка шага 1.3 «каталог cbr_survey описывает 13 рядов» сохранена; для `cbr_mo_br` скрипт печатает `каталог cbr_mo_br непуст: записей: 642` — проверка ненулевого числа записей, как и заложено в `scripts/mcp_check.py:433`. В роадмапе процитированы фактические формулировки.)

`dagu validate dagu/cbr.yaml` → **rc=0**, единственное сообщение — известный WARN:

```
time=2026-10-13T23:34:13.482+05:00 level=WARN msg="No auth.mode configured — defaulting to 'builtin'. …"
```

## 8. Изменённые файлы

- `docs/ROADMAP_KEY_RATE.md` (+20/−3): строка 125 (шаг 1.5 таблицы фаз) → `✅ **сделан этим PR** — приёмка ниже`; строка 108 (§5) → `✅ фаза 1.5, этим PR`; новый блок `#### Живая приёмка 2026-10-13 — шаг 1.5 (макроопрос ЦБ) — ✅ выполнен` (со ссылками на спеку/план и пунктами 1–5 с фактическими числами) вставлен после блока приёмки 1.3, перед `### Фаза 2`. Другие секции не трогались.
- `README.md` (±1): в абзаце про `cbr_mo_br` (стр. 122) `полный путь (≈560 КБ XLSX, …)` → `полный путь (~560 КБ: страница + XLSX, …)` — 560 КБ это сумма страницы (~260 КБ) и XLSX, как в спеке §3.1. Остальной текст строки не менялся.

Проверка правок: `mcp__jetbrains__get_file_problems` по обоим файлам — только pre-existing WEAK WARNING «Table is not correctly formatted» (кириллическая ширина во всех таблицах файлов, включая 8 таблиц, которых правка не касалась) и один pre-existing WARNING про SQL-диалект в README; новых ошибок нет.

Коммит:

```
git status --short   # до add: M README.md / M deploy/staging/dagu.service / M docs/ROADMAP_KEY_RATE.md
git add docs/ROADMAP_KEY_RATE.md README.md
git commit -m "roadmap key rate: mark 1.5 done (cbr_mo_br consensus acceptance)"
[feat/cbr-mo-br 583e82d] roadmap key rate: mark 1.5 done (cbr_mo_br consensus acceptance)
 2 files changed, 20 insertions(+), 3 deletions(-)
git log --oneline -1  # 583e82d roadmap key rate: mark 1.5 done (cbr_mo_br consensus acceptance)
git status --short    #  M deploy/staging/dagu.service   (незакоммиченная правка владельца, не тронута)
```

## Сомнения / заметки

1. **`Get xlsx file ` с пустым путём** в логе полного импорта — поле `xlsx.Path` не выставляется при импорте через `DataUrl` (`util/xls.go:163`). Косметика, не влияет на данные; не правилось (вне объёма Task 6).
2. **`Imported 9348 rows of cbr_mo_br` дважды** в первом прогоне — `util.ClickHouseImport.Import` логирует строку сама, и её же логирует `cbr/mo_br.go` (как у `cbr_mp_survey`). Принято как существующее поведение пакета.
3. Ожидание спеки 9 348 / 642 подтвердилось полностью: **октябрьская волна на дату приёмки не опубликована**, поэтому `max(date) = 2026-09-01`. Если ЦБ опубликует волну позже, первый прогон даст больше точек, а запись в `source_publications` обновится — механика но-опа это покрывает.
4. В `.superpowers/sdd/.../task-1-report.md` глубина по отдельным показателям указана как 43 волны — совпало с `uniqExact(date) = 43` из витрины.
5. Инструменты `mcp__clickhouse__*` и `mcp__jetbrains__*` в наборе были, использованы; админ-SQL — только через `make ch-sql`, SQL к витринам — только через `mcp__clickhouse__run_query`.
