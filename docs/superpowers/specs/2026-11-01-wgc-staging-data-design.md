# WGC staging: рабочий каталог DAG и отказ без выгрузок — протокол

> **Пункт:** `gold-thesis/phase-3-followup-8`, issue [#507](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/507), `role:go`. WGC-часть issue #484; Polyus-часть закрыта PR [#500](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/500).

## 1. Зачем

На staging `v_cb_gold_dynamics` отдаёт 7 строк стран с NULL во всех метриках. Причины, подтверждённые в #484:

- на хосте нет `/root/dagu/wgc/data` (и `/root/wgc/data`): выгрузки `Changes_latest_as_of_*.xlsx` и `World_official_gold_holdings_as_of_*.xlsx` скачиваются браузером вручную и на staging не попадали;
- `dagu/wgc.yaml` без `working_dir`: Dagu 2.18 запускает шаги в `dag-run-work/wgc/<id>`, откуда относительный `wgc/data` не разрешается;
- `deploy-staging-data` копирует только датапаки Polyus/ЮГК;
- импортёр при отсутствии файлов пишет `warn + 0 rows`, поэтому DAG остаётся зелёным — отказ тихий.

## 2. Решения

### 2.1. Рабочий каталог DAG

`working_dir: /root/dagu` в `dagu/wgc.yaml` (на уровне DAG, как в `financial.yaml` после #482). Все шаги цепочки (`wgc_etf_flows`, `wgc_cb_reserves`) видят `/root/dagu/wgc/data`.

### 2.2. Доставка выгрузок

`Makefile` цель `deploy-staging-data`:

- создаёт `/root/dagu/wgc/data`;
- копирует `wgc/data/*.xlsx` при наличии;
- если файлов нет — печатает явный `WARN`, не роняя деплой (папка в `.gitignore`, в чистом CI её нет).

Ручной runbook остаётся: скачать выгрузки на gold.org/goldhub, положить в `wgc/data/`, выполнить `make deploy-staging-data` (или скопировать в `/root/dagu/wgc/data` вручную).

### 2.3. Fail-loud по флагу DAG

`wgc/cb.go`:

- env `WGC_REQUIRE_DATA=1` (константа в `wgc/`) — требовать **оба** файла: `Changes_latest_as_of_*.xlsx` и `World_official_gold_holdings_as_of_*.xlsx`;
- если хотя бы одного нет — ошибка с каталогом и масками отсутствующих файлов, DAG краснеет;
- без флага — прежнее поведение: оба файла отсутствуют → `warn + 0`; частичный набор → warn и только доступная часть (дельты или уровни).

Почему оба файла: витрина `v_cb_gold_dynamics` показывает и MoM/YTD (дельты), и уровень; при одном файле часть колонок остаётся NULL, и «зелёный» DAG снова маскирует неполный источник. Ручной `make import STAT=wgc_cb_reserves` без флага сохраняет старое поведение — в том числе для локальной разработки без выгрузок.

В `dagu/wgc.yaml` шаг `wgc_cb_reserves` получает `WGC_REQUIRE_DATA=1`. Шаг ETF остаётся автоматическим (JSON API) и от выгрузок ЦБ не зависит.

### 2.4. Аудит остальных DAG

- `ppi/data/*`, `weeklysa/data/*` — `go:embed`, рантайм-путей нет;
- `edisclosure` — абсолютный `EDISCLOSURE_INPUT_DIR`;
- `financial` — `working_dir` добавлен #482 (уже в `main`);
- меняется только `wgc`.

## 3. Границы

- Не меняются DDL, таблицы, витрины, гранты, имена рядов.
- Не добавляется машинное скачивание выгрузок ЦБ (goldhub отдаёт файлы только браузеру).
- Не меняется расписание (ежедневно 20:17 МСК) и порядок шагов.

## 4. Критерий приёмки

1. `dagu validate dagu/wgc.yaml` — OK.
2. `go test ./wgc` — тесты: без флага пустая папка = warn+0; с `WGC_REQUIRE_DATA=1` пустая папка = ошибка; с флагом и одним файлом = ошибка с маской отсутствующего; с флагом и обоими файлами разбор идёт.
3. `make all`, `make docs-check`.
4. Staging: `make deploy-staging-binary`, `make deploy-staging-dagu`, `make deploy-staging-data`; `dagu start wgc` (или шаг `wgc_cb_reserves`) → `Imported 1942 rows` (или актуальное число), `v_cb_gold_dynamics` — 7 стран без NULL.
5. Повторный прогон — те же ключи/значения, exit 0.
6. `make mcp-check` — релевантные проверки `v_cb_gold_dynamics` и каталога `source='wgc'` зелёные; публичный MCP подтверждает витрину.
7. Протокол — комментарием в PR; роадмап — тем же PR.
