# P0.4a — план: импортёр `rosstat_cpi_spr` (веса и индексы из месячной публикации)

> Пункт `key-rate/phase-4-cb-comp-88-groups-live` P0.4a, issue [#569](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/569), `role:go`. Спека — [2026-11-13-rosstat-cpi-spr-design.md](../specs/2026-11-13-rosstat-cpi-spr-design.md). Ветка `feat/cb-comp-88-cpi-spr`, worktree `.worktrees/cb-comp-88-cpi-spr`.

## 0. Подготовка

- [x] Issue #569, claim-комментарий, Project V2: `In Progress`, `Branch`, `Item`, `Phase`, `Priority`.
- [x] Спека (первый коммит), план (второй коммит).
- [x] Research по доступности книги 2026 — issue [#481](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/481).

## 1. Код

- [ ] `rosstat/cpi_spr.go`: DDL `rosstat_cpi_spr`, собственный `Import()` (страница → `a[href*="ipc_spr_" i]`, отказ restore), `ImportFunc`, `AfterImport` (каталог + витрина), селектор и константы.
- [ ] `rosstat/cpi_spr_parse.go`: листы `MMYYYY`, месяц/год из заголовка «за <месяц> <год> г.» с фолбэком на имя листа; шапка в двух строках; вес и три индекса (`Nullable`); валидация §4 спеки.
- [ ] `rosstat/cpi_spr_test.go` + `testdata/cpi_spr.xlsx` (2 реальных листа 2026) + `testdata/price_page_spr.html`: 12 тестов — реальная книга, `0922025`, пропуск месяца, дубль, сумма, NULL-индексы, ссылка.
- [ ] Реестр `rosstat/importers.go` (13-я запись), энфорсер в `importers_test.go`, 13-й шаг `dagu/rosstat.yaml`.
- [ ] Витрина `v_rosstat_cpi_spr` (комментарии таблицы и колонок), каталог `cpi-spr:<код>:<weight|index_prev|index_ytd|index_yoy>`, грант в `sql/mcp_kimi_reader.sql`, проверка в `scripts/mcp_check.py`.

## 2. Документы

- [ ] `ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7, `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, строка P0.4a в `docs/ROADMAP_KEY_RATE.md`.
- [ ] Research-документ P0.4a не нужен: сверки уже в research по доступности книги 2026 (issue [#481](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/481)).

## 3. Прогоны

- [ ] `go test ./rosstat/ -run 'CPISPR|RosstatImporters' -v` — зелёный.
- [ ] `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml`.
- [ ] `make import STAT=rosstat_cpi_spr` дважды (строки + no-op), `make mcp-user`, `make mcp-check`.
- [ ] Сверка 2026-01 с книгой групп 2026 — 742/742 (SQL по `v_rosstat_cpi_spr` и `v_rosstat_cpi_weights`, если dev-сид из #481 на месте).

## 4. Приёмка

- [ ] PR: `Closes #569`, протокол живой приёмки комментарием, `Status=In Review`; `Done` — только после мержа и протокола.

## Stop-rules

См. §6 спеки: сумма листа вне 900…1100, кодов < 700, нечитаемый месяц/год или ненайденная ссылка — падение до записи.
