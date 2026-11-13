# P0.4 — план: Go-импортёр весов ИПЦ `rosstat_cpi_weights`

> Пункт `key-rate/phase-4-cb-comp-88-groups-live` P0.4, issue [#479](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/479), `role:go`. Спека — [2026-11-01-cb-comp-cpi-weights-design.md](../specs/2026-11-01-cb-comp-cpi-weights-design.md). Ветка `feat/cb-comp-88-weights`, worktree `.worktrees/cb-comp-88-weights`.
> ML-потребитель весов вынесен в `kmlebedev/bi-ml-models` — здесь только публикация данных.

## 0. Подготовка

- [x] Issue #479, claim-комментарий, Project V2: `In Progress`, `Branch`, `Spec / Plan`.
- [x] Спека (первый коммит) и план (второй коммит).
- [x] Probe форматов по Wayback-копиям; живой источник на 2026-11-01 отдавал 502.
- [x] Probe 2026-11-13: страница цен 200, файлы `CPR_*` — 404; написание имён в живой странице сменилось (`CPR_Tov_RF_…`, `CPR_Gr-tov-RF_…`).
- [x] Полные книги из Wayback (2026-11-13): товарная 2025 — 11 106 точек, групповая 2026 — 18 412 точек; найдена и исправлена потеря строки без кода (объединённая группа транспорта 2025–2026).

## 1. Go-импортёр `rosstat_cpi_weights`

- [x] `rosstat/cpi_weights.go`: DDL `rosstat_cpi_weights`, внешний `Import()` (страница → две ссылки → два XLSX → два батча), per-file HEAD-префлайт, отказ restore, `AfterImport` (каталог + витрина).
- [x] `rosstat/cpi_weights_parse.go`: разбор листов-годов, товарный и групповой макеты, `variant` 2022, `level`, синтетический `code='name:…'`, валидация §4 спеки.
- [x] `rosstat/cpi_weights_test.go` + `testdata/cpi_weights_{items,groups}.xlsx`: 15 тестов — оба макета, 2022, дубль-конфликт, мусор, сумма, отсутствие года, пустой лист, реальные фикстуры.
- [x] Селекторы регистронезависимы (`a[href*="cpr_tov_rf_2004-" i]`, `a[href*="cpr_gr-tov-rf_2001-" i]`); `TestCPIWeightsFindLink` — на `price_page.html` и `price_page_cpr_case.html`, групповая книга горизонта `2001-`, а не годовая.
- [x] Реестр `rosstat/importers.go` (12-я запись), `dagu/rosstat.yaml` (12-й шаг в хвосте), энфорсер в `importers_test.go`.
- [x] Витрина `v_rosstat_cpi_weights` (комментарии таблицы и колонок), каталог `cpi-weight:<code>:<items|groups>`, грант в `sql/mcp_kimi_reader.sql`, проверка в `scripts/mcp_check.py`.

## 2. Документы

- [x] `docs/IMPORTERS.md` (строка rosstat + раздел «Веса ИПЦ»), `ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7, `README.md`, `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`.
- [x] Отчёт `docs/research/2026-11-01-cb-comp-cpi-weights.md` + JSON: форматы, строки/годы, суммы, написание ссылок, «что не проверено».

## 3. Прогоны

- [x] `go test ./rosstat/ -run 'CPIWeights|RosstatImporters'` — зелёный.
- [x] `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml`.
- [ ] `make import STAT=rosstat_cpi_weights` ×2 (строки + no-op), `make mcp-user`, `make mcp-check` — **заблокировано источником**: страница цен 200, файлы `CPR_*` 404. Повторить, когда файлы начнут отдаваться.

## 4. Приёмка

- [ ] Живой импорт и `make mcp-check` — после восстановления файлов источника.
- [ ] PR: `Closes #479`, протокол приёмки комментарием, `Status=In Review`; `Done` — только после мержа и протокола.

## Stop-rules

См. §7 спеки: сумма товарного листа вне 90…100,5, отсутствие 2025/2026, ненайденная ссылка — падение до записи; недоступный источник — «не проверено», не `Done`.
