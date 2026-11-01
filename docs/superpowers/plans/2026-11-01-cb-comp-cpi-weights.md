# P0.4 — план: live-веса ИПЦ и переключение `load_weights`

> Пункт `key-rate/phase-4-cb-comp-88-groups-live` P0.4, issue [#479](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/479). Спека — [2026-11-01-cb-comp-cpi-weights-design.md](../specs/2026-11-01-cb-comp-cpi-weights-design.md). Ветка `feat/cb-comp-88-weights`, worktree `.worktrees/cb-comp-88-weights`.

## 0. Подготовка

- [x] Issue #479, claim-комментарий, Project V2: `In Progress`, `Branch`, `Spec / Plan`, `Acceptance`.
- [x] Спека (этот PR) — первым коммитом.
- [x] План — вторым коммитом.
- [ ] Probe файлов Росстата, когда источник ответит; записать §10 спеки. Пока источник 502 — фикстуры синтетические.

## 1. Go-импортёр `rosstat_cpi_weights`

- [ ] `rosstat/cpi_weights.go`: DDL `rosstat_cpi_weights`, `Import()` (страница → две ссылки → два XLSX → один батч), HEAD-префлайт по двум файлам, отказ restore, `AfterImport` (каталог + витрина).
- [ ] `rosstat/cpi_weights_parse.go`: разбор листов-годов, два макета, вариант 2022, `level`, валидация §4 спеки.
- [ ] `rosstat/cpi_weights_test.go` + `testdata/cpi_weights_{tov,gr}.xlsx`: оба макета, 2022, дубль-конфликт, мусор, сумма, отсутствие года; `FindLink` на HTML-странице.
- [ ] Реестр `rosstat/importers.go` (12-я запись), `dagu/rosstat.yaml` (12-й шаг в хвосте).
- [ ] Витрина `v_rosstat_cpi_weights` (комментарии таблицы и колонок), каталог `cpi-weight:<code>:<items|groups>`, грант в `sql/mcp_kimi_reader.sql`, проверка в `scripts/mcp_check.py`.

## 2. ML

- [x] `cb_comp_bottomup.py`: `load_archive_weights`, `load_live_weights`, `load_weights(source=...)`; `cb_comp_weights_check.py`: `compare_weights`, `coverage_report`.
- [ ] `cb_comp_bottomup.load_weights(..., source="live", query=None)`; `_year_weights` терпит отсутствующие листы; `WEIGHT_YEARS` до 2026 в live-путях.
- [ ] `cb_comp_h1_bs_combo.py`: `--weights {live,archive}` (default live), `inputs.weights_source`; `cb_comp_live_en.prepare(..., weights_source)`.
- [ ] `cb_comp_h1_shadow.py`, `cb_comp_groups_panel.py`, `cb_comp_h1_weekly*.py` — явно `archive` (P0.6/Task 4/4b заморожены).
- [ ] `scripts/models/cb_comp/cb_comp_weights_check.py` + `_test.py`; `make cb-comp-weights-check` / `-test`.
- [ ] Тесты: live vs archive на фикстуре витрины, процент/100, приоритет group, конфликт, отсутствующий код, покрытие.

## 3. Прогоны и гейт

- [ ] `make cb-comp-h1-bs-combo-test` — зелёный.
- [ ] `make cb-comp-h1-bs-combo ARGS="--archive … --weights archive"` — пины 1,035 / 0,957 / 1,010 / 0,894.
- [ ] `make cb-comp-h1-bs-combo ARGS="--archive … --weights live"` — после живого импорта; те же числа, `predictions.csv` побайтно.
- [ ] `make cb-comp-weights-check` — 2016–2024 `max|Δ| = 0`, сумма 2025–26, покрытие.
- [ ] `make import STAT=rosstat_cpi_weights` ×2 (строки + no-op), `make mcp-user`, `make mcp-check` — когда источник ответит.

## 4. Документы и приёмка

- [ ] `docs/research/2026-11-01-cb-comp-cpi-weights.md` + JSON: форматы, строки/годы, `max|Δ|`, суммы, покрытие, решение, «что не проверено».
- [ ] `docs/guides/cb-comp-live-model-map.md` §1.5/§1.10/P0.4; `docs/ROADMAP_KEY_RATE.md` — строка #419/P0.4.
- [ ] `docs/IMPORTERS.md`, `ARCHITECTURE.md` §6.1/§6.2/§6.3/§7, `README.md`, `docs/LIMITATIONS.md`.
- [ ] `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml`.
- [ ] PR: `Closes #479`, протокол приёмки комментарием, `Status=In Review`; `Done` — только после мержа и протокола.

## Stop-rules

См. §8 спеки: при расхождении весов 2016–2024 > 1e-12, отсутствии кода в live или сумме вне 100 ± 0,5 — `load_weights` не переключается, отрицательный результат публикуется.
