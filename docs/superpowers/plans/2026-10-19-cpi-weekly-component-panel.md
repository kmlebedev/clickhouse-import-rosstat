# План: недельная компонентная панель ИПЦ — Task 3 (#236)

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-components` (`docs/ROADMAP_KEY_RATE.md`, фаза 4).
**Issue:** [#236](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/236); родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76).
**Спека:** [2026-10-19-cpi-weekly-component-panel-design.md](../specs/2026-10-19-cpi-weekly-component-panel-design.md).
**Ветка:** `feat/cpi-weekly-component-panel`.
**Worktree:** `.worktrees/cpi-weekly-component-panel`.

## Global Constraints

- Только чтение ClickHouse; в БД не пишем; DDL/GRANT/Go-код не меняем.
- `scripts/cpi_monthly.py`, `scripts/cpi_monthly_v3.py`, `scripts/cpi_public_panel_audit.py` не меняются; mapping переиспользуется импортом.
- Не использовать 87 кодов ОКВЭД2, `model.xlsx`, BSU-панель и `panel.csv`; `mp_survey_data.xlsx` — не вход Task 3.
- Веса — годовые 2016–2024 (2013–2015 → 2016, 2025–2026 → 2024); fixed-2024 — робастность.
- Полнота месяца: `n_weeks ≥ 3`, `prev` не дальше 14 дней, максимальный разрыв ≤14 дней; иначе NaN.
- As-of: недельная точка `date + 2`, cutoff 8-е M+1; месячная метка — не раньше конца M+1.
- Тесты — stdlib `unittest`, офлайн, без сети/ClickHouse; `openpyxl` — только для синтетических весов/словаря.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека, план, issue

**Files:** `docs/superpowers/specs/2026-10-19-cpi-weekly-component-panel-design.md`, `docs/superpowers/plans/2026-10-19-cpi-weekly-component-panel.md`.

- Зафиксировать метод, правила NaN, веса, реконструкцию, гейт, as-of, паспорт.
- Коммиты: `docs: add weekly CPI component panel design`, `docs: add weekly CPI component panel plan`.

## Task 2: загрузка, mapping, веса

**Files:** `scripts/cpi_weekly_component_panel.py`.

1. Импорт `cpi_public_panel_audit as audit`; переиспользовать `load_weekly`, `load_monthly`, `load_weights`, `load_dictionary`, `build_mapping`, `coverage_by_year`, `component_coverage`, `cutoff_for`, `available_from_weekly`, `available_from_mes`, `month_start`, `month_end`, `shift_month`, `sample_months`, `compound_month`, `resolve_archive`, `sha256_file`, `git_sha`, `write_json`.
2. `load_weekly_products()` — 148 товарных рядов, агрегат исключён.
3. `load_monthly_components()` — 4 ряда, `value → pp`.
4. `build_weights_matrix(archive, matched_rows)` — по годам 2016–2024: `by_code`, `component_denominators`, `total_denominator`; кэш `lru_cache`.
5. `weight_year_for(month)` — 2016–2024 / 2016 / 2024.
6. Валидация mapping: 118 совпадений, 30 unmatched, coverage 2024 39,299 % (допуск 1e-6).

## Task 3: product panel

**Files:** `scripts/cpi_weekly_component_panel.py`.

1. `month_points(series_points, month, cutoff)` — фильтр по месяцу/as-of, дубли — ошибка.
2. `previous_point(series_points, first_date, cutoff)` — последняя точка до месяца.
3. `monthly_mm(series_points, month, cutoff)` → `(mm_pp|None, meta)` со статусами `ok`/`no_points`/`no_previous`/`lt_min_weeks`/`gap_too_large`.
4. `build_product_panel(weekly, mapping_rows, weights_matrix, months)` — 148 × месяцы 2009-04…2026-10; несопоставленные — `component=None`.
5. `assert_as_of(...)` — 0 нарушений; официальные метки недоступны на cutoff.
6. `status_counts` по периодам/годам.

## Task 4: component panel и метрики

**Files:** `scripts/cpi_weekly_component_panel.py`.

1. `build_component_panel(...)` — `food`/`nonfood`/`services`/`all_products`/`all_components`; `recon`, `official`, `error`, `component_weight`, `matched_weight`, `covered_weight`, `covered_share`, `n_products`, `weight_year`; NaN → `None`.
2. `reconstruction_metrics(...)` — RMSE/MAE/max_abs/bias/n/coverage по периодам и годам; yearly weights primary, fixed-2024 robustness.
3. `aggregate_validation(...)` — 146 месяцев, MAE/RMSE/max; delta vs Task 1.
4. `compare_task1_diagnostic(...)` — `all_products` 2022+ vs Task 1 (0,4089/0,5347/1,2580).
5. `decision(...)` — гейт, mode, reason.

## Task 5: outputs и паспорт

**Files:** `scripts/cpi_weekly_component_panel.py`.

1. CSV: products и components; UTF-8, `\n`, сортировка, NaN → пусто, 6 знаков.
2. Report: статус `DONE_WITH_CONCERNS`, короткий ответ, метод, coverage, panel/status, reconstruction, aggregate validation, gate, ограничения, воспроизводимость.
3. JSON: `schema=cpi-weekly-component-panel/v1`, `generated_at`, `git`, `config`, `inputs` (sha256), `mapping`, `panel`, `as_of`, `aggregate_validation`, `reconstruction`, `decision`, `passport`, `limitations`; NaN → `null`.
4. Passport: `run_id=uuid5(...)`, `config_hash`, `sample_hash`, `data_asof`, `max_official_month`, spec/plan sha256, Python/openpyxl, artifacts sha256. MLflow/`ML_LOG` не использовать.
5. CLI: `--archive`, `--json`, `--report`, `--products-csv`, `--components-csv`, `--min-weeks`, `--max-gap-days`.

## Task 6: тесты

**Files:** `scripts/cpi_weekly_component_panel_test.py`.

- Компаундирование; полнота (2 недели, gap 21, нет prev, gap после серии); месяц/дата; as-of; веса (год/фикс, ренормировка, `all_components`); гейт 0,499/0,5; CSV/JSON/паспорт; статическая проверка на `ОКВЭД|BSU|model.xlsx|panel.csv` и `CREATE|INSERT|ALTER|GRANT|DROP`.
- ~25–35 тестов, офлайн.

## Task 7: Makefile и документы

**Files:** `Makefile`, `docs/site-pages.json`, `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`, `README.md`, `.kimi-code/skills/ml-engineer/SKILL.md`.

- `cpi-weekly-panel`: `uv run --with openpyxl --python 3.12 python3 scripts/cpi_weekly_component_panel.py $(ARGS)`.
- `cpi-weekly-panel-test`: тот же запуск теста с `-v`.
- Manifest: отчёт в «Исследования».
- LIMITATIONS: 39,3 %, услуги 29,0 %, NaN-правила, веса по годам, нет винтажей.
- Roadmap: Task 3 с числами в абзаце `phase-4-ml-components`; общий статус `⏳`.
- README: строка команд.

## Task 8: живой прогон и приёмка

```bash
make cpi-weekly-panel
make cpi-weekly-panel-test
make all
make docs-check
git diff --check
```

Проверить: 148/118/30; coverage 39,299 %; 2026-10 `lt_min_weeks`; reconstruction по периодам; aggregate 146 мес. / MAE 0,1647; as-of 0; gate hybrid; повтор (CSV/метрики совпадают, JSON — только `generated_at`).

## Task 9: PR

- `Closes #236` вне backticks; #76 не закрывать.
- Project V2 #236: `In Review`; протокол — комментарием в PR.
- После мержа: #236 `Done`, статус роадмапа финализировать.
