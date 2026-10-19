# План: аудит публичной панели ИПЦ — Task 1 (#234)

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-components` (issue [#234](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/234), родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
**Спека:** [2026-10-19-cpi-public-data-audit-design.md](../specs/2026-10-19-cpi-public-data-audit-design.md).
**Ветка:** `feat/cpi-public-data-audit`.
**Worktree:** `.worktrees/cpi-public-data-audit`.

## Global Constraints

- Только чтение ClickHouse; никаких DDL/DML/GRANT, Go-кода и изменений схемы.
- Не использовать 87 кодов ОКВЭД2, `model.xlsx`, BSU-панель и `panel.csv`.
- Скрипт — Python, зависимости: stdlib + `openpyxl`; транспорт `ch_query` из `scripts/extrapolation_aisc.py`.
- Тесты — stdlib `unittest`, офлайн, без сети/ClickHouse.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека и план

**Files:** `docs/superpowers/specs/2026-10-19-cpi-public-data-audit-design.md`, `docs/superpowers/plans/2026-10-19-cpi-public-data-audit.md`.

- Зафиксировать входы, нормализатор, покрытие, PIT, компаундирование, гейт ≥50 %, решение hybrid.
- Коммит: `docs: add public CPI panel audit design`.

## Task 2: аудит-скрипт

**Files:** `scripts/cpi_public_panel_audit.py`.

1. `load_weekly()` — `v_rosstat_macro` + `v_series_catalog`; 149 W-рядов, 148 товарных + агрегат; per-series n/min/max, gaps.
2. `load_monthly()` — 4 константы `MONTHLY_SERIES` (копия `cpi_monthly.TARGET_SERIES`) из `v_rosstat_macro`; 429 точек.
3. `load_weights` / `load_dictionary` — openpyxl, `окато_код=643`, листья `уровень=7`.
4. `normalize_name` — raw / basic / extended (скобки + единицы).
5. `match_series` — к именам весов; 118 совпадений, 30 unmatched.
6. `coverage` — по 2016–2024 и по компонентам `6/7/9000`; группы `уровень_3`.
7. `audit_mp_survey` — 31 лист, 16 отраслевых × 297 месяцев; IBK/ценовые ожидания/издержки raw+SA; `weight="—"`, `basis="feature, not weighted"`.
8. `audit_pit` — cutoff 8-е M+1, `date+2`, месячная доступность; 0 нарушений.
9. `audit_compounding` — `Π(value/100) − 1`; агрегат 2009–2021; 2022+ — только диагностика.
10. `decision` — гейт 0,50; hybrid при 39,3 %.
11. `write_json()` — mapping/coverage/mp_survey/pit/compounding/decision.
12. `--archive`, `--mp-survey`, `--json`, `--out-dir`; скачивание публичного MP-файла с проверкой статуса и sha256.

## Task 3: офлайн-тесты

**Files:** `scripts/cpi_public_panel_audit_test.py`.

- Синтетические xlsx весов/словаря через openpyxl.
- Нормализатор: `Свёкла`/`Свекла`, `об.спирта`/`об. спирта`, `Масло подсолнечное, л`/`, кг`, снятие единиц.
- `match_series`/`coverage` на синтетике.
- PIT: `date+2 > cutoff` не входит; месячная метка M не доступна на cutoff.
- Компаундирование, `decision(0,499/0,500)`, JSON-ключи.
- Статическая проверка исходника на `ОКВЭД|BSU|model.xlsx|panel.csv`.

## Task 4: живой прогон и JSON

**Files:** `docs/research/2026-10-19-cpi-public-data-audit.json`.

```bash
make cpi-public-audit
make cpi-public-audit-test
```

Проверить: 149 W (148+агрегат), 4×429, 16×297; 118 совпадений, 39,299 %, 30 unmatched; по годам 29,5–39,6 %; компоненты 48,1/37,9/29,0; PIT 0; компаундирование 146 мес., MAE 0,1647 п.п.; `decision.mode=hybrid`, `bottomup_full=false`.

## Task 5: отчёт и документы

**Files:** `docs/research/2026-10-19-cpi-public-data-audit.md`, `docs/site-pages.json`, `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`.

- Отчёт: короткий вывод, инвентарь, полная таблица 118 ряд → компонент/группа → вес, 30 unmatched, покрытие по годам/компонентам, глубина/пропуски, компаундирование, PIT, «что не проверено».
- Manifest: запись в секции «Исследования».
- LIMITATIONS: 39,3 %, услуги 29 %, веса 2016–2024, агрегат до 2021-12, винтажей нет.
- Роадмап: Task 1 (#234) с числами и решением hybrid; общий пункт остаётся `⏳`.

## Task 6: проверки и PR

```bash
make cpi-public-audit-test
make all
make docs-check
git diff --check
```

- PR: `[key-rate/phase-4-ml-components] Task 1: аудит и mapping публичной панели`; `Closes #234` вне backticks; #76 не закрывать.
- Протокол — комментарием в PR; Project V2 `In Review`; после мержа — `Done` для #234.
