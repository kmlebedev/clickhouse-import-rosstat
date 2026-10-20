# План: per-sheet модели ИПЦ — атрибуция и парсимония (follow-up #223)

**Пункт:** follow-up к [`key-rate/phase-4-cpi-extra-channels`](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/223), issue [#223](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/223), `role:ml`.
**Спека:** [2026-10-20-per-sheet-models-improvement-design.md](../specs/2026-10-20-per-sheet-models-improvement-design.md).
**Research:** [2026-10-20-per-sheet-models-improvement.md](../../research/2026-10-20-per-sheet-models-improvement.md).
**Ветка / worktree:** `research/223-per-sheet-models-improvement` / `.worktrees/per-sheet-research`.
**Статус:** план будущей реализации. В этом PR — только research + спека + план + docs; код и прогоны — после approval.

## Global Constraints

- Только чтение ClickHouse; в БД не пишем; DDL/GRANT/Go-код не меняем.
- `cb_comp_bottomup.py` и `cb_comp_parity.py` по умолчанию не меняют поведение; A-оркестрация — новый скрипт.
- `cpi_monthly_industry.py`: новый флаг `--feature-set`, default `full`; числа Stage 2 не меняются.
- Walk-forward только расширяющимся окном; random split запрещён; CatBoost-дефолты, `random_seed=20261018`.
- As-of A — `mp_available(date, 1)`; B — `available_from_survey` (конец P+1); точка M не участвует.
- NaN ≠ 0; CatBoost нативно; линейные модели — медиана train внутри фолда.
- Один PR в конце; спека и план — первые коммиты; `Closes` не ставим (follow-up #223).

---

## Task 1: зафиксировать спеку/план и комментарий в #223

**Files:** `docs/superpowers/specs/2026-10-20-per-sheet-models-improvement-design.md`, `docs/superpowers/plans/2026-10-20-per-sheet-models-improvement.md`, `docs/research/2026-10-20-per-sheet-models-improvement.md`.

- [ ] Проверить, что гипотезы A1–A4/B1–B2, окна, контрасты, margin и правило решения зафиксированы до кода.
- [ ] Комментарий в #223: ссылки на research/spec/plan, краткий вывод, «код не менялся».
- [ ] Project V2: пункт #223 остаётся `In Review`; новый статус не выставляется.
- [ ] Коммит: `docs: add per-sheet research, design and plan`.

## Task 2: A1 — атрибуция на fixed split

**Files:** `scripts/cb_comp_per_sheet_research.py`, `scripts/cb_comp_per_sheet_research_test.py`.

1. Импортировать `cb_comp_bottomup as bu`, `cb_comp_parity` (только чтение), `eval_utils as eu`.
2. Загрузить архив CB_COMP, `public_panel.csv`, `sheet_map.csv`; собрать `specs`, `targets`, `weights`, `features_by_group` для `per_sheet`.
3. Обучить 88 групп × 4 h per_sheet-модели на фиксированном train; сохранить модели в памяти.
4. SHAP (TreeSHAP) на test-строках; агрегировать mean |SHAP| по `sheet`/`indicator`; top-5 на h1/h2/h3/h6.
5. Permutation importance на test (54 строки, 100 повторов, seed); агрегировать по листу/индикатору.
6. Leave-one-sheet-out и leave-one-indicator-out: удалить признаки, переобучить, сравнить RMSE; 88 × 4 × ≤4.
7. Артефакты: `build/cb-comp-per-sheet-research/attribution_{shap,permutation,leave_one_sheet_out}.csv`, `attribution_report.md`.
8. Тесты: синтетические specs/features; удаление листа не меняет другие; NaN не заменяется 0; SHAP-агрегация по именам; детерминизм seed.

**Acceptance:** top-листы и ΔRMSE воспроизводимы; тесты зелёные; отчёт с оговоркой «диагностика, не causality».

## Task 3: A2 — walk-forward, ensemble, h1_base

**Files:** `scripts/cb_comp_per_sheet_research.py` (продолжение), `scripts/cb_comp_per_sheet_research_test.py`.

1. `origins = [2019-01, 2019-07, …, 2024-07]` (12 полугодовых); для каждого — train до origin минус embargo, test = origin.
2. Варианты: `mean`, `per_sheet`, `ensemble` (0,5/0,5); h=1,2,3,6; `per_bsu` — 4 annual origin.
3. `h1_base`: h1 без per-sheet; сравнить с h1 per_sheet и h1 mean.
4. Собрать per-fold predictions; RMSE/MAE; DM-HLN; блочный бутстрап (3, 2000); MDE; Holm по 3 primary-контрастам.
5. Fixed split статьи — вторично; числа #222 не переписывать.
6. Артефакты: `build/cb-comp-per-sheet-research/walk_forward_{report.md,result.json,predictions.csv}`.
7. Тесты: origin не пересекается с train; embargo вырезает месяцы; ensemble = среднее; h1_base не содержит `mp_sheet_*`; детерминизм.

**Acceptance:** 12 origin × 3 варианта × 4 h × 88 групп; primary-контрасты посчитаны; при runtime > 2 ч — 6 origin и `quick=true` в паспорте.

## Task 4: B1 — парсимоничные feature sets

**Files:** `scripts/cpi_monthly_industry.py`, `scripts/cpi_monthly_industry_test.py`.

1. Добавить `--feature-set full|mean|component_mean|pca` (default `full`).
2. `mean`: для каждого индикатора — среднее по всем 16 листам; 3 × 2 = 6 признаков.
3. `component_mean`: среднее по mapped-листам компонента + general; 6 признаков.
4. `pca`: StandardScaler + PCA(3) fit только на train; 3 компоненты; имена `ind_pca_1..3`.
5. `feature_names` и `fit_catboost_industry` принимают feature set; `MODELS` пополняется `v3_ind_mean_bottomup`, `v3_ind_component_mean_bottomup`, `v3_ind_pca_bottomup`.
6. Тесты: 6/6/3 признака; PCA fit только на train (тестовая строка не влияет); NaN → медиана train для PCA; default `full` даёт прежние имена/числа.

**Acceptance:** `make cpi-monthly-industry-test` зелёный; default `full` не меняет Stage 2-числа; новые наборы детерминированы.

## Task 5: B2 — прогоны и оценка

**Files:** `scripts/cpi_monthly_industry.py`, `Makefile`.

```bash
make cpi-monthly-industry-test
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48 --feature-set full"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48 --feature-set mean"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48 --feature-set component_mean"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48 --feature-set pca"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample weekly --start 24 --feature-set full"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample weekly --start 24 --feature-set mean"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample weekly --start 24 --feature-set component_mean"
```

- Primary: `mean`/`component_mean` vs `full`; Holm (2 контраста); non-inferiority margin +2% RMSE.
- Secondary: PCA vs full; каждый vs `v3_bottomup`; `weekly`; MCS по 5 моделям.
- Артефакты: `build/cpi-monthly-industry/<sample>/…`; паспорт run; `quick=false`.
- Acceptance: primary-контрасты посчитаны; Stage 2-числа `full` воспроизводятся с допуском 1e-6; вердикт по правилу спеки.

## Task 6: отчёт и docs

**Files:**
- `docs/research/YYYY-MM-DD-per-sheet-models-results.md` — числа, DM/CI/MDE, вердикт, run_id, ограничения.
- `docs/ROADMAP_KEY_RATE.md` — короткая строка в #223 (follow-up, ссылки), без смены статуса.
- `docs/LIMITATIONS.md` — если появятся новые ограничения (as-of, PCA, 33 серии).
- `docs/site-pages.json` — регистрация отчёта.
- `README.md` / `docs/DEVELOPMENT.md` — только если добавлены новые `make`-цели.

**Acceptance:** отчёт с «что не проверено»; ссылки разрешаются; `make docs-check` зелёный.

## Task 7: приёмка и PR

```bash
make cb-comp-per-sheet-research-test
make cpi-monthly-industry-test
make all
make docs-check
```

- Повторный прогон A/B — те же метрики (детерминизм).
- Паспорт run: `ML_LOG=1`, `run_id`, config/sample hash, git SHA, артефакты sha256.
- PR: follow-up к #223, без `Closes`; протокол приёмки — комментарием в PR.
- Project V2: #223 остаётся `In Review`; новый issue не создаётся.
- Production-v3 не запускается; `promote` — только если правило спеки выполнено и владелец решил.

## Открытые вопросы (до кода)

1. **Стоимость A:** 12 672 модели. Если runtime > 2 ч — 6 origin (annual) с `quick=true`; согласовать с владельцем.
2. **Атрибуция:** SHAP на test-строках (54) — диагностика; если нужна устойчивость, добавить blocked holdout внутри train (дороже).
3. **PCA:** 3 компоненты фиксированы; если объяснённая дисперсия < 70%, добавить 5 как robustness, не выбирать по тесту.
4. **Non-inferiority margin B:** +2% RMSE — pre-registered; если владелец считает иначе, зафиксировать до прогона.
