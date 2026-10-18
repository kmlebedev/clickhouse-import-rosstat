# План: паритет ML-трека с CB_COMP (строгий путь)

**Дата:** 2026-10-18
**Пункт:** `key-rate/phase-4-ml-cb-comp-parity` (issue [#190](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/190))
**Спека:** [2026-10-18-cb-comp-parity-design.md](../specs/2026-10-18-cb-comp-parity-design.md)
**Ветка:** `feat/cb-comp-parity` (от `origin/feat/cb-comp-archive`, PR #193)
**Источник данных:** распакованный архив CB_COMP в `.worktrees/cb-comp-archive/build/cb-comp/extracted/` (gitignored)

---

## 1. Цель

Получить строгий ответ «статистически близок / не близок» для нашего месячного nowcast ИПЦ относительно `CB_COMP` (Кошкин–Крылов, 2025) на общем таргете `TARGET_FULL`, общем тестовом окне 2019-01…2024-07 и общих 54 normal-месяцах. Margin δ = 10% RMSE CB_COMP фиксируется в спеке до прогона. Ретраин CB_COMP не выполняется.

## 2. Порядок работ

### Шаг 0. Ветка и проверка окружения

- Worktree `.worktrees/cb-comp-parity`, ветка `feat/cb-comp-parity` от `origin/feat/cb-comp-archive` — создана.
- `make ch-status` — ClickHouse отвечает; `make env-check` — env задан (значения не печатаются).
- Архив не скачивается: используется распаковка соседнего worktree; путь проверяется `ls` по трём файлам (таргет, `StfCpiCB`, `catboostVCB_IBK.xlsx`).

### Шаг 1. Спека и план — первыми коммитами

- `docs/superpowers/specs/2026-10-18-cb-comp-parity-design.md` — спека: дизайн, предзарегистрированные окна/seed/margin/критерий, ограничения.
- `docs/superpowers/plans/2026-10-18-cb-comp-parity.md` — этот план.
- Коммит: `docs: add cb-comp parity spec and plan`.

### Шаг 2. Зависимость от ML-трека v2

- `scripts/cpi_monthly.py` — побайтовая копия из `origin/feat/key-rate-ml-v2` (PR #177 открыт, в `main` файла нет). Проверка идентичности: `git diff --no-index <(git show origin/feat/key-rate-ml-v2:scripts/cpi_monthly.py) scripts/cpi_monthly.py` — пусто.
- Копия нужна, чтобы parity-модуль импортировал те же `feature_names`, `base_features`, `mp_features`, `cutoff_for`, `available_from_survey`, а не дублировал правила as-of.
- Коммит: `scripts: vendor cpi_monthly v2 feature module from #177`.

### Шаг 3. `scripts/cb_comp_parity.py`

Состав модуля (имена — контракт с тестами):

1. **Константы:** `SEED=20261018`, `HORIZONS=(1,2,3,6)`, `HORIZONS_EXTRA=(4,5)`, `TRAIN_START=2010-03-01`, `TRAIN_END=2018-12-01`, `TEST_START=2019-01-01`, `TEST_END=2024-07-01`, `OUTLIER_MONTHS` (2020-04 + 2022-01…12), `MARGIN_FRACTION=0.10`, `BOOTSTRAP_BLOCK=3`, `BOOTSTRAP_N=2000`, `CB_RMSE_PINNED` (0.158805/0.296655/0.443623/0.971459).
2. **Архив:** `resolve_archive()`, `read_target_full()`, `read_cb_sheet()`, `read_cb_xlsx()`, `normal_months()`, `check_archive()`.
3. **Таргет из уровней:** `cumulative_growth(levels)` — `TARGET_h(M) = (Π level/100 − 1)·100`; используется тестами и как документированная формула.
4. **Выборка:** `feature_rows()` (через `cpi_monthly.load_*`), `build_rows()` (месяцы v2 + таргет архива), `assert_no_leakage()`.
5. **Модели:** `fit_direct(rows, names, h)`, `predict(rows, names, model)`; дефолты CatBoost запинованы в `CATBOOST_DEFAULTS`.
6. **Метрики:** `rmse/mae` из `eval_utils`; `block_bootstrap_delta_ratio()` (парный блочный бутстрап Δ и ratio, 5%/95%); `tost()`; `dm()` (HLN, h=1 и h=h).
7. **Вердикт:** `horizon_verdict()`, `overall_verdict()` — по критерию спеки §5.3.
8. **Артефакты:** `write_report()`, `write_predictions_csv()`, `write_result_json()`; имена — из спеки §7.
9. **CLI:** `--archive`, `--out-dir`, `--train-start`, `--test-end` (для отладки; значения по умолчанию — предзарегистрированные).
10. **Порядок `main()`:** архив → CB RMSE → margin (печать) → признаки → train → test → DM/TOST/ratio → артефакты. Margin печатается до обучения.

Проверки-гейты (падают, а не предупреждают): RMSE архива против пинов; `StfCpiCB` vs `catboostVCB_IBK.xlsx`; нормальный набор против флага; train/test не пересекаются; все 54 normal-месяца имеют прогноз CB и таргет.

### Шаг 3b. `scripts/cb_comp_bottomup.py` — предзарегистрированный bottom-up

Добавлен 2026-10-18 до прогона; direct-результат и его margin/окно не меняются.

1. **Панель:** `scripts/cb_comp_panel_export.R` выгружает из `data_panel_bi_sa.fst` два CSV (CPI: date/code/kind/value для России, `base_index`/`diff`; МП: date/code/kind/value/lag). Python-модуль вызывает Rscript, если CSV нет; тесты R не требуют.
2. **Спецификация групп:** `model.xlsx` лист `1.0.4.2`, `используется=True`, без агрегатов `1/6/7/9000` → 88 компонент; BSU-коды `мониторинг_код1..4`.
3. **Веса:** `cpi_weights_tovar.xlsx` (Россия) + `dictionary_cpi.xlsx`: листья (`уровень=7`) агрегируются по уровню группы; год весов = `M.year−1`; нормировка на доступные группы; ЖКУ `9470` вне компонент, его вес — проверка.
4. **Компонентный таргет:** `exp(Σ diff) − 1` в процентах; h=1/2/3/6.
5. **Признаки:** базовые (компонентные лаги + v2-макро + `month`) и 6 МП-признаков (среднее по BSU-кодам группы: 3 уровня + 3 изменения, as-of с учётом лага публикации).
6. **Гейт сборки:** агрегат фактических компонентных таргетов vs `TARGET_FULL`; порог RMSE h1 ≤ 0.10 п.п. и покрытие ≥ 0.85; иначе — `bottomup not assembled`, причина в отчёт.
7. **Модели и агрегат:** CatBoost по группе × h × варианту; `bottomup_mp` (MP для 72 с BSU, no-MP для 16) и `bottomup_nomp` (все 88); веса `Σw·ŷ/Σw`.
8. **Оценка:** те же 54 normal-месяца, `TARGET_FULL`, DM-HLN/TOST/ratio; вердикт «близок» — только если `bottomup_mp` проходит DM p>0.05 и TOST на всех h.

### Шаг 3c. Forecast origin `M−(h−1)` (поправка до финального прогона)

- Direct и bottom-up используют признаки месяца `origin = M−(h−1)` для цели M (как `frollsum(h)` + `shift(h−1, lead)` в `VA03_dummy_cpi_lag_IBK.R`): h1 — M, h2 — M−1, h3 — M−2, h6 — M−5. Набор признаков, таргет, окно, normal, margin и критерий не меняются.
- Тесты: `forecast_origin`, `rows_for_horizon`, `model_rows` (bottom-up), `assert_no_leakage_origin`/`assert_no_leakage_bottomup`.
- Причина: первый прогон брал M−1/M−2 для всех h и давал h>1 более свежие данные, чем CB_COMP; это не строгий паритет. Числа первого прогона в отчёт не входят.

### Шаг 4. `scripts/cb_comp_parity_test.py`

Офлайн-набор (без ClickHouse и без архива):

1. **Форма таргета:** `cumulative_growth` на синтетических уровнях; h-колонки; NaN на первых h−1 месяцах.
2. **Горизонты:** основные 1/2/3/6, дополнительные 4/5; порядок и состав запинованы.
3. **Нормализация/окна:** `normal_months` = 54; 2020-04 и 2022 исключены; 2014-12…2015-03 вне теста; границы train/test.
4. **Синтетический xlsx:** `read_target_full`/`read_cb_sheet` на временной книге (openpyxl); флаг `Учитывать`; расхождение флага и правила — исключение.
5. **DM:** одинаковые ошибки → p=1; сдвиг → p<0.05 и знак; HLN-конвенция h=1.
6. **TOST:** Δ=0 и одинаковые ошибки → CI внутри margin; ошибки, вдвое большие → CI вне margin; детерминизм бутстрапа (один seed — один результат); ratio-интервал.
7. **Утечка:** тестовый месяц в train → `assert_no_leakage` падает; cutoff(M) < публикации метки M; test-месяцы не участвуют в обучении; признаки не содержат `y`.
8. **Пины:** δ = 10% RMSE; `SEED`; конфигурация CatBoost; `CB_RMSE_PINNED`.
9. **Быстрый smoke модели:** CatBoost direct на 30 синтетических строках с `iterations=5` (параметр только для теста) — прогноз конечен, число строк совпадает.

### Шаг 5. Makefile и `.gitignore`

- `.PHONY`: `cb-comp-parity`, `cb-comp-parity-test`.
- `cb-comp-parity`: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_parity.py $(ARGS)`.
- `cb-comp-parity-test`: тот же env, `python3 scripts/cb_comp_parity_test.py -v`.
- `.gitignore`: три точных имени артефактов (`/cb_comp_parity_report.md`, `/cb_comp_parity_predictions.csv`, `/cb_comp_parity_result.json`).

### Шаг 6. Прогон и отчёт

- `make cb-comp-parity` — живой прогон на архиве и dev-ClickHouse; зафиксировать в протоколе: число месяцев, фактические train/test, RMSE CB (сверка с пинами), margin, RMSE/MAE наши, DM p (h=1 и h=h), 90% CI Δ, ratio + CI, вердикт.
- `docs/research/2026-10-18-cb-comp-parity.md` — отчёт: дизайн, таблицы по h, вердикт, ограничения §6 спеки, что осталось. «Не близок» пишется прямо, без смягчений.
- Если какой-то горизонт не считается (нет данных/пересечение) — отдельная строка «не посчитан, причина», без подмены.

### Шаг 7. Документация

- `docs/ROADMAP_KEY_RATE.md`, фаза 4: добавить подпункт `key-rate/phase-4-ml-cb-comp-parity` со статусом и числами (✅ — «измерение готово», вердикт — в тексте).
- README/ARCHITECTURE по скилу `sync-readme-architecture`: `scripts/` (новый скрипт и тест), таблица команд `make`, `ARCHITECTURE.md` §2.
- При необходимости — `docs/guides/stat-ml-models.md` (одна строка о parity-прогоне).

### Шаг 8. Проверки и PR

- `make all` — зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, сборки).
- `make cb-comp-parity-test` — зелёный (число тестов фиксируется в отчёте).
- Повторный `make cb-comp-parity` — артефакты детерминированы (сравнить JSON побайтово).
- Commit/push `feat/cb-comp-parity`; PR `--base main`, в теле — пункт, спека/план, источник, протокол приёмки, `Closes #190` (вне backticks). Project V2/issue — по скилу `github-tracker`, если MCP GitHub доступен; иначе `gh`-фолбэк.

## 3. Файлы

| Файл | Действие |
|---|---|
| `docs/superpowers/specs/2026-10-18-cb-comp-parity-design.md` | новый |
| `docs/superpowers/plans/2026-10-18-cb-comp-parity.md` | новый |
| `scripts/cpi_monthly.py` | копия из #177 (побайтово) |
| `scripts/cb_comp_parity.py` | новый |
| `scripts/cb_comp_parity_test.py` | новый |
| `Makefile` | две цели + `.PHONY` |
| `.gitignore` | три артефакта |
| `docs/research/2026-10-18-cb-comp-parity.md` | новый (после прогона) |
| `docs/ROADMAP_KEY_RATE.md` | статус пункта |
| `README.md`, `ARCHITECTURE.md` | по скилу |

## 4. Риски и как снимаются

| Риск | Снятие |
|---|---|
| Признаки v2 и старт train | Long-sample политика v2 держит 2010-03: `key_rate`/`usd_mm` до 2013 г. — NaN, CatBoost обрабатывает их нативно; окно не сокращается и не расширяется прокси-признаками |
| `cpi_monthly.py` из #177 изменится при мерже | Копия побайтовая; при расхождении — пересобрать копию и повторить прогон (числа от этого не подбираются) |
| Архивные листы поменяются/путь другой | Жёсткие гейты RMSE и кросс-проверка `StfCpiCB` vs xlsx; путь — `--archive` |
| CatBoost на 63 строках нестабилен | Seed фиксирован; дефолты; результат публикуется как есть, без тюнинга |
| TOST на 54 месяцах узкий/широкий | Margin зафиксирован заранее; CI и DM публикуются вместе; «не близок» допустим |
| Смешение с прокси-путём | Прокси в этом пункте не используется; в отчёте — отдельная оговорка |

## 5. Протокол приёмки (заполняется после прогона)

- Команда, дата, ветка/коммит.
- Архив: путь, число месяцев таргета, сверка RMSE CB (таблица h → значение → пин).
- Выборка: train (факт), test, normal, пропуски (причины).
- Таблица по h: RMSE ours/CB, MAE, DM p (h=1), DM p (h=h), Δ, 90% CI, δ, TOST (да/нет), ratio + CI, вердикт.
- Вердикт по модели `nowcast_v2_mp` (headline) и `nowcast_v2_nomp` (сенситивность).
- `make all`, `make cb-comp-parity-test`, повторный прогон (детерминизм).
- Что осталось / что не проверено.
