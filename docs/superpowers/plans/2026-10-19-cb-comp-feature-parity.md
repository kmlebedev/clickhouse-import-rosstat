# План: CB_COMP feature-composition parity

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-cb-comp-feature-parity` (`docs/ROADMAP_KEY_RATE.md` §6, фаза 4).
**Issue:** [#221](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/221).
**Спека:** [2026-10-19-cb-comp-feature-parity-design.md](../specs/2026-10-19-cb-comp-feature-parity-design.md).
**Ветка:** `exp/key-rate-cb-comp-feature-parity`.
**Worktree:** `.worktrees/key-rate-cb-comp-feature-parity`.

## Global Constraints

- `mean` — регрессионный гейт: числа #190 (0.1693 / 0.3395 / 0.5217 / 1.0882) должны воспроизвестись; иначе стоп.
- `scripts/cpi_monthly.py` и `scripts/cpi_nowcast.py` не меняются; v1/v2 baseline не трогаем.
- Скрипты только читают ClickHouse и архив; в ClickHouse не пишут; DDL/GRANT не делаются.
- CatBoost — дефолты, `random_seed=20261018`; никакого тюнинга по test.
- `--quick` для официальных чисел не используется.
- Тесты — stdlib `unittest`, офлайн, без сети/БД; существующие 69 тестов остаются зелёными.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека и план (этот коммит)

**Files:** `docs/superpowers/specs/2026-10-19-cb-comp-feature-parity-design.md`, `docs/superpowers/plans/2026-10-19-cb-comp-feature-parity.md`, `docs/ROADMAP_KEY_RATE.md`.

- Зафиксировать гипотезу, режимы, primary metric, DM/TOST, MDE, decision gate.
- Добавить пункт в роадмап со ссылкой на issue #221.
- Коммит: `docs: add CB_COMP feature parity design and plan`.

## Task 2: пункт, issue, claim

- Issue #221 создан, labels `roadmap:key-rate`, `role:ml`; Project V2: Status `In Progress`, Branch, Spec/Plan, Priority P0.
- Claim-комментарий в issue.
- Проверка: `issue_read` #221 возвращает нужные labels; Project item заполнен.

## Task 3: экспорт полной панели

**Files:** `scripts/cb_comp_panel_export.R`.

1. Оставить `cpi_panel.csv` и `mp_panel.csv` без изменений.
2. Добавить `panel.csv`: все `group_stat` (CPI, МП, кредит, ставки, рыночные индексы), все `kind` (`base_index`, `diff`, `trend`, `trend_deviation`), `value`, `lag`; character-колонки — в UTF-8.
3. Проверка: `Rscript scripts/cb_comp_panel_export.R <archive> build/cb-comp-parity` пишет три файла; `panel.csv` содержит `Кредитные ставки` и `Рыночные индексы`.

## Task 4: per-BSU режим в bottom-up

**Files:** `scripts/cb_comp_bottomup.py`, `scripts/cb_comp_bottomup_test.py`.

1. `FeatureMode = Literal["mean","per_bsu","full"]`; дефолт `mean`.
2. `mp_features(spec, month, lookup, mode)`:
   - `mean` — текущее поведение;
   - `per_bsu` — по каждому BSU-коду и индикатору: уровень и изменение; имена `mp_{code}_{indicator}` / `..._chg`; NaN при отсутствии точки;
   - `full` — `per_bsu` + `cpi_trend`, `cpi_trend_dev`, `usd_level`, `usd_chg`, `miacr`, `miacr_chg`, `interest_rate`, `interest_rate_chg`, `debt_mortgage`.
3. `feature_names(spec, with_mp, mode)` — группо-зависимые имена; `feature_table` и `run_bottomup` получают `feature_mode`; `mean` побитно сохраняет текущие числа.
4. `load_full_panel()` — читает `panel.csv`, если он есть; `load_mp_panel`/`load_cpi_diff` остаются для backward compatibility.
5. Метаданные результата: `feature_mode`, `feature_count_min/median/max`, `n_with_bsu`, `n_without_bsu`, `weights_coverage`.
6. Тесты:
   - `per_bsu` на синтетике: 2 BSU × 3 индикатора × 2 = 12 МП-признаков, всего 18;
   - `mean` даёт ровно 6 МП-признаков, имена и значения не изменились;
   - NaN для отсутствующего кода; группа без BSU — пустой dict;
   - `full` добавляет ожидаемые имена и не ломает `mean`;
   - `assert_no_leakage_origin` для per-BSU.

**Проверка:** `make cb-comp-parity-test` (20 тестов bottomup + 49 parity).

## Task 5: CLI, имена моделей и отчёт parity

**Files:** `scripts/cb_comp_parity.py`, `scripts/cb_comp_parity_test.py`.

1. CLI `--feature-mode {mean,per_bsu,full}`, default `mean`.
2. Передать режим в `bu.run_bottomup`; имена вариантов: `bottomup_mp`, `bottomup_mp__per_bsu`, `bottomup_mp__full`; `bottomup_nomp` — без суффикса.
3. В JSON/отчёт: режим, feature counts, покрытие весов, MDE, сравнение с `mean`.
4. `mean` даёт те же RMSE/DM/TOST, что #190; суффикс не появляется.
5. Тесты: CLI-парсинг режимов; суффиксы имён; поля отчёта; `mean` не изменился.

**Проверка:** `make cb-comp-parity-test` — зелёный.

## Task 6: паспорт run

**Files:** `scripts/cb_comp_parity.py`, `scripts/cb_comp_parity_test.py`.

1. Подключить `scripts/ml_experiment.py` по образцу `cpi_monthly.py`: `ML_LOG=1` → run name `cb_comp_parity__<feature_mode>`, git SHA, config/sample hash, метрики, per-fold предсказания, sha256 артефактов.
2. Если ClickHouse/ML_LOG недоступен — предупреждение, прогон не падает; в отчёте фиксируется «паспорт локальный».
3. Тест: при `ML_LOG=0`/недоступном ClickHouse скрипт не падает; run name формируется.

**Проверка:** `ML_LOG=1 make cb-comp-parity ARGS="--feature-mode mean"` пишет run; повтор даёт новый `run_id`, но те же метрики.

## Task 7: прогон

```bash
make env-check
make ch-status

mkdir -p build/cb-comp
curl -fL --retry 5 --retry-delay 3 --connect-timeout 30 \
  -o build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z \
  https://ndownloader.figshare.com/files/51053942
md5 -q build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z
# 797f00518c3a0fe6d4cd2c2ad22aa70a

uv run --with py7zr python - <<'PY'
import py7zr
with py7zr.SevenZipFile('build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z', 'r') as z:
    z.extractall(path='build/cb-comp/extracted')
PY

ML_LOG=1 make cb-comp-parity ARGS="--feature-mode mean"
ML_LOG=1 make cb-comp-parity ARGS="--feature-mode per_bsu"
ML_LOG=1 make cb-comp-parity ARGS="--feature-mode full"
make cb-comp-parity-test
```

Официальные числа — только полный прогон; `--quick` не используется.

## Task 8: отчёт и вердикт

**Files:** `docs/research/2026-10-19-cb-comp-feature-parity.md`.

- Таблица: режим × горизонт → RMSE/MAE/DM p/TOST/ratio/90 % CI/MDE.
- Feature counts и покрытие весов.
- Вердикт `promote`/`iterate`/`reject` по decision gate спеки §6.
- Раздел «что не проверено»: 16 групп без BSU, архив урезан, 87 кодов — внутренние данные, CatBoost 1.2.10 vs 1.2.7, 54 normal-месяца.
- Обновить `docs/ROADMAP_KEY_RATE.md`: статус пункта, числа, ссылка на отчёт; Project V2 — `In Review` при открытии PR.

## Task 9: приёмка и PR

- `make all`, `make docs-check`, `make cb-comp-parity-test` — зелёные.
- Протокол живой приёмки — комментарием в PR: команды, числа, расхождения, «что не проверено».
- `Closes #221` — вне backticks.
- После мержа: `Status = Done`, статус роадмапа `✅` с датой и числами.
