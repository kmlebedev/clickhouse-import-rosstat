# План: Task 4 — channel-complete core + dispersion recalibration

**Пункт:** `key-rate/phase-4-cbr-scenario-fan` — issue [#272](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/272)
**Task 4:** issue [#289](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/289)
**Спека:** [2026-10-20-cbr-scenario-fan-v3-design.md](../specs/2026-10-20-cbr-scenario-fan-v3-design.md)
**Роль:** `role:ml`; **фаза:** 4
**Ветка / worktree:** `feat/cbr-scenario-fan-v3` / `.worktrees/cbr-scenario-fan-v3`
**База:** `feat/cbr-scenario-fan-v2-revision` (stacked; после мержа #291 — rebase на `main`)
**Статус:** In Progress.

---

## 1. Цель и границы

Довести primary-веер до **channel-complete**: все 11 каналов `KEY_RATE_CHANNELS.md` §3 либо входят в ядро/сценарии, либо явно помечены как недоступные; пересчитать дисперсию под channel-core так, чтобы coverage 80 % вернулся к 0.8, а CRPS был значимо лучше `core_fan_conformal` 60.60. В `forecast_log` не писать, ClickHouse не менять, Go/DDL/витрины/гранты не трогать.

## 2. Задачи

### Task 0 — worktree и трекер (сделано)

- [x] Worktree `.worktrees/cbr-scenario-fan-v3`, ветка `feat/cbr-scenario-fan-v3` от revision (stacked на #291).
- [x] #289 — «Taking» комментарий, branch/worktree; Project/roadmap — после PR.

### Task 1 — спека и план (сделано)

- [x] `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v3-design.md`.
- [x] `docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v3.md`.
- Отдельный коммит `docs: add channel-complete fan design and plan`.

### Task 2 — новые каналы

Файл `scripts/key_rate_scenario_fan_v3.py` (новый), импорты `v2`, `fan1`, `cr`, `krm`, `sc`, `mlx`.

Загрузчики (все — `FORMAT JSON`, as-of):

- `load_ofz()` — `v_ofz_signal`: `ofz10_spread`, `ofz2_spread`, `slope_10y_1y`; daily.
- `load_brent()` — `v_moex_market` `series='brent_front'`; daily; `brent_3m` — лог-изменение за 3 мес.
- `load_bop()` — `v_cbr_bop`, `source='cbr_bop_standard'`, `series='Счет текущих операций (квартал)'`; as-of конец квартала + 3 мес.
- `load_savings()` — `v_cbr_macro` (`source='cbr'`): `Депозиты`, `потребительские ссуды - всего`, `ипотечные жилищные кредиты - всего`; `deposit_yoy`, `deposit_share`; конец M+1.
- `load_ccyb()` — `cbr_ccyb_indicators` (`credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy`); если raw недоступна — `missing`, счётчик.
- `load_text()` — `v_cbr_mp_summary_channels`: `text_channel_score` (число regex-каналов последнего резюме), `text_available`; `published + 1 день`.
- `load_panel()` — существующий `sc.build_panel` (22 признака).
- `asof`-хелперы: `feature_asof` (monthly/daily), `available_from_gdp` для квартала, `available_from_cpd` для месячных.

**Тесты:** `TestAsOf` — точка после заседания не участвует; месячный/квартальный/дневной лаг; `TestLoaders` — имена/покрытие.

### Task 3 — ядро

- `channel_features(row, panel_row, external_by_date)` — 22 panel + savings 2 + OFZ 3 + Brent 1 + BOP 1 + ccyb 3 + text 1 + `has_*` 5.
- `fit_enet(train, row)` — `ElasticNetCV` (`l1_ratio=[0.1,0.5,0.9]`, `cv=5`, seed 20261020), train-only median imputation/standardization; `Forecast(step, None)`.
- `channel_core_predictions(rows, panel, external)` — `krm.walk_forward` на расширенных строках (обёртка с `.decision`/`.features`), 84/68.

**Тесты:** `TestCore` — только train; NaN; детерминизм; все 11 каналов в матрице.

### Task 4 — дисперсия

- `disp_vol(residuals, past_vol, current_vol)`:
  - `vol = mean(z(|usdrub_3m|), z(|ofz_slope|), z(|brent_3m|))` по train;
  - `log(|e|+1) = γ0 + γ1·vol`; `scale_t`; `q_α = scale_t·quantile(e/scale, α)`;
  - fallback `disp_conformal` при `scale_t≤0`.
- `disp_calibrated(residuals, past_hits)` — `k = clip(1 + 2·(0.8 − coverage), 0.8, 1.5)`, применяется к conformal.
- `disp_conformal` — baseline из `v2`.
- Primary — `channel_fan_vol`; ablations `channel_fan_conformal`, `channel_fan_calibrated`.

**Тесты:** `TestDispersion` — `scale>0`; высокий vol расширяет; calibrated factor bounds; fallback.

### Task 5 — веера и метрики

- `channel_fan_*` = `v2.make_fan(name, ..., channel_core_step, steps, deltas, dispersion, ...)`.
- Reference: `core_fan_conformal` из `v2.run_v2_walk_forward` (paired comparison).
- Метрики — `fan1.summarize_fans`, `summarize_episodes`, `compare_fans`, `compare_fan_to_benchmark`; DM/бутстрап.
- `decide_v3` — правило спеки §6.

**Тесты:** `TestFan` — 35 точек/веса; coverage/CRPS; primary vs ablation.

### Task 6 — артефакты, CLI, `ML_LOG`

- `key_rate_scenario_fan_v3_report.md`
- `key_rate_scenario_fan_v3_predictions.csv`
- `key_rate_scenario_fan_v3_diagnostics.csv` — core step, feature coverage, `vol`, `scale`, quantiles.
- `key_rate_scenario_fan_v3_forecast.json` — as-of 2026-10-23.
- CLI: `--as-of`, `--out-dir`, `--rolling`.
- `ML_LOG=1` — run `key_rate_scenario_fan_v3`, `scenario_fan_v3__rate__start24`; артефакты sha256.
- `.gitignore`: `/key_rate_scenario_fan_v3_*`.

### Task 7 — Makefile и тесты

```make
key-rate-scenario-fan-v3:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_v3.py $(ARGS)

key-rate-scenario-fan-v3-test:
	uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_v3_test.py -v
```

### Task 8 — живой прогон и отчёт

```bash
make key-rate-scenario-fan-v3-test
ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v3
make all
make docs-check
```

Отчёт `docs/research/2026-10-20-cbr-scenario-fan-v3.md`: таблица «канал → ряд → участие», coverage/CRPS/ширина, DM/CI против `core_fan_conformal`/ordered/B1–B3, эпизоды 2022–2023/2024–2026, текущий as-of, вердикт, «что не проверено».

### Task 9 — документация и трекер

- `docs/KEY_RATE_MODEL.md` — «channel-complete веер».
- `docs/LIMITATIONS.md` — ограничения.
- `docs/site-pages.json` — research-отчёт.
- `docs/ROADMAP_KEY_RATE.md` — Task 4 #289, ссылки, статус.
- Issue #289 — протокол; Project — `In Review`/`In Progress`.
- PR — `Part of #272`, `Closes #289`; base `main` после мержа #291, иначе stacked base revision.

## 3. Приёмка

1. `make key-rate-scenario-fan-v3-test` — офлайн-тесты.
2. `ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v3` — 108 заседаний, 84/68, 72/56 фан-фолдов; coverage/CRPS; `v_ml_runs` `status=ok`, `git_dirty=0`; повтор детерминирован.
3. `make all`, `make docs-check` — зелёные.
4. `git diff` без Go/DDL/витрин/грантов/`forecast_log`.
5. PR — `Part of #272`, `Closes #289`, протокол.

## 4. Что не проверено

- PIT/винтажи savings/ccyb/OFZ/BOP; только консервативный as-of.
- TEXT 2024+, KEYRATE 2026+ — missing-индикаторы; KEYRATE не используется.
- `cbr_ccyb_indicators` raw может быть недоступна — fallback missing.
- 108 решений, ~35 признаков — ElasticNet; ablations.
- `disp_vol` — 1 композит; 2022 может потребовать regime-split.
- CPI Task 2 #285 — отдельно.

## 5. Риски

| Риск | Митигация |
|---|---|
| Переобучение на 108 решениях | ElasticNetCV, train-only CV, ablations, коэффициенты в отчёте |
| Coverage падает при лучшем core | `disp_vol`/`disp_calibrated`; primary только при coverage 0.8 |
| Raw ccyb недоступна | missing-индикатор + пометка в отчёте |
| Text/KEYRATE короткая история | missing-индикаторы, не нули |
| Stacked PR до мержа #291 | base revision; после мержа rebase на main |
