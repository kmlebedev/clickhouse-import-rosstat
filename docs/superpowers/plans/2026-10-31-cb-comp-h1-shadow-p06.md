# P0.6: ретро-robustness и paper shadow `wk_mix_ex@v1` — план

Спека: [2026-10-31-cb-comp-h1-shadow-p06-design.md](../specs/2026-10-31-cb-comp-h1-shadow-p06-design.md). Issue #453 (sub-issue #419). Ветка `exp/cb-comp-88-groups-p06-shadow`, worktree `.worktrees/cb-comp-88-p06`. Срок shadow-JSON — **до 2026-11-08**.

## Задачи

1. **Спека и план** — первый коммит ветки.
2. **Скрипт `scripts/models/cb_comp/cb_comp_h1_shadow.py`** (импорт `cb_comp_h1_weekly_ex`/`cb_comp_h1_weekly`, их код не меняется):
   - `t_live_rt(sa_asof, weights, months)`; `serve(...)` — цепочка fallback;
   - `cutoff_override(fn)` — временная подмена `cpi_monthly.cutoff_for` для недельных срезов 7/14/21/28;
   - `robustness(...)` — R-P0, блоки (а)–(е) спеки §3 → `build/cb-comp-h1-shadow/robustness.json`;
   - `prepare_through(..., through)` — таблица признаков Task 4 до месяца M; `shadow_forecast(..., month)` — `combo3`, `wk_mix_ex`, `wk_mix_d`, `live_cb_wk`, компоненты, `served`, S0 на 2026-07…09, паспорт as-of/sha256 → `docs/research/cb-comp-h1-shadow/<YYYY-MM>.json`.
3. **Офлайн-тесты** `cb_comp_h1_shadow_test.py` (спека §5) и make-цели `cb-comp-h1-shadow-robustness`, `cb-comp-h1-shadow-forecast`, `cb-comp-h1-shadow-test`.
4. **Shadow 2026-10:** `make cb-comp-public-export` (свежая МП), `make cb-comp-h1-shadow-forecast ARGS="--month 2026-10"`; проверить S0 и as-of; **коммит JSON до 2026-11-08**.
5. **Robustness:** `make cb-comp-h1-shadow-robustness` на входах Task 4b (R-P0), прогон ×2 — одинаковый sha256; отчёт `docs/research/2026-10-31-cb-comp-h1-shadow-p06.md` (решение-запись `decision=shadow`, таблицы блоков, флаги, ограничения).
6. **Документы:** гайд `cb-comp-live-model-map.md` (P0.6 статус, §1.8/§1.10/§8, реестр) — после мержа PR #452 или rebase на него; роадмап `ROADMAP_KEY_RATE.md` (#419: P0.6 / #453).
7. **Проверки и PR:** `make all`, `make docs-check`, `make cb-comp-h1-shadow-test`; PR с `Closes #453`; протокол приёмки — комментарием в PR; Project: `In Review`.

## Риски

- Октябрьский МП-опрос может ещё не быть в ClickHouse — МП берётся as-of (последняя доступная точка), это фиксируется в `asof`.
- Прогон раньше последнего недельного релиза перед cutoff (4 ноября — праздник) — ранний срез, цена — блок (д).
- CatBoost walk-forward под `publication` на 45 месяцах — самый долгий шаг; запускается в фоне.
