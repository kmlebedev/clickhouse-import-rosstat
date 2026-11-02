# План P0.3 (#480): живая цель `T_LIVE` 2024-08+ и политика пропусков

> Спека — [2026-11-01-cb-comp-p03-tlive-target-design.md](../specs/2026-11-01-cb-comp-p03-tlive-target-design.md). Роль `ml`. Ветка `research/cb-comp-88-groups-p03-tlive`.

## Задачи

1. **Скрипт** `scripts/models/cb_comp/cb_comp_tlive_target.py`.
   - `aggregate_with_coverage(sa, weights, months, codes)` → `(mm, stats)`: `n_groups`, `w_included`, `cov_group`, `cov_total`, `weight_year`, `weight_fallback`.
   - `cumulative(mm, h)` — та же формула, что в `cb_comp_live_bottomup`; `t_live(sa, …)` и `t_live_rt(sa_asof, …)`.
   - `direct_series(nsa, weights, months, runner)` — взвешенный NSA без ЖКУ → уровни → X-13 C0 → SA м/м (раннер инъектируется, по умолчанию `SpecRunner("C0", start)`).
   - `nsa_weights_check(...)` — агрегат NSA с `9470` против `ipc31074/Все товары и услуги`, 2022-01…2026-09.
   - `compare_to_target_full(...)` — RMSE/bias на 67 мес., normal-54, подпериодах.
   - `group_report(...)` — первый/последний месяц SA, вес по годам, блок `9218/9260/803/930.АГ`.
   - `gates(...)`, `snapshot(...)`, `main()` с `--version`, `--archive`, `--panel`, `--out-dir`, `--copy-json`, `--snapshot --month`.
2. **Тесты** `cb_comp_tlive_target_test.py` — офлайн: правило `renorm_v1`, покрытие, разрывы, детерминизм sha256, идемпотентность снимка, сравнение с `TARGET_FULL` на синтетике.
3. **Makefile**: `cb-comp-tlive-target`, `cb-comp-tlive-target-test`; строка в `README.md`.
4. **Прогон**: `@2009` и `@2010`, каждый дважды; сверить sha256; снимок `--month 2026-10`.
5. **Отчёт** `docs/research/2026-11-01-cb-comp-p03-tlive-target.md` и сводный JSON.
6. **Документы**: карта модели §1.3/§1.10/§6.2/§7.8, протокол Task 7, `docs/ROADMAP_KEY_RATE.md`, README.
7. **Приёмка**: `make cb-comp-tlive-target-test`, `make all`, `make docs-check`; протокол — комментарием в PR; статус роадмапа тем же PR.

## Порядок

- Спека и план — первые коммиты ветки, до кода.
- Скрипт → тесты → make/README → прогоны → отчёт и документы → PR.
- Модели не переобучаются, shadow не запускается: пункт только про цель.
