# План: research `key-rate/phase-4-weekly-core-proxy` (#348)

**Дата:** 2026-10-22. **Спека:** [2026-10-22-weekly-core-proxy-design.md](../specs/2026-10-22-weekly-core-proxy-design.md). **Issue:** [#348](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/348). **Ветка:** `research/348-weekly-core-proxy` (worktree `.worktrees/weekly-core-proxy`, база `origin/main` `262f4d9`, зависимость #347 смержена PR [#349](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/349)).

## Шаги

1. **Данные.** Только чтение через `lib.clickhouse.ch_query`:
   - `v_rosstat_macro` + `v_series_catalog` (`frequency='W'`) — товарные ряды и агрегат;
   - `v_cbr_macro` (`series='Базовый ИПЦ'`) — цель;
   - `v_rosstat_macro` (месячный ИПЦ) — диагностический headline.
   В ClickHouse не пишем.

2. **Скрипт** `scripts/models/cpi/weekly_core_proxy.py`:
   - чистые функции: `four_week_average`, `regular_grid`, `calendar_regressors`, `tariff_weeks`/`tariff_months`, `monthly_align`, `metrics`;
   - методы B0 / CAL / STL / X13 по предрегистрации спеки §3;
   - X-13 — вызов `scripts/models/cpi/weekly_core_proxy_x13.R` (CSV сетки + cutoff); `unavailable`/`infeasible` фиксируются, не подменяются;
   - запись `docs/research/2026-10-22-weekly-core-proxy.json` (`decision`, `generated_at`, `git_sha`, версии пакетов, `inputs`, `spec`, `monthly`, `metrics`, `metrics_ex_tariff`, `robustness`, `x13`, `notes`);
   - короткая сводка в stdout.

3. **Gate 0 X-13** (до полного цикла): установить `seasonal` в `build/weekly-core-proxy/r-lib`, прогнать `ts(x, frequency=52)`; зафиксировать `supported` / `infeasible` (текст) / `unavailable`.

4. **Тесты** `scripts/models/cpi/weekly_core_proxy_test.py` (офлайн):
   - 4-нед. средняя и годовое выражение, отбрасывание `n<4`;
   - интерполяция 14-дневного разрыва на 7-дневную сетку;
   - выравнивание месяца (last-in-month / mean-in-month);
   - тарифные недели/месяцы (2024-07-01, 2025-07-07, 2026-10-05 — да; обычная неделя — нет);
   - календарные регрессоры (праздники/рабочие дни на известной неделе января);
   - DM на одинаковых ошибках → `(0.0, 1.0)`.

5. **Makefile**: `weekly-core-proxy`, `weekly-core-proxy-test` рядом с `cpi-public-audit`; `uv run --with "statsmodels>=0.15" --with numpy --with scipy --with "holidays>=0.60" --python 3.12`; имена в `.PHONY`.

6. **Живой прогон**: `make weekly-core-proxy` → JSON; повтор — числовые секции не меняются (кроме `generated_at`/`git_sha`).

7. **Отчёт** `docs/research/2026-10-22-weekly-core-proxy.md` по шаблону research-скила: короткий вывод/вердикт, вход и доступ, качество и покрытие, PIT/as-of, таблица MAE/RMSE/bias/corr/DM/n по методам и базам, тарифный прогон, рекомендация, «что не проверено», команды. Числа — только из JSON.

8. **Регистрация и ограничения**:
   - `docs/site-pages.json` — страница `research-2026-10-22-weekly-core-proxy.md`, секция «Исследования»;
   - `docs/LIMITATIONS.md` — пункт по фактическому вердикту (индикативная / X-13 неприменим);
   - `docs/ROADMAP_KEY_RATE.md` — статус `phase-4-weekly-core-proxy` `❌` → `✅` с вердиктом и ссылкой на отчёт (зависимость уже в main).

9. **Проверка**:
   - `make weekly-core-proxy-test` — OK;
   - `make weekly-core-proxy` — JSON, повтор детерминирован;
   - `make docs-check` — зелёный;
   - `make all` — зелёный;
   - `git diff --stat` — только ожидаемые файлы (спека, план, скрипт, R, тесты, Makefile, JSON, отчёт, `site-pages.json`, `LIMITATIONS.md`, роадмап).

10. **PR**: `Closes #348` вне backticks; пункт, спека/план, отчёт, числа, команды; протокол — комментарием в PR и issue.

## Границы

Панели 36–39, импортёры `ppi_sa`/`cbr`, DDL, витрины, гранты не меняются. В ClickHouse не пишем. Паритет с «Базовым ИПЦ» не заявляется; in-sample не выдаётся за as-of.
