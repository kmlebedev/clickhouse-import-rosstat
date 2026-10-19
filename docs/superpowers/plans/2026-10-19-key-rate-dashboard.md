# План: `v_key_rate_dashboard` (пункт `key-rate/phase-4-dashboard`, issue #128)

**Спека:** [2026-10-19-key-rate-dashboard-design.md](../specs/2026-10-19-key-rate-dashboard-design.md).
**Ветка:** `feat/key-rate-dashboard-view`.
**Зависимость:** `key-rate/phase-4-forecast-log` (#127) — ✅.

## Задачи

### 1. Витрина и импортёр

- [x] `views/key_rate_views.go`: `keyRateDashboardView` (`util.View`) и импортёр `keyRateViews`:
  - `keyRateViewsList = []util.View{keyRateDashboardView}`;
  - `Name() = "key_rate_views"`;
  - `Import()` — цикл `util.CreateView`, `log.Infof("View %s created: %t", ...)`;
  - `init()` — `chimport.Stats = append(chimport.Stats, &keyRateViews{})`.
- [x] `Select` — по спеке §5: `WITH` one-row CTE (`kr`, `cpd`, `ie`, `ipc`, `ofz`, `ru`, `kf`, `ct`, `pe`, `bc`), `UNION ALL` строк из спеки §4, внешний `ORDER BY contour, indicator`.
- [x] 11 таблиц гейта в `Tables`: `cbr_key_rate`, `cbr_indicators_cpd`, `cbr_infl_exp`, `rosstat_ipc_mes`, `ofz_curve`, `cbr_ruania`, `macro_series`, `keyrate_futures`, `keyrate_contract`, `events_calendar`, `forecast_log`.
- [x] `Comment` таблицы и 14 комментариев колонок: контуры, `flag`/`stale`, формула `error_bp`, метаданные forecast, оговорки KEYRATE/ОФЗ, закрытость `forecast_log`.
- [x] Формулы:
  - `ipc_yoy = (exp(sum(log(percent/100))) − 1) × 100` по 12 последовательным месяцам (иначе NULL);
  - `base_ipc_saar = ((1 + v/100)^12 − 1) × 100` для двух соседних месяцев; `flag` — оба > 4 и `dateDiff('month', prev_date, as_of) = 1`;
  - `infl_exp_expected` — последняя и предыдущая волна, `flag = last > prev`;
  - `keyrate_implied_change_bp = round((settle − nullIf(last_rate, 0)) × 100, 1)`;
  - `ofz_slope = round((10y − 1y) × 100, 1)`;
  - `ruonia_spread = round((ruonia − last_rate) × 100, 1)`;
  - `next_meeting = dateDiff('day', today(), minOrNull(event_date))`;
  - `divergence = round((predicted − settle) × 100, 2)` для `taylor` и `ordered_logit`; при несовпадении `target_date` с ближайшим заседанием `value` NULL, `flag` 0, `stale` 1;
  - `forecast` — `row_number() OVER (PARTITION BY metric ORDER BY forecast_date DESC) = 1` по `metric LIKE 'cbr_rate:%'`.
- [x] `stale` и `flag` — по спеке §6.

### 2. Тесты

- [x] `views/key_rate_dashboard_test.go`:
  - имя и 11 таблиц гейта;
  - 14 алиасов с непустыми комментариями (явный список, не `selectedColumns`);
  - маркеры SQL: шесть контуров, `base_ipc_saar`, `keyrate_implied_change_bp`, `ofz_slope_10y_1y_bp`, `as_1y`/`as_3y`/`as_5y`/`as_10y`, `least(`, `next_meeting`, `error_bp`, `LIKE 'cbr_rate:%'`, `pow(1 +`, `FINAL`, `minOrNull`;
  - `keyRateDashboardView` есть в `keyRateViewsList`.
- [x] `go test ./views/` — зелёный.

### 3. Доступ и MCP

- [x] `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_key_rate_dashboard TO kimi_reader;`.
- [x] `scripts/mcp_check.py`: проверки из спеки §7 (витрина непуста, 5 фиксированных контуров + информационный `forecast`, `next_meeting.target_date` = `min(event_date)` из `v_events_calendar`, `base_ipc_saar`/`keyrate_implied_change_bp` есть, `error_bp` читается, `forecast_log` → 497, `probabilities`/`model` у `ordered_logit` непусты, если строка есть).
- [x] `make mcp-check` — зелёный.

### 4. Документация

- [x] `ARCHITECTURE.md`: §2 (`views/`), §6.1/список `Name()` (добавить `key_rate_views`), §6.3 (новый bullet `v_key_rate_dashboard`).
- [x] `README.md`: строка таблицы импортёров `views`.
- [x] `docs/IMPORTERS.md`: строка `views` и порядок запуска.
- [x] `docs/DEVELOPMENT.md`: `make import STAT=key_rate_views` в ручные команды.
- [x] `docs/KEY_RATE_PUBLICATIONS.md` §6: «Сводный дашборд каналов и порогов» — ✅.
- [x] `docs/ROADMAP_KEY_RATE.md`: §5 (таблица) и фаза 4 (строка ❌) — ✅ с числами приёмки, spec/plan, issue #128.
- [x] `make docs-check` — зелёный.

### 5. Живая приёмка

- [x] `make all` — зелёный.
- [x] `make import STAT=key_rate_views` — `View v_key_rate_dashboard created: true`; повтор — те же строки.
- [x] MCP `kimi_reader`: `SELECT count() FROM v_key_rate_dashboard` = 23; `count(DISTINCT contour)` = 6; значения/флаги — по спеке §10; `SELECT count() FROM forecast_log` → `ACCESS_DENIED`.
- [x] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- [x] Протокол — комментарием в PR #203 и issue #128.

## Порядок коммитов

1. `docs: add key rate dashboard spec`
2. `docs: add key rate dashboard plan`
3. `feat: add v_key_rate_dashboard view`
4. `test: cover key rate dashboard contract`
5. `docs: wire key rate dashboard into importer docs`
6. `docs: accept key rate dashboard item`

Коммиты 1–2 — до кода, как требует AGENTS.md.

## Проверки перед PR

```bash
make all
make import STAT=key_rate_views
make mcp-check
make docs-check
```

## Риски

- `forecast_log` и его колонки появляются при старте ingest; витрину создавать после `make run-ingest`.
- `ofz_curve` — снимок; `stale = 1` у ОФЗ — штатное поведение при разрыве > 7 дней.
- «Две недели подряд» по базовой инфляции — прокси «два месяца подряд», явно названный в `threshold`.
