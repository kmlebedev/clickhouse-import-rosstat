# phase-4-followup-2a: рыночный фактор распродаж — план

Спека: [2026-10-31-gold-attribution-market-factor-design.md](../specs/2026-10-31-gold-attribution-market-factor-design.md). Issue: #472. Ветка `feat/gold-attribution-market-factor`, worktree `.worktrees/gold-attribution-market-factor`. Роль `go`.

## Задачи

1. **Спека и план** — первый коммит ветки (этот файл и спека).
2. **FRED-ряды.** В `fred/fred.go`:
   - `fredSeries` += `NASDAQCOM`, `VIXCLS`, `BAMLH0A0HYM2`;
   - `fredSeriesMeta` += три записи с `title`, `unit`, `frequency`, `origin`, `description` (правило 11 AGENTS.md);
   - комментарий `v_fred_macro` — перечислить новые ряды;
   - `go test ./fred/` — тест полноты каталога уже покрывает список.
   Живой прогон: `make import STAT=fred`; проверка `SELECT series, max(date) FROM v_fred_macro WHERE series IN (...) GROUP BY series` через MCP.
3. **Витрина `v_gold_attribution`.** Новый файл `views/gold_attribution.go`:
   - источники: `v_fred_macro`, `v_gold_prices`, `v_gold_etf_flows_daily`;
   - дневные доходности/изменения, rolling-беты 180 (окно `ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING`), вклады, остаток, `stress_flag`/`stress_type`;
   - комментарии таблицы и каждой колонки; `Tables`-гейт — имена источников;
   - регистрация в `goldViewsList` (`views/gold_views.go`) **после** `goldEtfFlowsDailyView`;
   - если `covarPop`/`varPop` в окне не работают — fallback self-join (спека §5.1), зафиксировать в комментарии.
   Живой прогон: `make import STAT=gold_views`; проверка через MCP: последняя дата, `residual`, `stress_flag` на 01.09/04.02/30.01.
4. **Гранты и MCP-проверка.** `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_gold_attribution TO kimi_reader`; `scripts/mcp_check.py` — проверка витрины (читается, есть строки, `residual` не NULL на последней дате); `make mcp-user`, `make mcp-check`.
5. **Дашборд.** `grafana/gold_thesis.go` — панель «Рыночный фактор распродаж»: `contrib_*` и `residual` за 90 дней, `stress_flag` точками; `make grafana-build`, `make grafana-validate`; артефакт `dashboard/gold-thesis.json` — в PR.
6. **Research-отчёт.** `docs/research/2026-10-31-gold-attribution-market-factor.md`: таблица вкладов за 27–30.10, сверка с прототипом (spec §6), ограничения (spec §7). Добавить в `docs/site-pages.json` (секция «Исследования»).
7. **Документы.** `ARCHITECTURE.md` §6.2/§6.3 (витрина и описание), `README.md` (если список витрин/источников), `docs/GOLD_CONTOUR.md` (витрина в составе контура), `docs/GOLD_THESIS_DATA_ROADMAP.md` — статус пункта и ссылка на issue; `docs/ROADMAP_INDEX.md` — при необходимости (состав пунктов не перечисляется, но Project/Issue — да).
8. **Проверки.** `make all`, `make docs-check`, `make mcp-check`; `git status` — только файлы пункта; временные артефакты не в репозитории.
9. **PR.** `Closes #472`; в описании — пункт, источник, схема, запуск, пример проверки; протокол живой приёмки — комментарием в PR; Project: `In Review`; статус в роадмапе — `✅ сделан этим PR` (до мержа — `In Review`).

## Порядок и зависимости

- FRED-ряды (шаг 2) → витрина (шаг 3); без них view не наполнится.
- Витрина (шаг 3) → дашборд (шаг 5) и отчёт (шаг 6).
- Гранты (шаг 4) — после создания витрины; `mcp-check` — после грантов.
- Документы (шаг 7) — в том же PR, не отдельным.

## Риски

- Оконные `covarPop`/`varPop` могут не поддерживаться конкретной сборкой ClickHouse — fallback self-join; проверить на dev до PR.
- Витрина с шестью rolling-бетами может быть тяжёлой для MCP — ограничить 5 годами или материализовать; замерить `EXPLAIN ESTIMATE`/время.
- FRED отстаёт на 1 день; `HY OAS` только с 2023-10; `flow_tonnes` имеет пропуски — NULL, не 0.
- `bold` — прокси, не COMEX; не смешивать с `comex_front` в одном ряду без приоритета.

## Что не входит

- Новостной фактор (2b, #144) и follow-up 5.
- Многомерная rolling-регрессия/ridge — фаза 2; в этой фазе одномерные беты.
- CFTC COT/CTA, intraday, опционный skew.
- Запись в `forecast_log` и изменение сценариев.
