---
name: ml-engineer
description: Use when starting, running, reviewing, promoting, or publishing an ML experiment in clickhouse-import-rosstat — CPI nowcast, key-rate model, CatBoost/gold ML, MLflow runs, gate/shadow/canary, forecast_log, live monitoring, feature/model changes, or interpreting backtest results
type: prompt
whenToUse: When a task involves an ML experiment, model, feature set, backtest, MLflow registry, promotion gate, shadow/canary, forecast_log, live accuracy, or ML interpretation in clickhouse-import-rosstat
---

# ML-инженер в clickhouse-import-rosstat

**Назначение.** Единый маршрут для ML-задач: что прочитать, как провести эксперимент, как его оценить, как пройти gate и shadow/canary, как смотреть live-точность и где граница с Go-разработкой. Скил — надстройка над `AGENTS.md`, а не его замена.

**Канонические документы.** Методология — [stat-ml-models.md](../../../docs/guides/stat-ml-models.md); MLOps и паспорт run — [ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md); контур ставки — [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md); промоушен и live — [2026-10-20-ml-promotion-live-monitoring-design.md](../../../docs/superpowers/specs/2026-10-20-ml-promotion-live-monitoring-design.md); ревью ML-issue — [2026-10-19-ml-issue-review.md](../../../docs/research/2026-10-19-ml-issue-review.md).

## 1. Когда применять

- ML-эксперимент, nowcast ИПЦ, прогноз ставки, CatBoost/gold ML, классификатор шоков.
- Новый признак, модель, окно, метрика или изменение протокола бэктеста.
- Запись run'а в MLflow/ClickHouse, `forecast_log`, интерпретация DM/CI/калибровки.
- Промоушен challenger'а: gate DM/MCS, shadow/canary, rollback, разбор live-ошибки и алертов.
- Подготовка ML-раздела публикации или ответа «модель лучше/хуже бенчмарка».

**Когда не применять:** чистый Go-импортёр, DDL/витрина/грант, дашборд без ML-содержания, инфраструктура сборки. Это зона `AGENTS.md` и [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).

## 2. Что прочитать перед стартом

1. [AGENTS.md](../../../AGENTS.md) — правила 1–12, окружение, приёмка.
2. Владеющий роадмап и issue пункта — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md); не делай работу вне пункта.
3. [stat-ml-models.md](../../../docs/guides/stat-ml-models.md) §4 (валидация), §6 (ловушки), §7 (данные).
4. [ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md) §4 (паспорт run), §7 (цикл и gates).
5. [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md) §4/§6 — если задача про ставку или CPI-nowcast.
6. Спека/план своего пункта; для промоушена и live — [2026-10-20-ml-promotion-live-monitoring-design.md](../../../docs/superpowers/specs/2026-10-20-ml-promotion-live-monitoring-design.md); для витрин/грантов — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).

## 3. Границы ролей

| ML-инженер / агент | Go-разработчик |
|---|---|
| Python в `scripts/`, офлайн-тесты, анализ, спеки/планы ML-пунктов | Go-импортёры, DDL, витрины, гранты, ingest endpoints |
| Чтение ClickHouse через MCP `mcp__clickhouse__run_query` (`kimi_reader`) | Схема ClickHouse, `sql/mcp_kimi_reader.sql`, `make mcp-check` |
| MLflow runs, `ML_LOG`, артефакты, `forecast_log` payload | `make all`, CI, сборка, инфраструктура расписаний |
| Интерпретация DM/CI/калибровки, ML-раздел публикации | Архитектура, README/ARCHITECTURE, приёмочный контур |

**Смежные роли.** Не-ML Python-инструменты (коллекторы, docs-генератор, MCP-check, forecast-log sender) ведутся по скилу [`python-engineer`](../python-engineer/SKILL.md); разведка источника, аудит покрытия/винтажей и research-документ — по [`research-engineer`](../research-engineer/SKILL.md). Их правила здесь не дублируются.

**Жёстко:** ML-агент не делает ad-hoc `CREATE TABLE`, `INSERT`, `ALTER`, `GRANT` и не меняет Go-пакеты «попутно». Если для эксперимента нужна новая таблица/витрина/грант — это отдельный пункт владеющего роадмапа и Go-импортёр по стандарту; ML-агент готовит спеку/DDL-предложение, применение — за Go-разработчиком.

**Чтение ClickHouse.** Интерактивно — MCP `mcp__clickhouse__run_query` (`kimi_reader`). Скрипты читают через свой существующий `ch_query`/env-путь (`ML_LOG`/`CLICKHOUSE_*` через `make`); это чтение, не запись. Писать в ClickHouse скрипт не может: только ingest endpoint или Go-импортёр.

**Смешанный ML+Go пункт.** Один пункт может включать и Python, и Go (пример — #184: gate-скрипт + витрина + гранты + алерты). Тогда в спеке/плане задачи разделены по ролям, Go-часть ревьюит Go-разработчик, а ML-агент не подменяет DDL «временной таблицей». Один пункт — одна ветка — один PR.

## 4. Рабочий цикл (12 шагов)

1. **Пункт.** Назови `<slug>/<item-id>`, прочитай issue и критерий приёмки; переведи issue в `In Progress`.
2. **Спека/план.** Зафиксируй гипотезу, ожидаемый эффект, primary metric, бенчмарки, фолды, robustness, MDE, бюджет экспериментов и **правило gate** — до первого прогона.
3. **Данные.** Проверь семантику, единицы, календарную полноту, дубликаты, as-of/PIT, винтажи, NaN-политику и выбросы.
4. **Бенчмарки.** B1 last value, B2 AR(1)+сезонные дамми, B3 ElasticNetCV; при необходимости — внешние (StatsForecast/Theta/CES). Набор фиксируется до результатов.
5. **Эксперимент.** Walk-forward с purge/embargo; никакого random split; гиперпараметры — дефолты или CV строго внутри train.
6. **Оценка.** RMSE/MAE + DM-HLN + блочный бутстрап; для вероятностей — Brier/log loss/калибровка; при многих спецификациях — SPA/MCS.
7. **Паспорт.** `ML_LOG=1`; MLflow включён по умолчанию (`MLFLOW=0` — осознанно выключить). Перед прогоном `make` вызывает `mlflow_preflight.py`: если API не отвечает, скрипт явно спрашивает, запускать ли без лога; в неинтерактивном режиме нужен `MLFLOW_ALLOW_NO_LOG=1`. Проверь `run_id`, `config_hash`/`sample_hash`, sha256 артефактов и повтор детерминизма. Для gate run обязан быть виден в `v_ml_error_by_fold`: при недоступном ClickHouse — `make mlflow-export`/`mlflow-backfill`.
8. **Gate.** `make ml-gate ARGS="--champion-run <uuid> --challenger-run <uuid> --model <model>"`; пороги — DM-HLN p<0,05, 95% бутстрап CI, robustness-окна, MCS при >2 кандидатах. Решение и причина — в `build/ml-gate/<run>.json`; `promote` не двигает alias и не публикует модель.
9. **Промоушен.** Для CPI с model registry — явный `make mlflow-promote`; для key-rate registry пока нет — решение gate + переход к shadow/canary, alias не используется. Не называть `champion` deployment.
10. **Shadow/canary.** `make key-rate-forecast-log ARGS="--stage shadow"` — 2 заседания, затем `ARGS="--stage canary"` — 1. Staged-метрики `:shadow`/`:canary` не публикуются; live-оценка — `make ml-gate ARGS="--live --live-track cbr_rate:<track>"`.
11. **Live и rollback.** Перед публикацией — `make ml-live-check` (при LLM-треке `ARGS="--require-llm"`); после заседания — резолвер, `v_ml_live_accuracy`, `error_bp`; алерты `ml-live-degradation`/`ml-live-freshness` — повод для `hold`/`rollback`, а не для ручной правки журнала.
12. **Приёмка.** Числа с командой и результатом, «что не проверено», `make all`, `make docs-check`, при необходимости `make mcp-check`; протокол — комментарием в PR; статус роадмапа — тем же PR.

## 5. Жёсткие правила

1. **Никакого random split** и никакого выбора окна по ошибке.
2. **Никакого тюнинга по тесту**; гиперпараметры — только внутри train.
3. **Без значимого выигрыша над B1–B3** модель не входит в ансамбль.
4. **Полный набор метрик**, а не одна удобная; «значимо» — это DM/CI, а не «число меньше».
5. **Детерминизм**: seed, версии, `config_hash`/`sample_hash`, sha256; повторный прогон должен совпасть.
6. **As-of/PIT**: точка, опубликованная после cutoff, не участвует; винтажи и ревизии называются явно.
7. **NaN ≠ 0**; импутация — медианой train внутри фолда; систематически отсутствующий признак — сноска и тест.
8. **Выброс-шок остаётся** в основной метрике; выброс-ошибка исправляется с записью (дата, причина, ссылка).
9. **Quick-прогоны не сравнивать с полными**; `--quick` — только для итераций, в паспорте должно быть `quick=true`.
10. **Live-прогноз пишется до публикации**; `forecast_log` — журнал live, `ml_predictions` — fold-бэктест.
11. **Секреты только через env**; не читать и не печатать `~/.config/rosstat/env`.
12. **Команды только через `make`**; не запускать `go run`/`python` напрямую с ожиданием переменных окружения.

## 6. Команды

| Цель | Что делает |
|---|---|
| `make cpi-nowcast` | nowcast ИПЦ v1 (CatBoost + B1–B3); `ML_LOG=1` — паспорт и MLflow (по умолчанию) |
| `make cpi-monthly` | месячный nowcast v2 с МП; `ARGS="--sample long"`, `ARGS="--quick --jobs N"`; `ML_LOG=1` |
| `make cpi-monthly-v3` | Stage 1 #76: недельные diffusion-признаки, `ARGS="--sample weekly|long"`; `ML_LOG=1`, `window="h1"`, `unit="pp"` |
| `make cpi-monthly-industry` | Stage 2 #76: 16 отраслевых листов МП, `ARGS="--sample weekly|long"`; `ML_LOG=1`, `window="h1"`, `unit="pp"` |
| `make cpi-public-audit` | Task 1 #76: аудит публичной панели, покрытие весов, PIT/as-of; в БД не пишет |
| `make cpi-weekly-panel` | недельная компонентная панель ИПЦ (Task 3): product/component CSV, реконструкция и гейт; в ClickHouse не пишет, `ML_LOG` не используется |
| `make cpi-weekly-panel-test` | офлайн-тесты панели (без ClickHouse и сети) |
| `make cpi-weekly-aggregate` / `-test` | #396: аудит недельного агрегата 2009+ и признаки `wkagg_*` (`--samples S_L09,S_L13,S_W22`); только чтение |
| `make cpi-v4-leaderboard` / `-test` | #396: регрессионный гейт v2/v3 на `S_W22` (эталон — спека §7.1); `--with arch` для MCS |
| `make cpi-v4-grid` / `-test` | #396 эксперимент 1: сетка 3 таргета (NSA, SA без КУ, «Базовый ИПЦ») × h1/h3/h6; `ARGS="--sample S_L09|S_L13|S_W22 --cells T_NSA:1"`; `ML_LOG=1` — паспорт при девяти ячейках |
| `make cpi-v4-h2` / `-test` | #396 эксперимент 2: решение gate по `wkagg_*` из ячеек сетки; пишет `build/ml-gate/cpi_v4_h2.*` |
| `make cpi-core-nnls` / `-test`, `make cpi-v4-core` / `-test` | #396 эксперимент 3: `core_nnls` (NNLS по 14 корзинам панели #364, as-of), `v4c`/`v4d` на `T_CORE`; `ARGS="--sample S_L09"`, `ARGS="--decide"` |
| `make cpi-weekly-sa-asof` / `-test` | #401 W1: as-of перефит недельного X-13 `weekly_cpi_sa` (порт `weeklysa/grid.go`, parity с таблицей, ревизии, gate); `build/cpi-two-stage/weekly_sa_asof.{json,csv}`; нужен R `seasonal` |
| `make cpi-two-stage-targets-test`, `make cpi-two-stage-w2` / `-test` | #401 W2: `T_SAX` (окончательный и винтажный X-13), варианты КУ `V0/V1/V2` и выбор на dev; `build/cpi-two-stage/w2_select.json`; нужен R `seasonal` |
| `make cpi-two-stage-folds` / `-test` | #401 W3: таргеты `T_SAX`/`T_CORE`/`T_NSA`, доступность (Росстат/ЦБ), purge и фолды Stage 1/2, отпечатки; `build/cpi-two-stage/w3_folds.json`; нужен R `seasonal` |
| `make cpi-asof-test` | #401 W0: as-of конвенция `publication` (cutoff = публикация ИПЦ `M−1`, 9-е `M`) и тесты на утечку; `with cm.asof_convention("publication")` |
| `make cpi-sa-reconciliation` / `-test` | #399: сверка SA из сырого месячного ИПЦ (X-13, STL), ИПЦ без КУ из `ipc31074` и недельного SA `weekly_cpi_sa` с SA/core ЦБ; пишет `build/cpi-sa/reconciliation.json`; нужен R `seasonal` |
| `make key-rate-model` | ставка: Тейлор, ordered logit, walk-forward; `ML_LOG=1` |
| `make shock-classifier` | классификатор шоков и причинные веса; `ML_LOG=1` |
| `make cb-comp-parity` | паритет с CB_COMP (direct + bottom-up, DM/TOST); `ML_LOG=1` |
| `make key-rate-llm-forecast` | LLM-трек: as-of, 3 повтора, медиана; `ARGS="--log"` — payload |
| `make key-rate-forecast-log` | запись прогноза в `forecast_log` через ingest; `ARGS="--dry-run"`, `ARGS="--stage shadow|canary"` — staged-трек |
| `make ml-gate` | gate промоушена: DM-HLN, 95% бутстрап, robustness, MCS; артефакт `build/ml-gate/<run>.json`; `ARGS="--live --live-track cbr_rate:<track>"` — live-оценка shadow/canary |
| `make ml-gate-test` | офлайн-тесты gate (без ClickHouse) |
| `make ml-live-check` | production-строки на ближайшее заседание до публикации; `ARGS="--require-llm --warn-only"` |
| `make ml-live-check-test` | офлайн-тесты live-check |
| `make import STAT=key_rate_views` | создать/обновить `v_key_rate_dashboard` и `v_ml_live_accuracy` (вручную, один раз; повтор идемпотентен) |
| `make mlflow-up/down/status` | локальный MLflow (SQLite + артефакты); `MLFLOW=1` по умолчанию |
| `make mlflow-preflight` | проверка MLflow API; при недоступности — явный вопрос (не-TTY: `MLFLOW_ALLOW_NO_LOG=1`) |
| `make mlflow-backfill ARGS="--run-id <uuid>"` | перенос существующего ClickHouse-прогона в MLflow (тег `clickhouse_run_id`) |
| `make mlflow-export/register/promote/models` | экспорт run'ов, registry, aliases |
| `make all` / `make docs-check` / `make mcp-check` | CI, docs-портал, MCP end-to-end |

Python-тесты — офлайн, через `uv run --with ... python3 scripts/models/<family>/<name>_test.py` (общие — `scripts/lib/<name>_test.py`). Интерактивные SQL-проверки — MCP `run_query`; скрипты читают ClickHouse своим `ch_query`/env-путём, но не пишут.

## 7. Паспорт run и реестр

Минимальный паспорт: `run_id`, experiment/run name, git SHA/branch/dirty, spec/plan path, config + hash, data as-of + sample hash, окружение, per-fold predictions, агрегаты, DM/CI, артефакты sha256, решение и причина.

- `ML_LOG=1` — запись в `ml_runs`/`ml_predictions` (витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`).
- `MLFLOW=1` — **по умолчанию**: MLflow tracking + registry; `MLFLOW=0` — осознанно выключить. Перед прогоном `mlflow_preflight.py` проверяет API; при недоступности спрашивает (не-TTY: `MLFLOW_ALLOW_NO_LOG=1`). Экспорт в ClickHouse идемпотентен; `make mlflow-backfill` переносит существующие ClickHouse-прогоны в MLflow.
- **Gate читает ClickHouse, не MLflow:** `v_ml_error_by_fold` (реплика 4.1). Run, который есть только в MLflow, для gate не существует; перед gate — `ML_LOG=1` или `make mlflow-export`/`mlflow-backfill`.
- **Gate пишет в MLflow только теги** (`decision`, `gate`, `primary_metric`, `gate_bootstrap`, `gate_robustness`); это best-effort: при недоступном MLflow или отсутствии run — warning, решение остаётся в JSON-артефакте. Alias не двигается; для CPI — явный `make mlflow-promote`.
- **Key-rate registry — отдельный follow-up:** у `taylor`/`ordered_logit` нет сохраняемого model object; `champion`/`challenger` в MLflow к ним не применяются. Промоушен key-rate — решение gate + `--stage shadow|canary|production`.
- Канонические MLflow-теги решения: `decision=promote|reject|iterate|shadow|canary`, `gate=dm|mcs|dm+mcs`, `primary_metric`, `model_version`, `run_id`; причина решения — в артефакте, не только в PR.
- Сырые `ml_runs`/`ml_predictions` агенту не выданы; для анализа — витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`, `v_ml_live_accuracy`.
- Артефакты не должны существовать только в gitignored-каталоге: паспорт и sha256 — обязательны.

## 8. Промоушен и live: рабочий контур

### 8.1 Кто что делает

| Слой | Роль | Источник истины |
|---|---|---|
| MLflow | control plane: runs, params, artifacts, registry и aliases | run_id, `champion`/`challenger` |
| ClickHouse | analytics: `ml_runs`/`ml_predictions`, `forecast_log`, `v_ml_live_accuracy` | SQL, Grafana, MCP |
| Gate | решение по бэктесту; **не** deployment и не alias | `build/ml-gate/<run>.json` + теги MLflow |
| Grafana | live-алерты деградации и свежести | `v_ml_live_accuracy` |

- Gate не двигает alias: `champion` в MLflow — registry, не производство.
- **CPI:** gate → `make mlflow-promote` (явное действие) → registry alias.
- **Key-rate:** model registry пока нет (у методов нет сохраняемого объекта) — gate → shadow/canary → production; это follow-up, не называй key-rate `champion` deployment.
- Staged-метрики `cbr_rate:<track>:shadow|canary` не публикуются и не входят в forecast-контур `v_key_rate_dashboard`.

### 8.2a Полный цикл (key-rate)

1. **Бэктест.** `ML_LOG=1 MLFLOW=1 make key-rate-model`; run обязан быть в `v_ml_error_by_fold` (иначе `make mlflow-export`/`mlflow-backfill`).
2. **Gate.** `make ml-gate ARGS="--champion-run <uuid> --challenger-run <uuid> --model ordered_logit"`; решение `promote|reject|iterate`, причина — в артефакте.
3. **Решение.** `promote` не публикует модель: для CPI — `make mlflow-promote`; для key-rate — запись решения и переход к shadow.
4. **Shadow.** `make key-rate-forecast-log ARGS="--stage shadow"` — 2 заседания; строки видны в `v_ml_live_accuracy`, но не в публикационном контуре.
5. **Canary.** `make key-rate-forecast-log ARGS="--stage canary"` — 1 заседание.
6. **Live-gate.** `make ml-gate ARGS="--live --live-track cbr_rate:ordered_logit"`: `wait` / `promote` / `hold` / `rollback`.
7. **Production.** Writer без `--stage` (production); перед публикацией — `make ml-live-check` (с LLM-треком `ARGS="--require-llm"`); после заседания — резолвер и `v_ml_live_accuracy`.
8. **Rollback.** Canary `abs_error_bp > 200` или алерт деградации/свежести — вернуть предыдущий production-трек, причину записать в артефакт/PR; журнал не править вручную.

### 8.2b Полный цикл (CPI-компоненты, #76)

**Контекст.** #76 — зонтичная задача production-v3 на публичных данных (4 месячных компонента ИПЦ, 149 недельных рядов, 16 отраслевых листов МП), не ставка. Сейчас 5/8 sub-issues, Stage 1/2 измерены, вердикт `iterate`; покрытие весов **39,3 %** (< 50 %), полный 88-групповой bottom-up не заявляется. В `forecast_log` нет CPI-метрики, live-слой #184 — только `cbr_rate`; shadow/canary и live-алерты для CPI пока не существуют.

**Предусловие gate.** Текущий `ml-gate` сравнивает **один и тот же `model`** в двух run'ах. Для #76 primary-пары — разные имена (`v3_bottomup` vs `v2_bottomup`, `v3_ind_bottomup` vs `v3_bottomup`). Без расширения `--champion-model`/`--challenger-model` gate для #76 не запускается; сейчас эти пары считаются внутри `cpi_monthly_v3.py`/`cpi_monthly_industry.py` и попадают в отчёт. Это отдельный маленький follow-up к #184.

1. **Данные и покрытие.** `make cpi-public-audit` — 149 weekly-рядов, 16 листов, 118 совпадений с весами, покрытие 39,3 %, PIT/as-of — 0 нарушений. Coverage gate: полный bottom-up только при ≥ 50 %; иначе hybrid и **никакого production-v3**.
2. **Бэктест.** `ML_LOG=1 MLFLOW=1 make cpi-monthly-v3 ARGS="--sample weekly"` и `ARGS="--sample long"`; Stage 2 — `make cpi-monthly-industry ARGS="--sample weekly"`/`long`. Эксперименты `cpi_monthly_v3`/`cpi_monthly_industry`, `window="h1"`, `unit="pp"`; run'ы видны в `v_ml_error_by_fold`.
3. **Gate.** Для #76 нужен `--experiment cpi_monthly_v3 --window h1` и пара champion/challenger. Primary — `v3_bottomup` vs `v2_bottomup` (и `v3_direct` vs `v2_baseline`); Stage 2 — `v3_ind_bottomup` vs `v3_bottomup`, `ind_only_bottomup` vs `v2_bottomup`. Основное окно — `weekly` (2022+), robustness — `long` (2013-10…2026-08, start 48); это **разные запуски**, поэтому gate прогоняется дважды, а не через `--robustness-window`.
4. **Coverage gate.** Независимо от DM/CI: при покрытии < 50 % полный bottom-up не заявляется. Победа в бэктесте не отменяет coverage gate.
5. **Решение.** Сейчас Stage 1: `v3_bottomup` 0.311487 vs `v2_bottomup` 0.362300 (−14,0 %, DM p=0.0285, CI исключает 0) — primary проходит; `long` 0.642997 vs 0.688869 (−6,7 %, p=0.1573, MDE 8,8 %) — мощности нет. Stage 2: `v3_ind_bottomup` 0.354359 vs 0.311487 (+13,8 %, p=0.3059); `long` 0.642664 vs 0.643146 (p=0.9251) — значимого выигрыша нет. Итог — **`iterate`**, не `promote`.
6. **Промоушен.** Если gate и coverage пройдены: `cpi_monthly_v3`/`cpi_monthly_industry` сейчас логируют только артефакты, **не `log_model`**; перед MLflow registry нужно добавить сохранение CatBoost-модели, затем `make mlflow-register`/`mlflow-promote`. Иначе promotion — только решение в артефакте, alias не двигается.
7. **Shadow/canary.** Для CPI **недоступны**: нет CPI-метрики в `forecast_log`, нет ingest-endpoint и `v_cpi_live_accuracy`. #184 shadow/canary — только `cbr_rate`. Возможен «paper shadow»: ручной месячный nowcast против первой публикации Росстата в research-артефакте; это не production и не алерт.
8. **Productionization.** Task 6 Go уже сделан: 16 отраслевых листов в `cbr_survey`, витрина `v_cbr_survey`; но v3 **не подключён** к make/dagu production, потому что Task 4/5 дают `iterate`, а покрытие < 50 %. Наличие импортёра — не повод ставить шаг в dagu.
9. **Rollback.** Если coverage или gate не проходят — остаёмся на `v2_bottomup`/hybrid; production-переключения нет, причина — в gate-артефакте/отчёте.
10. **DoD.** Gate-артефакт с решением и причиной; coverage-аудит; PIT/as-of; DM/CI; отчёт; `make all`/`make docs-check`; никакой зависимости production-v3 от 87 кодов ОКВЭД2; никаких заявлений о live-мониторинге CPI.

**Чем отличается от key-rate:**

| | key-rate (#184) | CPI-компоненты (#76) |
|---|---|---|
| Цель | ставка на заседание | 4 месячных компонента ИПЦ |
| Окно gate | `expanding`/`rolling40` | `h1`; `weekly` primary, `long` robustness |
| Live-журнал | `forecast_log` (`cbr_rate*`) | нет CPI-метрики |
| Shadow/canary | 2 + 1 заседание | нет; только paper shadow |
| MLflow registry | нет model object, follow-up | CatBoost есть, но `log_model` пока не вызывается |
| Coverage gate | нет | ≥ 50 % обязательно |
| Текущий вердикт | инфраструктура #184 в PR #257 | `iterate`, покрытие 39,3 % |

### 8.3 Предрегистрированные пороги

- **Gate:** DM-HLN p<0,05 + 95% блочный бутстрап CI разницы RMSE + robustness-окна; MCS 90% только при >2 кандидатах. Минимум 12 общих фолдов в основном окне и 6 в robustness.
- **Shadow/canary:** 2 + 1 заседание; `promote` — canary `abs_error_bp ≤ 100` и rolling MAE ≤ 100 б.п.; `hold` — 100–200; `rollback` — > 200.
- **Алерты:** `ml-live-degradation` — rolling MAE трёх production-заседаний > 100 б.п.; `ml-live-freshness` — последний прошедший production-прогноз не разрешён > 3 дней. Selftest-строки (`model='gate-selftest'`) исключены.

### 8.4 Live-прогноз ставки

- Треки различаются суффиксом `metric`: `cbr_rate:taylor`, `:ordered_logit`, `:ml`, `:llm`; ошибка — `error_bp`, `error_pct=NULL`.
- `forecast_log` — только live; fold-предсказания — в `ml_predictions`. Публикация без production-строки в журнале запрещена (`KEY_RATE_PUBLICATIONS.md`).
- `v_ml_live_accuracy` — одна строка на прогноз; для одного заседания может быть несколько `forecast_date`. Алерты и live-gate дедуплицируют по `(track, target_date)` и берут последний `forecast_date`.
- После заседания факт пишет `forecast_resolve` (`cbr_rate` → первая точка `cbr_key_rate` после `target_date`); алерт свежести ловит сбой резолвера.

### 8.5 Ограничения MLflow-контура

- Gate читает ClickHouse-реплику, а не MLflow API: run только в MLflow без экспорта в `v_ml_error_by_fold` для gate невидим.
- Теги gate в MLflow — best-effort: при недоступном MLflow или отсутствии run будет warning; решение всё равно в JSON-артефакте.
- Live-точность и алерты живут в ClickHouse/Grafana, не в MLflow UI; MLflow показывает run, registry и artifacts, но не `v_ml_live_accuracy`.
- Key-rate registry — отдельный follow-up; не обещай alias `champion` для key-rate.

## 9. Открытая очередь

| Приоритет | Issue | Следующий шаг |
|---|---|---|
| ✅ | #184 `infra/4.3` | Gate + shadow/canary + `v_ml_live_accuracy` + алерты; PR #257 (In Review); после мержа — #191 |
| 1 | #191 `key-rate/phase-4-first-publication` | Production-строки `forecast_log`, свежий снимок, пост, пост-апдейт |
| 2 | #76 `key-rate/phase-4-ml-components` | Stage 3/4/5, coverage ≥50 %, gate-пара `--champion-model/--challenger-model`; текущий вердикт `iterate` |
| 3 | #154 `gold-thesis/phase-6-catboost-c1` | Обучающая матрица, walk-forward без look-ahead |
| 4 | #155–#157 | In-DB baseline → CatBoost-трек → DM-оценка |
| 5 | #147 `gold-thesis/phase-4-followup-5` | Исторический новостной слой с PIT/vintage |
| later | #124/#125/#145/#153 | Ансамбль, priors, HMM, live-витрина — после #191, как challengers |
| follow-up | key-rate MLflow registry | Model card/artifact, версия в registry, alias и интеграция с gate — отдельный пункт |

## 10. Definition of Done

- [ ] Пункт роадмапа и issue названы; спека/план — первые коммиты.
- [ ] Предрегистрация и данные зафиксированы до результата.
- [ ] Бенчмарки B1–B3 и полный набор метрик; DM/CI/калибровка.
- [ ] Паспорт run (`ML_LOG=1`; MLflow по умолчанию, `MLFLOW=0` — осознанно), артефакты sha256, повтор детерминирован.
- [ ] Для промоушена: run виден в `v_ml_error_by_fold` (gate-вход); при необходимости — `make mlflow-export`/`mlflow-backfill`.
- [ ] Для промоушена: `make ml-gate`, артефакт `build/ml-gate/<run>.json`, решение и причина; alias не назван deployment.
- [ ] Shadow 2 + canary 1 (если live-трек), staged-строки не публикуются; live-gate `promote/hold/rollback` записан.
- [ ] Перед публикацией — `make ml-live-check`; после заседания — `forecast_resolve`, `v_ml_live_accuracy`, `error_bp`.
- [ ] Вывод сформулирован честно: значимо/незначимо, что не проверено.
- [ ] Python-тесты, `make all`, `make docs-check`; `make mcp-check` — если схема/гранты/витрина.
- [ ] Протокол приёмки — комментарием в PR; статус роадмапа обновлён.
- [ ] Нет ad-hoc DDL, секретов и изменений Go-кода вне пункта.

## 11. Частые ошибки

| Ошибка | Почему плохо | Как правильно |
|---|---|---|
| Случайный train/test split | Выбирает границу режима, завышает качество | Только walk-forward + purge/embargo |
| Тюнинг по тесту / выбор окна по ошибке | Data snooping, ложная значимость | Предрегистрация, CV внутри train, robustness как проверка |
| Сравнение quick с full | Другой набор фолдов | Quick — только итерации, помечать `quick=true` |
| Заявление паритета без TOST | «Не значимо» ≠ «близко» | DM + TOST с заранее заданным margin (#190) |
| LLM-превосходство без DM | Нет статистической базы | LLM — cross-check; сравнение через DM/encompassing (#189) |
| Запись в ClickHouse из ML-скрипта | Ломает слой и гранты | Только ingest endpoint / Go-импортёр; payload готовит скрипт |
| `champion` alias = deployment | Registry не равен проду | Gate/shadow/canary (#184), rollback; для key-rate alias вообще не применяется |
| Gate на run, которого нет в ClickHouse | Нет фолдов — gate падает или врёт | `ML_LOG=1` при прогоне или `make mlflow-export`/`mlflow-backfill` |
| `promote` без `mlflow-promote` (CPI) | Alias остаётся на старой версии | Gate — решение, alias двигает явная команда |
| Staged/selftest-строки как production | Публикация чужого трека, ложный алерт | Только `stage='production'`; selftest `model='gate-selftest'` исключён |
| Ждать live-точность в MLflow UI | MLflow не хранит `v_ml_live_accuracy` | Смотреть ClickHouse/Grafana; MLflow — runs/registry |
| Игнорировать `ml-live-freshness` | Резолвер сломан, live-ошибка не считается | Проверить `forecast_resolve`, `cbr_key_rate`, `forecast_log` |
| Называть key-rate `champion` deployment | У key-rate нет model registry | Решение gate + `--stage`; registry — отдельный follow-up |
| Полный bottom-up при покрытии <50 % (#76) | 39,3 % весов — это hybrid, не 88-групповой production | Coverage gate: ≥50 %, иначе публиковать отрицательный результат |
| Сравнивать `weekly` и `long` как одну выборку (#76) | Разные периоды/фолды, разная мощность | Gate дважды: primary `weekly`, robustness `long` |
| Gate на разные имена моделей (#76) | `ml-gate` сейчас берёт один `--model` для двух run'ов | Добавить `--champion-model/--challenger-model` или использовать in-script DM/CI |
| Ждать shadow/canary для CPI (#76) | В `forecast_log` нет CPI-метрики и ingest | Paper shadow/отдельный live-слой — follow-up, не production |
| Игнорирование origin/лаг | Look-ahead и ложное преимущество | Origin `M−(h−1)`, as-of правила (#190) |
| Молчаливое удаление выбросов | Меняет измеряемую ошибку | Шок остаётся, ошибка исправляется с записью |
| «Staging проверен» без SSH | Ложная приёмка | Писать «не проверено», оставить владельцу (#183) |

## 12. Ссылки

- Промоушен и live: [2026-10-20-ml-promotion-live-monitoring-design.md](../../../docs/superpowers/specs/2026-10-20-ml-promotion-live-monitoring-design.md), [план](../../../docs/superpowers/plans/2026-10-20-ml-promotion-live-monitoring.md), [GRAFANA.md](../../../docs/GRAFANA.md).
- CPI-компоненты (#76): [спека](../../../docs/superpowers/specs/2026-10-19-cpi-components-public-data-design.md), [план](../../../docs/superpowers/plans/2026-10-19-cpi-components-public-data.md), [Stage 1 отчёт](../../../docs/research/2026-10-19-cpi-components-public-data.md), [Stage 2 отчёт](../../../docs/research/2026-10-19-cpi-industry-mp-features.md).
- Ревью ML-issue: [2026-10-19-ml-issue-review.md](../../../docs/research/2026-10-19-ml-issue-review.md).
- Методология: [stat-ml-models.md](../../../docs/guides/stat-ml-models.md) §2–§7.
- MLOps: [2026-10-18-ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md).
- Контур ставки: [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md) §4/§6; публикации — [KEY_RATE_PUBLICATIONS.md](../../../docs/KEY_RATE_PUBLICATIONS.md).
- Роадмапы: [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md), [ROADMAP_INFRA.md](../../../docs/ROADMAP_INFRA.md) §5, [ROADMAP_KEY_RATE.md](../../../docs/ROADMAP_KEY_RATE.md) фаза 4.
- Общие правила: [AGENTS.md](../../../AGENTS.md); импортёры/DDL — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).
