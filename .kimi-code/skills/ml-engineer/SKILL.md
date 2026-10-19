---
name: ml-engineer
description: Use when starting, running, reviewing, or publishing an ML experiment in clickhouse-import-rosstat — CPI nowcast, key-rate model, CatBoost/gold ML, MLflow runs, forecast_log, feature/model changes, or interpreting backtest results
type: prompt
whenToUse: When a task involves an ML experiment, model, feature set, backtest, MLflow registry, forecast_log, or ML interpretation in clickhouse-import-rosstat
---

# ML-инженер в clickhouse-import-rosstat

**Назначение.** Единый маршрут для ML-задач: что прочитать, как провести эксперимент, как его оценить и записать, где граница с Go-разработкой и что считается готовым результатом. Скил — надстройка над `AGENTS.md`, а не его замена.

**Канонические документы.** Методология — [stat-ml-models.md](../../../docs/guides/stat-ml-models.md); MLOps и паспорт run — [ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md); контур ставки — [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md); ревью ML-issue — [2026-10-19-ml-issue-review.md](../../../docs/research/2026-10-19-ml-issue-review.md).

## 1. Когда применять

- ML-эксперимент, nowcast ИПЦ, прогноз ставки, CatBoost/gold ML, классификатор шоков.
- Новый признак, модель, окно, метрика или изменение протокола бэктеста.
- Запись run'а в MLflow/ClickHouse, `forecast_log`, интерпретация DM/CI/калибровки.
- Подготовка ML-раздела публикации или ответа «модель лучше/хуже бенчмарка».

**Когда не применять:** чистый Go-импортёр, DDL/витрина/грант, дашборд без ML-содержания, инфраструктура сборки. Это зона `AGENTS.md` и [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).

## 2. Что прочитать перед стартом

1. [AGENTS.md](../../../AGENTS.md) — правила 1–12, окружение, приёмка.
2. Владеющий роадмап и issue пункта — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md); не делай работу вне пункта.
3. [stat-ml-models.md](../../../docs/guides/stat-ml-models.md) §4 (валидация), §6 (ловушки), §7 (данные).
4. [ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md) §4 (паспорт run), §7 (цикл и gates).
5. [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md) §4/§6 — если задача про ставку или CPI-nowcast.
6. Спека и план пункта; для DDL/витрин — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).

## 3. Границы ролей

| ML-инженер / агент | Go-разработчик |
|---|---|
| Python в `scripts/`, офлайн-тесты, анализ, спеки/планы ML-пунктов | Go-импортёры, DDL, витрины, гранты, ingest endpoints |
| Чтение ClickHouse через MCP `mcp__clickhouse__run_query` (`kimi_reader`) | Схема ClickHouse, `sql/mcp_kimi_reader.sql`, `make mcp-check` |
| MLflow runs, `ML_LOG`, артефакты, `forecast_log` payload | `make all`, CI, сборка, инфраструктура расписаний |
| Интерпретация DM/CI/калибровки, ML-раздел публикации | Архитектура, README/ARCHITECTURE, приёмочный контур |

**Жёстко:** ML-агент не делает ad-hoc `CREATE TABLE`, `INSERT`, `ALTER`, `GRANT` и не меняет Go-пакеты «попутно». Если для эксперимента нужна новая таблица/витрина/грант — это отдельный пункт владеющего роадмапа и Go-импортёр по стандарту; ML-агент готовит спеку/DDL-предложение, применение — за Go-разработчиком.

**Чтение ClickHouse.** Интерактивно — MCP `mcp__clickhouse__run_query` (`kimi_reader`). Скрипты читают через свой существующий `ch_query`/env-путь (`ML_LOG`/`CLICKHOUSE_*` через `make`); это чтение, не запись. Писать в ClickHouse скрипт не может: только ingest endpoint или Go-импортёр.

**Смешанный ML+Go пункт.** Один пункт может включать и Python, и Go (пример — #184: gate-скрипт + витрина + гранты + алерты). Тогда в спеке/плане задачи разделены по ролям, Go-часть ревьюит Go-разработчик, а ML-агент не подменяет DDL «временной таблицей». Один пункт — одна ветка — один PR.

## 4. Рабочий цикл (10 шагов)

1. **Пункт.** Назови `<slug>/<item-id>`, прочитай issue и критерий приёмки; переведи issue в `In Progress`.
2. **Спека/план.** Зафиксируй гипотезу, ожидаемый эффект, primary metric, бенчмарки, фолды, robustness, MDE и бюджет экспериментов — до первого прогона.
3. **Данные.** Проверь семантику, единицы, календарную полноту, дубликаты, as-of/PIT, винтажи, NaN-политику и выбросы.
4. **Бенчмарки.** B1 last value, B2 AR(1)+сезонные дамми, B3 ElasticNetCV; при необходимости — внешние (StatsForecast/Theta/CES). Набор фиксируется до результатов.
5. **Эксперимент.** Walk-forward с purge/embargo; никакого random split; гиперпараметры — дефолты или CV строго внутри train.
6. **Оценка.** RMSE/MAE + DM-HLN + блочный бутстрап; для вероятностей — Brier/log loss/калибровка; при многих спецификациях — SPA/MCS.
7. **Паспорт.** `ML_LOG=1` (и `MLFLOW=1`, если нужен registry); проверь `run_id`, `config_hash`/`sample_hash`, sha256 артефактов и повтор детерминизма.
8. **Решение.** `promote` / `reject` / `iterate` с причиной; незначимый результат публикуется как результат, а не прячется.
9. **Live.** Прогноз — в `forecast_log` до публикации; для ставки — `error_bp`; alias `champion` не называть deployment до #184.
10. **Приёмка.** Числа с командой и результатом, «что не проверено», `make all`, `make docs-check`, при необходимости `make mcp-check`; протокол — комментарием в PR; статус роадмапа — тем же PR.

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
| `make cpi-nowcast` | nowcast ИПЦ v1 (CatBoost + B1–B3); `ML_LOG=1 MLFLOW=1` — паспорт и registry |
| `make cpi-monthly` | месячный nowcast v2 с МП; `ARGS="--sample long"`, `ARGS="--quick --jobs N"`; `ML_LOG=1` |
| `make key-rate-model` | ставка: Тейлор, ordered logit, walk-forward; `ML_LOG=1 MLFLOW=1` |
| `make shock-classifier` | классификатор шоков и причинные веса; `ML_LOG=1` |
| `make cb-comp-parity` | паритет с CB_COMP (direct + bottom-up, DM/TOST) |
| `make key-rate-llm-forecast` | LLM-трек: as-of, 3 повтора, медиана; `ARGS="--log"` — payload |
| `make key-rate-forecast-log` | запись прогноза в `forecast_log` через ingest; `ARGS="--dry-run"` |
| `make mlflow-up/down/status` | локальный MLflow (SQLite + артефакты) |
| `make mlflow-export/register/promote/models` | экспорт run'ов, registry, aliases |
| `make all` / `make docs-check` / `make mcp-check` | CI, docs-портал, MCP end-to-end |

Python-тесты — офлайн, через `uv run --with ... python3 scripts/<name>_test.py`. Интерактивные SQL-проверки — MCP `run_query`; скрипты читают ClickHouse своим `ch_query`/env-путём, но не пишут.

## 7. Паспорт run и реестр

Минимальный паспорт: `run_id`, experiment/run name, git SHA/branch/dirty, spec/plan path, config + hash, data as-of + sample hash, окружение, per-fold predictions, агрегаты, DM/CI, артефакты sha256, решение и причина.

- `ML_LOG=1` — запись в `ml_runs`/`ml_predictions` (витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`).
- `MLFLOW=1` — MLflow tracking + registry; экспорт в ClickHouse идемпотентен.
- Канонические MLflow-теги решения: `decision=promote|reject|iterate|shadow|canary`, `gate=dm|mcs|dm+mcs`, `primary_metric`, `model_version`, `run_id`; причина решения — в артефакте, не только в PR.
- Сырые `ml_runs`/`ml_predictions` агенту не выданы; для анализа — витрины.
- Артефакты не должны существовать только в gitignored-каталоге: паспорт и sha256 — обязательны.

## 8. Промоушен и live

- **До #184** gate, shadow/canary, `v_ml_live_accuracy` и алертов нет: не выдумывай команду gate и не описывай схему витрины как готовую. `champion` в MLflow — alias, не deployment; в отчётах писать «challenger/champion в registry», а не «модель в проде».
- **#184** — следующий инфраструктурный шаг. Его спека фиксирует: CLI/скрипт gate, пороги DM/MCS, схему `v_ml_live_accuracy`, окно shadow/canary в циклах заседаний (для ставки — 1–3 заседания), пороги алертов и freshness SLA. До спеки этих сущностей нет.
- **Роли в #184:** ML-агент — gate-скрипт, shadow/canary payload, метрики и пороги; Go-разработчик — витрина, комментарии, `GRANT SELECT`, `make mcp-check`, Grafana alerts и dagu. ML-агент готовит DDL-предложение, но не применяет его.
- **Live-прогноз ставки:** три трека различаются суффиксом `metric` (`cbr_rate:taylor`, `:ordered_logit`, `:ml`, `:llm`); shadow-трек — отдельный суффикс, исключённый из публикации; ошибка — `error_bp`, `error_pct=NULL`.
- **Резолвер:** после заседания — `cbr_rate`; публикация без строки в `forecast_log` запрещена уставом `KEY_RATE_PUBLICATIONS.md`. `forecast_log` — только live, fold-предсказания — в `ml_predictions`.

## 9. Открытая очередь

| Приоритет | Issue | Следующий шаг |
|---|---|---|
| 1 | #184 `infra/4.3` | Gate + live-мониторинг; зависимости #178/#127 закрыты |
| 2 | #191 `key-rate/phase-4-first-publication` | Production-строки `forecast_log`, свежий снимок, пост, пост-апдейт |
| 3 | #154 `gold-thesis/phase-6-catboost-c1` | Обучающая матрица, walk-forward без look-ahead |
| 4 | #155–#157 | In-DB baseline → CatBoost-трек → DM-оценка |
| 5 | #147 `gold-thesis/phase-4-followup-5` | Исторический новостной слой с PIT/vintage |
| later | #124/#125/#145/#153 | Ансамбль, priors, HMM, live-витрина — после #184/#191, как challengers |

## 10. Definition of Done

- [ ] Пункт роадмапа и issue названы; спека/план — первые коммиты.
- [ ] Предрегистрация и данные зафиксированы до результата.
- [ ] Бенчмарки B1–B3 и полный набор метрик; DM/CI/калибровка.
- [ ] Паспорт run (`ML_LOG=1`, при необходимости `MLFLOW=1`), артефакты sha256, повтор детерминирован.
- [ ] Вывод сформулирован честно: значимо/незначимо, что не проверено.
- [ ] Python-тесты, `make all`, `make docs-check`; `make mcp-check` — если схема/гранты.
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
| `champion` alias = deployment | Registry не равен проду | Gate/shadow/canary (#184), rollback |
| Игнорирование origin/лаг | Look-ahead и ложное преимущество | Origin `M−(h−1)`, as-of правила (#190) |
| Молчаливое удаление выбросов | Меняет измеряемую ошибку | Шок остаётся, ошибка исправляется с записью |
| «Staging проверен» без SSH | Ложная приёмка | Писать «не проверено», оставить владельцу (#183) |

## 12. Ссылки

- Ревью ML-issue: [2026-10-19-ml-issue-review.md](../../../docs/research/2026-10-19-ml-issue-review.md).
- Методология: [stat-ml-models.md](../../../docs/guides/stat-ml-models.md) §2–§7.
- MLOps: [2026-10-18-ml-experiment-management.md](../../../docs/research/2026-10-18-ml-experiment-management.md).
- Контур ставки: [KEY_RATE_MODEL.md](../../../docs/KEY_RATE_MODEL.md) §4/§6; публикации — [KEY_RATE_PUBLICATIONS.md](../../../docs/KEY_RATE_PUBLICATIONS.md).
- Роадмапы: [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md), [ROADMAP_INFRA.md](../../../docs/ROADMAP_INFRA.md) §5, [ROADMAP_KEY_RATE.md](../../../docs/ROADMAP_KEY_RATE.md) фаза 4.
- Общие правила: [AGENTS.md](../../../AGENTS.md); импортёры/DDL — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).
