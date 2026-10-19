# Управление ML-экспериментами и непрерывное улучшение точности прогноза — инфраструктурный ресерч (2026-10-18)

> **Объект:** PR #177 (`key-rate/phase-4-ml-v2`, `scripts/cpi_monthly.py`), `scripts/cpi_nowcast.py`, `scripts/key_rate_model.py`, `scripts/eval_utils.py`, `forecast_log`/`v_forecast_accuracy`, `dagu`, ClickHouse + Grafana + MCP.
> **Задача:** зафиксировать лучшие практики управления экспериментами и готовый инструментарий, который обеспечивает сходимость (а не бесконечный перебор) задачи непрерывного улучшения точности прогноза.
> **Метод:** аудит репозитория (PR #177, спека/план, `docs/guides/stat-ml-models.md`, `docs/ROADMAP_KEY_RATE.md` §6, `ARCHITECTURE.md` §6.2/§6.3) + официальные документы инструментов (MLflow, Aim, ClearML, DVC, Optuna, Nixtla, sktime, Darts, Evidently, NannyML, `arch`) + устойчивая литература по MLOps и оценке прогнозов.
> **Границы:** это research-документ, не реализация. Версии/даты релизов проверены на 2026-10-18; self-hosted-стек не разворачивался. Цены SaaS не сравнивались.

---

## 1. Короткий вывод

PR #177 — методологически зрелый эксперимент и одновременно инфраструктурно «одноразовый»:

- **Сильно:** спека и план до кода; фиксированные признаки, бенчмарки, фолды и метрики; walk-forward с purge/embargo; DM-HLN и блочный бутстрап; байт-в-байт детерминированные артефакты; результат опубликован как есть, без тюнинга.
- **Слабо:** у прогона нет машиночитаемого паспорта; артефакты gitignored и перезаписываются (`clear_stale_artifacts`); история — только git/PR; нет реестра моделей и champion/challenger; live-прогноз не попадает в `forecast_log`; нет автоматического мониторинга ошибки и дрейфа; сравнение моделей делается вручную по тексту PR; окружение `uv run --with` не зафиксировано; нет отпечатка данных, поэтому изменение ClickHouse молча меняет результат.

**Рекомендация — три плоскости, а не один «ML-фреймворк»:**

1. **Control plane — MLflow** (self-hosted; dev — SQLite + локальные артефакты, staging — Postgres + MinIO/S3): runs, параметры, метрики, артефакты, model registry с алиасами `champion`/`challenger`.
2. **Analytics plane — ClickHouse + Grafana**: реплика run-метрик и fold-предсказаний (`ml_runs`, `ml_predictions`, `ml_models`), витрины `v_ml_leaderboard`, `v_ml_error_by_fold`, `v_ml_live_accuracy`; `forecast_log` остаётся журналом live-прогнозов.
3. **Evaluation/monitoring plane — `eval_utils.py` + Nixtla + `arch` + Evidently**: DM-HLN и блочный бутстрап остаются; добавляются MASE/масштабно-инвариантные метрики, сильные бенчмарки (`StatsForecast`), MCS/SPA при росте числа спецификаций, дрейф и мониторинг отложенного факта.

**Сходимость обеспечивает не инструмент, а протокол:** предрегистрация гипотезы и метрик → фиксированные фолды → значимость и устойчивость → gate на промоушен → shadow/canary → мониторинг → правило остановки (бюджет экспериментов, MCS, «плато»). Инструменты делают этот цикл наблюдаемым и воспроизводимым, но не заменяют решение.

**Что делать первым:** пункт `infra/4.1` «Реестр экспериментов и контур промоушена моделей», MVP из трёх срезов (см. §9). До его появления — этот документ и ручной чек-лист паспорта run.

---

## 2. Что уже правильно в PR #177 (не ломать)

| Практика | Как в PR #177 | Почему это ценно |
|---|---|---|
| Спека/план до кода | `docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md`, план | Гипотеза и протокол зафиксированы до результата |
| Фиксированные признаки | 7 признаков МП + базовые, без отбора по результату | Нет data snooping на этапе спецификации |
| Фиксированные бенчмарки | B1/B2/B3 до результатов | Модель обязана побить лестницу, а не «обойтись без неё» |
| Одинаковые фолды | «с МП» и «без МП» на одних фолдах | Парное сравнение корректно |
| Walk-forward + purge/embargo | 24/48 + embargo 1 | Защита от утечки и режимного дрейфа |
| DM-HLN + блочный бутстрап | 9 primary-пар + интервалы | Не только точка, но и неопределённость |
| Детерминизм | Два прогона байт-в-байт, sha256 | Воспроизводимость результата |
| Публикация as-is | «Ни одна разница не значима на 5%» | Нет подгонки под желаемый вывод |
| Робастность | start 48, embargo 1, последние 12 фолдов, «без 2022» | Проверка устойчивости, а не выбор удобного окна |

Вывод: **методологический каркас уже соответствует лучшим практикам**. Инфраструктура должна его сохранить, а не заменить.

---

## 3. Разрыв: as-is → target

| Аспект | Сейчас (PR #177 и родственные скрипты) | Target |
|---|---|---|
| Идентичность прогона | ветка/PR, имя файла | `run_id` (UUID) + `experiment_id` + `run_name` |
| Конфигурация | argparse + литералы в коде | YAML/JSON + hash; все дефолты в паспорте |
| Данные | живой ClickHouse, `as-of` в коде | SQL-запросы + `data_asof` + `sample_hash` (max date, row count, schema hash) |
| Окружение | `uv run --with` с плавающими версиями | lock/`uv.lock` + freeze Python/пакетов/OS |
| Метрики | текст отчёта + CSV | per-fold таблица + агрегаты + DM p + bootstrap CI |
| Артефакты | gitignored, перезаписываются | content-addressed (SHA256), immutable, ссылка из run |
| Сравнение | вручную по тексту PR | leaderboard/UI + SQL |
| Реестр моделей | нет | версии, алиасы `champion`/`challenger`, model card |
| Live-прогноз | JSON-файл | `forecast_log` + `v_ml_live_accuracy` |
| Мониторинг | нет | ошибка по времени, дрейф, алерты, свежесть данных |
| Решение | текст в PR/роадмапе | promotion gate + запись решения |
| История | git-ветки/PR | реестр run'ов, неудачные run'ы тоже сохраняются |

Ключевая проблема: **неудачный или промежуточный прогон исчезает**. А именно они нужны, чтобы понимать, какие гипотезы уже проверены и почему отвергнуты.

---

## 4. Best practices: паспорт run

Минимальный набор полей (в MLflow — params/tags/metrics/artifacts; в ClickHouse — колонки):

**Идентичность и lineage**
- `run_id`, `experiment_id`, `run_name`, `parent_run_id`
- `spec_path`, `spec_sha256`, `plan_path`, `pr_number`
- `git_sha`, `git_branch`, `git_dirty` (0/1)
- `author`, `created_at`, `status`, `duration_sec`

**Конфигурация**
- все CLI-аргументы и дефолты, `config_json`, `config_hash`
- seed, версия протокола/спеки

**Данные**
- SQL-запросы/источники, `data_asof`, `max_date`, `row_count`, `sample_hash`
- schema hash / версия DDL, список пропущенных месяцев/фолдов

**Окружение**
- Python, версии `catboost`, `scikit-learn`, `pandas`, `statsmodels`, `scipy`
- OS/CPU, `uv` lock hash

**Оценка**
- список фолдов, per-fold `predicted`/`actual`/`error`
- агрегаты RMSE/MAE/MASE, DM-HLN p, bootstrap CI
- `convergence_warnings`, число вырожденных фолдов, «недосходы»
- robustness-блоки (start 24/48, embargo, last 12, ex-2022)

**Артефакты**
- `report.md`, `predictions.csv`, `forecast.json`, модель/веса
- путь, SHA256, размер

**Решение**
- гипотеза, ожидаемый эффект, результат, решение (`promote`/`reject`/`iterate`), причина
- кто и когда принял; ссылка на PR/issue

Правило: **чего нет в паспорте run — того не было**. Отчёт можно переписать, CSV — потерять, PR — закрыть; паспорт должен остаться.

Пример паспорта (JSON, сокращённо):

```json
{
  "run_id": "0f3c2a10-7d1e-4c9b-9f6a-2b8e5d4c1a77",
  "experiment": "cpi_monthly_mp",
  "run_name": "catboost_bottomup_mp__start24__embargo0",
  "status": "finished",
  "git": {"sha": "33701db", "branch": "feat/key-rate-ml-v2", "dirty": false},
  "spec": {"path": "docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md", "sha256": "…"},
  "config": {"start": 24, "embargo_months": 0, "models": ["catboost_bottomup_mp"], "seed": 20261016},
  "config_hash": "sha256:…",
  "data": {"as_of": "2026-10-18", "sample_months": 155, "max_target_date": "2026-08-31", "sample_hash": "sha256:…"},
  "env": {"python": "3.12.11", "catboost": "1.2.x", "sklearn": "…", "pandas": "…", "statsmodels": "…"},
  "metrics": {"rmse_pp": 0.343, "mae_pp": 0.257, "mase_vs_b1": 0.703, "dm_p_vs_b3": 0.78},
  "artifacts": [
    {"name": "cpi_monthly_report.md", "sha256": "761248ba…", "bytes": 41234},
    {"name": "cpi_monthly_predictions.csv", "sha256": "01a66935…", "bytes": 182345}
  ],
  "decision": {"verdict": "iterate", "reason": "нет значимого выигрыша над B3", "reviewed_at": "2026-10-18"}
}
```

---

## 5. Ландшафт готового инструментария

### 5.1 Эксперименты и реестр моделей

| Инструмент | Лицензия/развёртывание | Что даёт | Ограничения для нас | Вердикт |
|---|---|---|---|---|
| **MLflow 3.x** | Apache 2.0, self-hosted | Runs, params/metrics/artifacts, UI, REST, autolog, model registry, aliases `champion`/`challenger` | Нет официального backend-store для ClickHouse; нужен Postgres/SQLite + S3/MinIO для артефактов | **Рекомендую** |
| Aim 3.x | Apache 2.0, self-hosted | Лёгкий трекер, быстрый UI, Python SDK | Нет полноценного model registry; последний релиз на PyPI 3.29.1 (2025-05-08), релизный цикл замедлился | Лёгкая альтернатива |
| ClearML | Apache 2.0, self-hosted | Tracking + orchestration + data + registry | Тяжелее, пересекается с dagu, выше порог входа | Не сейчас |
| W&B / Comet | Commercial/SaaS | Лучший UX, zero-ops | Внешняя зависимость, стоимость, данные уходят | Если разрешён SaaS |
| DVC | Apache 2.0 | Версионирование данных/артефактов, `dvc exp` | Данные в ClickHouse, а не в файлах; полезен только для артефактов/фикстур | Опционально |
| Hydra / OmegaConf | MIT | Конфиги, multirun, sweeper | Рефакторинг argparse; нужен при sweep'ах | Опционально |
| Optuna | MIT | HPO, pruning, RDB storage, dashboard, интеграции | Риск переобучения на коротких рядах; только внутри train | Ограниченно |

Версии на 2026-10-18: `mlflow` 3.17.1 (2026-10-13), `aim` 3.29.1 (2025-05-08), `clearml` 2.1.12 (2026-08-19), `dvc` 3.67.1, `optuna` 5.0.0 (2026-09-07).

### 5.2 Бэктестинг и оценка прогнозов

| Инструмент | Что даёт | Как использовать |
|---|---|---|
| **Nixtla `utilsforecast`** | `evaluate`, RMSE/MAE/MASE/quantile losses, weighted aggregation | Заменить самописные метрики на стандартные, оставить `eval_utils` для DM/бутстрапа |
| **Nixtla `StatsForecast`** | AutoARIMA, AutoETS, Theta, CES, Naive/SeasonalNaive, кросс-валидация | Усилить лестницу B1–B3, не переписывая текущий код |
| Nixtla `MLForecast` / `NeuralForecast` | ML/нейросетевые модели с лагами | Когда появятся сотни рядов/горизонтов |
| `sktime` | Единый API, backtesting, model selection | Кросс-проверка walk-forward |
| `darts` | `historical_forecasts`, метрики | Кросс-проверка/прототипы |
| `arch` | SPA, MCS | Защита от data snooping при росте числа спецификаций |
| `statsmodels` | DM-HLN, ARIMA, OLS | Уже используется |
| `forecast` (R) | Эталонные `ets`/`auto.arima` | Не нужен в Python-контуре |

Версии на 2026-10-18: `statsforecast` 2.1.1 (2026-07-16), `mlforecast` 1.1.0 (2026-07-10), `neuralforecast` 3.3.0, `utilsforecast` 0.2.17, `sktime` 1.2.0 (2026-09-22), `darts` 0.48.1 (2026-10-16).

**AutoML и автоматический отбор моделей.** Готовые генераторы challenger'ов, но не замена протоколу:

- `AutoGluon-TimeSeries` — автоматический стек/ансамбль с time-series CV; полезен, когда нужно быстро получить сильную базу, но тяжёлый и плохо объяснимый;
- `NeuralForecast` Auto-модели (`AutoNHITS`, `AutoTFT` и др.) — автоматический подбор гиперпараметров внутри train;
- `StatsForecast`/`MLForecast` — быстрые автоматические бенчмарки (AutoARIMA, AutoETS, Theta, CES), которые стоит добавить в лестницу B1–B3;
- `FLAML`/`AutoSklearn` — не time-series-специфичны; для наших рядов менее полезны.

Правило: AutoML может **предложить** challenger'а, но промоушен всё равно проходит через DM/MCS, robustness-окна и gate. Иначе автоматический перебор сам становится источником data snooping.

### 5.3 Мониторинг и дрейф

| Инструмент | Что даёт | Ограничения |
|---|---|---|
| **Evidently** | 100+ метрик, drift, regression performance, self-hosted platform | Для time series нужны собственные окна/фичи |
| NannyML | Оценка performance без меток (CBPE/DLE), drift | Последний релиз 0.13.1 (2025-07-12); проверять на наших данных |
| Grafana + ClickHouse | Полный контроль, алерты, MCP-доступ | Нужны свои таблицы/витрины |
| WhyLabs / Arize / Fiddler | SaaS observability | Внешняя зависимость |

Версии на 2026-10-18: `evidently` 0.7.23 (2026-09-11), `nannyml` 0.13.1 (2025-07-12).

### 5.4 Оркестрация и CI

| Инструмент | Вердикт |
|---|---|
| **dagu** (уже есть) | Оставить для расписаний; добавить шаги retrain/eval/resolve |
| Prefect / Dagster | Не сейчас: лучше lineage/UI, но новый сервис |
| Airflow / Kubeflow | Избыточно для 155 строк и 3 скриптов |
| GitHub Actions | Unit-тесты — да; полный бэктест 46–64 мин — nightly/self-hosted |
| CML | Опционально: комментарий с метриками в PR |

---

## 6. Рекомендуемая архитектура

```
┌─────────────────────────────────────────────────────────────┐
│ CONTROL PLANE: MLflow                                       │
│  dev: SQLite + build/mlruns                                 │
│  staging: Postgres + MinIO/S3                               │
│  experiments: cpi_monthly, cpi_nowcast, key_rate            │
│  registry aliases: champion / challenger                    │
└──────────────▲───────────────────────────────┬──────────────┘
               │ mlflow.log_*                  │ export (REST/CSV)
┌──────────────┴───────────────┐   ┌───────────▼──────────────┐
│ EXPERIMENT JOBS (Python)     │   │ ANALYTICS PLANE:          │
│  uv + config YAML + seed     │   │ ClickHouse                │
│  eval_utils + utilsforecast  │   │  ml_runs                  │
│  cpi_monthly/cpi_nowcast/    │   │  ml_predictions           │
│  key_rate_model              │   │  ml_models                │
└──────────────▲───────────────┘   │  forecast_log (live)      │
               │                   │  v_ml_leaderboard         │
        dagu / make                │  v_ml_error_by_fold       │
                                   │  v_ml_live_accuracy       │
                                   └───────────┬──────────────┘
                                               │
                                   ┌───────────▼──────────────┐
                                   │ Grafana + MCP (kimi_reader)│
                                   └──────────────────────────┘
```

### 6.1 Control plane: MLflow

- **Dev:** `mlflow server --backend-store-uri sqlite:///build/mlflow.db --default-artifact-root build/mlruns` — один процесс, без новых сервисов.
- **Staging:** docker-compose: `mlflow`, `postgres:16`, `minio`; артефакты в S3-совместимом хранилище; секреты — только через env, не в params/tags.
- **Почему MLflow:** стандарт, UI сравнения, REST API, model registry с алиасами, autolog, CatBoost flavor, артефакты. Нет нативного ClickHouse backend — это осознанная цена; ClickHouse остаётся аналитической плоскостью.
- **Почему не только ClickHouse:** собственная таблица run'ов — это ~300 строк кода и отсутствие UI/реестра; для «готового инструментария» MLflow дешевле.
- **Fallback:** перед ML-прогоном `mlflow_preflight.py` проверяет API; если MLflow недоступен, скрипт явно спрашивает, запускать ли без лога (в не-TTY нужен `MLFLOW_ALLOW_NO_LOG=1`). При разрешённом запуске скрипт пишет локальный `run.json` + артефакты и не падает; существующие ClickHouse-прогоны переносятся `make mlflow-backfill`.

### 6.2 Analytics plane: ClickHouse

Если MLflow — system of record, в ClickHouse нужна **реплика** для Grafana/MCP:

```sql
CREATE TABLE IF NOT EXISTS ml_runs (
    run_id UUID,
    experiment LowCardinality(String),
    run_name String,
    status LowCardinality(String),
    git_sha String,
    git_branch String,
    git_dirty UInt8,
    config_hash String,
    data_asof Date,
    sample_hash String,
    spec_path String,
    metrics_json String,
    params_json String,
    artifact_uri String,
    started_at DateTime,
    finished_at DateTime,
    duration_sec Float64,
    notes String
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id);

CREATE TABLE IF NOT EXISTS ml_predictions (
    run_id UUID,
    experiment LowCardinality(String),
    model LowCardinality(String),
    fold UInt16,
    target_date Date,
    predicted Float64,
    actual Float64,
    error Float64,
    is_resolved UInt8
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id, model, target_date);

CREATE TABLE IF NOT EXISTS ml_models (
    model_name LowCardinality(String),
    version UInt32,
    run_id UUID,
    alias LowCardinality(String),
    stage LowCardinality(String),
    metric_name LowCardinality(String),
    metric_value Float64,
    promoted_at DateTime,
    notes String
) ENGINE = ReplacingMergeTree
ORDER BY (model_name, version);
```

Витрины:
- `v_ml_leaderboard` — последний run на модель, агрегированные метрики;
- `v_ml_error_by_fold` — fold-level ошибка;
- `v_ml_live_accuracy` — live-прогноз из `forecast_log` против факта;
- `v_ml_champion_challenger` — текущий champion и challengers.

**Важно:** `forecast_log` не превращать в свалку бэктестов. Это журнал live-прогнозов; fold-предсказания — в `ml_predictions`.

### 6.3 Evaluation plane

- `eval_utils.py` остаётся каноническим для DM-HLN и блочного бутстрапа.
- `utilsforecast.evaluation.evaluate` — стандартные метрики (RMSE/MAE/MASE/quantile loss).
- `StatsForecast` — дополнительные бенчмарки (AutoARIMA, AutoETS, Theta, CES), чтобы лестница не была слабее внешней литературы.
- `arch` — SPA/MCS, когда спецификаций становится больше 5–7.
- `Evidently` — drift и regression performance; `NannyML` — только как эксперимент для отложенного факта, с проверкой на realized error.

### 6.4 Orchestration

- `dagu` уже есть: добавить шаги `ml_retrain`, `ml_backtest`, `ml_resolve_actuals`, `ml_export`.
- Полный бэктест 46–64 мин — не в каждом PR; nightly или по кнопке.
- `make` остаётся единственной точкой входа; MLflow включён по умолчанию (`MLFLOW=0` — осознанно), перед запуском `mlflow_preflight.py` проверяет API и спрашивает при недоступности; ClickHouse-паспорт — `ML_LOG=1`.

---

## 7. Контур непрерывного улучшения и «сходимость»

### 7.1 Цикл

1. **Гипотеза** — что и почему должно улучшиться; ожидаемый эффект; стоимость.
2. **Предрегистрация** — primary metric, бенчмарки, фолды, robustness-окна, minimum detectable effect, бюджет run'ов.
3. **Прогон** — фиксированные данные/фолды, паспорт run, артефакты.
4. **Оценка** — DM-HLN, bootstrap CI, MASE, bias/calibration, convergence warnings.
5. **Сравнение** — leaderboard, MCS/SPA, устойчивость по окнам.
6. **Решение** — promote / reject / iterate; запись причины.
7. **Промоушен** — `champion` alias, shadow 1–3 цикла, canary, rollback.
8. **Мониторинг** — live-ошибка, дрейф, свежесть данных, алерты.
9. **Разбор** — post-mortem, обновление backlog гипотез.

### 7.2 Gates

| Gate | Условие |
|---|---|
| Воспроизводимость | Тот же конфиг → те же метрики (допуск); артефакты sha256 |
| Валидность | Purge/embargo/as-of тесты зелёные; нет утечки |
| Значимость | DM-HLN p < 0.05 на общих фолдах **или** challenger в 90% MCS |
| Устойчивость | Знак эффекта сохраняется на start 24/48, embargo 0/1, last 12, ex-2022 |
| Экономика | Улучшение primary metric ≥ заранее заданного порога (например, 2–5%) |
| Операционность | Время/память/зависимости не хуже приемлемого |
| Мониторинг | Есть live-метрика, алерт и план rollback |

### 7.3 Правило остановки (это и есть «сходимость»)

- **Бюджет:** не более N экспериментов на квартал (например, 3) на один контур.
- **Без peeking:** фиксированный test-набор; sequential/group-sequential тесты, если нужен ранний останов.
- **MCS:** остановиться, когда champion в Model Confidence Set и ни один challenger не значимо лучше.
- **Плато:** если N циклов подряд нет значимого улучшения — объявить плато и переключиться на данные/признаки/постановку, а не на перебор гиперпараметров.
- **Holdout:** отдельный год/период, который не используется для выбора; открывается один раз в год.
- **Архив:** каждый reject сохраняется с причиной; повторный запуск — только с новой гипотезой.

### 7.4 Специфика контуров

- **CPI monthly nowcast:** primary — RMSE (п.п. м/м), MASE vs B1; DM-HLN; robustness 24/48, embargo, last 12, ex-2022; при росте моделей — MCS/SPA.
- **Key rate:** RMSE в б.п. (не `error_pct`), hit rate/QWK, Brier/log loss, calibration; `forecast_log.metric='cbr_rate'` + резолвер `cbr_rate` (уже в роадмапе).
- **Gold:** `xau_q_avg`, `error_pct`, `v_forecast_accuracy`; NAV не резолвится.

---

## 8. Anti-patterns

- Строить собственный трекер, если MLflow закрывает 90% задач.
- Тащить MLflow + DVC + Hydra + Optuna + Evidently + ClearML одновременно.
- Делать MLflow хранилищем сырых данных или `forecast_log` — свалкой бэктестов.
- Выбирать модель по тесту; крутить гиперпараметры на тех же фолдах, по которым потом сравниваете.
- Игнорировать multiple comparisons: 20 спецификаций → нужен SPA/MCS.
- Промоутить модель без deploy path: alias в registry — не deployment.
- Логировать секреты/токены в params/tags.
- Внедрять Kubeflow/Feast/Airflow для 155 наблюдений и трёх скриптов.
- Хранить артефакты только в gitignored-каталоге и перезаписывать их.
- Считать, что «инструмент сам обеспечит улучшение»: без протокола и правила остановки будет бесконечный тюнинг.

---

## 9. MVP: три среза

### Срез 1 — паспорт run + ClickHouse-реплика (1–2 дня)
- `scripts/ml_experiment.py`: `start_run` (git SHA, config hash, data fingerprint, env), `log_fold_metrics`, `log_summary`, `log_artifacts`, `log_predictions`.
- DDL `ml_runs`/`ml_predictions`/`ml_models` + витрины; `ARCHITECTURE.md` §6.2/§6.3.
- `make cpi-monthly` пишет паспорт и fold-предсказания; артефакты — sha256.
- Приёмка: прогон даёт `run_id`; повтор с тем же конфигом — те же метрики; `v_ml_leaderboard` показывает строку.

### Срез 2 — MLflow + registry (2–3 дня)
- `mlflow` dev-сервер (SQLite + local artifacts), `make mlflow-up/down`.
- Интеграция `mlflow.log_*`; `mlflow.catboost.log_model`; aliases `champion`/`challenger`.
- `scripts/mlflow_export.py` — экспорт run'ов в ClickHouse (идемпотентно).
- Приёмка: run виден в MLflow UI; экспорт даёт строки в ClickHouse; `make all` зелёный.

### Срез 3 — промоушен и live-мониторинг (2–3 дня)
- Протокол gate — отдельным guide или разделом этого документа.
- `forecast_log` для CPI/rate + резолвер; `v_ml_live_accuracy`; Grafana `ml-experiments`.
- Алерты: rolling error, drift, missing forecast, data freshness.
- Приёмка: live-прогноз записан до публикации; факт резолвится; панель показывает ошибку; `make mcp-check` зелёный.

**Критерий приёмки пункта:** один и тот же эксперимент можно (а) воспроизвести одной командой, (б) найти по `run_id` в MLflow и ClickHouse, (в) сравнить с champion на общих фолдах, (г) промоутить только через gate, (д) увидеть live-ошибку в Grafana.

---

## 10. Что это даёт контуру и чего не даёт

**Даёт:**
- История всех прогонов, включая неудачные; сравнение на одинаковых фолдах; меньше ручного копирования чисел в PR/роадмап.
- Воспроизводимость: `run_id` → git SHA, конфиг, данные, окружение, артефакты.
- Промоушен по правилу, а не по впечатлению; откат к предыдущему champion.
- Live-мониторинг ошибки и дрейфа; раннее обнаружение деградации.
- MCP-агенту — SQL-витрины с историей экспериментов и точностью.

**Не даёт:**
- Автоматического улучшения точности: данные, признаки и постановка важнее трекера.
- Замены статистической дисциплины: DM/MCS/устойчивость остаются обязательными.
- Бесплатной инфраструктуры: MLflow + Postgres/MinIO — это сервисы, которые надо сопровождать.
- Ответа на вопрос «какая модель правильная»: только на «какая модель значимо лучше на зафиксированном протоколе».

---

## 11. Источники

- MLflow Tracking: <https://mlflow.org/docs/latest/ml/tracking/>; Backend/Artifact Store: <https://mlflow.org/docs/latest/self-hosting/architecture/backend-store/>; Model Registry: <https://mlflow.org/docs/latest/ml/model-registry/>.
- Aim: <https://aimstack.io/>; PyPI `aim` 3.29.1 (2025-05-08).
- ClearML: <https://clear.ml/>; PyPI `clearml` 2.1.12 (2026-08-19).
- DVC Experiments: <https://doc.dvc.org/start/experiments/>.
- Optuna: <https://optuna.readthedocs.io/>; PyPI `optuna` 5.0.0 (2026-09-07).
- Nixtla UtilsForecast Evaluation: <https://nixtlaverse.nixtla.io/utilsforecast/evaluation.html>; StatsForecast/MLForecast/NeuralForecast.
- sktime: <https://www.sktime.net/>; Darts: <https://unit8co.github.io/darts/>.
- Evidently: <https://docs.evidentlyai.com/>; PyPI `evidently` 0.7.23 (2026-09-11).
- NannyML: <https://nannyml.readthedocs.io/>; PyPI `nannyml` 0.13.1 (2025-07-12).
- `arch` (SPA/MCS): <https://bashtage.github.io/arch/>.
- `statsmodels` DM-HLN: <https://www.statsmodels.org/>.
- Литература: Sculley et al., «Hidden Technical Debt in Machine Learning Systems» (NeurIPS 2015); Breck et al., «The ML Test Score» (2017); Google «Rules of Machine Learning»; Hyndman & Athanasopoulos, «Forecasting: Principles and Practice»; M4/M5 competitions; Diebold & Mariano (1995), Harvey–Leybourne–Newbold (1997), Hansen (2005), Hansen–Lunde–Nason (2011).
- Репозиторий: `docs/guides/stat-ml-models.md`, `docs/ROADMAP_KEY_RATE.md` §6, `docs/ROADMAP_INFRA.md`, `ARCHITECTURE.md` §6.2/§6.3, PR #177.
