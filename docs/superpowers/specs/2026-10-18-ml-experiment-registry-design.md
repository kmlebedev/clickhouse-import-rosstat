# Спека: реестр ML-экспериментов и паспорт run (`infra/4.1`)

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** [`infra/4.1`](../ROADMAP_INFRA.md) — первый срез фазы 4 «ML-инфраструктура».
**Решения владельца (2026-10-18):** первый PR — оба существующих Python-контура (`cpi_nowcast` и `key_rate_model`); DDL и витрины — отдельным Go-импортёром `ml_registry`; MLflow, model registry и промоушен — следующие пункты (`4.2`/`4.3`).
**Статус:** спека дизайна; реализация — после утверждения плана.
**Связанный ресерч:** [2026-10-18-ml-experiment-management.md](../research/2026-10-18-ml-experiment-management.md).

---

## 1. Зачем

PR #177 показал зрелый методологический каркас (спека до кода, фиксированные фолды и бенчмарки, DM-HLN, блочный бутстрап, детерминированные артефакты), но инфраструктурно прогон остаётся одноразовым:

- у run'а нет машиночитаемого паспорта: ветка/PR и текст отчёта — единственная история;
- артефакты gitignored и перезаписываются (`clear_stale_artifacts`), промежуточные и неудачные прогоны исчезают;
- fold-предсказания лежат в CSV и не доступны SQL/Grafana/MCP;
- сравнение моделей делается вручную по тексту PR;
- нет отпечатка данных и окружения: изменение ClickHouse или версии пакета молча меняет результат;
- нет единого места, где видно, какие гипотезы уже проверены и почему отвергнуты.

`infra/4.1` закрывает **только** этот слой: паспорт run + fold-предсказания + витрины. Модельный реестр, MLflow, промоушен и live-мониторинг остаются на `4.2`/`4.3`, чтобы первый PR не тащил новые сервисы.

---

## 2. Границы

**В объёме:**

- таблицы `ml_runs`, `ml_predictions` и витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`;
- Go-импортёр `ml_registry` (DDL + витрины), регистрация в `chimport`, `main.go`, `dagu/ml.yaml`;
- Python-хелпер `scripts/ml_experiment.py` и транспорт `ch_exec` для batch-вставки;
- интеграция `scripts/cpi_nowcast.py` и `scripts/key_rate_model.py` через `ML_LOG=1`;
- гранты `kimi_reader` на витрины, проверки `mcp-check`;
- обновление `ARCHITECTURE.md`, `docs/IMPORTERS.md`, `docs/OPERATIONS.md`, `docs/CONFIGURATION.md`, `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`.

**Вне объёма:**

- MLflow, model registry, алиасы `champion`/`challenger` (`4.2`);
- промоушен, shadow/canary, live-прогноз в `forecast_log`, `v_ml_live_accuracy`, алерты (`4.3`);
- Grafana-дашборд экспериментов (может быть добавлен в `4.2`/`4.3`);
- интеграция `scripts/cpi_monthly.py` из PR #177 (её нет на `main`; подключится после мержа);
- изменение `forecast_log` и его семантики;
- HPO, Optuna, DVC, Hydra.

---

## 3. Модель данных

### 3.1 `ml_runs` — паспорт прогона

```sql
CREATE TABLE IF NOT EXISTS ml_runs (
    run_id UUID,
    experiment LowCardinality(String),   -- 'cpi_nowcast', 'key_rate_model'
    run_name String,                     -- человекочитаемое имя прогона
    status LowCardinality(String),       -- 'finished', 'empty', 'failed'
    author String,                       -- git user.name / GIT_AUTHOR_NAME
    git_sha String,
    git_branch String,
    git_dirty UInt8,                     -- 1 — рабочее дерево было грязным
    config_hash String,                  -- sha256 канонического JSON конфигурации
    config_json String,                  -- полный конфиг прогона (CLI + дефолты)
    env_json String,                     -- Python/пакеты/OS, в которых шёл прогон
    data_asof Date,                      -- дата среза данных
    sample_hash String,                  -- sha256 канонического JSON выборки
    sample_meta_json String,             -- rows/first/last/skipped, без сырых строк
    spec_path String,                    -- спека/пункт, к которому относится run
    metrics_json String,                 -- агрегированные метрики
    artifacts_json String,               -- [{name,path,sha256,bytes}, ...]
    started_at DateTime,
    finished_at DateTime,
    duration_sec Float64,
    notes String
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id);
```

Семантика:

- одна строка — один завершённый (или пустой/упавший) прогон; `run_id` — UUID, генерируется на старте;
- `status='finished'` — прогон дошёл до артефактов; `'empty'` — данных/фолдов нет, но прогон диагностирован; `'failed'` — исключение (хелпер пишет его при наличии контекста);
- `config_json` содержит все CLI-аргументы и дефолты, включая seed; `config_hash` — от канонического JSON;
- `env_json` содержит Python, версии пакетов и OS/CPU; это часть паспорта, потому что без неё прогон не воспроизводится;
- `sample_meta_json` — только описание (`rows`, `first`, `last`, `skipped`, `months`), не сырые признаки; сырьё восстанавливается из ClickHouse по `data_asof`;
- `metrics_json` — вложенный словарь, схема зависит от эксперимента; общие ключи: `primary_metric`, `models`, `windows`;
- `artifacts_json` — пути относительно корня репозитория, sha256 и размер;
- `ReplacingMergeTree` по `(experiment, run_id)`: повторная запись того же run'а (ретрай) заменяет строку, разные run'ы не конфликтуют.

### 3.2 `ml_predictions` — fold-предсказания

```sql
CREATE TABLE IF NOT EXISTS ml_predictions (
    run_id UUID,
    experiment LowCardinality(String),
    model LowCardinality(String),        -- имя модели/метода/бенчмарка
    window LowCardinality(String),       -- 'start=24', 'start=48, embargo=1', 'expanding', 'rolling40'
    target_date Date,                    -- месяц (CPI) или дата заседания (key rate)
    predicted Float64,
    actual Float64,
    error Float64,                       -- predicted - actual, в unit
    unit LowCardinality(String),         -- 'pp' (процентные пункты), 'bp' (базисные пункты)
    meta_json String,                    -- probs/actual_y3/start/embargo/main
    created_at DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id, model, window, target_date);
```

Семантика:

- одна строка — один fold одного метода в одном окне;
- `error = predicted - actual` — инвариант, проверяется тестом; хранится для удобства SQL;
- `actual` не `Nullable`: таблица только для бэктестов, где факт известен; live-прогнозы сюда не пишутся;
- `unit` обязателен: `pp` для инфляции (п.п. м/м) и `bp` для ставки (б.п.); агрегировать RMSE между разными `unit` запрещено (витрина сохраняет `unit`);
- `meta_json` — только дополнительные поля: `probs` (key rate), `actual_y3`, `start`, `embargo`, `main`;
- `ReplacingMergeTree` по полному ключу: повторная вставка того же fold'а идемпотентна.

### 3.3 Почему не `forecast_log`

`forecast_log` — журнал **live-прогнозов** (что предсказано до факта и когда сверено), а не хранилище бэктестов. Fold-предсказания нужны для сравнения моделей на одинаковых фолдах и для SQL-аналитики; смешивать их с live-журналом значило бы сломать семантику `forecast_date`/`target_date` и `is_resolved`. Связь с live-прогнозами — предмет `4.3`.

---

## 4. Go-импортёр `ml_registry`

### 4.1 Файлы

| Файл | Содержание |
|---|---|
| `mlregistry/schema.go` | константы `mlRunsDdl`, `mlPredictionsDdl` — копии §3.1/§3.2 |
| `mlregistry/views.go` | `util.View` для `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold` |
| `mlregistry/import.go` | `Name() = "ml_registry"`, `Import()`: DDL → `util.CreateView` → `Imported 0 rows`; `init()` регистрирует импортёр |
| `mlregistry/import_test.go` | текстовые контракты DDL/витрин и регистрация |

### 4.2 Поведение

`Import()`:

1. `conn.Exec` каждого DDL (идемпотентно, `CREATE TABLE IF NOT EXISTS`);
2. для каждой витрины — `util.CreateView` (пропускает, если таблиц ещё нет; здесь они уже созданы);
3. логирует `ml_runs/ml_predictions ensured` и число созданных витрин;
4. возвращает `0` строк: это schema-only импортёр.

Импортёр **не читает внешний источник** и не пишет данные; он нужен, чтобы схема и витрины создавались тем же путём, что у остальных таблиц, и проверялись `make all`/`dagu`.

### 4.3 Регистрация и расписание

- `main.go` — blank-import `github.com/kmlebedev/clickhouse-import-rosstat/mlregistry`;
- `dagu/ml.yaml` — `type: chain`, `schedule: "7 6 * * *"`, шаг `ml_registry` с `CLICKHOUSE_IMPORT_STAT=ml_registry`; `overlap_policy: skip`;
- запуск вручную: `make import STAT=ml_registry`.

### 4.4 Почему не `views/`

`views/` создаёт витрины поверх уже существующих таблиц (`gold_views`), но таблицы `ml_*` создавать не умеет. Отдельный пакет `mlregistry` держит DDL и витрины в одном месте и не раздувает `views/` доменом экспериментов.

---

## 5. Витрины и доступ

### 5.1 `v_ml_runs`

`SELECT` всех полей паспорта из `ml_runs FINAL` (кроме `run_id` — он включён). Комментарий: «Один прогон ML-эксперимента: конфиг, git, данные, метрики, артефакты».

### 5.2 `v_ml_leaderboard`

```sql
SELECT
    p.experiment,
    p.run_id,
    r.run_name,
    r.data_asof,
    r.finished_at,
    p.model,
    p.window,
    p.unit,
    count() AS folds,
    sqrt(avg(p.error * p.error)) AS rmse,
    avg(abs(p.error)) AS mae
FROM (SELECT * FROM ml_predictions FINAL) AS p
LEFT JOIN (
    SELECT run_id, experiment, run_name, data_asof, finished_at
    FROM ml_runs FINAL
) AS r ON r.experiment = p.experiment AND r.run_id = p.run_id
GROUP BY p.experiment, p.run_id, r.run_name, r.data_asof, r.finished_at,
         p.model, p.window, p.unit
```

Комментарии колонок: `rmse`/`mae` — в единицах `unit`; сравнивать строки с разным `unit` нельзя; `folds` — число предсказаний модели в окне.

### 5.3 `v_ml_error_by_fold`

`SELECT experiment, run_id, model, window, target_date, predicted, actual, error, unit, meta_json, created_at FROM ml_predictions FINAL`.

### 5.4 Гранты

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT` на `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`;
- сырые `ml_runs`/`ml_predictions` агенту **не выдаются**;
- `scripts/mcp_check.py`: проверить, что три витрины видны `kimi_reader`, а `SELECT` из `ml_runs`/`ml_predictions` даёт `ACCESS_DENIED` (или проверка через `system.grants`/`list_tables`).

---

## 6. Python-хелпер `scripts/ml_experiment.py`

### 6.1 API

```python
@dataclass(frozen=True)
class PredictionRow:
    model: str
    window: str
    target_date: date
    predicted: float
    actual: float
    unit: str                  # 'pp' | 'bp'
    meta: dict = field(default_factory=dict)

class ExperimentRun:
    run_id: str
    experiment: str
    enabled: bool

    def log_metrics(self, metrics: dict) -> None: ...
    def log_predictions(self, rows: list[PredictionRow]) -> None: ...
    def log_artifacts(self, paths: list[Path]) -> None: ...
    def finish(self, status: str = "finished", notes: str = "") -> str: ...
    # возвращает 'clickhouse' | 'local' | 'disabled'

def start_run(
    experiment: str,
    run_name: str,
    config: dict,
    data_asof: date,
    sample_meta: dict,
    sample: object,
    spec_path: str = "",
) -> ExperimentRun: ...
```

`start_run`:

- генерирует `run_id = uuid4()`;
- снимает git (`rev-parse HEAD`, `branch --show-current`, `status --porcelain`), автора, версии пакетов;
- считает `config_hash = sha256(canonical_json(config))` и `sample_hash = sha256(canonical_json(sample))`;
- создаёт `build/ml-runs/<run_id>/` только при локальном fallback;
- `enabled` — `ML_LOG == "1"`;
- `run_name` можно переопределить переменной `ML_RUN_NAME` (пустая — значение по умолчанию из скрипта);
- каталог локального fallback — `ML_LOCAL_DIR` (по умолчанию `build/ml-runs`).

`ExperimentRun` — контекстный менеджер: `__exit__` при исключении вызывает `finish(status='failed', notes='<тип>: <текст>')` и пробрасывает исключение дальше; при штатном выходе требует явного `finish` (иначе `status='failed'`, `notes='finish не вызван'`). Это позволяет сохранять неудачные прогоны, не оборачивая каждый `return` в `try/except`.

`finish`:

- собирает `ml_runs`-строку и одним `INSERT … FORMAT JSONEachRow` пишет её и накопленные `ml_predictions`;
- при ошибке ClickHouse пишет `build/ml-runs/<run_id>/run.json` + `predictions.jsonl`, логирует warning и возвращает `'local'`; прогон не падает;
- при `enabled=False` ничего не пишет и возвращает `'disabled'`;
- вызывается один раз; повторный вызов — no-op.

### 6.2 Транспорт `ch_exec`

В `scripts/extrapolation_aisc.py` рядом с `ch_query` добавляется:

```python
def ch_exec(sql: str) -> None:
    """Выполняет write-SQL через HTTP-интерфейс ClickHouse (batch INSERT/DDL)."""
```

- тот же `ch_config()`, basic auth, маскирование пароля в ошибках, таймаут;
- тело запроса — SQL; для вставки используется `INSERT INTO <table> FORMAT JSONEachRow\n<jsonl>`;
- любой не-200 статус — `ClickHouseError` (как у `ch_query`);
- `ch_query` не меняется.

### 6.3 JSONEachRow

- `UUID` — строкой;
- `Date` — `YYYY-MM-DD`; `DateTime` — `YYYY-MM-DD HH:MM:SS`;
- `Float64` — числом; `NaN`/`Inf` запрещены (хелпер поднимает `ValueError`);
- строки JSON — `ensure_ascii=False`;
- один INSERT на таблицу; для `ml_predictions` допускается разбиение на чанки по 5 000 строк (на текущих объёмах не требуется).

### 6.4 Безопасность

- в `config_json`/`notes` не попадают пароли, токены и содержимое env;
- `ch_config` уже исключает `CLICKHOUSE_PASSWORD` (пароль `kimi_reader`) и маскирует ошибки;
- `git_dirty` фиксируется, но diff не сохраняется.

---

## 7. Интеграция контуров

### 7.1 `scripts/cpi_nowcast.py`

- `run_summary(..., ml_log: Optional[bool] = None, run_name: Optional[str] = None)`.
- `ML_LOG` читается в `main`/`run_summary`; по умолчанию выключено, поэтому существующие тесты и `--export-fixtures` не меняются.
- `experiment = "cpi_nowcast"`, `run_name` по умолчанию `cpi_nowcast__start{start}__embargo{embargo}`.
- `config`: `start`, `embargo_months`, `seed`, `models`, `features`, `weekly_window_weeks`, `target_series`.
- `sample`: список `{"month": ..., "y": ..., "features": ...}`; `sample_meta`: `rows`, `first`, `last`, `skipped`, `months`.
- `predictions`: все `WindowRun` → `PredictionRow(model, window=window_key(start, embargo), target_date=month, predicted=y_pred, actual=y_true, unit="pp", meta={"start":..., "embargo":..., "main":...})`.
- `metrics`:
  - `primary_metric = "rmse_pp"`;
  - `windows`: список `{window, folds, models: {name: {rmse, mae, n, skipped, notes}}}`;
  - `weights`: `[{window, month, weights}]`;
  - `sample`: `{rows, skipped}`.
- `artifacts`: report, predictions CSV, forecast JSON (если записан).
- Ветка «ни одного прогноза»: `finish(status="empty", notes=...)`, локальные артефакты очищаются как раньше.
- Логирование выполняется **после** записи артефактов; сбой логирования не меняет код возврата.

### 7.2 `scripts/key_rate_model.py`

- `run_summary(rolling, today, out_dir, ml_log: Optional[bool] = None, run_name: Optional[str] = None)`.
- `experiment = "key_rate_model"`, `run_name` по умолчанию `key_rate_model__{expanding|rolling40}`.
- `config`: `rolling`, `min_train`, `rolling_window`, `methods`, `forecast_methods`.
- `sample`: `rows` (meeting_date, rate_before, features); `sample_meta`: `rows`, `first`, `last`, `excluded`, `missing_rate`, `missing_features`, `fold_skips`.
- `predictions`: `PredictionRow(model=method, window=window, target_date=meeting_date, predicted=step_bp, actual=actual_step_bp, unit="bp", meta={"probs": [...], "actual_y3": ...})`.
- `metrics`: `primary_metric = "rmse_bp"`; по `(method, window)` — `rmse`, `mae`, `hit`, `n`; `fold_skips`.
- `artifacts`: report, predictions CSV, forecast JSON (если записан).
- Ранние выходы (пустой календарь/выборка): `finish(status="empty", notes=...)`.
- Логирование после артефактов; сбой логирования не меняет код возврата.

### 7.3 Общий инвариант

Локальные артефакты — **основной** результат прогона. ClickHouse-запись — дополнительный слой, который не должен ломать ни сам прогон, ни его детерминизм. При `ML_LOG` не заданном поведение скриптов побайтно прежнее.

---

## 8. Воспроизводимость и идемпотентность

- `run_id` новый на каждый прогон; повторный прогон не перезаписывает предыдущий.
- `config_hash` и `sample_hash` позволяют найти прогоны с тем же конфигом/данными.
- `ml_predictions` ключ `(experiment, run_id, model, window, target_date)`; повторная вставка того же run'а идемпотентна.
- Артефакты: sha256 в `artifacts_json`; повторный прогон с тем же конфигом должен дать те же sha256 (проверка приёмки).
- `git_dirty=1` не блокирует прогон, но виден в паспорте.

---

## 9. Тесты и приёмка

### 9.1 Офлайн-тесты

- `scripts/ml_experiment_test.py`:
  - канонический JSON/hash стабильны;
  - `run_id` — UUID; `config_hash`/`sample_hash` детерминированы;
  - `error = predicted - actual`;
  - артефакт sha256/bytes;
  - JSONEachRow: UUID/Date/DateTime/float;
  - fallback при `ClickHouseError` (локальный `run.json` + warning);
  - `ML_LOG` выключен — ни одного вызова `ch_exec`.
- `scripts/cpi_nowcast_test.py`: fake logger получает 2 952 предсказания (или число фолдов тестовой выборки), `unit='pp'`, окна `start=24`/`start=48`.
- `scripts/key_rate_model_test.py`: fake logger получает предсказания, `unit='bp'`, `probs`/`actual_y3` в `meta_json`.
- `mlregistry/import_test.go`: DDL содержит ключи/типы, витрины перечислены, `Name()='ml_registry'`, `init()` регистрирует.

### 9.2 Живая приёмка

1. `make import STAT=ml_registry` — таблицы и витрины созданы, повтор — no-op.
2. `ML_LOG=1 make cpi-nowcast` — run виден в `v_ml_runs` и `v_ml_leaderboard`; `v_ml_error_by_fold` содержит fold'ы.
3. `ML_LOG=1 make key-rate-model` — то же, `unit='bp'`.
4. Повтор `ML_LOG=1 make cpi-nowcast` — новый `run_id`, те же метрики/артефакты (sha256).
5. MCP: `v_ml_runs`/`v_ml_leaderboard`/`v_ml_error_by_fold` читаются `kimi_reader`; `ml_runs`/`ml_predictions` — `ACCESS_DENIED`.
6. `dagu validate dagu/ml.yaml` — exit 0.
7. `make all`, `make mcp-check`, `make docs-check`, `make docs-build` — зелёные.
8. Протокол — комментарием в PR.

---

## 10. Документация и схема

- `ARCHITECTURE.md` §6.1 — строка импортёра `ml_registry`; §6.2 — DDL `ml_runs`/`ml_predictions`; §6.3 — витрины; §2 — пакет `mlregistry/`.
- `docs/IMPORTERS.md` — строка и раздел `ml_registry`.
- `docs/OPERATIONS.md` — `make import STAT=ml_registry`, `ML_LOG=1`.
- `docs/CONFIGURATION.md` — `ML_LOG`, `ML_RUN_NAME`, `ML_LOCAL_DIR`.
- `sql/mcp_kimi_reader.sql` — гранты на витрины.
- `scripts/mcp_check.py` — проверки витрин и запрета сырых таблиц.
- `README.md` — обновить, если в нём есть список импортёров/пакетов (по правилу 10 AGENTS.md).
- `docs/ROADMAP_INFRA.md` — статус `4.1` обновляется тем же PR.

---

## 11. Риски и ограничения

- **ClickHouse недоступен** — локальный fallback; прогон не падает, но run не виден в витринах до повторной отправки. Отдельного `ml-replay` в этом срезе нет.
- **`ML_LOG` по умолчанию выключен** — существующие пользователи не получают записи, пока не включат env; это осознанно, чтобы не менять поведение скриптов.
- **Нет модели/реестра** — `4.1` только журнал экспериментов; `champion`/`challenger` и промоушен — `4.2`/`4.3`.
- **Нет live-прогноза** — `forecast_log` не трогаем; live-мониторинг — `4.3`.
- **Объём** — на текущих выборках (≈3 тыс. fold-строк CPI и ≈1 тыс. key rate) один INSERT на таблицу допустим; чанки по 5 000 заложены на будущее.
- **Ретеншн** — TTL не задаётся; данные экспериментов малы, но политика хранения — отдельный пункт при росте.
- **Секреты** — в паспорт не попадают; `ch_config` и `mask` уже это обеспечивают.
- **Дубли** — `ReplacingMergeTree` схлопывает только одинаковый ключ; повторный run с новым `run_id` — отдельная строка (это и нужно).

---

## 12. Файлы

**Новые:**

- `mlregistry/schema.go`, `mlregistry/views.go`, `mlregistry/import.go`, `mlregistry/import_test.go`;
- `dagu/ml.yaml`;
- `scripts/ml_experiment.py`, `scripts/ml_experiment_test.py`;
- `docs/superpowers/specs/2026-10-18-ml-experiment-registry-design.md` (эта спека);
- `docs/superpowers/plans/2026-10-18-ml-experiment-registry.md` (план).

**Изменяемые:**

- `main.go`, `scripts/extrapolation_aisc.py`, `scripts/cpi_nowcast.py`, `scripts/key_rate_model.py`;
- `ARCHITECTURE.md`, `docs/IMPORTERS.md`, `docs/OPERATIONS.md`, `docs/CONFIGURATION.md`, `README.md`;
- `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`;
- `docs/ROADMAP_INFRA.md`, `docs/site-pages.json` (research-страница уже добавлена).

---

## 13. Критерий приёмки пункта

> `make import STAT=ml_registry` создаёт `ml_runs`, `ml_predictions` и три витрины; `ML_LOG=1 make cpi-nowcast` и `ML_LOG=1 make key-rate-model` пишут паспорт и fold-предсказания; `v_ml_runs`/`v_ml_leaderboard` видны через MCP, сырые таблицы закрыты; повтор с тем же конфигом даёт те же метрики и артефакты (sha256), новый `run_id`; `make all`, `make mcp-check`, `make docs-check` зелёные.
