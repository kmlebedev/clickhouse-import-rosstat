# Спека: промоушен и live-мониторинг (`infra/4.3`, #184)

**Дата:** 20 октября 2026 г.
**Пункт роадмапа:** [`infra/4.3`](../ROADMAP_INFRA.md) — фаза 4 «ML-инфраструктура», issue [#184](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/184).
**Зависимости:** [#178](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/178) (`ml_runs`/`ml_predictions`, `v_ml_runs`/`v_ml_leaderboard`/`v_ml_error_by_fold`, `scripts/ml_experiment.py`) и [#127](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/127) (`forecast_log`, `error_bp`, `POST /v1/key_rate_forecast`, резолвер `cbr_rate`) закрыты.
**План:** [2026-10-20-ml-promotion-live-monitoring.md](../plans/2026-10-20-ml-promotion-live-monitoring.md).
**Решения владельца (2026-10-20):** gate — DM-HLN + 95% блочный бутстрап + робастность, MCS только при >2 кандидатах; shadow — 2 заседания, canary — 1; алерт деградации — rolling MAE трёх разрешённых заседаний > 100 б.п.

---

## 1. Пробел

`infra/4.1`/`4.2` дали паспорт прогона, fold-предсказания, MLflow и aliases `champion`/`challenger`, а `key-rate/phase-4-forecast-log` — live-журнал `forecast_log` и резолвер `cbr_rate`. Но:

- alias `champion` в MLflow — не deployment: gate по DM/MCS, shadow/canary и rollback отсутствуют;
- live-ошибка после заседания не собрана в витрину; `v_forecast_accuracy` знает только `error_pct` золотого контура и не различает треки/стадии;
- нет алерта на деградацию live-точности и на сбой резолвера;
- публикация не имеет автоматической проверки «прогноз записан до публикации» — требование `KEY_RATE_PUBLICATIONS.md` держится вручную;
- staged-прогнозы негде писать: `metric` в `forecast_log` не имеет стадии.

## 2. Цель и объём

Замкнуть цикл «бэктест → gate → shadow → canary → production → live-ошибка → алерт»:

1. **Gate** — CLI `scripts/ml_gate.py` (`make ml-gate`) сравнивает champion и challenger на общих фолдах, применяет предрегистрированное правило и пишет решение в артефакт + теги MLflow.
2. **Shadow/canary** — writer `scripts/key_rate_forecast_log.py` получает `--stage`; staged-метрики пишутся в `forecast_log` как `cbr_rate:<track>:shadow`/`:canary` и не попадают в публикационный контур.
3. **Live-проверка** — `scripts/ml_live_check.py` (`make ml-live-check`) проверяет, что production-прогнозы на ближайшее заседание записаны до публикации.
4. **Витрина** — `v_ml_live_accuracy` поверх `forecast_log FINAL`: трек, стадия, predicted/actual, `error_bp`, rolling-метрики, свежесть.
5. **Алерты** — Grafana-provisioning `ml-live-degradation` (rolling MAE > 100 б.п.) и `ml-live-freshness` (нет разрешённого прогноза > 3 дней после заседания).
6. **Интеграция** — ingest принимает stage-суффикс, резолвер резолвит staged-строки, `make mcp-check` проверяет витрину, документация и dagu-контур синхронизированы.

**Вне объёма:**

- MLflow model registry для key-rate: у методов `taylor`/`ordered_logit` нет сохраняемого model object; регистрация — follow-up, в #184 решение живёт в артефакте gate;
- live-журнал CPI-nowcast: в `forecast_log` нет CPI-метрики; `v_ml_live_accuracy` — только `cbr_rate*`;
- изменение схемы `forecast_log`: стадия — суффикс `metric`, ключ `(metric, forecast_date)` уже разделяет стадии;
- внешние уведомления Grafana (contact point): алерт виден и Firing в UI, маршрутизация — решение владельца, секреты в репозиторий не попадают;
- новый шаг dagu: `key_rate_views` — динамическая витрина, создаётся вручную; `forecast_resolve` и `ml_registry` уже в расписании.

## 3. Предрегистрация: правила и пороги

### 3.1 Gate по бэктесту

Gate работает на `v_ml_runs`/`v_ml_error_by_fold`; вход — два `run_id`, `experiment`, `model`, `window`, `unit`. Общие фолды — точное совпадение `(model, window, target_date)`; `unit` обязан совпадать.

**Основное окно:**

| Эксперимент | Основное окно | Робастность |
|---|---|---|
| `key_rate_model` | `expanding` | `rolling40` (аналог start=48/embargo=1 для короткого ряда заседаний) |
| `cpi_nowcast`, `cpi_monthly` | `start=24` | `start=48`, `embargo=1` (строки `window` из `v_ml_leaderboard`) |

**Правило (зафиксировано до результатов):**

1. Минимум **12** общих фолдов в основном окне; иначе `iterate` с причиной `insufficient_folds`. Для robustness-окна минимум **6** общих фолдов; более короткое окно пропускается с пометкой `insufficient_folds` и не участвует в rejection (у CPI `start=48` исторически 9 фолдов).
2. `promote` требует одновременно:
   - `RMSE_challenger < RMSE_champion` на общих фолдах основного окна;
   - DM-HLN (поправка Harvey–Leybourne–Newbold) `p < 0,05` в пользу challenger, `h=1`;
   - 95% блочный бутстрап CI разницы `RMSE_challenger − RMSE_champion` целиком `< 0` (`block=4`, `n=2000`, `seed=20261020`);
   - ни в одном robustness-окне нет значимого ухудшения: если `RMSE_challenger > RMSE_champion`, то DM `p ≥ 0,05`.
3. `reject` — значимое ухудшение в robustness-окне (DM `p < 0,05` и challenger хуже) или CI/точка указывают на устойчивое ухудшение.
4. `iterate` — нет значимого преимущества, но нет и значимого ухудшения; либо мало фолдов.
5. **MCS** — только если кандидатов >2 (флаг `--mcs`): challenger обязан попасть в 90% Model Confidence Set (`arch`); при двух кандидатах MCS не применяется, работает парный DM + CI.

`decision` — одно из `promote|reject|iterate`. Артефакт `build/ml-gate/<challenger-run>.json` содержит входы, метрики, DM, CI, robustness, решение и причину; при `MLFLOW_TRACKING_URI` теги run'а challenger: `decision`, `gate=dm`, `gate_bootstrap=1`, `gate_robustness=1`, `primary_metric`, `model_version` (если известна). Alias не двигается: для CPI с model registry — явный `make mlflow-promote`; для key-rate — ручной перевод writer'а на production.

### 3.2 Shadow/canary

- Challenger пишет live-прогноз **2 заседания** как `cbr_rate:<track>:shadow`, затем **1 заседание** как `cbr_rate:<track>:canary`.
- Staged-строки не публикуются и не входят в forecast-контур `v_key_rate_dashboard`.
- `ml_gate.py live` читает `v_ml_live_accuracy`; на каждое заседание берётся последний `forecast_date` (перезапуск writer'а не считается новым заседанием):
  - `wait` — меньше 2 разрешённых shadow или canary ещё не разрешён;
  - `promote` — canary разрешён, `abs_error_bp ≤ 100` и rolling MAE (3 разрешённых) ≤ 100 б.п.;
  - `hold` — canary разрешён, но `100 < abs_error_bp ≤ 200` или rolling MAE > 100;
  - `rollback` — canary `abs_error_bp > 200`.
- Реальный цикл 3 заседаний (≈4,5 месяца при 8 заседаниях/год) в PR не завершается: проверяется механизм на исторических/тестовых строках, это фиксируется в протоколе.

### 3.3 Live-алерты

- **`ml-live-degradation`:** по production-трекам, rolling MAE трёх последних разрешённых заседаний > **100 б.п.**; `for: 5m`, `severity: warning`.
- **`ml-live-freshness`:** последний прошедший production-прогноз (`target_date <= today()`) не разрешён более **3 дней**; если прошедших production-строк нет, правило не срабатывает (полноту журнала до публикации проверяет `make ml-live-check`); `for: 5m`, `severity: warning`.
- Порог 100 б.п. — ориентир от бэктеста (`MAE ≈ 72 б.п.` у taylor/ordered logit), ≈1,4×; он не выбирается по live-результатам.
- Оба правила исключают `model='gate-selftest'`: синтетические строки приёмки не должны поднимать production-алерт.
- Contact point не задаётся; алерт живёт в Grafana UI и состоянии Firing.

## 4. Модель данных

Схема `forecast_log` **не меняется**. Стадия — суффикс `metric`:

```
cbr_rate                         — совместимая базовая метрика (production, без трека)
cbr_rate:<track>                 — production-трек
cbr_rate:<track>:shadow          — shadow-трек
cbr_rate:<track>:canary          — canary-трек
```

`<track>` — закрытый список ingest: `taylor`, `ordered_logit`, `ml`, `llm`. Стадия допустима только вместе с треком; `cbr_rate:shadow` и `cbr_rate::shadow` отвергаются. `target_date` — дата заседания, `forecast_date` — дата исходного прогноза; `target_date >= forecast_date`.

## 5. Ingest-контракт

`ingest/key_rate_forecast.go`:

- `keyRateForecastTrack` разбирает `metric` на трек и стадию; возвращает ошибку на неизвестный трек/стадию;
- `ValidateKeyRateForecast` сохраняет прежние проверки (`predicted`, `probabilities`, `data_version`, `model`, `prompt_version` для LLM);
- ошибка — `400`, запись не выполняется; ответ — `200 {"inserted":1}`;
- `forecast/resolve.go` не меняется: `metric LIKE 'cbr_rate%'` уже включает staged-строки, `error_bp` и метаданные переносятся как есть.

## 6. Writer `scripts/key_rate_forecast_log.py`

- `--stage production|shadow|canary` (default `production`);
- суффикс применяется к строкам модели (`cbr_rate:taylor`, `cbr_rate:ordered_logit`) и LLM (`cbr_rate:llm` → `cbr_rate:llm:shadow`);
- `--dry-run` печатает строки с суффиксом и не делает HTTP;
- production-поведение по умолчанию побайтно прежнее;
- неизвестная стадия — ошибка с кодом 1; staged-строки идут тем же `POST /v1/key_rate_forecast`.

## 7. Live-проверка публикации `scripts/ml_live_check.py`

- читает `v_events_calendar` (ближайшее будущее заседание) и `v_ml_live_accuracy`;
- ожидаемые production-треки: `cbr_rate:taylor`, `cbr_rate:ordered_logit`; `--require-llm` добавляет `cbr_rate:llm`;
- для каждого трека обязательна строка: `stage='production'`, `target_date = next_meeting`, `forecast_date < target_date`, непустой `data_version`;
- `--warn-only` — печать предупреждений без кода 1; по умолчанию код 1 при пропуске;
- тексты ошибок называют трек и дату, без секретов; если витрины нет — подсказка `make import STAT=key_rate_views`.

## 8. Витрина `v_ml_live_accuracy`

Создаётся импортёром `key_rate_views` (`views/ml_live_accuracy.go`, список `keyRateViewsList`) поверх `forecast_log FINAL`, `metric LIKE 'cbr_rate:%'`:

| Колонка | Смысл |
|---|---|
| `metric` | полная метрика журнала |
| `track` | второй сегмент: `taylor`, `ordered_logit`, `ml`, `llm` |
| `stage` | `production` (без суффикса), `shadow`, `canary` |
| `model` | `forecast_log.model` |
| `forecast_date`, `target_date` | даты журнала |
| `predicted`, `actual` | прогноз и факт |
| `error_bp` | `error_bp` из журнала (NULL до резолва) |
| `abs_error_bp` | `abs(error_bp)` |
| `is_resolved` | `1`, если `actual IS NOT NULL` |
| `days_since_target` | `dateDiff('day', target_date, today())` |
| `data_version`, `prompt_version` | метаданные прогноза |

Комментарии таблицы и каждой колонки — в `util.View`. `v_key_rate_dashboard` в forecast-контуре **исключает** `metric LIKE '%:shadow'`/`'%:canary'`, чтобы staged-строки не попадали в публикационный дашборд.

## 9. Доступ и MCP

- `GRANT SELECT` на `v_ml_live_accuracy` в `sql/mcp_kimi_reader.sql`;
- сырой `forecast_log` остаётся закрытым (`ACCESS_DENIED 497`);
- `scripts/mcp_check.py`: витрина читается; пустая — информационная строка; stage-значения только из `production|shadow|canary`; raw `forecast_log` — отказ;
- `make import STAT=key_rate_views` — создание витрины; повтор идемпотентен;
- dagu: новых шагов нет; `forecast_resolve` (`dagu/forecast.yaml`, 20:23) резолвит факт, `ml_registry` (`dagu/ml.yaml`, 06:07) держит схему; `dagu validate` обоих файлов.

## 10. Grafana-алерты

Provisioning-файл `grafana/provisioning/alerting/ml-live.yaml` (`apiVersion: 1`, группа `ml-live`, `folder: ML live`):

- `ml-live-degradation`: запрос по `v_ml_live_accuracy`, production, `is_resolved=1`, `model!='gate-selftest'`; на каждое `(track, target_date)` берётся последний `forecast_date` (`row_number()`), затем rolling MAE 3 заседаний; threshold `> 100`;
- `ml-live-freshness`: последний прошедший `target_date` production-строки; если для него нет `is_resolved=1` более 3 дней — `999`, иначе `0`;
- `for: 5m`, `noDataState: OK`, `execErrState: Error`, labels `severity=warning`, `contour=ml-live`.

Локальный стенд читает `GF_PATHS_PROVISIONING=$(CURDIR)/grafana/provisioning` (Makefile `grafana-up`) — файл подхватывается без отдельного деплоя. Staging: `grafana-provision-alerting-staging` копирует файл в `/etc/grafana/provisioning/alerting/` и печатает команду admin-reload/restart; вызывается из `grafana-deploy-staging`. `scripts/grafana_alert_check.py` проверяет UID/порог/запрос через `GET /api/v1/provisioning/alert-rules`; `make grafana-check` вызывает его вместе с dashboard-smoke. Contact point не задаётся.

## 11. Тесты

**Python:**

- `ml_gate_test.py`: общие фолды, DM-направление, CI, robustness, `insufficient_folds`, `mcs` при >2, артефакт, отсутствие `ch_exec`;
- `ml_live_check_test.py`: все треки есть / нет трека / только shadow / просроченный `forecast_date` / `--warn-only`;
- `key_rate_forecast_log_test.py`: стадии, суффиксы, LLM-payload, `--dry-run`, неизвестная стадия;
- `key_rate_model_test.py`, `mlflow_registry_test.py` — без регрессий.

**Go:**

- `ingest/key_rate_forecast_test.go`: `cbr_rate:taylor:shadow`, `cbr_rate:llm:canary`, отказ `cbr_rate:shadow`, `cbr_rate:taylor:prod`, пустая стадия;
- `forecast/resolve_test.go`: staged-метрика и метаданные сохраняются;
- `views/ml_live_accuracy_test.go`: имя, таблица-источник, колонки, stage/track-разбор, комментарии;
- `views/key_rate_dashboard_test.go`: forecast-контур исключает staged;
- YAML-тест алертов (root module, `gopkg.in/yaml.v3`): запрос, пороги 100/3, UID.

## 12. Критерий приёмки

1. `make all` — зелёный; `make docs-check`, `make docs-build`.
2. Python-наборы (`ml_gate_test.py`, `ml_live_check_test.py`, `key_rate_forecast_log_test.py`, `key_rate_model_test.py`, `mlflow_registry_test.py`) — зелёные.
3. `make import STAT=key_rate_views` — `v_ml_live_accuracy` создана; `make mcp-check` — витрина читается, raw `forecast_log` — 497.
4. `make ml-gate` на двух существующих run'ах — артефакт с решением; повтор детерминирован.
5. `make key-rate-forecast-log ARGS="--stage shadow --dry-run"` — метрики `:shadow`; живая запись через ingest — строки в `v_ml_live_accuracy`; `v_key_rate_dashboard` их не показывает.
6. `make import STAT=forecast_resolve` — staged-строка получает `actual`/`error_bp`; повтор идемпотентен.
7. `make ml-live-check` — fail при отсутствующем production-треке, pass после записи.
8. `make grafana-validate`, `make grafana-check` (dashboard + alert rule); локальный firing-тест временным порогом; staging — copy/reload, при недоступном SSH — «не проверено».
9. `dagu validate dagu/forecast.yaml`, `dagu validate dagu/ml.yaml`.
10. Протокол — комментарием в PR; `Closes #184`; статус роадмапа — тем же PR.

## 13. Риски и ограничения

- **Цикл shadow/canary** — 3 заседания (≈4,5 месяца); в PR проверяется механизм, не завершённый цикл.
- **Тестовые live-строки** — `model='gate-selftest'`, `data_version='gate-selftest:...'`; удаление — только владелец (`ALTER TABLE … DELETE`), агент destructive SQL не выполняет.
- **MCS** — при двух кандидатах не применяется; при >2 требуется `arch`, иначе `iterate` с явной причиной.
- **Key-rate без model registry** — alias не двигается; решение gate + ручной stage-перевод, это записано в артефакте.
- **Grafana contact point** — не настраивается; внешние уведомления — решение владельца.
- **Staging** — может быть недоступен; тогда «не проверено», как в #183.
- **`forecast_log`** — схема не меняется; стадия в `metric` сохраняет ключ и идемпотентность ReplacingMergeTree.
