# План: запись прогноза ключевой ставки в `forecast_log`

**Дата:** 18 октября 2026 г.
**Пункт:** `key-rate/phase-4-forecast-log`, issue [#127](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/127).
**Спека:** [2026-10-18-key-rate-forecast-log-design.md](../specs/2026-10-18-key-rate-forecast-log-design.md).

Порядок: схема → резолвер → ingest → writer → документы → приёмка. Каждый шаг заканчивается проверкой; `make all` — на шаге 7.

## 1. Схема и миграция

Файлы: `ingest/schema.go`.

- [ ] `forecastLogDdl` дополнить `error_bp Nullable(Float64)` (после `error_pct`); комментарий `metric` — `'cbr_rate:<track>'`.
- [ ] Добавить константу `forecastLogAddErrorBp = "ALTER TABLE forecast_log ADD COLUMN IF NOT EXISTS error_bp Nullable(Float64)"` с комментарием, почему `CREATE TABLE IF NOT EXISTS` недостаточно.
- [ ] `ensureTables`: после `CREATE` выполнить `ALTER` (идемпотентно).
- [ ] Проверка: `go test ./ingest/` (тест DDL/миграции — шаг 5).

## 2. Резолвер `cbr_rate`

Файлы: `forecast/resolve.go`, `forecast/resolve_test.go`.

- [ ] `forecastRow` получает `Metric string`; `resolvable()` (xau) читает `metric` константой и заполняет поле.
- [ ] Новая выборка `rateResolvable(ctx)` — `metric LIKE 'cbr_rate%' AND target_date <= today()`, `ORDER BY metric, forecast_date`; поля те же, `actual_is_null`/`coalesce`.
- [ ] Метод стора `rateAfter(ctx, target)` — `SELECT rate FROM cbr_key_rate FINAL WHERE date > {target:Date} ORDER BY date LIMIT 1`; «нет строки» → `(0, false, nil)` (не ошибка), прочие ошибки — ошибка.
- [ ] `resolveForecasts`: обработать rate-строки (факт `rateAfter`, `error_bp = (predicted − actual) × 100`, `error_pct = NULL`, метрика как есть) и xau-строки (прежняя логика, `error_pct`, `error_bp = NULL`); один батч.
- [ ] `forecastResolution` — `Metric`, `ErrorPct *float64`, `ErrorBp *float64`; `writeResolutions` пишет `forecast_log` с `error_bp` (порядок колонок — DDL).
- [ ] `needsWrite` — без изменений (сравнение `actual`); для rate-строк дополнительно считать «нет факта» пропуском с `log.Warn`.
- [ ] Тесты: `TestErrorBp` (14.35/14.00 → +35), `TestResolveRateWritesErrorBp` (метрика, `error_pct=nil`), `TestResolveRateMissingFact` (пропуск, не ошибка), `TestResolveKeepsXauBehaviour` (окно/`error_pct` не изменились), `TestRateResolvableQueryFilters` (`metric LIKE 'cbr_rate%'`, `target_date <= today()`), `TestRateAfterQueryUsesStrictlyLaterDate` (`date >`), обновить фейк-стор.
- [ ] Проверка: `go test -race ./forecast/`.

## 3. Ingest: endpoint и запись

Файлы: `ingest/key_rate_forecast.go` (новый), `ingest/clickhouse.go`, `ingest/server.go`.

- [ ] `KeyRateForecast` — поля `forecast_date`, `target_date`, `metric`, `predicted`, `probabilities`, `data_version`, `model`, `prompt_version`; `ValidateKeyRateForecast` по спеке §4 (даты/диапазон `Date`, whitelist `cbr_rate[:track]`, `predicted` конечный 0..1000, вероятности 0–100 сумма 100 ±0.1, непустые `data_version`/`model`, `prompt_version > 0` для LLM).
- [ ] `forecastLogInsert` в `ingest/clickhouse.go` — добавить `error_bp`; `InsertModelRun` шлёт `nil` в новую колонку.
- [ ] `InsertKeyRateForecast(ctx, f)` — одна строка, `PrepareBatch → Append → Send`, `Abort` на ошибке `Append`; даты парсит из строк; `actual`/`error_pct`/`error_bp` = nil.
- [ ] `BatchWriter` — новый метод; `Server` — маршрут `POST /v1/key_rate_forecast`, обработчик: decode → validate → insert → `{"inserted": 1}`; метаданные — `log.Infof` (не терять молча).
- [ ] Тесты: `ingest/key_rate_forecast_test.go` — валидация таблицей; `server_test.go` — `401`, `400` (неизвестное поле, плохая метрика, пустой `data_version`, LLM без `prompt_version`), `200` и вызов writer'а; `clickhouse_test.go` — строка батча, `Abort` на `Append`.
- [ ] Проверка: `go test -race ./ingest/`.

## 4. Артефакт модели: `rate_before`

Файлы: `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`.

- [ ] `CurrentForecast` — поле `rate_before: Optional[float]`; `current_forecast` заполняет его из `inputs[0]`.
- [ ] `forecast_json` — ключ `rate_before` (JSON детерминирован; при отсутствии входов — `null`, но такой артефакт и так не пишется без методов).
- [ ] Docstring `forecast_json`/`CurrentForecast` — новый ключ и зачем writer'у уровень.
- [ ] Тест `test_forecast_json_matches_the_artifact_format` — `rate_before` в списке ключей и равен ставке на дату среза.
- [ ] Проверка: `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py` — зелёный.

## 5. Писатель `scripts/key_rate_forecast_log.py`

Файлы: `scripts/key_rate_forecast_log.py` (новый), `scripts/key_rate_forecast_log_test.py` (новый), `Makefile`, `.gitignore`.

- [ ] Чистые функции разбора: `model_rows(artifact)` → две строки (`taylor`, `ordered_logit`), `llm_rows(artifact)`, `data_version(model_artifact)`, преобразование шага в уровень, вероятности ×100 и ключи `-1/0/+1` → `cut/hold/hike`.
- [ ] Ошибки: нет `rate_before`/`forecast_date`/`meeting_date`/`methods` — понятный текст; нет методов — код 1; отсутствующий метод — `warn`; LLM-артефакт без обязательных полей — ошибка.
- [ ] Env `INGEST_URL`/`INGEST_TOKEN`: проверка до сети, имена в ошибке, значения не печатаются.
- [ ] HTTP POST JSON, `Authorization: Bearer`, таймаут, non-2xx → ошибка с телом ≤512 байт; токен не попадает в текст.
- [ ] CLI: `--model-artifact` (по умолчанию `key_rate_model_forecast.json`), `--llm-artifact` (по умолчанию `key_rate_llm_forecast.json`, отсутствие — штатно), `--dry-run`.
- [ ] `Makefile`: цель `key-rate-forecast-log` (`python3 scripts/key_rate_forecast_log.py $(ARGS)`), в `.PHONY`; `.gitignore` — `/key_rate_llm_forecast.json`.
- [ ] Тесты (stdlib `unittest`, HTTP замокан): разбор артефакта, шаг → уровень, вероятности, LLM, ошибки env/полей, non-2xx, `--dry-run` без HTTP, нет методов.
- [ ] Проверка: `python3 scripts/key_rate_forecast_log_test.py -v`.

## 6. Документы

Файлы: `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_KEY_RATE.md`.

- [ ] §6.2 — DDL `forecast_log` с `error_bp` и комментарием `cbr_rate:<track>`; §6.1 `forecast_resolve` — ветка `cbr_rate`, формула `error_bp`; ingest-раздел — `POST /v1/key_rate_forecast` и поля.
- [ ] §5 env — `INGEST_URL` (клиент) рядом с `INGEST_TOKEN`.
- [ ] README — env-таблица `INGEST_URL`, контур прогноза: endpoint и `make key-rate-forecast-log`.
- [ ] ROADMAP §5: строка `forecast_log` — суффиксы `metric`, `error_bp`; «Известное расхождение» — закрыто. §6, фаза 4: пункт `❌ запись прогноза…` → `✅ сделан этим PR` с числами приёмки.
- [ ] `docs/LIMITATIONS.md` — если там есть абзац про запись `forecast_log`, обновить (проверить `grep`).

## 7. Приёмка

- [ ] `make all` — зелёный.
- [ ] `make key-rate-model` — артефакт с `rate_before`.
- [ ] `make ch-status` — ClickHouse отвечает (иначе зафиксировать и не поднимать).
- [ ] `make run-ingest INGEST_ADDR=:18081` (фон), `INGEST_URL=http://127.0.0.1:18081 make key-rate-forecast-log` — строки в `forecast_log`.
- [ ] Живой резолвер: временный артефакт на прошедшее заседание (2026-09-11, `predicted=13.0`) → `make key-rate-forecast-log` → `make import STAT=forecast_resolve` → `actual=14.0`, `error_bp=−100`, `error_pct=NULL`; затем удалить тестовую строку (или использовать отдельную `forecast_date` и назвать её в протоколе).
- [ ] `make ch-sql FILE=...` — `SELECT … FROM forecast_log FINAL WHERE metric LIKE 'cbr_rate%'`.
- [ ] `make mcp-check` — зелёный.
- [ ] Python-тесты: `key_rate_model_test.py`, `key_rate_forecast_log_test.py`.
- [ ] Commit/push, PR с `Closes #127`, протокол приёмки — комментарием в PR.
