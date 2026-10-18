# Запись прогноза ключевой ставки в `forecast_log` — дизайн

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** `key-rate/phase-4-forecast-log` — [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §6, фаза 4, issue [#127](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/127).
**План:** [2026-10-18-key-rate-forecast-log.md](../plans/2026-10-18-key-rate-forecast-log.md).
**Сопутствующие документы:** [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §7 п. 5 и §10, [KEY_RATE_MODEL.md](../../KEY_RATE_MODEL.md) §6, [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.1/§6.2, issue [#189](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/189) (LLM-трек).

## 1. Пробел

Контур прогноза ключевой ставки считает прогноз (`scripts/key_rate_model.py`), но **не пишет его в журнал**: артефакт `key_rate_model_forecast.json` локальный и gitignored. `forecast_log` резервирует `metric='cbr_rate'`, однако:

- `forecast_log.error_pct = (predicted − actual) / actual` для ставки семантически неверна: 100 б.п. — это ≈7 % при ставке 14 % и ≈25 % при ставке 4 %, поэтому шкала несопоставима между режимами;
- `forecast/resolve.go` умеет резолвить только `xau_q_avg` и не умеет `cbr_rate`;
- несколько треков на одну дату заседания (`taylor`, `ordered_logit`, `ml`, `llm`) должны различаться в журнале, иначе строки схлопнутся по ключу `(metric, forecast_date)`;
- [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §7 п. 5 и §10 запрещают выпускать публикацию без записи прогноза в журнал.

## 2. Цель и объём

Замкнуть цикл «прогноз → журнал → факт → ошибка в базисных пунктах»:

1. DDL `forecast_log` получает `error_bp Nullable(Float64)`; `error_pct` остаётся для метрик, где относительная ошибка осмысленна (`xau_q_avg`).
2. Треки различаются суффиксом `metric`: `cbr_rate:taylor`, `cbr_rate:ordered_logit`, `cbr_rate:ml`, `cbr_rate:llm`. `ORDER BY (metric, forecast_date)` **не меняется**; колонка `model` — follow-up, не входит в этот пункт.
3. `forecast/resolve.go` получает ветку `metric LIKE 'cbr_rate%'`: факт — ставка после заседания из `cbr_key_rate`, `error_bp = (predicted − actual) × 100`, `error_pct = NULL`. Ветка `xau_q_avg` не меняется.
4. Запись — только через ingest-контур: `POST /v1/key_rate_forecast`. Второго пути записи нет; MCP остаётся read-only.
5. `scripts/key_rate_forecast_log.py` читает артефакт модели (и, при наличии, LLM-артефакт) и отправляет строки в ingest.
6. ARCHITECTURE §6.2, README и статус пункта роадмапа — тем же PR.

**Вне объёма:** колонка `model`, постоянное хранение `probabilities`/`data_version`/`prompt_version` в `forecast_log` (см. §7), витрина `v_key_rate_dashboard`, ML-трек v2, LLM-трек #189 (он готовит артефакт, но не пишет в ClickHouse), изменение расписания DAG.

## 3. Схема и миграция

Канонический DDL ([ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.2) получает одну колонку:

```sql
CREATE TABLE IF NOT EXISTS forecast_log (
    forecast_date Date,
    target_date Date,
    metric LowCardinality(String),   -- 'xau_q_avg','fed_decision','nav','cbr_rate:<track>'
    predicted Float64, actual Nullable(Float64),
    error_pct Nullable(Float64),
    error_bp Nullable(Float64)       -- ошибка в базисных пунктах (только cbr_rate*)
) ENGINE = ReplacingMergeTree ORDER BY (metric, forecast_date);
```

`CREATE TABLE IF NOT EXISTS` не добавляет колонку в уже существующую таблицу, поэтому `ingest.ensureTables` после DDL выполняет идемпотентный
`ALTER TABLE forecast_log ADD COLUMN IF NOT EXISTS error_bp Nullable(Float64)` — прецедент: идемпотентные `ALTER` в `news_watch` и `craw/gost.go`.

**Ключ и версии.** `ORDER BY (metric, forecast_date)` не меняется: треки различаются значением `metric`, а повторная запись того же трека на ту же `forecast_date` заменяет строку (ReplacingMergeTree). Пересчёт прогноза другим числом в тот же день — та же строка; перенос прогноза на другую дату — новая строка. Отдельного измерения «версия модели» в ключе нет — это ограничение follow-up вместе с колонкой `model`.

## 4. Контракт ingest-endpoint

`POST /v1/key_rate_forecast`, авторизация и rate limit — общие для ingest (`Bearer INGEST_TOKEN`, `429` после burst). Тело — один JSON-объект (не массив): writer отправляет по строке на трек, максимум 4 запроса на прогон.

```json
{
  "forecast_date": "2026-10-18",
  "target_date": "2026-10-23",
  "metric": "cbr_rate:ordered_logit",
  "predicted": 13.3665,
  "probabilities": {"cut": 81.65, "hold": 16.24, "hike": 2.12},
  "data_version": "key-rate-model-v0:2026-10-16:n=108",
  "model": "ordered_logit",
  "prompt_version": 0
}
```

Поля и валидация (ошибка — `400`, запись не выполняется):

| Поле | Правило |
|---|---|
| `forecast_date` | `YYYY-MM-DD`, диапазон `Date` [1970-01-01, 2149-06-06] (колонка объявлена `Date`) |
| `target_date` | то же; `target_date >= forecast_date` (журнал хранит дату исходного прогноза, а не дату вставки) |
| `metric` | `cbr_rate` или `cbr_rate:<track>`, `track` ∈ {`taylor`, `ordered_logit`, `ml`, `llm`}; `ORDER BY` не трогаем |
| `predicted` | конечное число, ожидаемая ставка в % годовых, `0 < predicted < 1000` |
| `probabilities` | необязательный объект; ключи `cut`/`hold`/`hike` (алиасы `-1`/`0`/`+1`), значения 0–100, сумма 100 ± 0.1; пустой объект — «распределения нет» |
| `data_version` | непустая строка после `TrimSpace`: публикация без версии данных запрещена ([KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §7 п. 5) |
| `model` | непустая строка после `TrimSpace`; для `cbr_rate:llm` — имя модели провайдера |
| `prompt_version` | `uint16`; для `cbr_rate:llm` обязан быть > 0, для остальных треков 0 |

Неизвестные поля — `400` (`DisallowUnknownFields`, как у остальных endpoint'ов). Ответ — `200 {"inserted": 1}`; ошибка записи — `500 insert failed` с деталями в логе. Пустой `metric`/чужой префикс, `NaN`/`Inf` в `predicted` и вероятностях — `400`.

**Запись.** `InsertKeyRateForecast` пишет **одну строку** батчем `PrepareBatch → Append (err checked) → Send`: `forecast_date`, `target_date`, `metric`, `predicted`, `actual = NULL`, `error_pct = NULL`, `error_bp = NULL`. `Abort` — только в ветке ошибки `Append`.

**Что не хранится сейчас.** `probabilities`, `data_version`, `model`, `prompt_version` принимаются и валидируются контрактом, но колонок под них в `forecast_log` пока нет: по решению владельца в этом пункте схема меняется только на `error_bp`, а `model` — отдельный follow-up. Endpoint пишет их в лог ingest-сервера (info) и не теряет молча: writer и #189 уже шлют полный контракт, поэтому добавление колонок позже не потребует менять writer. Ограничение названо в §7 и в статусе роадмапа.

## 5. Резолвер `cbr_rate`

`forecast/resolve.go` сохраняет существующую ветку `xau_q_avg` без изменений и добавляет вторую выборку:

```sql
SELECT forecast_date, target_date, metric, predicted,
       if(actual IS NULL, 1, 0) AS actual_is_null, coalesce(actual, 0) AS actual
FROM forecast_log FINAL
WHERE metric LIKE 'cbr_rate%' AND target_date <= today()
ORDER BY metric, forecast_date
```

Факт — **ставка после заседания**: первая точка `cbr_key_rate FINAL` со строго большей датой, чем `target_date`:

```sql
SELECT rate FROM cbr_key_rate FINAL
WHERE date > {target:Date} ORDER BY date LIMIT 1
```

Почему «первая точка после», а не `d+7`: решение вступает в силу со следующего дня, а ряд `cbr_key_rate` — дневной с пропусками выходных (пятничное заседание → понедельник); первая точка после даты заседания и есть действующая ставка сразу после решения. Окно `d+7` из `key_rate_model.py` нужно там для ярлыка выборки, но в резолвере оно могло бы захватить следующее заседание.

- `error_bp = (predicted − actual) × 100` — predicted и actual в % годовых, ошибка в базисных пунктах; знак сохраняется (прогноз ниже факта — отрицательная ошибка).
- `error_pct` записывается как `NULL` — относительная ошибка для ставки не публикуется.
- `actual` пишется в `%` годовых (уровень), а не шаг.
- Нет точки после `target_date` (заседание сегодня, ставка ещё не вступила) — `log.Warn`, строка остаётся нерезолвленной, прогон не падает.
- `actual <= 0` — порча ряда, `log.Warn`, пропуск.
- Перезапись — только при изменившемся `actual` (как у `xau_q_avg`), иначе `log.Debugf`.
- `metric` строки переносится в батч как есть: `cbr_rate:taylor`, `cbr_rate:ordered_logit`, `cbr_rate:ml`, `cbr_rate:llm` не схлопываются и не теряют трек.
- Сбой любой из выборок/записей — ошибка прогона (не «0 строк»); частичный батч не отправляется.

Ветка `xau_q_avg` остаётся прежней: квартальное окно, `comex_front > moex_fix_usd`, `error_pct`, `error_bp = NULL`. Оба набора строк собираются в один батч.

## 6. Писатель `scripts/key_rate_forecast_log.py`

**Вход.** `key_rate_model_forecast.json` (обязателен, корень репозитория или `--model-artifact PATH`) и, при наличии, `key_rate_llm_forecast.json` (`--llm-artifact PATH`; отсутствие — штатно, трек `llm` не пишется). Writer не читает ClickHouse: всё нужное есть в артефактах, а единственный путь записи — ingest.

**Артефакт модели получает `rate_before`.** Сейчас в JSON есть `methods.taylor.step_bp` и `methods.ordered_logit.expected_step_bp`, но нет уровня ставки до заседания, без которого шаг в б.п. не перевести в ожидаемую ставку. `scripts/key_rate_model.py` добавляет в `key_rate_model_forecast.json` поле `rate_before` (ставка на дату среза, та же, что уходит в методы). Это минимальное расширение контракта артефакта; старый файл без поля writer отвергает с сообщением «перегенерируйте `make key-rate-model`», а не угадывает.

**Строки.**

| Артефакт | `metric` | `predicted` | `probabilities` | `model` | `prompt_version` |
|---|---|---|---|---|---|
| `taylor` | `cbr_rate:taylor` | `rate_before + step_bp/100` | — | `taylor` | 0 |
| `ordered_logit` | `cbr_rate:ordered_logit` | `rate_before + expected_step_bp/100` | `probs` ×100, ключи `-1/0/+1` → `cut/hold/hike` | `ordered_logit` | 0 |
| LLM-артефакт | `cbr_rate:llm` | `expected_rate` или `current_rate + expected_step_bp/100` | `probabilities` (cut/hold/hike) | `model` из артефакта | `prompt_version` из артефакта |
| ML-артефакт | `cbr_rate:ml` | резерв: в этом пункте writer его не читает | — | — | — |

`data_version` модели — `key-rate-model-v0:<forecast_date>:n=<sample_size>` (та же конвенция «дата + размер выборки», что в отчёте v0); у LLM — `data_version` из артефакта (sha256 снимка по спеке #189). Если в артефакте модели отсутствует метод (вырожденное окно) — writer печатает `warn` и шлёт остальные; если не осталось ни одного метода — ошибка с кодом 1.

**Env.** `INGEST_URL` (например, `http://127.0.0.1:8081`) и `INGEST_TOKEN` — те же имена, что у сервера (`cmd/ingest`, README/ARCHITECTURE §5); URL клиента — новый, серверного адреса `INGEST_ADDR` для этого недостаточно. Нет любой из переменных — понятная ошибка с **именами** переменных, код 1; значения не печатаются. Non-2xx/обрыв — код 1 с компактным телом ответа (≤512 байт); токен в тексты не попадает.

**Запуск.** `make key-rate-forecast-log` (переменные окружения подключает Makefile); `ARGS="--llm-artifact PATH"` и `--dry-run` (напечатать строки, не отправляя) — для отладки. Повтор идемпотентен на стороне ClickHouse: та же `(metric, forecast_date)` заменяет строку.

## 7. Ограничения и follow-up

- **Колонки метаданных.** `probabilities`, `data_version`, `model`, `prompt_version` пока не хранятся в `forecast_log` (см. §4). Это осознанно ограничивает проверяемость журнала: версия данных и распределение видны только в артефактах и логе ingest. Follow-up: добавить колонки (в первую очередь `model` для #189) и писать их из endpoint; контракт endpoint уже готов.
- **Версионирование прогноза в тот же день.** Ключ `(metric, forecast_date)` заменяет строку при пересчёте; «изменение прогноза = новая запись» из [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §8 в этом пункте выполняется переносом `forecast_date`, а не версией в ключе. Follow-up — вместе с колонкой `model`.
- **ML-трек** (`cbr_rate:ml`) зарезервирован, но артефакта ML-прогноза ставки ещё нет; writer его не читает.
- **`v_key_rate_dashboard`** и публикационный прогон — отдельные пункты фазы 4.

## 8. Тесты

**Go.**

- `forecast/resolve_test.go`: факт и `error_bp` (predicted 14.35, actual 14.00 → +35 б.п.), `error_pct = NULL`, метрика трека сохраняется; отсутствие точки после заседания — пропуск без ошибки; `xau_q_avg` не затронут (прежние окно/`error_pct`); сбой выборки/записи — ошибка; тексты запросов фильтруют `metric LIKE 'cbr_rate%'` и `target_date <= today()`.
- `ingest/key_rate_forecast_test.go` / `server_test.go`: валидация (даты, префикс и whitelist `metric`, `predicted`, вероятности, `data_version`, `prompt_version` для LLM, неизвестное поле), авторизация (`401`), запись (`200 {"inserted": 1}`), ошибка writer'а (`500`).
- `ingest/clickhouse_test.go`: строка батча (метрика/даты/predicted, `actual`/`error_pct`/`error_bp` = nil), `Abort` на ошибке `Append`; DDL содержит `error_bp`, миграция — `ADD COLUMN IF NOT EXISTS`.

**Python.**

- `scripts/key_rate_forecast_log_test.py`: разбор артефакта модели (шаг → уровень, вероятности ×100, `data_version`), LLM-артефакт при наличии, отсутствие `rate_before` — ошибка, нет env — ошибка с именами, non-2xx — ошибка без токена в тексте, `--dry-run` не делает HTTP, нет методов — код 1.
- `scripts/key_rate_model_test.py`: `rate_before` в JSON-артефакте и в списке ключей.

## 9. Критерий приёмки

- `make all` — зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки).
- `make key-rate-model` — артефакт с `rate_before`; `make key-rate-forecast-log` при запущенном ingest — строки `cbr_rate:taylor` и `cbr_rate:ordered_logit` в `forecast_log` (FINAL), `actual`/`error_pct`/`error_bp` = NULL; повтор — те же значения (ReplacingMergeTree).
- Живой резолвер: строка `cbr_rate:taylor` на прошедшее заседание (например, 2026-09-11) после `make import STAT=forecast_resolve` получает `actual` (ставка после заседания), `error_bp = (predicted − actual) × 100`, `error_pct = NULL`; `xau_q_avg`-строки резолвятся как раньше.
- `POST /v1/key_rate_forecast` без токена — `401`, с неверным `metric`/`data_version` — `400`, с корректным телом — `200 {"inserted": 1}`.
- `make mcp-check` — зелёный (схема ingest не затронула витрины; `forecast_log` агенту не выдана).
- ARCHITECTURE §6.1/§6.2, README, статус пункта в ROADMAP — обновлены; `Closes #127` в PR.
