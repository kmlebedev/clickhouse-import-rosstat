# Публикационный прогон: команды

Этот файл — рабочий сценарий перед публикацией по ключевой ставке: одной командой импортировать все источники контура, запустить модели ИПЦ и ставки, записать прогноз в журнал и подготовить витрину с графиками. Порядок мысли и требования к тексту — в [KEY_RATE_PUBLICATIONS.md](../KEY_RATE_PUBLICATIONS.md) §4/§7/§9; каналы и канонические ряды — в [KEY_RATE_CHANNELS.md](../KEY_RATE_CHANNELS.md) §8; модель и метрики — в [KEY_RATE_MODEL.md](../KEY_RATE_MODEL.md). Сам текст собирается по скилу главного редактора (`.kimi-code/skills/editor-in-chief/SKILL.md`).

Применяется к форматам «предзаседание», «постзаседание», «после резюме» и «недельный разбор». Команды запускаются только через `make`: Makefile сам подключает `~/.config/rosstat/env`, поэтому ключи и DSN в командной строке не передаются.

## 1. Что нужно заранее

| Что | Проверка | Если нет |
|---|---|---|
| Файл окружения с `CLICKHOUSE_URL` | `make env-check` | заполнить `~/.config/rosstat/env` ([CONFIGURATION.md](../CONFIGURATION.md)) |
| ClickHouse отвечает | `make ch-status` | поднимает владелец: `make ch-up` |
| MLflow для паспортов run'ов | `make mlflow-status` | `make mlflow-up`; либо прогон без журнала: `MLFLOW=0 make …` |
| Ingest для записи в `forecast_log` | `make run-ingest` в отдельном терминале | нужны `INGEST_TOKEN` и `INGEST_URL` (например `http://127.0.0.1:8081`) |
| LLM для LLM-трека | `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL` в env-файле | шаг `make key-rate-llm-forecast` пропускается, трек `cbr_rate:llm` не пишется |
| `pdftotext` для `cbr_mt_forecast` | `command -v pdftotext` | `apt install -y poppler-utils` (Debian/Ubuntu) |
| Grafana для smoke графиков | `make grafana-up` или тест-стенд | шаг с `make grafana-check` пропускается |

## 2. Шаг 1. Все источники контура одной командой

```bash
make import STAT=key_rate_sources
```

`key_rate_sources` — не импортёр, а именованная группа Makefile: раскрывается в список из 50 `Name()` и запускает их **одним процессом и одним соединением**. Порядок исполнения — порядок регистрации в `chimport.Stats`, а не порядок списка.

**Что входит** (обоснование — контракт публикации §6 и карта каналов §8):

- `cbr` (26): ставка, курс, M2 и кредит, кредиты ЮЛ/ФЛ, депозиты, ожидания, ставки по вкладам и кредитам, RUONIA, базовый ИПЦ, КБД, мониторинг предприятий, консенсус, резюме ДКП, платёжный баланс, приложения ДИП, среднесрочный прогноз, форма 101, макропруденциальный канал, календарь публикаций;
- `rosstat` (7): ИПЦ месяц/неделя, недельный агрегат BI, бюллетени недельного ИПЦ, рынок труда, зарплаты, ВВП;
- `minfin` (6): федеральный и консолидированный бюджет, нефтегазовые доходы, запас и обслуживание долга, аукционы ОФЗ;
- `moex` (3): Brent, вменённая траектория KEYRATE, кривая ОФЗ;
- `bank` (4): расходы Сбера (nowcast), форвардный срез портфелей, ипотечные ставки;
- `fedstat` (2): недельные средние цены и альтернативная мера ИПЦ (31074);
- `news_watch`, `events_calendar`: событийный фон и календарь заседаний.

**Что не входит осознанно:** золото и DCF-контуры, ИЦП/розница/сельхоз/инвестиции ЕМИСС (вне модели, §8.1), `key_rate_views` (витрина читает `forecast_log`, её место — после записи в журнал), `forecast_resolve` (резолвер факта, пост-заседание), `ml_registry`.

**Как читать результат:**

- каждый импортёр печатает `Imported N rows of <имя>`; повторный прогон у источников с точкой контроля публикации — `no-op` и `Imported 0 rows`;
- ошибка одного импортёра не останавливает остальные: `main.go` логирует её, продолжает цикл и завершает прогон кодом 1; успешные к этому моменту уже записаны, повтор безопасен;
- недоступный источник не роняет прогон сразу: HTTP-слой повторяет запрос с backoff 30 с → 1 → 2 → 5 мин, поэтому мёртвый источник добавляет несколько минут, а остальные импортёры идут после него;
- строка `no importer named "<имя>"` означает, что имя в группе устарело (импортёр переименован) — это дефект Makefile, а не источника;
- первый прогон самый долгий: `cbr_dip_bulletin` качает 49 выпусков (4–6 мин), `cbr_mt_forecast` разбирает PDF. При ежедневном Dagu большинство шагов — no-op, и полный прогон занимает минуты.

Группа живёт только в Makefile: `CLICKHOUSE_IMPORT_STAT=key_rate_sources` напрямую (без `make`) не сработает — там нужен список имён через запятую.

## 3. Шаг 2. ИПЦ

```bash
make cpi-nowcast                 # nowcast месячного ИПЦ до публикации Росстата
make cpi-monthly                 # v2: nowcast с мониторингом предприятий ЦБ
make cpi-live3-horizons ARGS="--as-of 2026-10-21 --meeting 2026-10-23"
```

| Команда | Артефакты в корне репозитория (gitignored) | Когда нужна |
|---|---|---|
| `make cpi-nowcast` | `cpi_nowcast_report.md`, `cpi_nowcast_predictions.csv`, `cpi_nowcast_forecast.json` | базовая оценка месяца |
| `make cpi-monthly` | `cpi_monthly_report.md`, `cpi_monthly_predictions.csv`, `cpi_monthly_forecast.json` | основной ML-трек с МП; `ARGS="--quick"` — быстрый режим, числа неофициальные |
| `make cpi-live3-horizons` | печать h1/h3/h6 по заседаниям в stdout; `ARGS="--out FILE"` — JSON | вход канала ожиданий к конкретному заседанию |

`--as-of` — дата среза (по умолчанию сегодня), `--meeting` — дата заседания. Без `--meeting` скрипт печатает последние шесть месяцев; для публикации всегда указывайте оба.

## 4. Шаг 3. Ставка

```bash
make key-rate-model              # статистический трек: taylor + ordered_logit
make key-rate-scenario-fan-v3    # сценарный веер: channel-complete ядро
make key-rate-llm-forecast       # экспериментальный LLM-трек (нужны LLM_*)
```

| Команда | Артефакты | Примечание |
|---|---|---|
| `make key-rate-model` | `key_rate_model_report.md`, `key_rate_model_predictions.csv`, `key_rate_model_forecast.json` | строки `cbr_rate:taylor` и `cbr_rate:ordered_logit` для журнала |
| `make key-rate-scenario-fan-v3` | `key_rate_scenario_fan_v3_report.md`, `…_predictions.csv`, `…_diagnostics.csv`, `…_forecast.json` | в `forecast_log` не пишет; `ARGS="--out-dir DIR"` меняет каталог |
| `make key-rate-llm-forecast` | `key_rate_llm_forecast.json`, `key_rate_llm_forecast_report.md` | без `LLM_*` падает с понятной ошибкой; в ClickHouse не пишет |

## 5. Шаг 4. Журнал — до публикации

Ingest уже запущен (`make run-ingest`, отдельный терминал). Запись идёт только через него, MCP для записи закрыт намеренно.

```bash
make key-rate-forecast-log ARGS="--dry-run"    # посмотреть payload, ничего не отправляя
make key-rate-forecast-log                     # POST /v1/key_rate_forecast
```

`make key-rate-forecast-log` читает `key_rate_model_forecast.json` (обязателен) и `key_rate_llm_forecast.json` (если есть) и пишет production-строки `cbr_rate:*` с вероятностями, моделью, `data_version` и `prompt_version`. Staged-строки `:shadow`/`:canary` в публикацию не входят.

## 6. Шаг 5. Витрина, проверка журнала, графики

```bash
make import STAT=key_rate_views        # v_key_rate_dashboard и v_ml_live_accuracy
make ml-live-check ARGS="--require-llm" # production-строки на ближайшее заседание
make grafana-build                     # артефакты dashboard/*.json из кода grafana/
make grafana-validate                  # vet + тесты дашбордов, без стенда
```

Порядок важен: `key_rate_views` создаёт `v_ml_live_accuracy`, поэтому `make ml-live-check` запускается **после** него. `--require-llm` добавляется только если в статье заявлен LLM-трек; `ARGS="--warn-only"` превращает пропуск трека в предупреждение.

На стенде (нужен `GRAFANA_TEST_TOKEN` и поднятая Grafana):

```bash
make grafana-check                     # smoke: дашборд виден, первые панели отдают точки
```

Если менялись витрины или гранты — `make mcp-check`.

## 7. Шаг 6. Текст

1. Свежесть входов и `max(date)` — по [KEY_RATE_PUBLICATIONS.md](../KEY_RATE_PUBLICATIONS.md) §6; устаревший вход называется «нет обновления», а не заполняется старым числом.
2. Сборка текста — §4 шаблона (голос, скелет, антипаттерны).
3. Чек-лист §9 и `checklist.md` публикации; расхождение статистики, LLM и рынка публикуется, а не усредняется.

## 8. Весь прогон подряд

```bash
make env-check
make ch-status

# источники
make import STAT=key_rate_sources

# модели
make cpi-nowcast
make cpi-monthly
make cpi-live3-horizons ARGS="--as-of $(date +%F) --meeting <дата заседания>"
make key-rate-model
make key-rate-scenario-fan-v3
make key-rate-llm-forecast

# журнал и витрина
make key-rate-forecast-log ARGS="--dry-run"
make key-rate-forecast-log
make import STAT=key_rate_views
make ml-live-check ARGS="--require-llm"

# графики
make grafana-build
make grafana-validate
```

`make run-ingest` должен быть уже запущен в отдельном терминале — иначе шаг журнала упадёт на подключении к ingest.

### Постзаседание

После релиза в 13:30 МСК прогон повторяется только по изменившимся источникам, иначе квартальный блок §4.7 и обновлённый прогноз останутся на прошлом выпуске:

```bash
make import STAT=cbr_dip_bulletin     # новый выпуск ДИП: месячные SAAR для квартального блока
make import STAT=cbr_mt_forecast      # PDF среднесрочного прогноза (опорное заседание)
make import STAT=key_rate_views
```

`v_cbr_mt_forecast` читается по `release_date = meeting_date`; до импорта числа из пресс-релиза идут с пометкой «по пресс-релизу». Проверка свежести — `max(publication_date)` у `v_cbr_dip_bulletin` и `max(release_date)` у `v_cbr_mt_forecast`.

## 9. Экспериментальные треки (в публикацию по умолчанию не входят)

| Команда | Что даёт | Статус |
|---|---|---|
| `make key-rate-cbr-rule` | калиброванное правило ДДКП ЦБ, `key_rate_cbr_rule_*` | вердикт `reject` (issue #268), в журнал не пишет |
| `make key-rate-scenario-fan` / `make key-rate-scenario-fan-v2` | предыдущие версии веера | сравнение с v3 |
| `make shock-classifier` | классификатор явных шоков ставки | описательный, в журнал не пишет |
| `make cpi-monthly-v3`, `make cpi-monthly-industry` | недельные и отраслевые МП-признаки | эксперименты Stage 2 |
| `make cpi-pipeline-stage1` / `make cpi-pipeline-stage2` | конвейер CPI → SA → ставка | Gate 2 FAIL, stage 3 не строился |
| `make cb-comp-live` / `make cb-comp-live-compare` | live-пайплайн CB_COMP и сравнение | паритет не заявлен |
| `make cpi-public-audit`, `make cpi-weekly-panel` | аудит публичной панели и недельная компонентная панель | артефакты в `docs/research/` |

## 10. Частые сбои

| Симптом | Причина | Что делать |
|---|---|---|
| `no importer named "…"` | имя импортёра в группе устарело | сверить с [IMPORTERS.md](../IMPORTERS.md) и поправить `STATS_key_rate_sources` в Makefile |
| один источник красный (`level=error`), остальные `Imported …` | источник изменил страницу или файл; прогон при этом дошёл до конца и вернул код 1 | починить импортёр отдельным пунктом; повтор прогона безопасен и не затирает успешные |
| `make key-rate-model` падает на preflight MLflow | сервер MLflow не запущен | `make mlflow-up` или `MLFLOW=0 make key-rate-model` |
| `key_rate_views` падает на `forecast_log` | ingest ещё не создавал таблицу | сначала `make run-ingest` и `make key-rate-forecast-log` |
| `ml-live-check` не видит витрину | `key_rate_views` не запускался | `make import STAT=key_rate_views` |
| `key-rate-llm-forecast` — `MissingEnvError` | нет `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL` | добавить в env-файл или пропустить трек |
| `cbr_mt_forecast` — ошибка разбора PDF | нет `pdftotext` | установить `poppler-utils` |

## 11. Ссылки

- [KEY_RATE_PUBLICATIONS.md](../KEY_RATE_PUBLICATIONS.md) — тезисы, шаблон, контракт данных, порядок прогона, чек-лист.
- [KEY_RATE_CHANNELS.md](../KEY_RATE_CHANNELS.md) — 11 каналов и канонические ряды.
- [KEY_RATE_MODEL.md](../KEY_RATE_MODEL.md) — модель, бэктест, метрики.
- [IMPORTERS.md](../IMPORTERS.md) — импортёры и расписание.
- [GRAFANA.md](../GRAFANA.md) — дашборды как код и стенды.
- [2026-10-18-key-rate.md](2026-10-18-key-rate.md) — первая публикация как образец сборки.

---

*Документ описывает порядок расчёта и не является инвестиционной рекомендацией.*
