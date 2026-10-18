# Контур прогноза золота

> Часть документации проекта; входная точка — [README.md](../README.md).

## Составные части

Составные части контура (подробно — ARCHITECTURE.md §6.3 и §6.5):

- витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_imf_macro`, `v_eia_petroleum`, `v_gold_forecasts`, `v_gold_etf_flows`, `v_cb_gold_dynamics`, `v_gold_etf_flows_daily`, `v_gold_etf_funds_daily`, `v_news_events`, `v_news_flash`, `v_news_message_versions`, `v_news_tags` и календарь `v_events_calendar` — читаются агентом через MCP (пользователь `kimi_reader`, только `v_*`); гранты — в `sql/mcp_kimi_reader.sql`;
- витрины метрик компаний сектора `v_company_financials`, `v_company_metric_sources`, `v_company_operating` (импортёр `company_views`) — читаются агентом через MCP; сейчас наполнены данными Полюса (ARCHITECTURE.md §6.3);
- витрина `v_dcf_assumptions` — NPV рудников Полюса по каждому (deck, контур ставки, актив) плюс суммарный NAV рудников; пишет её импортёр `dcf_engine` (пакет `dcf/`, `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`), сырая `nav_by_asset` агенту не выдана;
- импортёр `forecast_resolve` (пакет `forecast/`, `dagu/forecast.yaml`, 20:23 МСК) — обратная связь журнала прогнозов: заполняет `actual`/`error_pct` в `forecast_log` сверкой `xau_q_avg` со средним `gold_prices` за квартал `target_date` (ARCHITECTURE.md §6.1);
- ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели, ручных рядов, цен COMEX и консенсус-прогнозов;
- плагин Kimi `gold-nav` — отдельный репозиторий, сценарная сессия прогноза и DCF NAV PLZL.

### Ingest-endpoint

Запуск: `make run-ingest` (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN` в `~/.config/rosstat/env`; `INGEST_ADDR` по умолчанию `:8081`). При старте создаются `model_runs`, `forecast_log`, `macro_series`, `gold_prices`, `gold_forecasts`, если их ещё нет. Витрины `views/` создаются отдельно: `make import STAT=gold_views` после первого запуска ingest.

| Метод и путь | Тело | Ответ |
|---|---|---|
| `POST /v1/model_run` | JSON прогона: клиентский `run_id` (UUID), `trigger_type`, `price_deck`, `probabilities`, `gold_scenario` с `horizon`, значения NAV | `200 {"inserted": 1}`; `400` при ошибке валидации |
| `POST /v1/manual_series` | JSON-массив `{"series", "date": "YYYY-MM-DD" (диапазон Date32 [1900-01-01, 2299-12-31]), "value", "source"?}` → `macro_series`; `source` необязателен (по умолчанию `manual`), допустимы `manual`, `webbridge`, `imf` | `200 {"inserted": N}`; `400` при ошибке любой точки |
| `POST /v1/gold_prices` | JSON-массив `{"venue", "date": "YYYY-MM-DD" (диапазон Date32), "usd"}` → `gold_prices`; допустимая venue — `comex_front` (словарь расширяемый), `usd` > 0 | `200 {"inserted": N}`; `400` при ошибке любой точки |
| `POST /v1/gold_forecasts` | JSON-массив `{"author", "published": "YYYY-MM-DD" (диапазон Date32), "horizon", "scenario", "low", "high", "point", "probability"}` → `gold_forecasts`; `author`/`horizon` непусты, `scenario` ∈ {`point`,`range`,`bull`,`base`,`bear`}, `low`/`high`/`point` ≥ 0 и хотя бы одно > 0, `high ≥ low` (когда заданы оба), `probability` 0–100 (0 — «не задана») | `200 {"inserted": N}`; `400` при ошибке любой строки |

Все запросы — с заголовком `Authorization: Bearer <INGEST_TOKEN>`; без него или с неверным токеном — `401`. Сверх `INGEST_RATE_PER_MIN` (по умолчанию 60 в минуту) — `429`; тело больше 1 MiB — `413`; неизвестное поле в JSON — `400`. Повторный `POST /v1/model_run` с тем же `run_id` ничего не записывает. Формат тела — в `ingest/model_run.go` (структура `ModelRun`).

### Плагин gold-nav

Плагин — отдельный репозиторий (не входит в этот проект). Локальная копия рядом: `../gold-nav`. Спецификация: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

- Kimi Code: `/plugins install <github-url>` (ссылка на репозиторий — после публикации);
- Kimi Work: зарегистрировать каталог плагина в персональном маркете (`kimi-daimon kimi-plugin register-personal <путь к gold-nav>`) и включить в «Плагины → Персональный».

Переменные окружения пользователя плагина (только имена, значения выдаёт автор): `GOLD_NAV_MCP_TOKEN` — read-only токен MCP; `GOLD_NAV_INGEST_URL` и `GOLD_NAV_INGEST_TOKEN` — только у автора. Команды плагина: `/gold-nav:session` (сценарная сессия) и `/gold-nav:verify` (сверка прогнозов с фактом).

Живая приёмка плагина в Kimi Code и Kimi Work не выполнена; см. раздел «Результат проверки» в README плагина.
