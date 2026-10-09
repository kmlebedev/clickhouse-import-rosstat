# Дизайн: плагин `gold-nav` и пересмотр дорожной карты под kimi datasource / Kimi Work

**Дата:** 2026-10-10
**Статус:** на ревью
**Связанные документы:** [ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md), [ARCHITECTURE.md](../../../ARCHITECTURE.md), [статья «Золото Q3-2026»](../../Золото_Q3-2026_итоги_и_прогноз_Q4-2026.md)

---

## 1. Замысел и критерии успеха

**Целевая постановка (уточнённая в диалоге):** ценность проекта — в прогнозе (сценарный прогноз золота → DCF → NAV PLZL), данные вторичны. Агент, строящий прогноз, должен получать необходимые данные нативно и с минимумом токенов:

- **мировые данные** (золото USD, ставка ФРС, TIPS, DXY, ULSD, инфляционные ожидания) — через готовые datasource-возможности Kimi (плагин kimi-datasource в Kimi Code; плагины Global Finance Data / IMF в Kimi Work), без собственных импортёров там, где данные уже есть готовые;
- **РФ-данные** (ЦБ, Росстат, Минфин, MOEX, ОФЗ) — из ClickHouse автора через mcp-clickhouse по одному запросу к композитным витринам;
- всё это должно работать **и в Kimi Code, и в Kimi Work**, и быть **шарибельным** для пользователей из РФ, не разбирающихся в программировании: установка одной командой, хостинг данных у автора.

**Критерии успеха:**

1. Сценарная сессия прогноза проходит end-to-end в Kimi Code и в Kimi Work: входы получены, сценарии пересчитаны, run записан в `model_runs`.
2. В Go ETL не пишутся импортёры для данных, покрытых datasource (eia, lbma-нативный, wgc, fedwatch — вычеркнуты из плана).
3. Не-программист устанавливает контур одной командой/кнопкой и запускает прогноз командой `/gold-nav:session`.
4. MCP остаётся строго read-only; запись — только через валидирующий ingest-endpoint.

## 2. Что проверено (фактическая база)

### 2.1 Покрытие рядов роадмапа через kimi datasource (живые пробы 2026-10-09)

| Ряд | Источник | Результат |
|---|---|---|
| FEDFUNDS, DFII10, DTWEXBGS, T5YIE, T5YIFR (5y5y) | `fred` | ✅ есть; поддержаны винтажные параметры (`realtime_start/end`, `output_type`) — защита от look-ahead при верификации прогнозов |
| Золото USD | `yahoo_finance` | ✅ `GC=F` (COMEX), дневные OHLCV; ограничение — история ≤ 2 лет |
| DXY | `yahoo_finance` | ✅ `DX-Y.NYB` |
| ULSD (дизельная нога crack) | `yahoo_finance` | ✅ `HO=F` |
| LBMA AM/PM fix | — | ❌ FRED-серия удалена, поиск пуст; обход — `GC=F` |
| EIA запасы дистиллятов | — | ❌ источника нет; `HO=F` даёт только цену |
| WGC ETF-потоки, CME FedWatch | — | ❌ источников нет (WebBridge/ручной ввод) |
| Котировки PLZL | — | ❌ `PLZL.ME` на Yahoo пуст; покрыто своим `moex_iss` |

### 2.2 Спайк в запущенном Kimi Work (v3.2.15, 2026-10-10)

- Плагины в Kimi Work **есть**: маркетплейс (Featured/Finance/…), кнопка «+ Создать плагин» → встроенный агент Plugin Builder.
- Формат плагина — тот же `kimi.plugin.json`, что в Kimi Code; типы `skill-only | mcp | mcp+skills`; поддержан **remote MCP по URL**; регистрация в персональный маркет официальным CLI `kimi-daimon kimi-plugin register-personal`, установка из вкладки «Персональный», горячее подключение без перезапуска.
- Дата-плагины на машине: `yahoo_finance` («Global Finance Data»), `imf`, `world_bank_open_data`, `igo_open_data`, `sec_edgar`, `scholar`, `xtt-public-markets-investing` (установлен); в маркете Finance: S&P, Wind, Caixin, CLS News и др. FRED-плагина нет — покрывается Yahoo (`^TNX`) + IMF.
- Предположение «в Kimi Work нельзя свой MCP и нет datasource» **опровергнуто**.
- Не проверено (тратит кредиты): живой end-to-end запрос данных в сессии Kimi Work — входит в приёмку.

## 3. Архитектура решения

```
                        ┌─────────────────────────────────────────┐
                        │  Сервер автора (24/7)                    │
  РФ-источники ──ETL──► │  ClickHouse (сырые + витрины v_*)        │
  (Go, dagu cron)       │       │                    │             │
                        │       ▼                    ▼             │
                        │  mcp-clickhouse       ingest-endpoint    │
                        │  (HTTP, read-only,    (POST /v1/*,       │
                        │   токен, kimi_reader)  Bearer-токен)     │
                        └───────┼────────────────────┼─────────────┘
                                │ HTTPS + токен      │ HTTPS + токен (только автор)
            ┌───────────────────┼────────────────────┘
            ▼                   ▼
  Kimi Code                Kimi Work
  /plugins install         Plugin Builder → personal market
  плагин gold-nav          тот же плагин gold-nav
  + kimi-datasource        + Global Finance Data / IMF
            └───────────────────┬
                                ▼
              Сценарная сессия: мировые данные из datasource-
              плагинов среды + РФ-данные из v_model_inputs
              → сценарии → NAV PLZL → POST run'а на ingest
```

**Принципы:**

1. Данные, нативно доступные агенту через datasource-плагины, в ClickHouse **не дублируются**. ClickHouse хранит: РФ-источники (чего у datasource нет), исторический базлайн (fred/bls/bea/moex/gold — уже собрано), модель и журнал прогнозов.
2. Токены агента экономятся витринами-однострочниками и фиксированным сценарием сессии, а не самописным семантическим MCP-сервером (вариант B отклонён как дублирование).
3. Кастомный плагин `gold-nav` — артефакт упаковки и дистрибуции (skill + MCP-конфиг + команды), а не новый сервис данных.

## 4. Пересмотр дорожной карты (изменения в ROADMAP_DCF_POLYUS.md)

**Фаза 1 (данные) — сокращается.** Из плана Go-импортёров убираются:
- `eia` — crack-нога берётся в сессии из `HO=F`; запасы дистиллятов — ручной/сессионный ввод в `macro_series` (`source='manual'`) до WebBridge-импортёра (фаза 5);
- `lbma_gold` (нативный) — `GC=F` из datasource; ожидание Nasdaq Data Link снято с критического пути;
- `wgc` (ETF-потоки), `fedwatch` — ручной ввод / WebBridge (фаза 5), не блокеры.

Остаются и уже готовы: `fred`, `bls`, `bea`, `moex_iss`, `ofz_curve`, `gold` (MOEX-фикс), весь РФ-контур. Их статус меняется с «источник для сессии» на «исторический базлайн + Grafana + верификация»; первичный источник мировых данных для прогнозной сессии — datasource.

**Фаза 2 (семантика + MCP) — центральная:** витрины `v_model_inputs`, `v_gold_dashboard`, плагин `gold-nav`, mcp-clickhouse в HTTP-режиме с токеном, ingest-endpoint.

**Фазы 3–5** — методология без изменений; WebBridge-импортёры `fedwatch`/`wgc_etf` остаются в фазе 5, но не блокируют прогнозный контур.

**Quick wins (обновлённые):** (1) `events_calendar` Q4-2026 (чистый SQL); (2) `v_model_inputs` + `v_gold_dashboard`; (3) плагин `gold-nav` с skill «сценарная сессия» — после этого контур «прогноз за одну сессию» работает.

## 5. Витрины

### 5.1 `v_model_inputs` — один SELECT = все входы DCF

| Блок | Колонки | Источник |
|---|---|---|
| Золото | `gold_moex_fix_usd`, `gold_date` | `gold_prices` (venue=`moex_fix_usd`); сверка с `GC=F` в сессии |
| FX | `usdrub`, `usdrub_date` | `cbr_currency_usd` |
| Ставки РФ | `cbr_key_rate`, `ofz_1y/3y/5y/10y`, `rgbi` | `cbr_key_rate`, `ofz_curve` |
| Инфляция РФ | `ipc_mes_last`, `ipc_week_ytd` | `ipc_mes`, `ipc_weeks` |
| Ставки США (базлайн) | `fedfunds`, `dfii10`, `dgs10`, `t5yie`, `dtwexbgs` | `macro_series` (fred) |
| Рынок | `plzl_close`, `plzl_date` | `stock_prices` |
| Модель | `last_run_id`, `last_nav_per_share`, `last_run_date` | `model_runs` |

Реализация: `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`, подзапросы `argMax(value, date)`; комментарии таблицы и колонок через `ALTER TABLE ... MODIFY COMMENT` / `COMMENT COLUMN`; грант `kimi_reader` в `sql/mcp_kimi_reader.sql`.

### 5.2 `v_gold_dashboard` — дашборд верификации из статьи §6

Одна строка = индикатор: `indicator | value | threshold | flag | as_of | stale`. Индикаторы: crack ULSD (прокси), запасы дистиллятов, FedWatch (дек. повышение), ETF-потоки, DXY. Честное ограничение: crack/запасы/FedWatch/ETF не покрыты ни datasource, ни ETL — живут в `macro_series` (`source='manual'`/`'webbridge'`), заполняются из сессии через ingest-endpoint или руками; флаг `stale=1` подсвечивает устаревшие значения вместо фальшивой автоматизации.

## 6. Плагин `gold-nav`

Отдельный публичный GitHub-репозиторий (получателям не нужен Go-проект):

```
gold-nav/
  kimi.plugin.json            # name: gold-nav, тип mcp+skills
  README.md                   # установка: Kimi Code (/plugins install <url>);
                              # Kimi Work (Plugin Builder → регистрация в personal market)
  skills/
    gold-forecast-session/SKILL.md   # сценарная сессия (6 шагов, ниже)
    dcf-methodology/SKILL.md         # формулы NAV, НДПИ, двухконтурная ставка, price decks, пороги
  commands/
    session.md                # /gold-nav:session
    verify.md                 # /gold-nav:verify — сверка forecast_log с фактом
  mcpServers:
    clickhouse: { "url": "https://<хост-автора>/mcp",
                  "headers": { "Authorization": "Bearer ${GOLD_NAV_MCP_TOKEN}" } }
```

Токен в репозиторий не зашивается: пользователь задаёт `GOLD_NAV_MCP_TOKEN` при установке; получателям выдаётся read-only токен, ingest-токен — только у автора.

**Сценарная сессия (6 шагов, воспроизводимо между средами и пользователями):**

1. Входы РФ — один запрос `v_model_inputs` (+ флаги `v_gold_dashboard`).
2. Входы мировые — datasource-плагины среды: `GC=F`, `DX-Y.NYB`, `HO=F`; при верификации — `fred_query` с `realtime_end` = дата прогноза (защита от look-ahead). SKILL.md описывает обе ветки: Kimi Code (kimi-datasource) и Kimi Work (Global Finance Data / IMF).
3. Календарь — ближайшие события `events_calendar` и их пороги.
4. Сценарии — пересчёт bull/base/bear по правилам статьи, нормировка к 100%, формат WGC Gold Outlook.
5. NAV — по методологии rev.2 (sum-of-parts; до готовности фазы 3 — упрощённый AISC×production, что фиксируется в run).
6. Запись — JSON run'а на ingest-endpoint → `model_runs` + `forecast_log`; локально markdown-выжимка.

## 7. Ingest-endpoint и безопасность

Новый пакет `ingest/` в `clickhouse-import-rosstat` (отдельная команда/бинарник):

- `POST /v1/model_run` — валидация (вероятности = 100 ±0.1; deck ∈ {spot_flat, consensus_lt, own_scenario}; обязательные поля), батч-вставка в `model_runs` + `forecast_log`.
- `POST /v1/manual_series` — ручные ряды (crack, FedWatch, ETF-потоки, запасы дистиллятов) в `macro_series` с `source='manual'`.
- Auth: Bearer `INGEST_TOKEN` из env; TLS через reverse proxy (Caddy/nginx в docker-compose фазы 0); rate limit; logrus; только два фиксированных контракта, никакого произвольного SQL.

| Компонент | Держатель | Права |
|---|---|---|
| mcp-clickhouse (HTTP) | автор + получатели (read-токен) | `kimi_reader`: SELECT только на `v_*` |
| ingest-endpoint | только автор | два POST-контракта |
| Токены в репозитории плагина | — | запрещены, только `${ENV}`-плейсхолдеры |
| Сырые таблицы | — | недоступны извне (проверено ACCESS_DENIED) |

## 8. Приёмка

1. `make all` зелёный; тесты валидации ingest (httptest).
2. Kimi Code: `/gold-nav:session` → входы получены → run в `model_runs` (проверка через MCP `run_query`).
3. Kimi Work: установка плагина из personal market → `/gold-nav:session` → тот же результат; живой тест Global Finance Data (`GC=F`).
4. Негативные: сырой запрос через MCP → ACCESS_DENIED; невалидный JSON → 400 без записи; без токена → 401.

## 9. Документы к изменению в рамках реализации

- `docs/ROADMAP_DCF_POLYUS.md` — пересмотр по §4 настоящей спеки;
- `ARCHITECTURE.md` — §6.1 (статусы импортёров), §6.3 (витрины), §6.5 (HTTP MCP, ingest), новый раздел про `gold-nav`;
- `README.md` — блок про плагин (по skill `sync-readme-architecture`).

## 10. Отклонённые варианты

- **Самописный семантический MCP-сервер** (get_dcf_inputs/write_model_run) — дублирует mcp-clickhouse + витрины; +1 компонент на поддержку. Оставлен как эволюция, если токены станут проблемой.
- **Datasource-only без ClickHouse для мировых данных** — теряется исторический базлайн, верификация на истории и Grafana.
- **Snapshot-экспортёр JSON/CSV как основной канал Kimi Work** — деградировал в опцию: спайк показал нативные каналы данных в Kimi Work.
