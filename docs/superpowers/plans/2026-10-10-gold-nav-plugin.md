# Gold-NAV Plugin + Datasource-ориентированный контур — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Пересмотреть роадмап под datasource-контур и построить работающий контур «сценарный прогноз за одну сессию»: витрины-однострочники, ingest-endpoint, плагин `gold-nav` для Kimi Code и Kimi Work.

**Architecture:** Мировые данные агент берёт из datasource-плагинов в момент сессии; РФ-данные — из ClickHouse через mcp-clickhouse (read-only, витрины `v_*`). Запись прогнозов — только через валидирующий HTTP ingest-endpoint (новый пакет `ingest/`, бинарник `cmd/ingest`). Плагин `gold-nav` — отдельный репозиторий (sibling-каталог), упаковка skill'ов + remote MCP.

**Tech Stack:** Go 1.27, clickhouse-go/v2 (native protocol), logrus, httptest (тесты); Kimi plugin manifest `kimi.plugin.json` (mcp+skills).

**Spec:** `docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md`

## Global Constraints

- Вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`; построчный `conn.Exec` в цикле запрещён.
- Секреты только через `os.Getenv`; никаких токенов/паролей в коде, комментариях, манифесте плагина (только `${ENV}`-плейсхолдеры).
- DDL всегда `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, измерения `LowCardinality(String)`, значения `Float64`; DDL новых таблиц — строго канонические из ARCHITECTURE.md §6.2.
- Витрины: `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`; комментарии через `ALTER TABLE ... MODIFY COMMENT` и `ALTER TABLE ... COMMENT COLUMN`; грант `kimi_reader` добавляется в `sql/mcp_kimi_reader.sql`.
- Команды, которым нужен ClickHouse или env-ключи, запускать только через `make` (Makefile подключает `~/.config/rosstat/env`); ClickHouse поднимает пользователь (`make ch-up`).
- Проверка после кода: `make all` (gofmt, golangci-lint, go vet, go test -race, сборка) зелёный.
- Коммиты: кратко, по-английски, в духе истории (`add fred importer`).

## Review Focus

1. `model_runs` пуст (ещё ни одного run'а) — `v_model_inputs` обязан отдать строку с NULL в колонках `last_run_*`, а не упасть и не отдать 0 строк; агент в сессии должен понять «модель ещё не считалась».
2. Дубль POST одного и того же run'а на ingest (ретрай агента) — `model_runs` с `run_id UUID DEFAULT generateUUIDv4()` создаст два run'а; клиент обязан передавать свой `run_id`, сервер обязан его принимать (ReplacingMergeTree схлопнет дубль по ORDER BY).
3. Ручной ряд с датой раньше 1970 (теоретически CPIAUCSL с 1947) — `macro_series.date` имеет тип `Date32` именно поэтому; ingest не должен резать дату до 1970.
4. Подстановка `${GOLD_NAV_MCP_TOKEN}` в `mcpServers` манифеста плагина — механика env-подстановки в Kimi Work не подтверждена документацией; при установке проверить, иначе README обязан давать обход (локальная правка манифеста после установки).
5. Ручной индикатор в `v_gold_dashboard`, которого ещё ни разу не вводили (нет строк в `macro_series` с этим `series`) — витрина обязана показать строку с `value NULL, stale 1`, а не потерять индикатор.

---

### Task 1: Пересмотр ROADMAP_DCF_POLYUS.md

**Files:**
- Modify: `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: —
- Produces: актуальный роадмап, на который ссылаются Task 6 и AGENTS.md-правило 10.

- [ ] **Step 1: Внести правки по спеке §4**

Точечные изменения (не переписывать документ):
- §2.2 «карта пробелов»: строкам eia/lbma/wgc/fedwatch/plzl добавить колонку-пометку или переписать столбец «Источник»: eia → «datasource `HO=F` (crack) + ручной ввод запасов», LBMA → «datasource `GC=F`; Nasdaq Data Link снят с критического пути», PLZL → «`moex_iss` ✅», WGC/FedWatch → «ручной ввод / WebBridge (фаза 5)».
- §7 фаза 1: удалить `eia`, нативный `lbma_gold`, `wgc`, `fedwatch` из плана импортёров; изменить статус `fred`/`bls`/`bea` на «исторический базлайн + Grafana + верификация; первичный источник для сессии — datasource».
- §7 фаза 2: добавить `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`, плагин `gold-nav`, mcp-clickhouse в HTTP-режиме с токеном, ingest-endpoint.
- Quick wins: заменить список на (1) `events_calendar` Q4-2026, (2) `v_model_inputs` + `v_gold_dashboard`, (3) плагин `gold-nav`.
- §11 (Kimi Work): дописать результаты спайка 2026-10-10 — плагины/персональный маркет/remote MCP в Kimi Work подтверждены, datasource-эквиваленты (Global Finance Data, IMF) доступны; добавить модель шаринга «хостинг у автора» и ссылку на спеку.

- [ ] **Step 2: Самопроверка консистентности**

Прогнать `Grep` по файлу: не осталось ли упоминаний `eia`-импортёра как планируемого Go-кода вне фазы 5. Ожидание: упоминания только в контексте datasource/ручного ввода/фазы 5.

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP_DCF_POLYUS.md
git commit -m "revise roadmap for datasource-first contour"
```

### Task 2: events_calendar Q4-2026 + витрина v_events_calendar

**Files:**
- Create: `sql/events_calendar_q4_2026.sql`
- Create: `calendar/events.go`
- Modify: `main.go` (blank-import пакета calendar)
- Modify: `sql/mcp_kimi_reader.sql`

**Interfaces:**
- Consumes: `util.CreateView(ctx, conn, util.View)` из `util/views.go`; канонический DDL `events_calendar` из ARCHITECTURE.md §6.2 (копировать как есть).
- Produces: таблица `events_calendar` с сидом Q4-2026; витрина `v_events_calendar` (все колонки + комментарии); импортёр `Name() = "events_calendar"`.

- [ ] **Step 1: Написать `sql/events_calendar_q4_2026.sql`**

`CREATE TABLE IF NOT EXISTS events_calendar` (канонический DDL из ARCHITECTURE.md §6.2) + `INSERT INTO events_calendar` одним батчем с событиями из ARCHITECTURE.md §7 (EIA ср еженед. — как recurring-комментарий в title одной записью на ближайшую среду; 23.10 СД ЦБ; 01.11 запрет дизеля; 10.11 CPI; 09.12 FOMC+dot plot; 18.12 СД ЦБ + ребалансировка MOEX; 14.10 CPI сент.; 06.11 NFP; 04.12 NFP; 10.12 CPI; ~конец окт. WGC GDT). `threshold` — JSON-строки по образцу `'{"crack":">50"}'`.

- [ ] **Step 2: Написать импортёр `calendar/events.go`**

Регистрация через `init()`: `Import()` выполняет DDL `CREATE TABLE IF NOT EXISTS events_calendar` (тот же канонический), затем `util.CreateView` для `v_events_calendar` (Select = `SELECT * FROM events_calendar FINAL`, Tables = `["events_calendar"]`, комментарий таблицы «Календарь событий-триггеров прогноза золота/NAV; status: pending|done|verified», комментарии каждой колонки). Сид данных — НЕ в Go (он в sql-файле из Step 1, идемпотентный за счёт ReplacingMergeTree).

- [ ] **Step 3: Добавить грант и blank-import**

`GRANT SELECT ON default.v_events_calendar TO kimi_reader;` в `sql/mcp_kimi_reader.sql`; `_ "github.com/kmlebedev/clickhouse-import-rosstat/calendar"` в `main.go`.

- [ ] **Step 4: Прогнать против живого ClickHouse**

Run: `make import STAT=events_calendar` (ClickHouse поднят пользователем), затем `clickhouse-client --multiquery < sql/events_calendar_q4_2026.sql` через `make`-обёртку с env.
Expected: `SELECT count() FROM v_events_calendar` ≥ 10.

- [ ] **Step 5: Commit**

```bash
git add sql/events_calendar_q4_2026.sql calendar/events.go main.go sql/mcp_kimi_reader.sql
git commit -m "add events_calendar Q4-2026 and v_events_calendar"
```

### Task 3: Ingest-endpoint (пакет ingest + cmd/ingest)

**Files:**
- Create: `ingest/model_run.go` (типы + валидация)
- Create: `ingest/server.go` (HTTP-сервер, хендлеры, запись в CH)
- Create: `ingest/schema.go` (EnsureTables: DDL `model_runs`, `forecast_log`)
- Create: `ingest/model_run_test.go`, `ingest/server_test.go`
- Create: `cmd/ingest/main.go`
- Modify: `Makefile` (цель `build-ingest`)

**Interfaces:**
- Consumes: `clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))` как в `main.go`; канонические DDL `model_runs`, `forecast_log`, `macro_series` из ARCHITECTURE.md §6.2 (копировать как есть).
- Produces:
  - `type ModelRun struct { RunID string `json:"run_id"`; TriggerType string `json:"trigger_type"`; TriggerRef string `json:"trigger_ref"`; GoldScenario map[string]any `json:"gold_scenario"`; UsdrubPath map[string]any `json:"usdrub_path"`; PriceDeck string `json:"price_deck"`; DiscountRate float64 `json:"discount_rate"`; Wacc float64 `json:"wacc"`; NavPerShare float64 `json:"nav_per_share"`; NavBull, NavBase, NavBear float64; MarketPrice float64 `json:"market_price"`; UpsidePct float64 `json:"upside_pct"`; Comment string `json:"comment"`; Probabilities map[string]float64 `json:"probabilities"` }`
  - `func ValidateModelRun(r ModelRun) error` — price_deck ∈ {spot_flat, consensus_lt, own_scenario}; trigger_type ∈ {calendar, news, manual}; sum(probabilities) ∈ 100±0.1; run_id обязателен (клиентский, для идемпотентности — Review Focus #2).
  - `type ManualSeriesPoint struct { Series string `json:"series"`; Date string `json:"date"`; Value float64 `json:"value"` }` (source фиксирован `'manual'`; Date формата `YYYY-MM-DD`, Date32 — даты до 1970 валидны, Review Focus #3).
  - HTTP: `POST /v1/model_run`, `POST /v1/manual_series` (массив точек), оба с `Authorization: Bearer $INGEST_TOKEN`; 200 `{ "inserted": N }`, 400 с текстом ошибки валидации, 401 без/с неверным токеном.

- [ ] **Step 1: Написать failing-тесты валидации `ingest/model_run_test.go`**

Тесты: валидный run (probability bull 25/base 40/bear 35) проходит; сумма 90 → ошибка; `price_deck="wacc_guess"` → ошибка; пустой `run_id` → ошибка; `trigger_type="cron"` → ошибка.

- [ ] **Step 2: Запустить тесты — убедиться, что падают**

Run: `go test ./ingest/ -run TestValidateModelRun -v`
Expected: FAIL (функция не определена).

- [ ] **Step 3: Реализовать `ValidateModelRun` в `ingest/model_run.go`**

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./ingest/ -v`
Expected: PASS.

- [ ] **Step 5: Написать failing-тесты HTTP-слоя `ingest/server_test.go`**

`httptest.NewServer` + поддельный writer-интерфейс (интерфейс `BatchWriter { EnsureTables(ctx) error; InsertModelRun(ctx, ModelRun) error; InsertManualSeries(ctx, []ManualSeriesPoint) error }`, мок в тесте): без токена → 401; невалидный JSON → 400 и мок не вызван; сумма вероятностей 90 → 400; валидный run → 200 и мок вызван с распарсенным `run_id`; `Date="1947-01-01"` в manual_series проходит валидацию (Date32).

- [ ] **Step 6: Реализовать `ingest/server.go`, `ingest/schema.go`, `ingest/clickhouse.go`**

`EnsureTables` — три канонических DDL `IF NOT EXISTS` (`model_runs`, `forecast_log`, `macro_series`); вставки только `PrepareBatch`/`Append`/`Send`. В `InsertModelRun`: `run_id` берётся из запроса (не DEFAULT), `gold_scenario`/`usdrub_path` сериализуются в JSON-строки; из того же run'а пишутся производные строки `forecast_log` — `metric='nav'` (predicted = `nav_per_share`) и `metric='xau_q_avg'` (predicted = вероятностно-взвешенная точка из `gold_scenario`/`probabilities`), `target_date` = конец горизонта сценария, `actual`/`error_pct` = NULL.

- [ ] **Step 7: `cmd/ingest/main.go` + цель Makefile**

env: `CLICKHOUSE_URL`, `INGEST_TOKEN` (пусто → Fatal), `INGEST_ADDR` (default `:8081`). Makefile: `build-ingest: go build -o build/ingest ./cmd/ingest`.

- [ ] **Step 8: `make all` зелёный + живой прогон**

Run: `make all`; затем `make`-обёрткой запустить `build/ingest` и `curl -X POST localhost:8081/v1/model_run` с валидным JSON.
Expected: 200, строка видна в `model_runs FINAL`; повторный POST с тем же `run_id` не создаёт вторую строку.

- [ ] **Step 9: Commit**

```bash
git add ingest/ cmd/ingest/ Makefile
git commit -m "add ingest endpoint for model_runs and manual series"
```

### Task 4: Витрины v_model_inputs и v_gold_dashboard

**Files:**
- Create: `views/model_inputs.go`, `views/gold_dashboard.go`
- Modify: `main.go` (blank-import `views`)
- Modify: `sql/mcp_kimi_reader.sql`

**Interfaces:**
- Consumes: `util.CreateView` (`util/views.go`); таблицы `gold_prices`, `cbr_currency_usd`, `cbr_key_rate`, `ofz_curve`, `ipc_mes`, `ipc_weeks`, `macro_series`, `stock_prices`, `model_runs` (Task 3 создаёт `model_runs`; CreateView пропускает витрину до появления всех таблиц — поэтому `Name()` импортёра витрин `gold_views` запускается после ingest-первого-прогона; зафиксировать в комментарии пакета).
- Produces: витрины со составом колонок из спеки §5.1/§5.2; импортёр `Name() = "gold_views"`.

- [ ] **Step 1: `views/model_inputs.go`**

`util.View{Name: "v_model_inputs", Tables: [9 таблиц выше], Select: ...}` — один ряд: по каждому ряду `argMax(value, date)` + `argMax(date, date)`; золото `venue='moex_fix_usd'`; FRED-ряды по `(source='fred', series=...)`; `last_run_*` — `argMax` по `model_runs FINAL` с `toNullable` (Review Focus #1: при пустой таблице — NULL, строка всё равно есть). Комментарии всех колонок (что измеряет, источник, дата актуальности).

- [ ] **Step 2: `views/gold_dashboard.go`**

`SELECT` из заранее зафиксированного списка индикаторов (`crack_ulsd_proxy`, `distillate_stocks`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy`) левым соединением к последним значениям `macro_series FINAL` по `(source IN ('manual','fred'), series)`; колонки: `indicator, value Nullable(Float64), threshold String, flag UInt8, as_of Nullable(Date32), stale UInt8` (`stale=1` если `as_of` NULL или старше 14 дней — Review Focus #5). Комментарий таблицы: пороги из статьи §6 (crack >50 bear / FedWatch >65% >80% / DXY >102).

- [ ] **Step 3: Гранты + blank-import + прогон**

`GRANT SELECT ON default.v_model_inputs TO kimi_reader;` и `... v_gold_dashboard ...` в `sql/mcp_kimi_reader.sql`; blank-import в `main.go`; `make import STAT=gold_views`.
Expected: `SELECT * FROM v_model_inputs` — ровно 1 строка; `SELECT * FROM v_gold_dashboard` — 5 строк, у невведённых `stale=1`.

- [ ] **Step 4: Commit**

```bash
git add views/ main.go sql/mcp_kimi_reader.sql
git commit -m "add v_model_inputs and v_gold_dashboard views"
```

### Task 5: Репозиторий плагина gold-nav

**Files:**
- Create: `../gold-nav/kimi.plugin.json`
- Create: `../gold-nav/README.md`
- Create: `../gold-nav/skills/gold-forecast-session/SKILL.md`
- Create: `../gold-nav/skills/dcf-methodology/SKILL.md`
- Create: `../gold-nav/commands/session.md`, `../gold-nav/commands/verify.md`

(Каталог-sibling `/Users/whitefox/GolandProjects/gold-nav`; публикацию на GitHub делает пользователь.)

**Interfaces:**
- Consumes: витрины Task 4 (`v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`); ingest-контракты Task 3 (`POST /v1/model_run`, `/v1/manual_series`, Bearer-токен); формат manifest'а из официальной документации плагинов Kimi.
- Produces: устанавливаемый плагин; команды `/gold-nav:session`, `/gold-nav:verify`.

- [ ] **Step 1: `kimi.plugin.json`**

```json
{
  "name": "gold-nav",
  "version": "0.1.0",
  "description": "Scenario gold forecast to DCF NAV of Polyus (PLZL); reads RF macro via hosted ClickHouse MCP",
  "skills": "./skills/",
  "commands": "./commands/",
  "mcpServers": {
    "clickhouse": {
      "url": "https://PLACEHOLDER_HOST/mcp",
      "headers": { "Authorization": "Bearer ${GOLD_NAV_MCP_TOKEN}" }
    }
  },
  "interface": { "displayName": "Gold NAV Forecast", "shortDescription": "Gold scenario forecast to PLZL NAV" }
}
```

- [ ] **Step 2: `skills/gold-forecast-session/SKILL.md`**

6 шагов спеки §6: (1) `v_model_inputs` + `v_gold_dashboard` через MCP; (2) мировые входы — две ветки (Kimi Code: kimi-datasource `GC=F`/`DX-Y.NYB`/`HO=F`, при верификации `fred_query` с `realtime_end`; Kimi Work: плагины Global Finance Data/IMF, тикеры те же); (3) `v_events_calendar` ближайшие события и пороги; (4) пересчёт bull/base/bear, нормировка к 100%; (5) NAV (до фазы 3 — упрощённый AISC×production, зафиксировать в comment run'а); (6) POST JSON на ingest (клиентский `run_id` = UUID сессии), markdown-выжимка локально. Формат записи прогноза — фиксированный JSON-шаблон (поля `ModelRun` Task 3).

- [ ] **Step 3: `skills/dcf-methodology/SKILL.md` + `commands/*.md`**

Методология rev.2 из спеки/ARCHITECTURE §6.4 (sum-of-parts, НДПИ `10%×max(gold−1900,0)`, двухконтурная ставка 5% real USD + ОФЗ, price decks, пороги триггеров из статьи). `session.md` — frontmatter description + вызов сценария; `verify.md` — чтение `forecast_log`, сверка predicted vs actual, отчёт об ошибках.

- [ ] **Step 4: README.md**

Установка: Kimi Code — `/plugins install <github-url>`; Kimi Work — Plugin Builder → регистрация в personal market (`kimi-daimon kimi-plugin register-personal`); настройка `GOLD_NAV_MCP_TOKEN` (выдаёт автор); явно описать обход, если env-подстановка в `mcpServers.headers` не сработает (Review Focus #4): отредактировать установленную копию манифеста.

- [ ] **Step 5: Локальная регистрация и проверка в Kimi Work**

`kimi-daimon kimi-plugin register-personal ../gold-nav` (путь из SKILL plugin-builder); в Kimi Work «Плагины → Персональный» плагин виден и включается. Зафиксировать результат (в т.ч. работоспособность `${GOLD_NAV_MCP_TOKEN}`) в README.

- [ ] **Step 6: Commit**

```bash
cd ../gold-nav && git init && git add -A && git commit -m "initial gold-nav plugin"
```

### Task 6: Синхронизация ARCHITECTURE.md и README.md

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`

**Interfaces:**
- Consumes: результаты Task 1–5; skill `sync-readme-architecture`.
- Produces: §6.1 (статусы импортёров), §6.2 (без изменений DDL), §6.3 (три новые витрины), §6.5 (HTTP-режим MCP + ingest-endpoint + пакет `ingest/`), новый подраздел про `gold-nav`; README — блок подключения плагина.

- [ ] **Step 1: Внести правки по skill `sync-readme-architecture`**

Вызвать `Skill(sync-readme-architecture)` и следовать его чек-листу: структура репозитория (пакеты `calendar/`, `views/`, `ingest/`, `cmd/ingest/`), таблица §6.1 (статусы + datasource-пометки), §6.3 (v_model_inputs, v_gold_dashboard, v_events_calendar), §6.5 (ingest-контракты и токены без значений), ссылка на спеку и плагин.

- [ ] **Step 2: Commit**

```bash
git add ARCHITECTURE.md README.md
git commit -m "sync architecture and readme with gold-nav contour"
```

### Task 7: Приёмка end-to-end

**Files:**
- Modify: `docs/superpowers/plans/2026-10-10-gold-nav-plugin.md` (отметки о прохождении)

- [ ] **Step 1: `make all` зелёный** (gofmt, golangci-lint, go vet, go test -race, сборка обоих бинарников).
- [ ] **Step 2: Kimi Code:** в новой сессии вызвать `/gold-nav:session` → run появился в `model_runs` (проверка через MCP `run_query` от `kimi_reader`).
- [ ] **Step 3: Kimi Work:** `/gold-nav:session` → тот же результат; заодно живой запрос `GC=F` через Global Finance Data.
- [ ] **Step 4: Негативные проверки:** `run_query` к сырой таблице (`macro_series`) от `kimi_reader` → ACCESS_DENIED; `curl` на ingest без токена → 401; JSON с суммой вероятностей 90 → 400 и в БД ничего не записано.
- [ ] **Step 5: Отчёт пользователю** — результаты 4 пунктов + список известных ограничений (GC=F история ≤2 лет, ручные индикаторы со `stale=1` до наполнения).
