# Фаза 1 контура золота: COMEX-цена и резервы ЦБ — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** добавить в ingest-контур запись цены COMEX (`gold_prices.venue='comex_front'`) и золотых резервов 7 ЦБ (IMF RA_GOLD, `macro_series.source='imf'`), витрину `v_imf_macro` и каталоги рядов; разово заполнить данные.

**Architecture:** запись — только через ingest-endpoint (новый `POST /v1/gold_prices` + опциональный `source` в `/v1/manual_series`); чтение — витрина `v_imf_macro` (шаблон `v_fred_macro`) и существующая `v_gold_prices` (venue-agnostic); каталог — `gold/series_meta.go` для source='gold' и сид `sql/` для source='imf' (у него нет импортёра).

**Tech Stack:** Go 1.27, clickhouse-go/v2 (только батчи), CLICKHOUSE via `make`; данные — kimi-datasource (yahoo `GC=F`, IMF `imf_irfcl_data`).

**Spec:** [docs/superpowers/specs/2026-10-11-gold-phase1-comex-cb-reserves-design.md](../specs/2026-10-11-gold-phase1-comex-cb-reserves-design.md)

## Global Constraints

- Вставка только батчами: `PrepareBatch → Append (err checked) → Send`, `Abort` только в ветке Append.
- DDL — `CREATE TABLE IF NOT EXISTS`; копия DDL `gold_prices` в ingest обязана совпадать с `gold/gold.go` (прецедент `company_financials`).
- Витрины: `CREATE OR REPLACE VIEW … DEFINER = default SQL SECURITY DEFINER`, комментарии через `ALTER TABLE … MODIFY COMMENT` / `COMMENT COLUMN`; каждая новая витрина получает `GRANT SELECT` в `sql/mcp_kimi_reader.sql`.
- Команды с окружением — только через `make` (env-файл подключает Makefile); секреты в код/логи не писать.
- Проверка на выходе — `make all` (gofmt, golangci-lint, go vet, go test -race, сборка).
- Все тексты (комментарии, доки) — по-русски, стиль репозитория.

## Review Focus

- `source` в `/v1/manual_series`: отсутствие поля и пустая строка — оба означают `manual`; невалидный source — 400 и **ни одной записи**.
- Существующий тест `TestManualSeriesUnknownFieldIsRejected` сейчас шлёт именно `"source"` как «неизвестное поле» — после добавления поля он обязан быть переработан на настоящее неизвестное поле, иначе `DisallowUnknownFields` останется без регресс-теста.
- `venue` сверяется точно (`comex_front`); любое иное значение (в т.ч. другой регистр) — 400.
- `usd` отсутствует в JSON → 0 → 400 (`usd > 0` — инвариант цены).
- Дата: `2026-1-1` и `2026-13-01` — 400 (строгая проверка формата, как в manual_series).

---

### Task 1: ingest — `POST /v1/gold_prices`

**Files:**
- Modify: `ingest/model_run.go` (тип точки + валидация; рядом с `ManualSeriesPoint`)
- Modify: `ingest/clickhouse.go` (константа insert + метод `InsertGoldPrices`)
- Modify: `ingest/server.go` (маршрут + handler + интерфейс `BatchWriter`)
- Modify: `ingest/schema.go` (DDL-копия `gold_prices`, добавить в `ensureTables`)
- Test: `ingest/server_test.go`, `ingest/clickhouse_test.go`

**Interfaces:**
- Produces: `type GoldPricePoint struct { Venue string `json:"venue"`; Date string `json:"date"`; Usd float64 `json:"usd"` }`; `func ValidateGoldPricePoint(p GoldPricePoint) error`; `var validGoldVenues = map[string]bool{"comex_front": true}`; `BatchWriter.InsertGoldPrices(ctx context.Context, points []GoldPricePoint) error`; `const goldPricesInsert = "INSERT INTO gold_prices (venue, date, usd)"`; `const goldPricesDdl` в `ingest/schema.go`.

- [ ] **Step 1: Написать падающие тесты**

В `ingest/server_test.go`: расширить `mockBatchWriter` полем `goldPrices [][]GoldPricePoint` и методом `InsertGoldPrices` (append в поле, вернуть `m.err`). Тесты:

```go
func TestGoldPricesOK(t *testing.T) {
	writer := &mockBatchWriter{}
	server := newTestServer(writer)
	defer server.Close()

	const body = `[
		{"venue": "comex_front", "date": "2026-10-09", "usd": 4216.3},
		{"venue": "comex_front", "date": "2026-10-08", "usd": 4157.0}
	]`
	status, payload := post(t, server.URL+"/v1/gold_prices", testToken, body)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, payload)
	}
	// ответ {"inserted":2}; writer.goldPrices[0] содержит обе точки, Venue/Date/Usd сохранены
}

func TestGoldPricesValidationErrors(t *testing.T) {
	// таблица кейсов, каждый — 400 и len(writer.goldPrices) == 0:
	//   venue "Comex_Front" (регистр), venue "spot" (не в whitelist),
	//   date "2026-1-1", date "2026-13-01", usd 0, usd -1
}
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test ./ingest/ -run 'TestGoldPrices' -v`
Expected: FAIL — маршрут `/v1/gold_prices` отдаёт 404/405 (handler не зарегистрирован).

- [ ] **Step 3: Реализация**

- `ingest/model_run.go`: `GoldPricePoint` + `ValidateGoldPricePoint` (venue — whitelist `validGoldVenues`, ошибка перечисляет допустимое; date — `manualSeriesDateLayout` + сравнение `Format == вход`; `usd > 0`).
- `ingest/clickhouse.go`: `goldPricesInsert`; `InsertGoldPrices` — пустой срез → nil; батч; `time.Parse` даты (ошибка → `Abort`); `Append(venue, date, usd)`; `Send`.
- `ingest/server.go`: метод в интерфейсе `BatchWriter`; `s.mux.HandleFunc("POST /v1/gold_prices", s.handleGoldPrices)`; handler по образцу `handleManualSeries` (валидация всех точек до записи, 400 на первую ошибку, `writeInserted(w, len(points))`).
- `ingest/schema.go`: `goldPricesDdl` — копия из `gold/gold.go` дословно; добавить в цикл `ensureTables` с комментарием про обязательное совпадение копий.

- [ ] **Step 4: Тест writer'а (`ingest/clickhouse_test.go`)**

`TestInsertGoldPricesAppendsRows`: через `newTestWriter(store)` вставить 2 точки → `store.batchesFor(goldPricesInsert) == 1`, в `sent` 2 строки с полями в порядке `(venue, date, usd)`; `usd` — float64, `date` — `time.Time`.
`TestInsertGoldPricesAppendErrorAborts`: `store.failAppend[goldPricesInsert]` → ошибка, `abortedFor == 1`, `batchesFor == 0`.
Пустой срез → `len(store.sent) == 0`.

- [ ] **Step 5: Прогнать тесты**

Run: `go test ./ingest/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ingest/
git commit -m "ingest: add gold_prices endpoint"
```

### Task 2: ingest — `source` в `POST /v1/manual_series`

**Files:**
- Modify: `ingest/model_run.go` (+`Source` в `ManualSeriesPoint`, whitelist; обновить комментарий `macroSeriesDdl` в `ingest/schema.go` на фактические source)
- Modify: `ingest/clickhouse.go` (`InsertManualSeries` использует `point.Source`, пусто → `manualSeriesSource`)
- Test: `ingest/server_test.go`, `ingest/clickhouse_test.go`

**Interfaces:**
- Produces: `Source string \`json:"source"\`` в `ManualSeriesPoint`; `var validManualSources = map[string]bool{"manual": true, "webbridge": true, "imf": true}`. Поведение без поля — прежнее (`manual`).

- [ ] **Step 1: Переработать и добавить тесты (падающие)**

- `TestManualSeriesUnknownFieldIsRejected` — заменить `"source": "fred"` на настоящее неизвестное поле (например `"units": "pct"`), тест сохраняет смысл «DisallowUnknownFields режет мусор».
- `TestManualSeriesSourceAccepted`:

```go
const body = `[{"series": "ra_gold_pol", "date": "2026-08-01", "value": 45600, "source": "imf"}]`
// 200; writer.series[0][0].Source == "imf"
```

- `TestManualSeriesSourceRejected`: `"source": "fred"` → 400, `len(writer.series) == 0`.
- `TestManualSeriesSourceDefault`: без поля → 200; `Source` в writer'е пустая строка (дефолт применяет writer — проверить в clickhouse-тесте).

- [ ] **Step 2: Убедиться, что новые тесты падают**

Run: `go test ./ingest/ -run 'TestManualSeries' -v`
Expected: `TestManualSeriesSourceAccepted` FAIL (неизвестное поле → 400).

- [ ] **Step 3: Реализация**

- `ManualSeriesPoint.Source`; в `ValidateManualSeriesPoint` — `if p.Source != "" && !validManualSources[p.Source] { … }` с перечислением допустимых.
- `InsertManualSeries`: `source := point.Source; if source == "" { source = manualSeriesSource }` → `Append(source, …)`.
- `ingest/schema.go`: комментарий `macroSeriesDdl` `-- 'fred','eia','wgc','cme'` → `-- 'fred','bls','bea','manual','webbridge','imf'` (фактические значения).

- [ ] **Step 4: Тест writer'а**

`TestInsertManualSeriesSourcePassedThrough`: точки с `Source: "imf"`, `Source: ""` → в батче source `"imf"` и `"manual"` соответственно.

- [ ] **Step 5: Прогнать тесты**

Run: `go test ./ingest/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ingest/
git commit -m "ingest: allow imf source in manual_series"
```

### Task 3: витрина `v_imf_macro` + грант

**Files:**
- Create: `views/imf_macro.go`
- Modify: `views/gold_views.go` (добавить в `goldViewsList`; строка списка — единственный источник правды для тестов, см. комментарий в файле)
- Modify: `sql/mcp_kimi_reader.sql` (`GRANT SELECT ON default.v_imf_macro TO kimi_reader`)
- Test: `views/imf_macro_test.go`

**Interfaces:**
- Produces: `var imfMacroView = util.View{ Name: "v_imf_macro", Tables: []string{"macro_series"}, Select: "SELECT date, series, value FROM macro_series FINAL WHERE source = 'imf'", … }`; `goldViewsList = []util.View{modelInputsView, goldDashboardView, forecastAccuracyView, dcfAssumptionsView, imfMacroView}`.

- [ ] **Step 1: Написать падающий тест**

`views/imf_macro_test.go` по образцу `TestDcfAssumptionsViewIsBuiltByGoldViews` (`views/dcf_assumptions_test.go:90`):

```go
func TestImfMacroViewIsBuiltByGoldViews(t *testing.T) {
	// imfMacroView.Name присутствует в goldViewsList; Comment не пуст;
	// Columns описывают date/series/value; Select содержит "source = 'imf'"
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./views/ -run TestImfMacroView -v`
Expected: FAIL — `imfMacroView` не существует.

- [ ] **Step 3: Реализация**

- `views/imf_macro.go`: `imfMacroView` с комментарием таблицы («Золотые резервы (monetary gold) центробанков из IMF IRFCL: 7 стран (POL, CHN, RUS, UZB, KAZ, CZE, TUR), единицы — млн USD по рыночной стоимости; описание каждого ряда — в v_series_catalog») и комментариями колонок (`date` — «Первый день месяца наблюдения», `series` — «Код ряда: ra_gold_<iso3>», `value` — «млн USD, рыночная стоимость (не тонны)»).
- `views/gold_views.go`: добавить `imfMacroView` в список.
- `sql/mcp_kimi_reader.sql`: грант (рядом с остальными витринами).

- [ ] **Step 4: Прогнать тесты**

Run: `go test ./views/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add views/ sql/mcp_kimi_reader.sql
git commit -m "views: add v_imf_macro for IMF gold reserves"
```

### Task 4: каталоги рядов

**Files:**
- Modify: `gold/series_meta.go` (+ запись `comex_front`; обновить комментарий `goldPricesView`: «Сейчас один ряд» → две площадки)
- Create: `gold/series_meta_test.go`
- Create: `sql/imf_ra_gold_catalog.sql`

**Interfaces:**
- Produces: `util.SeriesMeta{Source: "gold", Series: "comex_front", Unit: "USD за унцию (фьючерс, не спот)", Frequency: "D", …}`; сид-файл с 7 строками `INSERT INTO series_catalog` (`source='imf'`, `series` = `ra_gold_pol|chn|rus|uzb|kaz|cze|tur`).

- [ ] **Step 1: Написать падающий тест (`gold/series_meta_test.go`)**

Проверка: `goldSeriesMeta` содержит записи для обоих venue (`moex_fix_usd`, `comex_front`) и у каждой непустые Title/Unit/Frequency/Origin/Description (образец — `moex/series_meta_test.go`).

Run: `go test ./gold/ -run TestSeriesMeta -v`
Expected: FAIL — `comex_front` не описан.

- [ ] **Step 2: Реализация**

- `gold/series_meta.go`: запись `comex_front` — Origin «Yahoo Finance GC=F (фронт-месяц COMEX), сессионный ввод через POST /v1/gold_prices»; Description — «Цена фронт-месяца фьючерса COMEX из сессионного канала datasource; ряд начинается с разового backfill 2 лет (лимит источника); **не спот и не LBMA** — базис фьючерса к споту не устраняется; пишется сессионно, не импортёром». Обновить Comment `goldPricesView` (две площадки).
- `sql/imf_ra_gold_catalog.sql` — по образцу `sql/gold_dashboard_manual_series_catalog.sql`: шапка (назначение, применение `make ch-sql FILE=…`, идемпотентность ReplacingMergeTree по `(source, series)`), 7 строк: Title «Золотые резервы: <страна> (IMF IRFCL, monetary gold)», Unit «млн USD (рыночная стоимость)», Frequency «M», Origin «IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source='imf')», Description — что измеряет + как читать дельту: «Денежное золото резервов в млн USD по рыночной стоимости (не тонны); месячная точка — первый день месяца. Дельта месяца ≈ изменение стоимости; для тонн требуется деление на цену золота. Лаг публикации ~2 месяца».

- [ ] **Step 3: Прогнать тест**

Run: `go test ./gold/ -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add gold/ sql/imf_ra_gold_catalog.sql
git commit -m "gold: describe comex_front and imf reserve series"
```

### Task 5: ARCHITECTURE и README

**Files:**
- Modify: `ARCHITECTURE.md` (§6.5 — endpoint `/v1/gold_prices` и `source` в `/v1/manual_series`; §6.2 — комментарий source у `macro_series` и примечание о копии DDL `gold_prices` в ingest; §6.3 — `v_imf_macro` в списке витрин; шапка «Последнее обновление»)
- Modify: `README.md` — по чек-листу скила `sync-readme-architecture` (затронуты: endpoint'ы ingest, витрина, `sql/`-сид)

- [ ] **Step 1: Применить скил `sync-readme-architecture`** (`.kimi-code/skills/sync-readme-architecture/SKILL.md`) и внести перечисленные правки.
- [ ] **Step 2: Проверка**: `grep -n "gold_prices\|v_imf_macro" ARCHITECTURE.md README.md` — каждое упоминание соответствует коду.
- [ ] **Step 3: Commit**

```bash
git add ARCHITECTURE.md README.md
git commit -m "docs: sync architecture with phase 1 ingest and view"
```

### Task 6: данные, приёмка, статус роадмапа

**Files:**
- Modify: `docs/GOLD_THESIS_DATA_ROADMAP.md` (Фаза 1 → ✅, runbook, §1/§6-факты)
- Apply: `sql/imf_ra_gold_catalog.sql`, `make import STAT=gold_views`, `make mcp-user`

- [ ] **Step 1: Предусловие — пользователь выполнил `/login`** (datasource сбрасывает токен; без него шаг 2 невозможен).
- [ ] **Step 2: Backfill.** `GC=F` (period `2y`, interval `1d`) через datasource → JSON → `POST /v1/gold_prices`; `RA_GOLD` × 7 стран (`imf_irfcl_data`, `start_year=2024`, `frequency=M`) → `POST /v1/manual_series` с `source='imf'`, дата — первый день месяца. Сервер: `INGEST_TOKEN=… make run-ingest` (или env-файл).
- [ ] **Step 3: БД и витрины.** `make ch-sql FILE=sql/imf_ra_gold_catalog.sql`; `make import STAT=gold_views`; `make mcp-user`.
- [ ] **Step 4: Проверки (MCP, `kimi_reader`).**
  - `SELECT count(), min(date), max(date) FROM v_gold_prices WHERE series='comex_front'` ≈ 500, до 2026-10-09;
  - `SELECT series, max(date) FROM v_imf_macro GROUP BY series` — 7 рядов;
  - дельты одним запросом (пример для PR): YTD по странам через `argMaxIf`/`argMinIf`;
  - `SELECT … FROM gold_prices` под `kimi_reader` → `ACCESS_DENIED` (сырая таблица закрыта);
  - `make mcp-check` — зелёный.
- [ ] **Step 5: Роадмап.** Фаза 1 → ✅ с runbook и фактами; §1: строка «ЦБ-покупки» ⚠️→✅ (IMF), строка цены — +`comex_front`; §6: новые факты для статьи.
- [ ] **Step 6: `make all`, Commit, PR в конце**

```bash
git add docs/GOLD_THESIS_DATA_ROADMAP.md
git commit -m "docs: phase 1 done — comex price and cb reserves in contour"
```

---

## Self-Review (выполнен при написании)

- **Покрытие спеки:** §3.1 → Task 1–2; §3.2 → Task 3–4; §3.3 → Task 6; §3.4 → тесты Task 1–2; §3.5 → тесты в каждом; §5 (что не входит) — ни один таск не трогает `v_cb_gold_dynamics`/gold-nav/staging. ✔
- **Типы согласованы:** `GoldPricePoint{venue,date,usd}`, `InsertGoldPrices`, `goldPricesInsert/Ddl`, `imfMacroView`, `goldViewsList` — имена совпадают во всех тасках. ✔
- **Review Focus** — каждый пункт привязан к тесту (Task 1 step 1, Task 2 step 1). ✔
- **Пропорция:** план ≈ спека по объёму; тела функций не переписаны, даны сигнатуры и значения. ✔
