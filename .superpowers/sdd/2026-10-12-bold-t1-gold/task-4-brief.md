### Task 4: витрины, гранты, mcp-check и расписание

**Files:**
- Create: `views/bold_etf_daily.go`, `views/bold_etf_daily_test.go`, `dagu/bold.yaml`
- Modify: `views/gold_views.go` (срез `goldViewsList`), `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`

**Interfaces:**
- Consumes: имена рядов и таблицы из Task 1–2; `util.View` (`Name`, `Tables`, `Select`, `Comment`, `Columns`), `views.goldViewsList` (`views/gold_views.go:35`).
- Produces: `var goldEtfFlowsDailyView = util.View{Name: "v_gold_etf_flows_daily", Tables: []string{"macro_series"}, …}` и `var boldFundsDailyView = util.View{Name: "v_gold_etf_funds_daily", Tables: []string{"bold_fund_snapshot"}, …}`.

- [ ] **Step 1: Написать падающие тесты**

```go
func TestGoldEtfFlowsDailyViewShape(t *testing.T) // имя, Tables == [macro_series], Select содержит все 7 имён рядов и `source = 'bold'`, порядок `date DESC`
func TestBoldFundsDailyViewShape(t *testing.T)    // имя, Tables == [bold_fund_snapshot], Select содержит bold_fund_snapshot FINAL и порядок `date DESC, ticker, name`
func TestBoldViewsRegistered(t *testing.T)        // обе витрины входят в goldViewsList
func TestBoldViewsHaveComments(t *testing.T)       // Comment непустой у обеих, и Columns покрывает все выходные колонки: 8 у v_gold_etf_flows_daily, 10 у v_gold_etf_funds_daily
```
(образец — `views/gold_etf_flows_test.go`; комментарии таблицы и каждой колонки — требование правила 11, поэтому непустоту `Comment`/`Columns` проверяем явно, а не «если тест есть»).

- [ ] **Step 2: Реализовать витрины и регистрацию**

- `views/bold_etf_daily.go`: пивот по образцу `views/gold_etf_flows.go` — mapping-массив `(series, metric)` для 7 рядов, `IN (...)` с именами, `maxIfOrNull` по метрикам, `GROUP BY date`, `ORDER BY date DESC`; комментарий таблицы — из спеки §4.4 (73 фонда, ≈83 %, не складывать с WGC, календарные окна, bold.report/ByteTree); комментарий каждой колонки.
- `views/boldFundsDailyView`: `SELECT date, ticker, name, units_oz, aum_usd, price_usd, units_change_oz, aum_change_usd, shares, shares_change FROM bold_fund_snapshot FINAL ORDER BY date DESC, ticker` + комментарии (в т.ч. «история копится с 2026-10; у источника пер-фондовой истории нет»).
- `views/gold_views.go:35` — добавить обе переменные в `goldViewsList`.

- [ ] **Step 3: Гранты и mcp-check**

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_gold_etf_flows_daily TO kimi_reader;` и `GRANT SELECT ON default.v_gold_etf_funds_daily TO kimi_reader;` (рядом с существующими).
- `scripts/mcp_check.py`: в блок ETF добавить видимость двух витрин (`list_tables`), читаемость (`count()`), `SELECT count() AS n FROM v_series_catalog WHERE source='bold'` → ожидание `7`, и недоступность сырой `bold_fund_snapshot` (`ACCESS_DENIED`/«Not enough privileges») по образцу проверки `macro_series`.

- [ ] **Step 4: Расписание**

`dagu/bold.yaml` — по образцу `dagu/wgc.yaml`: `type: chain`, `schedule: "7 8 * * *"`, `overlap_policy: skip`, шаги `bold_gold_price` → `bold_etf_flows`, `retry_policy: {limit: 3, interval_sec: 300}`; комментарий-шапка: данные источника готовы к 07:32 МСК, инкремент (4 запроса у ETF-импортёра + 1 у ценового) и полный режим; всего до 5 запросов в сутки — режим щадящий.

- [ ] **Step 5: Проверки и commit**

Run: `go test ./views/... -v -count=1 && make all`
Expected: PASS, `make all` зелёный (gofmt, golangci-lint 0 issues, `go vet`, `go test -race`, обе сборки).

```bash
git add views/bold_etf_daily.go views/bold_etf_daily_test.go views/gold_views.go sql/mcp_kimi_reader.sql scripts/mcp_check.py dagu/bold.yaml
git commit -m "add bold daily views, grants, mcp checks and schedule"
```

---

