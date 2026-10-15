### Task 4: Импортёр `cbr/mo_br.go` + префлайт + проводка (реестр, dagu, MCP)

**Files:**
- Create: `cbr/mo_br.go`, `cbr/mo_br_test.go`
- Modify: `cbr/importers.go` (14-я запись), `cbr/importers_test.go` (allowlist `macro_series`), `dagu/cbr.yaml` (шаг `cbr_mo_br` последним), `sql/mcp_kimi_reader.sql` (грант `v_cbr_mo_br`), `scripts/mcp_check.py` (две строки)

**Interfaces:**
- Consumes: `parseMoBr` (T3), `util.EnsurePublications`/`HeadFingerprint`/`PublicationKnown`/`SavePublication`/`Publication` (T2), `getPage`/`findLink`/`cbrUrl` (`cbr/links.go`), `util.ClickHouseImport` (паттерн `cbr/mp_survey.go`).
- Produces: `moBrImporter` с `Name() == "cbr_mo_br"` (TableName `macro_series`); витрина `v_cbr_mo_br`; шаг dagu `cbr_mo_br`.

- [ ] **Step 1: Написать тесты** (`cbr/mo_br_test.go`)

- `TestMoBrImporterNameAndTable`: `moBrImporter.Name() == "cbr_mo_br"`, `TableName == "macro_series"`;
- `TestMoBrDdlPinsMacroSeries`: DDL содержит `macro_series`, `ReplacingMergeTree`, `ORDER BY (source, series, date)`;
- в `cbr/importers_test.go`: allowlist проверки (б) — `macro_series` разрешён для `cbr_mp_survey` **и** `cbr_mo_br` (правка условия), число записей реестра 14, `dagu/cbr.yaml` содержит `cbr_mo_br`.

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./cbr/ -run 'MoBr|CbrImporters' -v` → FAIL (нет `moBrImporter`).

- [ ] **Step 3: Реализовать `cbr/mo_br.go`**

```go
const (
    moBrPageUrl   = "https://www.cbr.ru/statistics/ddkp/mo_br/"
    moBrLinkSel   = `a[href$="full.xlsx"]`
    moBrName      = "cbr_mo_br"
    moBrTable     = "macro_series"
)
const moBrDdl = `CREATE TABLE IF NOT EXISTS %s ( … как у mpSurveyDdl … )` // своя копия по прецеденту пакета
```

`Import(ctx, conn)`:
1. `getPage(moBrPageUrl)` → `findLink(page, moBrLinkSel)` → `url := cbrUrl + href`, `name := path.Base(url)`;
2. `fp, fpErr := util.HeadFingerprint(url)`; при `fpErr` — `log.Warnf` и полный импорт (деградация, без записи версии);
3. `util.EnsurePublications(ctx, conn)`; если `fp != ""`: `points, known, _ := util.PublicationKnown(ctx, conn, moBrName, name, fp)`; если `known`: `dbPoints := SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'`; равны → `log.Infof("mo_br: no-op, версия %s уже импортирована, %d точек на месте", …)`, `return int64(points), nil`;
4. полный путь: DDL → `util.GetXlsx(url)` → `parseMoBr` → батч `PrepareBatch("INSERT INTO macro_series")` (`Append(source, series, date, value)`, `err!`) → `count = batch.Rows()` → `Send` → при `fp != ""` `util.SavePublication(…FirstSeen: time.Now(), Points: uint64(count))` → `util.UpsertSeriesCatalog(ctx, conn, meta)` → `util.CreateView(ctx, conn, vCbrMoBrView)`;
5. `vCbrMoBrView = util.View{Name: "v_cbr_mo_br", Tables: []string{"macro_series"}, Select: "SELECT source, series, date, value FROM macro_series FINAL WHERE source = 'cbr_mo_br'", Comment: …, Columns: …}` — комментарии по спеке §3.5 (дата волны/публикация +2–6 дней; прогнозы ≠ факты; `answers` — число ответов; ссылка на `source_publications`).

Регистрация: `var moBrImporter = &moBrStat{ClickHouseImport: util.ClickHouseImport{TableName: moBrTable, CreateTable: []string{moBrDdl}}}`; в `cbr/importers.go` — `{moBrImporter, "macro_series"}`; в `dagu/cbr.yaml` шаг последним:

```yaml
  - id: cbr_mo_br
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=cbr_mo_br
    retry_policy:
      limit: 3
      interval_sec: 300
```

`sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_cbr_mo_br TO kimi_reader;`. `scripts/mcp_check.py` (в cbr-секцию рядом с `v_cbr_survey`): `SELECT count() FROM v_cbr_mo_br` → expect > 0; `SELECT count() FROM v_series_catalog WHERE source = 'cbr_mo_br'` → expect > 0.

- [ ] **Step 4: Прогнать — PASS + проверки проводки**

Run: `go test ./cbr/ -run 'MoBr|CbrImporters' -v && go test ./cbr/ -count=1 && go build ./... && gofmt -l cbr/` → зелёные.

Run: `grep -c "  - id:" dagu/cbr.yaml` → 14; `dagu validate dagu/cbr.yaml` → exit 0; `python3 -m py_compile scripts/mcp_check.py` → ok.

- [ ] **Step 5: Commit**

```bash
git add cbr/mo_br.go cbr/mo_br_test.go cbr/importers.go cbr/importers_test.go dagu/cbr.yaml sql/mcp_kimi_reader.sql scripts/mcp_check.py
git commit -m "cbr: cbr_mo_br importer with source_publications preflight, view, registry and dagu step"
```

---

