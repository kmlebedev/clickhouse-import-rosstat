### Task 6: Импортёр `cbr_mp_summary` + проводка (реестр, dagu, гранты, MCP, витрины)

**Files:**
- Create: `cbr/mp_summary.go`, `cbr/mp_summary_test.go`
- Modify: `cbr/importers.go` (15-я запись), `cbr/importers_test.go` (позитивная проверка dagu-шага), `dagu/cbr.yaml` (шаг `cbr_mp_summary` последним), `sql/mcp_kimi_reader.sql` (три гранта), `scripts/mcp_check.py` (три чтения + закрытость трёх сырых)

**Interfaces:**
- Consumes: всё выше; `getPage`/`cbrUrl` (`cbr/links.go`, `cbr/households.go`).
- Produces: `mpSummaryImporter` с `Name() == "cbr_mp_summary"`; витрины `v_cbr_mp_summary`, `v_cbr_mp_summary_blocks`, `v_cbr_mp_summary_channels`; шаг dagu `cbr_mp_summary`.

- [ ] **Step 1: Написать тесты** (`cbr/mp_summary_test.go`): `TestMPSummaryImporterNameAndTable` (Name == "cbr_mp_summary", TableName == "cbr_mp_summary" — литералами); `TestMPSummaryDdlPins` — три DDL-литерала содержат `cbr_mp_summary`, `cbr_mp_summary_block`, `cbr_mp_summary_channel`, ReplacingMergeTree и ожидаемые ключи ORDER BY; `TestMPSummaryViewPins` — `Select` трёх витрин содержит фильтры (`source`-фильтров нет — таблицы свои; проверить `FINAL` и имена таблиц); в `cbr/importers_test.go` — реестр 15 записей: запись `{mpSummaryImporter, "cbr_mp_summary", ""}` (третья колонка пуста — префлайта нет, дельта по индексу; тест (д) post-#55 требует пустые обе стороны) и явная обратная проверка `scheduled["cbr_mp_summary"]` по образцу владельцев `macro_series` (иначе пропавший шаг dagu не заметит ни один тест).

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать `cbr/mp_summary.go`** (`Import(ctx, conn)`):
  1. `conn.Exec` × 3 DDL-литерала (таблицы фиксированных имён);
  2. `getPage(mpSummaryIndexUrl)` → `parseMPDecIndex` → известные url: `SELECT DISTINCT url FROM cbr_mp_summary FINAL`; отсутствующие — `getPage(cbrUrl + href)` + `parseMPSummary` (+`Published` из ref);
  3. если нет новых и нет отстающих LLM — `log.Infof("cbr_mp_summary: no-op, все %d резюме на месте", N)` и `return 0, nil`;
  4. батч №1 шапка, №2 блоки, №3 regex-каналы (по документу: `tagMPChannels`; каждому матчу `Method='regex'`, `Model=''`, `PromptVersion=0`), `Seq` — сквозной по документу на таблицу; `Send` каждого (сбой = следующий прогон повторит, ключи Replacing идемпотентны);
  5. LLM-догон: если `mpLLMConfigFromEnv()` ok — документы без строк `(method='llm', model=?, prompt_version=1)` (SELECT DISTINCT meeting_date). `tagMPChannelsLLM` по каждому; ошибка документа → `log.Warnf` и продолжение; батч №4 каналы (`Method='llm'`, `Model=model`, `PromptVersion=mpPromptVersion`); если конфига нет — `log.Warnf("…: LLM-разметка пропущена (нет LLM_BASE_URL/LLM_API_KEY/MP_SUMMARY_LLM_MODEL)")`;
  6. `log.Infof` итогов: «резюме: %d новых, блоков: %d, каналов regex: %d, LLM: %d»; `UpsertSeriesCatalog` не нужен; витрины — `util.CreateView` × 3 (в конце);
  7. `return count, nil` (count = сумма Append'ов).
- DDL-литералы — дословно из спеки §3.3; Go-структура `mpSummaryStat{ util.ClickHouseImport }` с `Name()` override (как `mpSurveyStat`).
- Проводка: `cbr/importers.go` — 15-я запись `{mpSummaryImporter, "cbr_mp_summary", ""}` (третья колонка — литерал `countPoints`; пусто: префлайта нет сознательно — дельта по индексу, спека §3.1; обновить комментарии реестра: «14 записей» → 15); `dagu/cbr.yaml` — шаг-шаблон существующих; `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_cbr_mp_summary TO kimi_reader;` ×3; `scripts/mcp_check.py` — по образцу `v_cbr_mo_br`: чтения `v_cbr_mp_summary` (>0), `v_cbr_mp_summary_blocks` (>0), `v_cbr_mp_summary_channels` (>0) и `ACCESS_DENIED`-проверки трёх сырых таблиц.

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -count=1 && go build ./... && gofmt -l cbr/ && golangci-lint run ./cbr/...`; `grep -c "  - id:" dagu/cbr.yaml` → 15; `dagu validate dagu/cbr.yaml` → exit 0; `python3 -m py_compile scripts/mcp_check.py`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary.go cbr/mp_summary_test.go cbr/importers.go cbr/importers_test.go dagu/cbr.yaml sql/mcp_kimi_reader.sql scripts/mcp_check.py && git commit -m "cbr: cbr_mp_summary importer with channel tagging, views and wiring"`.

---

