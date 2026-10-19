# План: публичный источник 87 кодов ОКВЭД2 и снятие оговорки (Task 7, #248)

**Цель:** `rosstat_okved2` импортирует публичный классификатор ОКВЭД2 (3 102 строки, 87/87 BSU-кодов); `cbr_mp_bsu_map` сеет кроссворд 87 кодов → 16 отраслевых листов и публикует `v_cbr_mp_bsu_map` / `v_cbr_mp_public_panel`; research-отчёт фиксирует «live 87 кодов — нет»; оговорка переформулирована; `make all` / `make docs-check` / `make mcp-check` зелёные.

**Спека:** [2026-10-19-cb-comp-public-bsu-source-design.md](../specs/2026-10-19-cb-comp-public-bsu-source-design.md).

**Инварианты:**
- Публичная книга ЦБ содержит 16 отраслевых листов, а не 87 кодов; это зафиксировано в спеке и отчёте.
- 87 кодов — стандартные ОКВЭД2; live-источника значений на 87 кодах нет.
- Существующие 381 ряд `cbr_survey` не меняются.
- `rosstat_okved2` и `cbr_mp_bsu_map` — reference-таблицы, не ряды `series_catalog`.
- `docs/KEY_RATE_PUBLICATIONS.md` в корневом worktree не трогаем.

---

## Task 0. Ветка, issue, спека/план

- [x] worktree `.worktrees/cb-comp-public-bsu-source`, ветка `feat/cb-comp-public-bsu-source` от `main`.
- [x] комментарий в #248, Project V2: `Status=In Progress`, `Branch`, `Phase`, `Item`, `Acceptance`, `Depends on`.
- [ ] спека и план — первый коммит ветки.
- [ ] `Spec / Plan` в Project V2 после коммита.

**Проверка:** `git log --oneline -1` — docs-коммит; `git status` чистый.

---

## Task 1. Research-отчёт

**Файл:** `docs/research/2026-10-19-cb-comp-public-bsu-source.md`.

1. Таблица источников §1 спеки: ЦБ `mp_survey_data.xlsx`, `survey_data_group.xlsx`, `mp_survey_data_gu.xlsx`, `mp_survey_data_facts.xlsx`, API ЦБ, Росстат ОКВЭД2, figshare-архив, ЕМИСС/Росстат.
2. Записать живые ответы: URL, HTTP, размер, формат, число листов/строк, дата среза/версия.
3. Явный ответ: live public source на 87 кодах — **нет**; коды — да (87/87), значения — статический архив 2009-01…2024-09.
4. Таблица кроссворда: 87 кодов → 13 используемых листов; 21 consumer + 1 investment + 4 intermediate + остальные rule; `Промышленность`, `Обработка`, `Торговля (45+46+47)` — агрегаты.
5. Ограничения: 39 серий, не 87; кроссворд — не внутренний mapping ЦБ; паритет не заявляется.

**Проверка:** отчёт содержит таблицу и решение; ссылки на источники открываются.

---

## Task 2. `rosstat_okved2` — парсер и импортёр

**Файлы:** `rosstat/okved2.go`, `rosstat/okved2_parse.go` (если парсер > ~150 строк), `rosstat/okved2_test.go`, `rosstat/testdata/okved2_meta.csv`, `rosstat/testdata/okved2_data.csv`.

1. Константы: `rosstatOkved2Name = "rosstat_okved2"`, `rosstatOkved2MetaURL`, `rosstatOkved2Ddl`, `rosstatOkved2CountPoints`.
2. `okved2MetaURL(body []byte) (string, error)` — CP1251 → CSV → первая строка `data-*.csv` с URL; ошибка, если не найдено.
3. `parseOkved2(body []byte) ([]okved2Row, error)` — BOM, `;`, `section/code/name`; `code=" "` → level 1; `level`/`parent_code`; fail-fast на пустом/битом.
4. `rosstatOkved2Stat` с встроенным `util.ClickHouseImport`:
   - `TableName: "rosstat_okved2"`;
   - `CreateTable: []string{rosstatOkved2Ddl}`;
   - `PublicationCountPoints`, `PublicationSource`;
   - `BeforeImport`: `GetFileBytes(metaURL)` → `okved2MetaURL` → `s.DataUrl = url`;
   - `BodyFunc`: parse → batch.Append (без списка CB_COMP — пакет `rosstat` не зависит от `cbr`);
   - `AfterImport`: `util.CreateView(ctx, conn, rosstatOkved2View)`.
5. Регистрация в `rosstat/importers.go`: `{rosstatOkved2Importer, "rosstat_okved2", "SELECT count() FROM rosstat_okved2 FINAL"}`.
6. `dagu/rosstat.yaml`: последний шаг `rosstat_okved2`.
7. `sql/mcp_kimi_reader.sql`: грант на `v_rosstat_okved2`.

**87/87:** проверяет `cbr_mp_bsu_map` по `rosstat_okved2` (SQL `count() WHERE code IN (<87>)`), а live-приёмка — тем же запросом. `rosstat` не зависит от списка CB_COMP; его тест проверяет структуру и полный счёт классификатора.

**Проверка:** `go test ./rosstat -run 'Okved2'`; `go test ./rosstat` — реестр/DAG зелёные.

---

## Task 3. `cbr_mp_bsu_map` — 87 кодов, маппер, импортёр

**Файлы:** `cbr/mp_bsu_codes.go`, `cbr/mp_bsu_map.go`, `cbr/mp_bsu_map_test.go`, `cbr/testdata/cb_comp_bsu_codes.csv`, `cbr/testdata/okved2_87.csv` (87 строк классификатора: code;name;section).

1. `var cbCompBSUCodes = []string{...}` — 87 кодов из `model.xlsx` `1.0.4.2`; комментарий: figshare DOI, дата извлечения, 87 уникальных.
2. `type bsuIndustry struct { Sheet, Slug, Basis, Confidence string }`.
3. `publicIndustryForCode(code string) (bsuIndustry, error)`:
   - top-level class; section ranges; manufacturing overrides; `services` for 55–96; `transport` for 49–53; trade 45/46/47.
   - `industrySheet` берётся из `mpSurveyIndustrySheets` (точное имя/слаг) — ошибка, если слага нет.
4. `bsuMapRows() ([]bsuMapRow, error)` — 87 строк, проверка дублей.
5. `cbrMpBsuMapStat` с собственным `Import()`:
   - `conn.Exec` DDL;
   - если `rosstat_okved2` существует: `SELECT count() FROM rosstat_okved2 FINAL WHERE code IN (<87>)`; != 87 — ошибка с указанием, каких кодов нет; таблицы нет — `log.Warn` и продолжать (витрина с именами появится после классификатора);
   - `PrepareBatch("INSERT INTO cbr_mp_bsu_map")` → 87 Append → Send;
   - `util.CreateView` для `v_cbr_mp_bsu_map` и `v_cbr_mp_public_panel`;
   - `log.Infof("Imported %d rows of %s", count, name)`.
6. Регистрация в `cbr/importers.go`: `{cbrMpBsuMapImporter, "cbr_mp_bsu_map", ""}`.
7. `dagu/cbr.yaml`: последний шаг `cbr_mp_bsu_map`; `keyScheduled` в `cbr/importers_test.go`.
8. `sql/mcp_kimi_reader.sql`: гранты на две витрины.
9. `cbr/views.go`: `cbrMpBsuMapView`, `cbrMpPublicPanelView` с SQL и комментариями.

**SQL `v_cbr_mp_public_panel`:** три `UNION ALL`:
```sql
SELECT m.code, m.industry_sheet, m.industry_slug, 'ibk' AS indicator, ms.date, ms.value
FROM cbr_mp_bsu_map m FINAL
INNER JOIN macro_series ms FINAL
  ON ms.source = 'cbr_survey'
 AND ms.series = concat('industry_', m.industry_slug, '_ibk_sa')
UNION ALL
... 'price_exp' ... '_price_expectations_sa'
UNION ALL
... 'costs' ... '_cost_change_sa'
```
`v_cbr_mp_bsu_map`:
```sql
SELECT m.code, o.name AS okved_name, o.section, m.industry_sheet, m.industry_slug,
       m.basis, m.confidence, m.source
FROM cbr_mp_bsu_map m FINAL
LEFT JOIN (SELECT code, any(name) AS name, any(section) AS section
           FROM rosstat_okved2 FINAL WHERE level > 1 GROUP BY code) o
  ON o.code = m.code
ORDER BY m.code
```
`Tables: []string{"cbr_mp_bsu_map", "rosstat_okved2"}` для первой; `Tables: []string{"cbr_mp_bsu_map", "macro_series"}` для второй.

**Проверка:** `go test ./cbr -run 'MpBsuMap|MpBsuCodes'`; `go test ./cbr` — реестр/DAG/существующие тесты.

---

## Task 4. MCP-check и grants

**Файлы:** `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`.

1. Гранты: `v_rosstat_okved2`, `v_cbr_mp_bsu_map`, `v_cbr_mp_public_panel`.
2. Проверки в `mcp_check.py`:
   - `v_rosstat_okved2`: 3 102 строки; 87/87 BSU-кодов (запрос по списку или `count() WHERE code IN (...)`).
   - `v_cbr_mp_bsu_map`: 87 строк, 0 пустых `okved_name`.
   - `v_cbr_mp_public_panel`: 87 кодов, 13 slug, 3 indicator, 297 дат, `count() > 0`.
   - spot: `10.1`, `29.10`, `23`, `47.22`, `46.32`, `45.11`, `01.4`, `05`, `43.2`, `49.3`, `86.90.4`.
3. Не хардкодить 77 517: число строк растёт с месяцами.

**Проверка:** `python3 -m py_compile scripts/mcp_check.py`; живой `make mcp-check`.

---

## Task 5. Документация и оговорка

1. `ARCHITECTURE.md`: §2 (`rosstat/`, `cbr/`), §6.1 (два импортёра), §6.2 (DDL), §6.3 (три витрины), §7 (шаги), §8/§6.5 (MCP/grants).
2. `README.md`: список импортёров/витрин.
3. `docs/IMPORTERS.md`: строки `rosstat` и `cbr`.
4. Оговорка: `docs/ROADMAP_KEY_RATE.md:240`; Task 2/6 — если «внутренние»; `docs/research/2026-10-18-cb-comp-parity.md`; `docs/superpowers/specs/2026-10-19-cb-comp-feature-parity-design.md:17,148`; `docs/research/2026-10-19-cpi-industry-mp-features.md:28`; `docs/superpowers/specs/2026-10-19-cpi-industry-mp-features-design.md:23,106`; `docs/publications/2026-10-18-key-rate.md` §9/footnote; `docs/publications/2026-10-18/checklist.md`.
5. `docs/ROADMAP_KEY_RATE.md` — статус Task 7 `In Progress`/`In Review` по правилам; после мержа — `✅` с протоколом.

**Проверка:** `grep -R "87 кодов.*внутренн\|внутренних 87"` не находит живых формулировок; `make docs-check`.

---

## Task 6. Живая приёмка

```bash
make ch-status
make import STAT=rosstat_okved2      # Imported 3102 rows; 87/87
make import STAT=rosstat_okved2      # no-op, без скачивания
make import STAT=cbr_mp_bsu_map      # Imported 87 rows
make import STAT=cbr_mp_bsu_map      # 87 rows FINAL, идемпотентно
make import STAT=cbr_mp_survey       # если 16 листов ещё не в dev
make mcp-check                       # ПРОВЕРКА ПРОЙДЕНА
make all
make docs-check
dagu validate dagu/rosstat.yaml && dagu validate dagu/cbr.yaml
```

**Ожидания:** `rosstat_okved2 FINAL` 3 102; `cbr_mp_bsu_map FINAL` 87; `v_cbr_mp_public_panel` 77 517 / 13 / 3 / 297 на снимке 2026-10; spot-коды из §4.6 спеки.

**Протокол:** комментарием в PR; после мержа — `Done` и роадмап.

---

## Task 7. PR

- Один PR, `Closes #248` вне backticks.
- Описание: пункт, источники, схема, правила кроссворда, запуск, проверка, ограничения.
- `Status = In Review`, поле `PR`.
- После мержа: `Status = Done`, роадмап `✅` с числами протокола.
