# План: импортёр `cbr_mt_forecast` — диапазоны среднесрочного прогноза ЦБ (пункт §4.2 №7)

> **Для исполняющих агентов:** ОБЯЗАТЕЛЬНЫЙ СУБ-СКИЛ: `superpowers:subagent-driven-development`. Шаги помечены чекбоксами (`- [ ]`) — по ним ведётся прогресс. План аргументирован от спеки: исполнять только вместе с [`docs/superpowers/specs/2026-10-17-cbr-mt-forecast-design.md`](../specs/2026-10-17-cbr-mt-forecast-design.md).

**Goal:** ввести в контур ключевой ставки «эталон ЦБ» — диапазоны среднегодовой ключевой ставки и прогноза инфляции из «Комментария к среднесрочному прогнозу» (таблица «Табл 1» xlsx), витриной `v_cbr_mt_forecast`, чтобы публикационный прогон ([KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §5–§6) читал их из БД, а не вручную.

**Architecture:** новый импортёр `cbr_mt_forecast` в пакете `cbr/` — набор выпусков (прецедент `cbr_dip_bulletin`): `Import()` = DDL → производные → индекс (`cbr.ru/dkp/mp_dec/decision_key_rate/`) → гейт по `release_date` → по каждому новому выпуску полный цикл шаблона `util.ClickHouseImport` (страница выпуска → ссылка на xlsx → разбор «Табл 1» → батч → строка версии). Таблица винтажная: ключ `(indicator, year, release_date)`.

**Tech Stack:** Go 1.2x, `github.com/xuri/excelize/v2` (сырые значения), `goquery` (индекс/страница), `util.ClickHouseImport`/`util.GetPage`/`util.FindLink`, ClickHouse (ReplacingMergeTree), dagu, logrus; Python-скрипты репозитория (`scripts/xlsx_trim.py`, `scripts/mcp_check.py`).

**Worktree/ветка:** `.worktrees/cbr-mt-forecast`, ветка `feat/cbr-mt-forecast` от `main`. Все команды — из этого дерева; живой прогон — только через `make` (env подключает Makefile).

**Spec:** [`docs/superpowers/specs/2026-10-17-cbr-mt-forecast-design.md`](../specs/2026-10-17-cbr-mt-forecast-design.md) (пинит DDL, 16 кодов, правила разбора, поток, отступления, приёмку). Разведка: [`.superpowers/sdd/2026-10-17-cbr-mt-forecast/recon/source-report.md`](../../../.superpowers/sdd/2026-10-17-cbr-mt-forecast/recon/source-report.md).

## Global Constraints

- Стандарт пакета импортёров обязателен: [`docs/IMPORTER_PACKAGE_STANDARD.md`](../../IMPORTER_PACKAGE_STANDARD.md) (§1.2 — ссылаться, не пересказывать). Конвенции пакета `cbr/` — как в `cbr/dip_bulletin.go`, `cbr/dip_page.go`, `cbr/dip_parse.go`, `cbr/dip_meta.go` (они свежие и проходят тест-энфорсер).
- Вставка только батчами (`PrepareBatch` → `Append` с проверкой → `Send`), сетевых вызовов в `init()` нет, HTTP — только через `util.*`, логи — только logrus, `Float64` для значений, каждый `Parse*`/`Append` — с проверкой `err` (правила AGENTS.md).
- Разбор офлайн: парсер принимает `*excelize.File`/`[]byte`, в сеть не ходит; значения — `RawCellValue` (стандарт §6.1–6.2).
- Имена рядов объявляет код (карта «подпись → код», стандарт §6.8); реестр и запросы чека в `cbr/importers.go` — литералами (§8.2).
- Фикстуры — реальные урезанные файлы (`scripts/xlsx_trim.py`, §11.2), тесты офлайн, харнесс `appendRecorder` (§11.6).
- Команды разработки: `go test -race -count=1 ./cbr/` (по пакету), `make all` (полный гейт), `dagu validate dagu/cbr.yaml`, живой прогон — `make import STAT=cbr_mt_forecast …`.
- Дисциплина субагентов (AGENTS.md): модель — явным `model:` (имплементеры `deepseek/deepseek-flash`, ревью задач `anthropic/haiku`, финальное ревью ветки `deepseek/deepseek-flash`); в каждый промпт coder-субагента вписан блок про `mcp__jetbrains__*`/`mcp__clickhouse__*`; коммиты — кратко по-английски.
- Секреты не печатать; содержимое `~/.config/rosstat/env` не читать.

## Review Focus (входы/режимы, которые вероятнее всего кусаются; тесты привязаны к задачам)

1. **Дрейф вёрстки «Табл 1» в следующем выпуске** (переименованная строка, новый показатель, другой горизонт 3–4 года, факт-колонка с другой пометкой) — политика: unknown-подпись → warn+пропуск, пропавший core → ошибка, пропавший не-core → warn (тесты Task 1).
2. **Значение ячейки**: приклеенная цифра сноски (`'14,5-14,61'` → 14.6) против истинной дроби, скобочные отрицательные, оба тире, целые `7–12`; страховка `lo ≤ hi` (тесты Task 1).
3. **Дрейф индекса**: карточка без даты публикации / дата ≠ слагу / исчезли страничные карточки / новые PDF-карточки — fail-fast против тихого пропуска (тесты Task 2).
4. **Повторный прогон и гейт**: no-op без скачиваний; подмена файла под тем же `release_date` гейтом не отслеживается — задокументировано в комментарии запроса и приёмке (тест чистых функций Task 2 + живая приёмка).
5. **No-op-путь и restore**: производные публикуются и при пустой дельте (самолечение витрины/каталога — прецедент `rosstat_ipc_weeks_bulletins`); восстановление из архива работает шаблонным путём (`DataUrl` + `ImportFunc`), как у дипа — свидетельство в живой приёмке (повторный прогон после пересоздания витрины).

---

### Task 1: Разбор «Табл 1» + реестр показателей + фикстуры

**Files:**
- Create: `cbr/mt_forecast_meta.go` (реестр 16 показателей + `cbrMtForecastSeriesMeta`)
- Create: `cbr/mt_forecast_parse.go` (нормализация, разбор книги и значений)
- Create: `cbr/mt_forecast_parse_test.go` (тесты разбора и каталога)
- Create: `cbr/testdata/mt_forecast_05082026.xlsx`, `cbr/testdata/mt_forecast_07052026.xlsx` (урезанные zip-хирургией)
- Create: `cbr/testdata/mt_forecast_page_05082026.html` (копия живой страницы выпуска; потребляет тест Task 2)

**Interfaces:**
- Produces (нужны Task 2): `type mtForecastPoint struct { Indicator string; Year uint16; Lo, Hi float64 }`; `func mtForecastParseTabl1(xlsx *excelize.File) ([]mtForecastPoint, error)`; `var cbrMtForecastSeriesMeta []util.SeriesMeta` (16 записей, source `cbr_mt_forecast`, frequency `A`); `func mtForecastIndicatorByLabel(label string) (mtForecastIndicator, bool)`.
- Шаблоны-эталоны: `cbr/dip_parse.go`/`dip_parse_test.go` (разбор книги + appendRecorder), `cbr/mo_br_meta.go` + `mo_br_series_meta_test.go` (реестр и каталог).

- [x] **Step 1: Фикстуры.** Из дерева: `python3 scripts/xlsx_trim.py trim build/recon-2026-10-17/dl_commet_graph_05082026.xlsx cbr/testdata/mt_forecast_05082026.xlsx --keep 'Табл 1'` (и то же для 07052026); затем `python3 scripts/xlsx_trim.py verify build/recon-2026-10-17/dl_commet_graph_05082026.xlsx cbr/testdata/mt_forecast_05082026.xlsx --keep 'Табл 1'` → PASS (и для второй пары). Убедиться, что excelize открывает фикстуры и «Табл 1» читается. Плюс страница выпуска: `cp build/recon-2026-10-17/page_05082026.html cbr/testdata/mt_forecast_page_05082026.html` и `grep -c commet_graph_05082026.xlsx cbr/testdata/mt_forecast_page_05082026.html` ≥ 1 (страницу коммитим как есть — живая разметка).
- [x] **Step 2: Тесты разбора (сначала падающие).** В `cbr/mt_forecast_parse_test.go`:
  - таблица спотов из спеки §3.9 (64 точки / 4 колонки у 05082026; `key_rate_avg` 2026 = (14.5, 14.6) — приклеенная сноска; `cpi_dec` 2028 = (4.0, 4.0); `gross_capital_formation` 2026 = (-3.5, -1.5); `fixed_capital_investment` 2026 = (-1.5, 0.5); `m2` 2026 = (7, 12); `bank_claims_mortgage` 2028 = (10, 15); и 48 точек у 07052026, `key_rate_avg` 2026 = (14.0, 14.5), `cpi_dec` 2026 = (4.5, 5.5); `year` и границы);
  - двусторонний контракт каталога (§11.4): каждая разобранная подпись есть в карте, каждый из 16 кодов — среди разобранных на объединении фикстур; `cbrMtForecastSeriesMeta` описывает ровно 16 кодов;
  - fail-fast: книга без листа «Табл 1»; лист без строки-показателя; шапка без годов; пустая прогнозная ячейка; `'5,0-4,0'` (lo > hi) → ошибка с адресом; неразбираемая ячейка (`'около 5'`);
  - warn-ветки: неизвестная подпись — пропуск (не ошибка); отсутствие core-строки в книге — ошибка; отсутствие не-core — предупреждение (последние два — на синтетически удалённой строке из настоящей книги через копию листа; допустимо `xlsx.SetCellValue` в тесте — разметку не менять);
  - харнесс `appendRecorder`: `mtForecastAppendPoints(release)` кладёт 64 строки для 05082026 с ожидаемым первым/последним (`sorted by row order` — зафиксировать фактический порядок).
- [x] **Step 3: Прогнать — падает** (`go test ./cbr/ -run 'MtForecast' -v` — компиляция не проходит: функций нет).
- [x] **Step 4: `cbr/mt_forecast_meta.go`.** Реестр `mtForecastIndicators` (16 записей: `Code`, `Label` — нормализованная подпись из спеки §3.2, `Title`, `Unit`, `Core`), `mtForecastIndicatorByLabel`, `var cbrMtForecastSeriesMeta = mtForecastMeta()` — описания из спеки §3.7 (origin: «ЦБ, „Комментарий к среднесрочному прогнозу“/„Среднесрочный прогноз“, таблица „Табл 1“, строка „…“»).
- [x] **Step 5: `cbr/mt_forecast_parse.go`.** Слоистая нормализация и разбор строго по спеке §3.3: базовая нормализация → подпись (тире→`-`, срез хвостовых цифр) → значение (юникод-надстрочные, точка/диапазон регулярками, сноска-приклейка только у ровно 2-знаковых дробей, `lo ≤ hi`); шапка — ближайшая строка выше первой строки-показателя с ≥2 годами; факт-колонки (`факт`/`оценка`) пропускаются; конец таблицы — `^\d+ `; fail-fast и warn по §3.3.
- [x] **Step 6: Прогнать — зелёный** (`go test -race -count=1 ./cbr/ -run 'MtForecast'`), затем весь пакет `go test -race -count=1 ./cbr/`.
- [x] **Step 7: Commit** — `cbr: mt_forecast parse — Табл 1, meta registry, fixtures`.

### Task 2: Импортёр (индекс → гейт → цикл выпусков), витрина, регистрация, расписание

**Files:**
- Create: `cbr/mt_forecast.go` (DDL, константы, `Import()`, разбор индекса и страницы)
- Create: `cbr/mt_forecast_test.go` (индекс/страница/гейт/DDL-pin)
- Modify: `cbr/views.go` (витрина `cbrMtForecastView` + `afterCbrMtForecast`)
- Modify: `cbr/importers.go` (запись реестра), `cbr/importers_test.go` (ожидаемые наборы)
- Modify: `dagu/cbr.yaml` (шаг в хвост)

**Interfaces:**
- Consumes (из Task 1): `mtForecastParseTabl1`, `cbrMtForecastSeriesMeta`, `mtForecastPoint`.
- Produces: `cbr_mt_forecast` (таблица и `Name()`), `v_cbr_mt_forecast`, `afterCbrMtForecast([]util.SeriesMeta)`.

- [x] **Step 1: Тесты (сначала падающие).** В `cbr/mt_forecast_test.go`:
  - `mtForecastIndexReleases` на `cbr/testdata/mp_summary_index.html`: 11 карточек «Комментарий к среднесрочному прогнозу Банка России» → 2 страничные (`release_date` 2026-05-07 с `meeting_date` 2026-04-24; 2026-08-05 с 2026-07-24) + 9 PDF-пропусков; расхождение даты карточки и слага → ошибка (проверка на копии фикстуры в памяти — дата карточки подменяется в байтах страницы, файл на диске не меняется); индекс без страничных карточек → ошибка;
  - `mtForecastPageXlsxURL` на `cbr/testdata/mt_forecast_page_05082026.html` (`util.FindLink` по `mtForecastXlsxSel`) → `/Content/Document/File/194307/commet_graph_05082026.xlsx` (отсутствие ссылки → ошибка);
  - `mtForecastNewReleases`: пустое множество известных → все (старые первыми), полное → пусто;
  - DDL-pin по прецеденту `TestMPSummaryDdlPins`: литерал `cbr_mt_forecast` внутри `mtForecastDdl` и колонки/ключ совпадают со спекой §3.1.
- [x] **Step 2: Прогнать — падает.**
- [x] **Step 3: `cbr/mt_forecast.go`.** Константы (`mtForecastIndexUrl`, `mtForecastCardTitle`, `mtForecastHrefMarker = "comment_"`, `mtForecastXlsxSel = a[href$=".xlsx"]`, `mtForecastDdl`, `mtForecastCountPoints = SELECT count() FROM cbr_mt_forecast FINAL`, `mtForecastKnownReleasesQuery`, `mtForecastReleaseCountQuery` с плейсхолдером); `mtForecastStat`/`Name()`; `Import()` — порядок по спеке §3.5 (DDL → `afterCbrMtForecast` → индекс → гейт → цикл: страница → `local := s.ClickHouseImport` + `DataUrl`+`ImportFunc` → поста-чек строк выпуска → `errors.Join`; no-op-сообщение со счётчиками); `mtForecastAppendPoints`. Комментарии — в стиле дипа (почему гейт, почему производные раньше, что не отслеживает подмена файла).
- [x] **Step 4: `cbr/views.go`.** `cbrMtForecastView` (Select и комментарии таблицы/колонок — спека §3.7) + `afterCbrMtForecast(meta)` (каталог + `CreateView`; прецедент `afterCbrDip`).
- [x] **Step 5: Регистрация + расписание.** Запись в `cbr/importers.go`: `{cbrMtForecastImporter, "cbr_mt_forecast", "SELECT count() FROM cbr_mt_forecast FINAL"}` (литералы); импортер-var с `TableName`, `CreateTable`, `PublicationCountPoints` (без `AfterImport` — производные публикует `Import()`); шаг `cbr_mt_forecast` в хвост `dagu/cbr.yaml` (retry_policy как у соседей); обновить ожидаемые наборы в `cbr/importers_test.go` (карта префлайтов, ключевые шаги, счётные комментарии — «20-я запись», «20-й шаг»).
- [x] **Step 6: Прогнать — зелёный.** `go test -race -count=1 ./cbr/` (включая энфорсер реестра), `dagu validate dagu/cbr.yaml` (если dagu доступен; иначе зафиксировать в отчёте), `gofmt`, `go vet ./cbr/`.
- [x] **Step 7: Commit** — `cbr: add mt_forecast importer (index, gate, per-release cycle)`.

### Task 3: MCP-контур — проверки, грант, снимок покрытия

**Files:**
- Modify: `scripts/mcp_check.py` (блок `cbr_mt_forecast` по образцу блока `cbr_dip_bulletin` строки ~832–929)
- Modify: `sql/mcp_kimi_reader.sql` (+1 строка)
- Modify: `sql/key_rate_coverage.sql` (+1 ветка `UNION ALL`)

**Interfaces:** Produces: проверки `make mcp-check` для витрины/каталога/винтажности/закрытости сырой таблицы; грант `kimi_reader`; ветка снимка покрытия для §4.1 роадмапа.

- [x] **Step 1: `sql/mcp_kimi_reader.sql`** — `GRANT SELECT ON default.v_cbr_mt_forecast TO kimi_reader;` (в конец списка).
- [x] **Step 2: `sql/key_rate_coverage.sql`** — ветка `cbr_mt_forecast`: группировка по `indicator`, `count()`, `min(release_date)`, `max(release_date)` + комментарий: даты — публикации выпусков (винтаж), само число точек умножается на число выпусков, число выпусков — отдельным запросом `uniqExact(release_date)` (стиль комментария дипа).
- [x] **Step 3: `scripts/mcp_check.py`** — блок по образцу дипа (строки ~832–929): витрина `v_cbr_mt_forecast` существует, читается и непуста; каталог — ровно 16 (`count() AS n … WHERE source = 'cbr_mt_forecast'`); винтажность — `uniqExact(release_date) > 1`, пустых `release_date`/`meeting_date` нет; сырая `cbr_mt_forecast` закрыта (`ACCESS_DENIED`/497, `UNKNOWN_TABLE` — провал). Комментарии — с причинно-следственной связью (как у дипа).
- [x] **Step 4: Проверка синтаксиса** — `python3 -c "import ast; ast.parse(open('scripts/mcp_check.py').read())"`; `python3 scripts/mcp_setup_user_test.py` (если есть офлайн-тест — зелёный).
- [x] **Step 5: Commit** — `cbr: mt_forecast MCP checks, grants, coverage snapshot`.

### Task 4: Документация — ARCHITECTURE, README, роадмап, публикации

**Files:**
- Modify: `ARCHITECTURE.md` (шапка «Последнее обновление»; §2 — скрипты не меняются; §6.1 — строка импортёра; §6.2 — канонический DDL `cbr_mt_forecast`; §6.3 — витрина `v_cbr_mt_forecast`; §8 п.13 — счёт объявлений префлайта)
- Modify: `README.md` (таблица импортёров и разделы, которые правит skill `sync-readme-architecture` — прогнать его чек-лист)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.1 — строка покрытия `cbr_mt_forecast`; §4.2 №7 → ✅ со ссылкой на спеку/план; §4.3 №7 → ✅ с уточнённой разведкой: xlsx только у 2 новейших выпусков, 9 — PDF, «Среднесрочный прогноз» — день заседания; §5 — строка `cbr_mt_forecast` в таблице модели данных; §6 фаза 2 — отметка шага в цепочке; **новый пробел**: «PDF-выпуски 2024–2026-02 и „Среднесрочный прогноз“ без машиночитаемого формата» — отдельным пунктом §4.2 со ссылкой на `source-report.md`)
- Modify: `docs/KEY_RATE_PUBLICATIONS.md` (§6 — строка «Диапазоны среднесрочного прогноза ЦБ» ❌ → ✅ с витриной и датой; §5 — абзац «Источник значений для сверки»: теперь `v_cbr_mt_forecast`, ручная фиксация остаётся только для PDF-выпусков)
- Modify: `docs/superpowers/plans/2026-10-17-cbr-mt-forecast.md` — отметки выполнения и место для протокола

**Interfaces:** Produces: документы, согласованные с кодом (skill `sync-readme-architecture`), включая названные отступления спеки §5 (витрина не long-форма — в приёмочном блоке роадмапа при закрытии пункта).

- [x] **Step 1: `ARCHITECTURE.md`** — по чек-листу skill'а `sync-readme-architecture` (вызвать его): §6.1/§6.2/§6.3/§8 п.13 + шапка.
- [x] **Step 2: `README.md`** — таблица импортёров + всё, что найдёт чек-лист.
- [x] **Step 3: `docs/ROADMAP_KEY_RATE.md` + `docs/KEY_RATE_PUBLICATIONS.md`** — по списку выше. Статус ✅ пишется со ссылкой на спеку/план без чисел прогона; там, где нужны числа (`§4.1` — строка покрытия; «приёмка …» в §4.2 №7), ставится формулировка «живая приёмка — отдельным коммитом» — реальные числа и дату дозаполняет контроллер после прогона (порядок стандарта §16.1 п. 8). Плейсхолдеры-маркеры в документе не оставлять.
- [x] **Step 4: Commit** — `cbr: mt_forecast docs — architecture, readme, roadmaps`.

---

## Живая приёмка (после Task 4; выполняет контроллер, не субагент)

- [ ] `make all` — зелёный.
- [ ] `make import STAT=cbr_mt_forecast ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` — ожидание: **2 выпуска, 112 точек** (48 + 64), 2026-05-07 … 2026-08-05.
- [ ] Повторный прогон — «новых выпусков нет (страниц 2, PDF-выпусков 9, в таблице 2 выпусков)», `Imported 0 rows`, без скачиваний; при желании — свидетельство самолечения: пересоздание витрины повторным прогоном.
- [ ] `make ch-sql FILE=sql/key_rate_coverage.sql` — строка `cbr_mt_forecast`; spot-сверка с живым xlsx.
- [ ] `make mcp-user` → `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- [ ] `dagu validate dagu/cbr.yaml` — exit 0; архив — 2 объекта и 2 строки `registry.jsonl` в dev-хранилище.
- [ ] Коммит статуса ✅ с протоколом в теле (числа прогона в §4.1/§4.2 роадмапа), PR + приёмочный комментарий; merge — решение владельца.

## Само-ревью плана

- **Покрытие спеки:** §2 источник → Step-разведка уже сделана, фикстуры Task 1; §3.1 DDL → Task 2 Step 3 + pin-тест; §3.2 коды → Task 1 Step 4; §3.3 разбор → Task 1 Steps 2–5; §3.4 идентичность → Task 2 Step 1/3; §3.5 поток → Task 2 Step 3; §3.6 точка контроля → Task 2 Step 5; §3.7 производные → Task 2 Step 4 + Task 3; §3.8 расписание → Task 2 Step 5; §3.9 фикстуры/тесты/проверки → Task 1/2/3; §3.10 приёмка → блок «Живая приёмка»; §4 файлы → задачи; §5 отступления → Task 4 (названы в документах/приёмке); §6 границы → не входит ни в одну задачу (по определению).
- **Согласованность имён:** `mtForecastParseTabl1`, `mtForecastIndexReleases`, `mtForecastNewReleases`, `mtForecastAppendPoints`, `mtForecastPageXlsxURL`, `afterCbrMtForecast`, `cbrMtForecastSeriesMeta` — согласованы между задачами.
- **Review Focus → тесты:** (1) Task 1 Steps 2/5; (2) Task 1 Steps 2/5; (3) Task 2 Step 1; (4) Task 2 Steps 1/3 + приёмка; (5) Task 2 Step 3 + приёмка.
