# План: розничные кредитные ставки ЦБ — `cbr_int_rate_loans_ind`, `cbr_mortgage_rate`, `cbr_psk` (остаток §4.2 №8, фаза 2, issue #119)

**Дата:** 3 ноября 2026 г.
**Спека:** [2026-11-03-cbr-retail-loan-rates-design.md](../superpowers/specs/2026-11-03-cbr-retail-loan-rates-design.md)
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №8 (остаток: ставки
по кредитам физлицам, в т.ч. ипотека по ПСК).
**Ветка:** `feat/retail-loan-rates`. **Worktree:** `.worktrees/retail-loan-rates`.
**Исполнение:** `superpowers:subagent-driven-development` — волны coder-субагентов
`deepseek/deepseek-flash` (по скилу `subagent-dispatch`, крупные задачи), ревью задач —
`anthropic/haiku`, финальное ревью ветки — `deepseek/deepseek-flash`. Живая приёмка —
контроллером (этой сессией).

---

## Критерий приёмки пункта

Зелёные `make all`, `make docs-check`, `dagu validate dagu/cbr.yaml`; три живых прогона
`make import STAT=cbr_int_rate_loans_ind|cbr_mortgage_rate|cbr_psk` с записью фактов
(команда, число строк, крайние даты); no-op каждого повтора; `make mcp-check` —
ПРОВЕРКА ПРОЙДЕНА (включая новые проверки каталога и deny 497 на сырых таблицах);
`ARCHITECTURE.md` / `README.md` / `docs/IMPORTERS.md` / статус роадмапа — обновлены тем
же PR. Issue #119 — `Closes #119` вне backticks.

## Задачи

### Task 1 (волна 1, coder): `cbr_int_rate_loans_ind` — импортёр, фикстура, тесты

- `cbr/int_rate_loans_ind.go`: `cbrIntRateLoansInd` = `util.ClickHouseImport`, статичный
  `DataUrl: https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_ind.xlsx`, DDL спеки §3.2,
  разбор листа `ставки_руб` спеки §2.1/§3.3, имена рядов из кода (16: `Всего: <срок>` /
  `Автокредиты: <срок>`), `PublicationCountPoints: SELECT count() FROM cbr_int_rate_loans_ind FINAL`.
- `AfterImport: afterCbrMacro(cbrIntRateLoansIndSeriesMeta)` подключает Task 2 (мета в той же задаче).
- `cbr/series_meta.go`: `cbrIntRateLoansIndSeriesMeta` — 16 записей, unit «% годовых», F=M,
  origin — файл/лист/группа/срок; description — спека §3.5 A (включая зафиксированное
  отставание публикации: ряды кончаются 2025-12 на снимке 2026-11-03).
- Фикстура `cbr/testdata/loans_ind.xlsx` — живой файл целиком (~104 КБ; источник указан в спеке;
  скачать можно `curl -A "Mozilla/5.0" -o … <url>`).
- `cbr/int_rate_loans_ind_test.go`: двусторонний контракт каталога через `checkCbrSeriesMeta`;
  разбор фикстуры — ожидания по точкам: 16 рядов, 144 месяца, с пропусками `-`/пусто (по факту
  разбора: «Всего: свыше 1 года» ~33 точки, автокредиты краткие частично) — не жёсткий
  литерал, а правило + полный count() снимка; даты — последний день месяца; spot: 2014-01
  «Всего: до 30 дней, включая «до востребования»» = 28.04 (проверить по фикстуре);
  2025-12 «Всего: свыше 1 года» отсутствует; fail-fast: нет листа / нет «% годовых» /
  расхождение заголовков сроков / 0 точек.

### Task 2 (волна 1, coder, тот же агент): контур волны 1

- `cbr/views.go`: `cbrMacroView.Tables` += `cbr_int_rate_loans_ind`; `UNION ALL SELECT 'cbr',
  toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans_ind FINAL` после строки
  `cbr_int_rate_loans`; Comment/Columns — спека §3.5.
- `cbr/importers.go`: запись после `cbr_int_rate_loans` (порядок = порядок dagu).
- `cbr/importers_test.go`: +1 в `expectedPublicationCountPoints` (счётные комментарии
  «одиннадцать» → «двенадцать»).
- `dagu/cbr.yaml`: шаг `cbr_int_rate_loans_ind` после `cbr_int_rate_loans`, `retry_policy`
  по образцу соседей.
- `sql/key_rate_coverage.sql`: UNION ALL по `cbr_int_rate_loans_ind` (по образцу).
- `scripts/mcp_check.py`: каталог A — `SELECT count() FROM v_series_catalog WHERE source='cbr'
  AND origin LIKE '%loans_ind.xlsx%'` = 16; deny 497 на сырой `cbr_int_rate_loans_ind`.

### Task 3 (волна 1, контроллер): приёмка волны 1

1. `make all` — зелёный; `go test -race -count=1 ./cbr/` — зелёный.
2. `make import STAT=cbr_int_rate_loans_ind` — ожидание ~2,0–2,2 тыс. строк, 2014-01-31 …
   2025-12-31; spot-сверка 3 значений с живым файлом через MCP.
3. Повтор — no-op; `source_publications` — строка версии.
4. `make mcp-check` — зелёный (включая новые проверки); `dagu validate dagu/cbr.yaml`.
5. Ревью задачи (haiku) — до приёмки; fix-loop ≤ 3, при необходимости эскалация
   `deepseek/deepseek-flash`.

### Task 4 (волна 2, coder): `cbr_mortgage_rate` + контур волны 2

- `cbr/mortgage_rate.go`: `cbrMortgageRate` = `util.ClickHouseImport`, статичный
  `DataUrl: https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_08_Rates_housing.xlsx`,
  DDL спеки §3.2, разбор листа `ставка в рублях` спеки §2.2/§3.3 (широкий формат; якорь
  `РОССИЙСКАЯ ФЕДЕРАЦИЯ`), `PublicationCountPoints: SELECT count() FROM cbr_mortgage_rate FINAL`.
- `cbr/series_meta.go`: `cbrMortgageRateSeriesMeta` — 1 запись (спека §3.5 B);
  `AfterImport: afterCbrMacro(cbrMortgageRateSeriesMeta)`.
- Фикстура `cbr/testdata/rates_housing.xlsx` — живой файл целиком (~166 КБ).
- `cbr/mortgage_rate_test.go`: контракт каталога; разбор — 93 точки, 2019-01-31 …
  2026-09-30, spot 2026-09 = 8.61, 2026-08 = 10.09 (сверить с фикстурой); fail-fast: нет
  листа / нет строки РФ.
- Контур волны 2: `cbr/views.go` (Tables + UNION + комментарии), `cbr/importers.go`
  (13-я запись), `cbr/importers_test.go`, `dagu/cbr.yaml` (шаг в хвосте), `sql/key_rate_coverage.sql`,
  `scripts/mcp_check.py` (+1 ряд каталога `origin LIKE '%Rates_housing.xlsx%'`, deny 497).
- Приёмка волны 2 (контроллер): `make all`, импорт (93 строки), повтор no-op, MCP spot,
  `make mcp-check`, `dagu validate`. Ревью (haiku) — до приёмки.

### Task 5 (волна 3, coder): `cbr_psk` — корпус ПСК + витрина + грант

- `cbr/psk.go`: импортёр `cbrPsk` со своим гейтом ленты (по образцу `cbr_mt_forecast`:
  лента страницы `/statistics/bank_sector/psk/`, известные id файлов → пропуск; новые →
  скачивание/разбор/запись), DDL спеки §3.2, три адаптера эр (PDF `pdftotext -layout` через
  `BodyFunc`; XLS — `xls.OpenReader`, прецедент `fedstat/excel.go`; XLSX — excelize),
  нормализация к «код | категория | avg | max | период», период из заголовка выпуска
  (последний день квартала), тип `ko` только.
- `cbr/series_meta.go`: `cbrPskSeriesMeta` — по категориям эр (спека §3.5 C); витрина
  `v_cbr_psk` в `cbr/views.go` с комментариями таблицы и колонок; `AfterImport` —
  каталог + витрина.
- `cbr/importers.go`: 14-я запись, countPoints `""` (гейт ленты — литерал пустой, по
  образцу `cbr_mt_forecast`); `cbr/importers_test.go`; `dagu/cbr.yaml` (хвост);
  `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON v_cbr_psk TO kimi_reader` (по образцу);
  `sql/key_rate_coverage.sql`; `scripts/mcp_check.py` (витрина читается агентом; сырой
  `cbr_psk` закрыт — 497).
- Фикстуры: `cbr/testdata/psk_ko_latest.xlsx` (14082026, ~17 КБ), `cbr/testdata/psk_ko_2020.xls`
  (14082020), `cbr/testdata/psk_ko_2018.pdf` (16112018).
- `cbr/psk_test.go`: разбор трёх эр на фикстурах (XLSX: 20 листьев, spot 6.1.1 avg 17.779 /
  max 23.705 за 2026-06-30; XLS: категории 1–5, точка-десятичный; PDF: запятая-десятичный,
  период 2018-09-30); гейт ленты (известный id → 0 скачиваний); fail-fast: нет листа AVG /
  непарсящийся заголовок периода / 0 точек по новому выпуску.
- Приёмка волны 3 (контроллер): `make all`; импорт — ориентир ~0,8–1,0 тыс. строк
  (48 выпусков: 23 PDF + 13 XLS + 12 XLSX по ~15–20 категорий), первый квартал 2014-09-30,
  последний 2026-06-30; повтор — no-op по ленте; MCP spot витрины (6.1.1/6.1.2 vs ставка
  источника B — согласованность порядка величин); `make mcp-user` при необходимости;
  `make mcp-check`; `dagu validate`. Ревью (haiku) — до приёмки.

### Task 6 (контроллер): документация и роадмап

- `ARCHITECTURE.md`: шапка §2 (новая запись), §6.1 (пакет `cbr/` — +3 импортёра; шаги
  цепочки), §6.2 (канонические DDL трёх таблиц + витрина `v_cbr_psk`), §6.3 (списки
  витрин/таблиц), §7 (контур ЦБ — счётчики шагов), §8 п. 13 (счётчик объявлений: 20 → 21
  для cbr с префлайтом; `cbr_psk` — с гейтом ленты). Сверка — skill `sync-readme-architecture`.
- `README.md`, `docs/IMPORTERS.md` — соответствующие строки пакета `cbr`.
- `docs/ROADMAP_KEY_RATE.md`: §4.2 №8 — ✅ остаток закрыт (три источника, числа приёмки);
  §4.1 — строки новых рядов в таблицу покрытия. Факты приёмки — команда/числа/даты.

### Task 7 (контроллер): финальное ревью, PR, протокол

- Финальное ревью ветки (`superpowers:requesting-code-review`) на `deepseek/deepseek-flash`.
- Один PR: пункт, `Closes #119`, три источника, схемы, запуск, примеры проверок; протокол
  живой приёмки — комментарием (стандарт §16.1 п. 8). Project: `In Review`, поле PR.

## Порядок диспатча

T1+T2 одному coder-субагенту (`deepseek/deepseek-flash`) → ревью (haiku) → T3 приёмка →
T4 coder → ревью → приёмка → T5 coder → ревью → приёмка → T6 → T7. Волны строго
последовательные: общие файлы (`cbr/importers.go`, `cbr/views.go`, `dagu/cbr.yaml`,
`scripts/mcp_check.py`, `sql/key_rate_coverage.sql`) конфликтуют при параллели.
Fix-round'ы: до 3 на задачу на том же coder, далее эскалация контроллеру.

## Контракт промпта coder-субагента (в каждой волне)

Роль `go-engineer` (путь к скилу прочитать обязательно: `.kimi-code/skills/go-engineer/SKILL.md`);
правила `AGENTS.md` текстом (батч `PrepareBatch`→`Append`→`Send`; DDL `CREATE TABLE IF NOT
EXISTS`/`ReplacingMergeTree`/`LowCardinality(String)`/Float64; HTTP только через
`util.GetXlsx`/`util.HttpClient`; сети в `init()` нет; все `strconv`/`time.Parse`/
`batch.Append` с проверкой; logrus, не `fmt.Print*`; секретов нет; новый код не в
`financial/`); блок: «У тебя есть инструменты `mcp__jetbrains__*` (GoLand MCP) и
`mcp__clickhouse__*`. Используй `mcp__jetbrains__*` для проверки правок и навигации вместо
grep и лишней сборки; SQL к ClickHouse — только через `mcp__clickhouse__run_query`. Если
MCP-инструментов в наборе нет — скажи об этом в отчёте (`DONE_WITH_CONCERNS`)»; точные пути
файлов и образец `cbr/int_rate_loans.go`; границы скопа (не трогать чужие волны);
`make import` не гонять (приёмка — контроллером); worktree `.worktrees/retail-loan-rates`.

## Протокол живой приёмки (состав — стандарт §16.1 п. 8)

Команды, числа строк, крайние даты, spot-сверки из ClickHouse, no-op повторы, строки
версий `source_publications`, `make mcp-check`, `dagu validate`, строки архива
(`make archive-registry`) — комментарием в PR после приёмки.
