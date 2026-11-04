# ВТБ: квартальные супердополнения — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `rus_vtb_group_ifrs` забирает квартальные супердополнения ВТБ (`financial_data_supplement_<ММГГ>_rus.xlsx`): квартальные балансовые остатки склеиваются с месячными рядами, потоки P&L и коэффициенты идут рядами «(9М)»/«(3кв)», кредитное качество и Н20.x (даты 1-го числа следующего месяца) — новыми рядами; каталог дополнен на 31 описание.

**Architecture:** Две семьи файлов собираются с одной страницы результатов и сортируются по дате отчётности общим списком; гейт пофайловых версий, таблица, DDL, restore-отказ — без изменений. `vtbIfrsImportFunc` диспетчеризует разбор по листам книги: месячный путь — существующие три прохода, квартальный — новый `parseVtbSupplementBook` (state machine по секциям листа «1.Ключевые показатели», литеральная карта имён, суффиксы гранулярности). Производные — общий `afterBankMacro`, каталог `vtbSeriesMeta` с частотой «Q» у новых групп.

**Tech Stack:** Go (модуль репозитория), `github.com/PuerkitoBio/goquery`, `github.com/xuri/excelize/v2`, `clickhouse-go/v2`, `logrus`; проверки — `make`, dev-ClickHouse.

**Spec:** `docs/superpowers/specs/2026-11-04-bank-vtb-supplement-design.md` — обязательна к прочтению. Ссылки «спека §N» — на неё, «стандарт §N» — на `docs/IMPORTER_PACKAGE_STANDARD.md`.

## Global Constraints

- Правила 1–6 AGENTS.md: секреты только env; HTTP только `util`; один батч; ошибки не проглатывать; logrus.
- Таблица `rus_vtb_group_ifrs` — имя, DDL, ключ, движок без изменений; новых таблиц/витрин/грантов/шагов dagu нет (спека §3).
- Месячные 13 файлов — поведение без изменений: фикстуры `vtb_ifrs*.xlsx` и их ожидания зелёные без правок.
- Разбор — чистая функция (§6.1), сырые значения (§6.2), fail-fast (§6.4); незнакомая подпись со значениями — ошибка, не warn (спека §4.3).
- Фикстуры — живые артефакты как есть (§11.1–11.2); тесты офлайн (§11.3); спот-значения литералами из разведки спеки §2.2.
- Каждый коммит — после `gofmt` и зелёного адресного `go test -race ./bank/`; сеть и ClickHouse — только через `make`.

## Review Focus

1. **Диспетчеризация по листам**: книга без «Ключевые балансовые показатели» и без «1.Ключевые показатели» (или с обоими) — ошибка со списком листов, не молчаливый выбор (спека §4.2; тест fail-fast).
2. **Ключ (name, date) внутри файла**: суффиксы «(9М)»/«(3кв)» и карта обязаны исключать дубль — тест уникальности на фикстуре (например «Чистая прибыль (9М)» и «Чистая прибыль (3кв)» обе датированы 2026-09-30).
3. **Сноска против суффикса**: «РАСХОДЫ НА РЕЗЕРВЫ (2)» → «РАСХОДЫ НА РЕЗЕРВЫ» до карты; «(Н20.0)» не снимается (в скобках не цифры); суффикс берётся из колонки, не из подписи.
4. **Дата отчётности семьи**: `ММГГ` вне 01–12 / несуществующая дата (нет такого кейса у последнего дня месяца) — ошибка с именем файла; месячный `as-of` без изменений.
5. **Каталог**: каждый ряд, разобранный из фикстуры супердополнения, описан (`TestBankSeriesMetaCoversFixtures`, четвёртый кейс); у Н20.x в описании — оговорка про дату 1-го числа следующего месяца.

---

### Task 1: Спека, план, пункт роадмапа (первые коммиты)

**Files:**
- Create: `docs/superpowers/specs/2026-11-04-bank-vtb-supplement-design.md`
- Create: `docs/superpowers/plans/2026-11-04-bank-vtb-supplement.md`
- Modify: `docs/ROADMAP_KEY_RATE.md` (§6 «Фаза 3.2», перед «Фаза 4»)

- [x] **Step 1:** Спека и план написаны, пункт роадмапа вставлен (issue #526).
- [ ] **Step 2:** Коммит: `git add` трёх файлов, сообщение `docs: spec and plan for bank vtb supplement phase 3.2`.

### Task 2: Фикстуры

**Files:**
- Create: `bank/testdata/vtb_supplement_0926.xlsx` (живая книга 9М'2026, 123 372 Б)
- Modify: `bank/testdata/vtb_results_page.html` (снимок 2026-11-04: 13 месячных xlsx + 5 supplement + верхнерегистровые ссылки старого пути)

- [ ] **Step 1:** Скопировать книгу из разведки (`/tmp/vtb_supplement_0926.xlsx`) в `bank/testdata/` как есть; свежий снимок страницы (`curl` через `make`-окружение, UA из `util`) положить вместо старого `vtb_results_page.html`.
- [ ] **Step 2:** Проверить: `grep -c financial_data_supplement bank/testdata/vtb_results_page.html` — 5 уникальных href `.xlsx`; `unzip -l` книги — 4 листа («Содержание», «1.Ключевые показатели», «2.P&L», «3.Баланс »).
- [ ] **Step 3:** Коммит: `bank: add vtb supplement 0926 fixture and refresh results page snapshot`.

### Task 3: Ссылки двух семей (`bank/vtb_fin_rez.go`)

**Files:**
- Modify: `bank/vtb_fin_rez.go`

**Interfaces:**
- `vtbLink` — новое поле семьи (`Kind`: месячный/квартальный) — заполняется в `vtbIfrsXlsLinks`, читают тесты.
- `vtbIfrsXlsLinks(page)` — теперь 18 ссылок на новой фикстуре; сигнатура без изменений.
- `vtbIfrsImportFunc(xlsx, batch)` — диспетчеризация по листам (спека §4.2).

- [ ] **Step 1:** Константы: `vtbSuppLinkSelector = "a[href*=\"financial_data_supplement\"]"`, регексп `vtbSuppReportRe = financial_data_supplement_(\d{2})(\d{2})_rus\.xlsx` (как у месячного — поиск по всему href), функция `vtbSuppReportDate(href)` (месяц 01–12, дата — последний день месяца 20YY, проверка существования; ошибки с именем файла).
- [ ] **Step 2:** `vtbIfrsXlsLinks`: два селектора, общий фильтр `.xlsx` и дедуп, семя в `vtbLink`, сортировка `SliceStable` по дате отчётности без изменений; сообщение ошибки пустого списка называет оба имени файла.
- [ ] **Step 3:** Диспетчеризация: `vtbHasSheet(xlsx, anchor) bool` (молчаливая проверка через `GetSheetList`); `parseVtbBook` — точка входа: ровно одна семья по листам → свой парсер; 0 или 2 — ошибка со списком листов книги. Месячный путь — существующие `parseVtbBookSheet`/`parseVtbPnlSheet`/`parseVtbRatiosSheet` без изменений.
- [ ] **Step 4:** `gofmt`, `go test -race -count=1 ./bank/ 2>&1 | tail` — месячные тесты зелёные (ожидания списка ссылок ещё старые — красным только `TestVtbIfrsXlsLinks`, чинится в задаче 5).

### Task 4: Разбор супердополнения (`bank/vtb_supplement.go`)

**Files:**
- Create: `bank/vtb_supplement.go`

**Interfaces:**
- `parseVtbSupplementBook(xlsx *excelize.File) ([]vtbPoint, error)` — вызывается из `parseVtbBook`; точки общего типа `vtbPoint` (существующего).

- [ ] **Step 1:** Регекспы периодов: `vtbSuppYtdRe = ^(\d+)[МM]'(\d{2})$`, `vtbSuppQtrRe = ^(\d+)кв'(\d{2})$`; `vtbSuppNoteRe = \s*\(\d+\)$` (сноска перед картой). Колонка-период: дата (последний день месяца N / месяца 3N) + суффикс « (NМ)»/« (Nкв)» из подписи колонки.
- [ ] **Step 2:** `vtbSuppSeriesMap` — литеральная карта из спеки §4.4 (27 пар: 9 баланс + 8 P&L + 5 коэффициентов + 3 качества + 2 капитала).
- [ ] **Step 3:** Проход листа «1.Ключевые показатели» (`vtbSheet(xlsx, vtbSuppSheet)`, `RawCellValue`): классификация строки-шапки (периоды или серийные даты через `vtbSerialColumns`), состояние текущих колонок, точки через `vtbRowPoints` с именем «база+суффикс»; строки без значений — пропуск; значения без подписи / незнакомая подпись / не число / NaN/Inf — ошибка с листом, строкой и подписью (§6.4). Контроль: ни одной шапки или ни одной точки — ошибка.
- [ ] **Step 4:** `gofmt`, `go build ./bank/`.

### Task 5: Тесты разбора и ссылок

**Files:**
- Create: `bank/vtb_supplement_test.go`
- Modify: `bank/vtb_fin_rez_test.go`

- [ ] **Step 1:** `vtb_fin_rez_test.go`: `vtbFixtureLinks` — 18 записей (5 новых с `Supplement: true`, даты 2025-09-30 / 2025-12-31 / 2026-03-31 / 2026-06-30 / 2026-09-30 на своих позициях сортировки); `TestVtbIfrsXlsLinks` — 13 → 18. Fail-fast имени супердополнения: замена `0926` → `1326` (месяц 13) — ошибка с именем файла.
- [ ] **Step 2:** `vtb_supplement_test.go`: разбор фикстуры — 100 точек (18 баланс + 32 P&L + 20 коэффициентов + 6 качество + 24 капитал); споты из спеки §2.2 (39095.9 / 407.5 / 182.3 / 0.105, NIM 0.026 (9М'26) и 0.027 (3кв'26)); ряды «Чистая прибыль» месячного набора в разборе супердополнения отсутствуют (склейка только суффиксных); уникальность (name, date) в разборе.
- [ ] **Step 3:** Fail-fast на живой фикстуре (мутации в памяти, §11.1): переименован лист «1.Ключевые показатели»; незнакомая подпись (правка A12); сноска «(3)» у подписи (парсится после снятия — позитивный); «NaN» в значении; значение «н/д»; значения без подписи; пустой разбор (все значения секции стерты). Каждая — ошибка, называющая таблицу и причину; ожидания литералами (§8.5).
- [ ] **Step 4:** Диспетчеризация: книга с двумя якорями и без обоих — ошибка (позитивный тест обеих семей уже есть).
- [ ] **Step 5:** `go test -race -count=1 ./bank/` — зелёный; коммит `bank: parse vtb quarterly supplements`.

### Task 6: Каталог и метаданные

**Files:**
- Modify: `bank/series_meta.go`, `bank/series_meta_test.go`

- [ ] **Step 1:** `bankSourceGroup.Frequency` (пусто → «M»); `vtbSeriesMeta` — четыре новые группы с частотой «Q»: потоки 16 имён (8 баз × « (9М)»/« (3кв)»), коэффициенты 10, качество 3 (новые имена), капитал 2 (новые имена, в описании — дата 1-го числа следующего месяца). Итого 36 + 31 = 67. Имена списками из карты (литералы, не конкатенация в цикле — сверка глазами с картой).
- [ ] **Step 2:** `series_meta_test.go`: кейс фикстуры супердополнения в `TestBankSeriesMetaCoversFixtures` (parsed — `parseVtbBook(vtbOpenFixture(t, vtbSuppBookFixture))`), литералы 36 → 67 и 132 → 163, состав `wantSources` — ВТБ 67.
- [ ] **Step 3:** `go test -race -count=1 ./bank/` — зелёный; коммит `bank: catalog vtb supplement series`.

### Task 7: Документация

**Files:**
- Modify: `ARCHITECTURE.md` §6.2 (строка `rus_vtb_group_ifrs` — две семьи файлов) и §6.3 (`v_bank_macro` — состав без изменений, формулировка про ВТБ), `README.md` (таблица импортёров/состав архива: +5 пофайловых версий ВТБ), `docs/IMPORTERS.md` (раздел ВТБ — семья супердополнений, суффиксы, Н20.x).

- [ ] **Step 1:** Правки тремя файлами по факту кода (скил `sync-readme-architecture`: README и ARCHITECTURE — в одном PR).
- [ ] **Step 2:** `make docs-check` — зелёный; коммит `docs: vtb supplement source in architecture and readme`.

### Task 8: Полная проверка и приёмка

- [ ] **Step 1:** `make all` — зелёный (gofmt, lint, vet, `go test -race`, сборки).
- [ ] **Step 2:** Живой прогон `make import STAT=rus_vtb_group_ifrs` — первый: 13 месячных «пропуск», 5 супердополнений импортированы; повтор: `Imported 0 rows`, 18 файлов «пропуск». Spot-сверка через MCP: значения спеки §7 п. 3; VTBR-метрики в `v_company_financials` продлены до 2026M09; месячные 720 строк без изменений.
- [ ] **Step 3:** `make mcp-check` — зелёный (банковский контур); `dagu validate dagu/bank.yaml` — exit 0.
- [ ] **Step 4:** Архив версий: +5 объектов и строк `source_publications` по ВТБ.
- [ ] **Step 5:** Ревью ветки (task-reviewer на отличной модели, скил `subagent-dispatch`); fix-loop; PR с `Closes #526`, описанием по шаблону; протокол — комментарием в PR; поля Project: `In Review`, `PR`.
