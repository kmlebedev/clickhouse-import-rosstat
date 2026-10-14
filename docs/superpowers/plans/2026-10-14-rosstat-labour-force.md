# Рынок труда Росстата (`rosstat_labour_force_mes`) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить в контур ключевой ставки шесть помесячных рядов рынка труда РФ (`rosstat.gov.ru/labour_force` → `trud-1_15-s.xlsx`, лист «5») в таблицу `rosstat_labour_force_mes`, в каталог `series_catalog` и в витрину `v_rosstat_macro`.

**Architecture:** Импортёр `rosstat_labour_force_mes` в существующем пакете `rosstat/` по стандарту пакета: шаблон `util.ClickHouseImport` с переопределением `Import()` (ссылка на файл берётся со страницы), разбор отделён от сети (чистая `parseLabourForce(*excelize.File)`), производные (каталог + витрина) публикует `afterRosstatMacro`, точка контроля публикации объявлена — файл перезаписывается на месте раз в месяц при недельном шаге расписания.

**Tech Stack:** Go, `github.com/xuri/excelize/v2`, `github.com/ClickHouse/clickhouse-go/v2`, goquery через `util.GetPage`/`util.FindLink`, logrus. Без новых зависимостей.

**Spec:** [docs/superpowers/specs/2026-10-14-rosstat-labour-force-design.md](../specs/2026-10-14-rosstat-labour-force-design.md) — исполнитель читает и спеку, и план.

## Global Constraints

- Новые таблицы: `LowCardinality(String)` для измерений, `Date` для дат, **`Float64`** для значений, `ENGINE = ReplacingMergeTree`, `ORDER BY (name, date)`, DDL — `CREATE TABLE IF NOT EXISTS %s (...)` (`%s` обязателен: шаблон подставляет имя сам).
- Вставка — только `PrepareBatch`/`batch.Append`/`batch.Send`; построчный `conn.Exec` в цикле запрещён.
- Сеть — только через `util` (`util.GetPage`, `util.FindLink`, `util.GetXlsx`); собственный `net/http` и `colly` запрещены; сетевых вызовов в `init()` нет.
- Логирование — только logrus; `fmt.Print*` в пакете запрещён.
- Каждый `strconv.Parse*`, `batch.Append`, `time.Parse` — с проверкой ошибки; ошибки не проглатываются и возвращаются вызывающему.
- Значения ячеек читаются **сырыми**: `xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`.
- Разметка ищется **по признаку** (имя листа, подпись-якорь, подпись колонки), а не по магическому индексу строки/колонки.
- Fail-fast: нет листа; нет шапки; колонка ряда не найдена или найдена дважды; нечисловое значение; пустой разбор — **ошибка с именем таблицы и листа**, не «0 строк». `panic` в разборе запрещён.
- Пустая ячейка — **не точка** ряда (это отсутствие данных, а не ноль).
- Имена рядов и имя таблицы **закрепляются кодом** (`rosstat_labour_force_mes`, шесть имён из спеки §3.3); заголовки источника не используются как имена.
- Селектор ссылки и адрес страницы — **константы пакета** рядом с импортёром.
- Реестр `rosstat/importers.go`: таблица и запрос чека — **литералы**; персональных `init()` в файлах импортёров нет.
- Тесты офлайн (без сети и без БД), фикстуры — реальные урезанные артефакты источника.
- Точка контроля публикации: `PublicationCountPoints = "SELECT count() FROM rosstat_labour_force_mes FINAL"`, `PublicationSource` не заполняется (`Name() == TableName`).
- Новых витрин и новых грантов не заводится: ряды идут в существующую `v_rosstat_macro`, уже выданную `kimi_reader` (гранты переживают `CREATE OR REPLACE VIEW`); сырая `rosstat_labour_force_mes` агенту не выдаётся; `sql/mcp_kimi_reader.sql` не меняется.
- Документация и статус роадмапа — тем же PR.

## Review Focus

Пять классов входных данных, которые спека подразумевает, но ни один тест из неё напрямую не выводится, — по одному тесту на каждый в задаче, владеющей кодом:

1. **Пустая ячейка последнего месяца нового года** (данных ещё нет) → точки нет, а не значение 0 или ошибка.
2. **Ячейка не число и не пустая** (текст, мусор) → ошибка разбора с именем листа и строки, не молчаливый пропуск.
3. **Подпись месяца с одной и двумя сносками** (`январь1)`, `октябрь1)2)`) → сноски снимаются, месяц распознаётся; незнакомая подпись → ошибка.
4. **Строка-секция года без данных / мусорная строка в середине** → год переносится вперёд, а строки после конца таблицы (текст сносок) разбор завершают, а не роняют.
5. **Ссылка на файл не найдена** (Росстат переименовал файл) → ошибка с именем ожидаемого файла, а не `GetXlsx("")` и не 0 строк.

---

### Task 1: Фиксация разведки §4.2 и нового порядка фазы 2 в роадмапе

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.2, §6 фаза 2)

**Interfaces:**
- Consumes: результаты разведки источников от 2026-10-14 (приведены ниже дословно).
- Produces: обновлённую карту пробелов, на которую ссылаются последующие пункты фазы 2.

- [ ] **Step 1: Переписать строки §4.2 по проверенным данным**

Таблица §4.2 («Чего нет — карта пробелов») получает по каждому открытому пункту проверенный источник. Тексты — дословно из разведки:

| # | Что записать |
|---|---|
| 4 | ✅ **взят в работу этим PR** (`rosstat_labour_force_mes`, спека `docs/superpowers/specs/2026-10-14-rosstat-labour-force-design.md`): `rosstat.gov.ru/labour_force` → `trud-1_15-s.xlsx`, лист «5», 6 рядов, 2017-01+ |
| 6 | URL карты **битый** (`/blog/personal_blog/KonstantinLebedev/rss/` → 404); рабочий — `https://smart-lab.ru/rss/personal_blog/KonstantinLebedev/` (200, RSS 2.0 UTF-8, полный текст в `description`, **только 20 последних постов**) |
| 7 | индекс `cbr.ru/dkp/mp_dec/decision_key_rate/` (200) → страница `…/comment_DDMMYYYY/` → xlsx `…/commet_graph_YYYYMMDD.xlsx` (опечатка «commet» — в источнике), 11 выпусков 2024-02-27…2026-08-05; «Табл 1» — диапазоны ставки и инфляции по годам, «Рис 1» — ставка по дням с 2014-01-01 |
| 8 | **прямые стабильные xlsx:** `/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx` (+ `deposits`, `by_activity`, `by_region`, `without_concessional`), лист `ставки_руб`, помесячно с января 2014 |
| 9 | прямых файлов нет, `XML_zcyc.asp` мёртв (302→404); только HTML-таблица формы `UniDbQuery`, лимита выборки нет, глубина 04.01.2003 … сегодня; нужен разбор HTML-таблиц (хелпера в `util` нет) |
| 10 | URL карты **404**; рабочий `https://www.cbr.ru/hd_base/mkr/mkr_base/` (200), только HTML, без полного querystring формы история даёт 0 строк, глубина 13.07.2000 |
| 11 | URL карты **404**; рабочие `/hd_base/bliquidity/` (дневная, с 01.01.2014) и `/hd_base/BSliquidityMA/` (периоды усреднения, с 10.01.2018), только HTML |
| 12 | АЦН-индикаторы — прямой xlsx `…/indicators/ccyb_indicators_time_series.xlsx` (месячно с 02.2008); само значение АЦН рядом не публикуется; МПЛ — скрап лендинга, 2023Q1…2026Q4; надбавки к коэффициентам риска — 33 файла с датой в имени; отдельного xlsx-канала по НКЛ не найдено |
| 13 | **Brent** — прямой FRED CSV `fredgraph.csv?id=DCOILBRENTEU` (200, дневная с 20.05.1987; разделитель `,`). **Urals — машиночитаемого ряда нет** ни в ЦБ, ни в Минфине, ни в Минэке, ни в ЕМИСС |
| 14 | **прямые стабильные xlsx:** `vfs/statistics/credit_statistics/bop/bop_est.xlsx` (оценка, месячно+квартально) и `bal_of_payments_standart.xlsx` (квартально с 1 кв. 1994, 131 колонка) |
| 15 | Минфин, xlsx **вычисляется** (дата публикации в имени, угадывание → 404): `Svedeniya_<YYYYMMDD>.xlsx` (нефтегаз, НДПИ, с янв. 2018) и `Prilozhenie_3_dannye_109-111_—_mes.xlsx` (дефицит/профицит, YTD-накоплением, с 2011); без браузерного UA — 503 |
| 16 | подтверждено: числового ряда ЦБ не публикует; прокси уже есть (`cbr_mp_survey`) |
| 17 | **недоступно:** SA-ИПЦ/SAAR у Росстата нет (0 совпадений «сезон» на странице цен); SA только для промпроизводства (`sezon_YYYY_MM-YYYY.xlsx`) |
| 18 | числового ряда нет; ближайшее — медианы `cbr_mo_br` и диапазоны из п. 7 |

- [ ] **Step 2: Переписать приоритетную строку фазы 2 (§6)**

Новый порядок с причиной перестановки (стоимость транспорта): починка пустых рядов Сбера → **№4 рабочая сила (этот пункт)** → №8 ставки ЮЛ и №14 платёжный баланс (прямые стабильные файлы) → №13 Brent (FRED; Urals — отдельным пунктом, источника нет) → №12 АЦН-индикаторы → №15 Минфин (скрап) → **один инфраструктурный пункт «HTML-таблицы ЦБ»** на №9/#10/#11 (общий разбор таблиц + обязательная форма `UniDbQuery`) → №7 «Комментарий» → №6 smart-lab (в news-контур) → №17 снять/переформулировать.

- [ ] **Step 3: Коммит**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "docs: fix key rate gap map with verified sources"
```

---

### Task 2: Разбор `parseLabourForce` и фикстуры

**Files:**
- Create: `rosstat/labour_force.go` (на этой задаче — только константы и разбор)
- Create: `rosstat/labour_force_test.go`
- Create: `rosstat/testdata/labour_force.xlsx`
- Create: `rosstat/testdata/labour_force_page.html`
- Modify: `rosstat/series_meta_test.go:18-53` (харнесс `appendRecorder`)

**Interfaces:**
- Consumes: ничего из предыдущих задач.
- Produces (используется в Task 3 и Task 4):
  - `rosstatLabourForceTable = "rosstat_labour_force_mes"` — const;
  - `labourForceLinkSelector = `a[href$="trud-1_15-s.xlsx"]`` — const (селектор ссылки);
  - `labourForcePage = "https://rosstat.gov.ru/labour_force"` — const;
  - `type labourForcePoint struct { series string; date time.Time; value float64 }`;
  - `func parseLabourForce(xlsx *excelize.File) ([]labourForcePoint, error)`;
  - `func labourForceLabel(raw string) string` — снимает хвостовые сноски и сжимает пробелы;
  - харнесс `appendedRow.value` — **`float64`** (было `float32`) плюс новое поле `valueType string` — имя Go-типа записанного значения.

- [ ] **Step 1: Собрать фикстуру книги (реальную, урезанную)**

```bash
curl -sS -o /tmp/trud1.xlsx "https://rosstat.gov.ru/storage/mediabank/trud-1_15-s.xlsx"
python3 -c "import openpyxl; wb=openpyxl.load_workbook('/tmp/trud1.xlsx'); print(wb.sheetnames)"
```

Урезать «хирургией» zip, сохранив листы `Содержание` и `5` целиком (разметка и значения не меняются): из `xl/workbook.xml` убрать элементы `<sheet>` лишних листов, из `xl/_rels/workbook.xml.rels` — соответствующие `<Relationship>`, из `[Content_Types].xml` — `<Override PartName="/xl/worksheets/sheetN.xml">`, файлы `xl/worksheets/sheetN.xml` удалённых листов в архив не включать. Проверить, что книга открывается:

```bash
python3 -c "import openpyxl; wb=openpyxl.load_workbook('rosstat/testdata/labour_force.xlsx', data_only=True); print(wb.sheetnames); ws=wb['5']; print(ws.max_row, ws.max_column)"
```

Ожидаемо: листы `['Содержание', '5']`, `135 7`. Если урезанная книга не открывается — положить в `testdata/` полную копию файла как есть (343 КБ) и назвать это в отчёте задачи.

- [ ] **Step 2: Собрать HTML-фикстуру страницы**

Скачать `https://rosstat.gov.ru/labour_force?print=1` (95 КБ) и сохранить фрагмент с карточкой «Занятость и безработица» — так, чтобы в фикстуре были **и** ссылка на `trud-1_15-s.xlsx`, **и** соседняя `trud_1_15-72.xlsx` (иначе селектор не проверяется). Проверка:

```bash
grep -c "trud-1_15-s.xlsx\|trud_1_15-72.xlsx" rosstat/testdata/labour_force_page.html
```

Ожидаемо: обе ссылки присутствуют.

- [ ] **Step 3: Написать падающие тесты разбора**

В `rosstat/labour_force_test.go` — тесты (имена и утверждения; код фикстуры открывается так же, как в `rosstat/salaries_mes_test.go:31`):

- `TestLabourForceReadsFixture` — `parseLabourForce` по фикстуре даёт **690 точек**, шесть различных рядов, минимальная дата `2017-01-31`, максимальная `2026-07-31`.
- `TestLabourForceSpotValues` — точные значения из источника (см. §2 спеки): `Рабочая сила` `2017-01-31` = `76489.193`; `Уровень безработицы` `2017-01-31` = `5.6`; `Рабочая сила` `2026-07-31` = `76389.482`; `Уровень занятости` `2026-07-31` = `61.5`; `Уровень безработицы` `2026-07-31` = `2.3`.
- `TestLabourForceFootnoteLabels` — `labourForceLabel("январь1)")` == `"январь"`, `labourForceLabel("октябрь1)2)")` == `"октябрь"`, `labourForceLabel("Уровень занятости, \nв %")` == `"Уровень занятости, в %"` (Review Focus 3).
- `TestLabourForceMissingSheetFails` — книга без листа «5» → ошибка, в тексте `rosstat_labour_force_mes` и `5` (отсутствие данных не означает «0 точек»).
- `TestLabourForceMissingHeaderFails` — лист «5» без строки шапки → ошибка с именем листа.
- `TestLabourForceNonNumericCellFails` — книга-копия фикстуры, где в строке месяца значение заменено текстом → ошибка разбора, значение не пропущено (Review Focus 2).
- `TestLabourForceEmptyCellIsNotPoint` — книга, где у последнего месяца нового года значения пусты → точек меньше ровно на число пустых ячеек, нулей в ряду нет (Review Focus 1).
- `TestLabourForceIgnoresTrailingFootnoteRows` — после последней строки месяца идут строки текста сносок → разбор возвращает точки и не падает (Review Focus 4).
- `TestLabourForceFindLink` — `util.FindLink(page, labourForceLinkSelector)` по HTML-фикстуре даёт `/storage/mediabank/trud-1_15-s.xlsx` (а не `trud_1_15-72.xlsx`); на странице без ссылки — ошибка (Review Focus 5).

Для книг-мутаторов в тестах использовать существующий `newTestWorkbook` (`rosstat/series_meta_test.go:73`) — но **фикстурные** тесты обязаны открывать реальный `testdata/labour_force.xlsx`.

- [ ] **Step 4: Прогнать тесты — падают**

Run: `go test -race ./rosstat/ -run 'LabourForce' -v`
Expected: FAIL — `undefined: parseLabourForce`, `undefined: labourForceLabel`, `undefined: labourForceLinkSelector`.

- [ ] **Step 5: Расширить харнесс `appendRecorder` до float64**

В `rosstat/series_meta_test.go` поле `appendedRow.value` — `float64`, добавить рядом поле `valueType string` (имя Go-типа значения, `fmt.Sprintf("%T", v[2])`); в `Append` заменить жёсткое приведение к `float32` (строки 47-50) на разбор обоих типов:

```go
var value float64
switch v := v[2].(type) {
case float32:
	value = float64(v)
case float64:
	value = v
default:
	return fmt.Errorf("appendRecorder: значение %T, ожидалось float32 или float64", v[2])
}
```

`valueType` нужен потому, что сам `value` — уже `float64` у обоих типов, и без него тест не отличит `float32`-запись (`rosstat_salaries_mes`, legacy) от `float64`-записи новой таблицы: схема колонки иначе не проверяется ничем.

Run: `go test -race ./rosstat/`
Expected: PASS — все существующие тесты зелёные (расширение типа обратно совместимо).

- [ ] **Step 6: Реализовать разбор в `rosstat/labour_force.go`**

Константы и разбор:

```go
const (
	rosstatLabourForceTable = "rosstat_labour_force_mes"
	labourForceSheet        = "5"
	labourForcePage         = "https://rosstat.gov.ru/labour_force"
	labourForceLinkSelector = `a[href$="trud-1_15-s.xlsx"]`
)

type labourForcePoint struct {
	series string
	date   time.Time
	value  float64
}
```

Таблица рядов — пин имён и якорей шапки (шесть записей из спеки §3.3): «Рабочая сила», «Занятые» (якорь `занятые`), «Безработные» (якорь `безработные`), «Уровень участия в составе рабочей силы», «Уровень занятости», «Уровень безработицы». Якоря строки «в том числе» (`занятые`, `безработные`) лежат в **строке ниже** верхней строки шапки — искать якорь по обеим строкам найденной шапки.

Алгоритм `parseLabourForce`:
1. лист `labourForceSheet` — есть, иначе ошибка с именем таблицы и листа;
2. `xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`;
3. строка шапки — та, где нормализованная ячейка равна `Рабочая сила`; не найдена — ошибка;
4. для каждого из шести рядов колонка ищется по нормализованной подписи в строке шапки и строке под ней, ровно одно совпадение, иначе ошибка с именем ряда;
5. ниже шапки: строка из одной ячейки-года (`2017`…`2026`) задаёт текущий год; строка, чья подпись месяца после `labourForceLabel` распознана через `util.MonthsToNum`, даёт точки по колонкам пункта 4; строка, где подпись не год и не месяц (текст сносок) — конец разбора; месяц обязан возрастать внутри года, иначе ошибка;
6. пустая ячейка — пропуск (`continue`); нечисловая — ошибка `значение ячейки %q не число` с именем таблицы, листа и строки; `NaN`/`Inf` — ошибка;
7. дата точки — последний день отчётного месяца, UTC: `time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)`;
8. пустой результат — ошибка с именем таблицы и листа.

- [ ] **Step 7: Прогнать тесты — проходят**

Run: `go test -race -count=1 ./rosstat/ -run 'LabourForce' -v`
Expected: PASS (все девять тестов Step 3).

- [ ] **Step 8: Коммит**

```bash
git add rosstat/labour_force.go rosstat/labour_force_test.go rosstat/testdata/labour_force.xlsx rosstat/testdata/labour_force_page.html rosstat/series_meta_test.go
git commit -m "add rosstat labour force parser with fixtures"
```

---

### Task 3: Импортёр, каталог, витрина, реестр

**Files:**
- Modify: `rosstat/labour_force.go` (DDL, `ImportFunc`, переопределение `Import()`, переменная импортёра)
- Modify: `rosstat/series_meta.go` (`labourForceSeriesMeta`, `rosstatMacroView`)
- Modify: `rosstat/importers.go:50-55` (пятая запись реестра)
- Modify: `rosstat/importers_test.go:40-43` (`expectedPublicationCountPoints`)
- Modify: `rosstat/labour_force_test.go` (контракт каталога, проверка ссылки, `Append`-ошибка)

**Interfaces:**
- Consumes: `parseLabourForce`, `rosstatLabourForceTable`, `labourForceLinkSelector`, `labourForcePage` (Task 2).
- Produces:
  - `labourForceDdl` — const, DDL в `%s`-форме;
  - `labourForceCountPoints = "SELECT count() FROM rosstat_labour_force_mes FINAL"` — const;
  - `func labourForceDataUrl() (string, error)`;
  - `func labourForceImport(xlsx *excelize.File, batch driver.Batch) error`;
  - `type LabourForceStat struct { util.ClickHouseImport }` + `func (s *LabourForceStat) Import(ctx context.Context, conn driver.Conn) (int64, error)`;
  - `var labourForceImporter = &LabourForceStat{...}`;
  - `var labourForceSeriesMeta = []util.SeriesMeta{...}` (6 записей).

- [ ] **Step 1: Написать падающие тесты импортёра**

- `TestLabourForceImportAppendsFloat64` — `labourForceImport` на фикстуре пишет **690** строк в `appendRecorder`, шесть различных рядов, и у каждой строки `valueType == "float64"` — именно это пинит схему `Float64` новой таблицы (запись `float32` прошла бы в колонку Float64 молча, и разница видна только по типу в харнессе).
- `TestLabourForceAppendErrorNotSwallowed` — `appendRecorder{appendErr: ...}` → `labourForceImport` возвращает ошибку (эталон `rosstat/salaries_mes_test.go:280`).
- `TestLabourForceSeriesMetaCoversParsedRows` — **двусторонний** контракт: множество имён рядов из разбора == множество `Series` в `labourForceSeriesMeta`; у каждой записи непустые `Title`, `Unit`, `Frequency == "M"`, `Origin`, `Description`; `Source == rosstatSource` (каталог проверяется против разбора, а не против себя).
- `TestLabourForcePublicationDeclaration` — `labourForceImporter.Publication().CountPoints == labourForceCountPoints`, `.Source == rosstatLabourForceTable`, `.Url == ""` (URL объявляется только `DataUrl` шаблона).

Run: `go test -race ./rosstat/ -run 'LabourForce' -v`
Expected: FAIL — `undefined: labourForceImport`, `undefined: labourForceSeriesMeta`, `undefined: labourForceImporter`.

- [ ] **Step 2: Реализовать импортёр в `rosstat/labour_force.go`**

DDL:

```go
const labourForceDdl = `CREATE TABLE IF NOT EXISTS %s (
      name LowCardinality(String)
    , date Date
    , value Float64
) ENGINE = ReplacingMergeTree ORDER BY (name, date);
`
```

- `labourForceDataUrl()`: `util.GetPage(labourForcePage)` → `util.FindLink(page, labourForceLinkSelector)` → `rosstatUrl + href`; обе ошибки оборачиваются с именем страницы и ожидаемого файла (Review Focus 5).
- `labourForceImport(xlsx, batch)`: `parseLabourForce` → `batch.Append(point.series, point.date, point.value)` (значение — `float64`, **не** `float32`), ошибка `Append` возвращается.
- `LabourForceStat.Import` — как `rosstat/salaries_mes.go:209`: вычислить `s.DataUrl = url`, делегировать в `s.ClickHouseImport.Import(ctx, conn)`.
- `labourForceImporter` — переменная уровня пакета с полями `TableName`, `CreateTable: []string{labourForceDdl}`, `ImportFunc: labourForceImport`, `AfterImport: afterRosstatMacro(labourForceSeriesMeta)`, `PublicationCountPoints: labourForceCountPoints`.

- [ ] **Step 3: Описать шесть рядов в `rosstat/series_meta.go`**

`var labourForceSeriesMeta = []util.SeriesMeta{...}` — шесть записей, `Source: rosstatSource`, `Frequency: "M"`, `Origin`: «rosstat.gov.ru/labour_force, файл trud-1_15-s.xlsx, лист «5», шапка в строках 5–6; строка = месяц, год — строкой-секцией выше». `Unit`: `тыс. чел.` у трёх уровней, `%` у трёх уровней-ставок. `Description` обязательно называет: (а) отчётный месяц и **последний день месяца** как дату точки; (б) ряд **без сезонной корректировки**; (в) глубину **с 2017 г.**; (г) для всех шести — разрыв покрытия: «с октября 2022 г. данные идут без статистической информации по ДНР, ЛНР, Запорожской и Херсонской областям (сноска 2 источника), сравнение с более ранними периодами требует осторожности».

- [ ] **Step 4: Расширить витрину `rosstatMacroView`**

В `rosstat/series_meta.go:27-41`: добавить `rosstatLabourForceTable` в `Tables`; в `Select` — строку

```
UNION ALL SELECT 'rosstat', name, date, toFloat64(value) FROM rosstat_labour_force_mes FINAL
```

и дополнить `Comment` витрины упоминанием рынка труда (рабочая сила, занятые, безработные и уровни участия/занятости/безработицы, помесячно).

- [ ] **Step 5: Зарегистрировать импортёр**

`rosstat/importers.go` — пятая запись **последней** (порядок повторяет `dagu/rosstat.yaml`):

```go
{labourForceImporter, "rosstat_labour_force_mes", "SELECT count() FROM rosstat_labour_force_mes FINAL"},
```

`rosstat/importers_test.go` — в `expectedPublicationCountPoints` (строка 41-42) добавить `"rosstat_labour_force_mes": "SELECT count() FROM rosstat_labour_force_mes FINAL",`.

- [ ] **Step 6: Прогнать тесты пакета**

Run: `go test -race -count=1 ./rosstat/`
Expected: PASS, включая `TestRosstatImportersPrefixedAndRegistered` (он же требует, чтобы имя шага было в `dagu/rosstat.yaml` — это Task 4; если тест падает именно на расписании, порядок задач соблюдён неверно).

- [ ] **Step 7: Коммит**

```bash
git add rosstat/labour_force.go rosstat/labour_force_test.go rosstat/series_meta.go rosstat/importers.go rosstat/importers_test.go
git commit -m "register rosstat_labour_force_mes importer with catalog and view"
```

---

### Task 4: Расписание и MCP-проверки

**Files:**
- Modify: `dagu/rosstat.yaml`
- Modify: `scripts/mcp_check.py:514-590` (блок Росстата)

**Interfaces:**
- Consumes: имя импортёра `rosstat_labour_force_mes` (Task 3), сырая таблица `rosstat_labour_force_mes`, витрина `v_rosstat_macro`, каталог `source='rosstat'`.
- Produces: расписание и проверки приёмки.

- [ ] **Step 1: Добавить шаг в расписание**

`dagu/rosstat.yaml` — пятым и **последним** шагом цепочки (`type: chain`: падение нового шага не должно лишать данных четыре существующих ряда):

```yaml
  - id: rosstat_labour_force_mes
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=rosstat_labour_force_mes
    retry_policy:
      limit: 3
      interval_sec: 300
```

- [ ] **Step 2: Обновить проверки `scripts/mcp_check.py`**

Четыре правки в блоке Росстата:
1. строка 546-547 — в перечень «прогнать импортёры» добавить `rosstat_labour_force_mes`;
2. строка 566 — ожидаемое число рядов каталога `141` → `147`, строка 567 — метка: `"рядов rosstat: 147 (4 ИПЦ + 131 недельный + 5 ВВП + 1 зарплата + 6 рабочая сила)"`;
3. строка 569-574 — в текст расхождения `want 141` → `want 147` и в перечень импортёров добавить пятое имя;
4. строки 582-585 — в кортеж закрытых сырых таблиц добавить `("rosstat_labour_force_mes", "рабочая сила и безработица")`.

- [ ] **Step 3: Проверить синтаксис и расписание**

```bash
python3 -m py_compile scripts/mcp_check.py && echo "py ok"
dagu validate dagu/rosstat.yaml
go test -race -count=1 ./rosstat/
```

Expected: `py ok`; `dagu validate` — exit 0 (единственное сообщение — общее для всех DAG-файлов предупреждение `auth.mode`); тесты пакета зелёные.

- [ ] **Step 4: Коммит**

```bash
git add dagu/rosstat.yaml scripts/mcp_check.py
git commit -m "schedule rosstat_labour_force_mes and extend mcp checks"
```

---

### Task 5: Документация и живая приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§2, §6.1, §6.3, §7, §8 п. 13)
- Modify: `README.md` (таблица импортёров, эксплуатационный раздел)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.1, §4.2 п. 4 → ✅, §6 блок живой приёмки)

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: закрытый пункт роадмапа с числами приёмки.

- [ ] **Step 1: Обновить документацию**

- `ARCHITECTURE.md` §2 — файлы пакета `rosstat/`; §6.1 — строка импортёра `rosstat_labour_force_mes` (источник, лист, шесть рядов, точка контроля публикации, расписание); §6.3 — состав витрины `v_rosstat_macro` (плюс рынок труда); §7 — расписание `dagu/rosstat.yaml` (пятый шаг); §8 п. 13 — список импортёров с точкой контроля публикации: **14 → 15**.
- `README.md` — таблица импортёров и раздел эксплуатации (`make import STAT=rosstat_labour_force_mes`).
- `docs/ROADMAP_KEY_RATE.md` — §4.1: новая строка ряда; §4.2 п. 4: ✅ со ссылкой на блок приёмки; §6: блок «Живая приёмка 2026-10-14 — фаза 2, шаг 2».
- Порядок и полнота — skill `sync-readme-architecture`.

- [ ] **Step 2: `make all`**

Run: `make all`
Expected: `0 issues.` (gofmt + golangci-lint), `go vet ./...` без замечаний, `go test -race ./...` — все пакеты `ok`, обе сборки прошли.

- [ ] **Step 3: Живой прогон**

Run: `make import STAT=rosstat_labour_force_mes`
Expected: `Imported 690 rows of rosstat_labour_force_mes` (число берётся из факта; при обновлении файла Росстатом оно больше). Записать число строк и крайние даты.

Повтор:
Run: `make import STAT=rosstat_labour_force_mes`
Expected: `no-op, версия <fingerprint> уже импортирована, 690 точек на месте` — без скачивания файла (в логе нет `Get xlsx file`).

- [ ] **Step 4: Spot-сверка с живым источником**

Скачать живой `trud-1_15-s.xlsx`, взять ячейки последнего отчётного месяца и сверить с витриной (SQL — только через MCP `clickhouse`, пользователь `kimi_reader`; сырая таблица агенту закрыта, поэтому сверка идёт по витрине):

```
SELECT series, date, value FROM v_rosstat_macro WHERE series IN ('Рабочая сила','Уровень безработицы','Уровень занятости') ORDER BY date DESC LIMIT 3
```

Expected: значения совпадают с ячейками источника (в приёмке записать пары «ячейка → значение»).

- [ ] **Step 5: MCP-проверка**

Run: `make mcp-check`
Expected: **ПРОВЕРКА ПРОЙДЕНА** (exit 0), в том числе `витрина v_rosstat_macro непуста: строк: … , рядов: …` (было 29 050 / 141, станет больше), `каталог rosstat полон: рядов rosstat: 147`, `сырая rosstat_labour_force_mes закрыта (рабочая сила и безработица)`.

- [ ] **Step 6: Записать приёмку в роадмап и закоммитить**

В блок §6 роадмапа — числа из шагов 2–5 (команда, число строк, крайние даты, no-op, споты, результат `make mcp-check`).

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_KEY_RATE.md
git commit -m "docs: rosstat labour force importer in architecture, readme and roadmap"
```

---

## Приёмка пункта (после Task 5)

1. `make all` — зелёный.
2. `go test -race -count=1 ./rosstat/` — `ok`.
3. Живой прогон + повторный прогон (no-op) — числа записаны.
4. Spot-сверка с живым файлом Росстата.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
6. `dagu validate dagu/rosstat.yaml` — exit 0.
7. Документация и статус роадмапа — тем же PR.

Предпосылка: dev-ClickHouse поднят владельцем (`make ch-up` ассистент не запускает). Дальше — `superpowers:requesting-code-review` (финальное ревью ветки на `deepseek/deepseek-flash`, effort `max`) и `superpowers:verification-before-completion`, затем один PR.
