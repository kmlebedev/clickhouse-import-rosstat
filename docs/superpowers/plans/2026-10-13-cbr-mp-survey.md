# cbr_mp_survey Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Импортёр `cbr_mp_survey`: 4 листа `mp_survey_data.xlsx` (мониторинг предприятий ЦБ) → 13 рядов в `macro_series` (`source='cbr_survey'`), каталог `series_catalog`, витрина `v_cbr_survey` для MCP-агента, расписание `dagu/surveys.yaml`.

**Architecture:** Новый пакет `surveys/` по конвенции репозитория (прецедент `eia/`): разбор через `excelize` (структура — `GetRows`, даты и числовые значения — raw-чтение `RawCellValue` + `ExcelDate64ToTime`), запись одним батчем в существующую `macro_series` через `util.ClickHouseImport` (шаблон Б), после `Send` — каталог `util.UpsertSeriesCatalog` и витрина `util.CreateView` (обёртка вокруг `ClickHouseImport.Import`, прецедент `cbr/views.go`).

**Tech Stack:** Go (модуль `github.com/kmlebedev/clickhouse-import-rosstat`), `github.com/xuri/excelize/v2`, `github.com/ClickHouse/clickhouse-go/v2/lib/driver`, `logrus`; ClickHouse `ReplacingMergeTree`; dagu.

**Spec:** `docs/superpowers/specs/2026-10-13-cbr-mp-survey-design.md` (закоммичена, `a94f060`, ветка `feat/key-rate-mp-survey`).

## Global Constraints

- Работаем только в ветке `feat/key-rate-mp-survey`; один PR в конце (не открывать промежуточные). Объём строго = шаг 1.3 роадмапа; ничего попутного.
- В рабочем дереве есть незакоммиченная правка владельца `deploy/staging/dagu.service` — **не трогать и не коммитить**; перед каждым коммитом проверять `git status`.
- Новых зависимостей не добавлять (excelize, clickhouse-go, logrus уже в `go.mod`).
- Вставки в ClickHouse — только батчем `PrepareBatch → Append (проверка err) → Send` (правило 2 AGENTS.md); построчный `conn.Exec` в цикле запрещён.
- HTTP — только через `util.GetXlsx`/`util.GetFile`; сетевых вызовов в `init()` нет; логирование — `logrus` (без `fmt.Print` в прод-коде).
- Ветка/`Name()`/имена зафиксированы: пакет `surveys`, `Name()` = `cbr_mp_survey`, таблица `macro_series`, `source = 'cbr_survey'`, витрина `v_cbr_survey`.
- 13 рядов (имена точные): `business_climate`, `business_climate_sa`, `price_expectations`, `price_expectations_sa`, `credit_conditions`, `price_growth_expected`, `price_growth_expected_sa`, `capacity_utilization`, `capacity_utilization_sa`, `staff_availability`, `staff_availability_sa`, `headcount_expectations`, `headcount_expectations_sa`.
- Даты: месячные ряды — 1-е число отчётного месяца (UTC); квартальные — 1-е число месяца, **следующего** за отчётным кварталом (1к26 → 2026-04-01; конвенция `rosstat/vvp_kvartal.go:getQuarterDate`).
- Единицы: «пункты (баланс ответов)» — ИБК, ценовые ожидания, условия кредитования, обеспеченность персоналом, ожидания численности; «%» — загрузка мощностей; «% годовых» — ожидаемый прирост отпускных цен.
- DDL `macro_series` — три копии обязаны совпадать: `ARCHITECTURE.md` §6.2, `ingest/schema.go`, `surveys/mp_survey.go` (изменяется только комментарий списка `source` — добавляется `'cbr_survey'`).
- Тесты офлайн (сеть не участвует); фикстура `surveys/testdata/mp_survey_data.xlsx` — урезанная копия реального файла (225 437 Б, sha256 `221d9d85ff3893288b397381474296740742fcbc087d2df41f58e4d594b3cd7b`); полная копия (2,1 МБ) не коммитится.
- Каждый таск заканчивается зелёными `go build ./...` и `go test ./surveys/` (перед финальной приёмкой — `make all`), gofmt-чистым кодом и коммитом; коммиты кратко, по-английски.

## Review Focus

1. **Смена структуры источника** (лист переименован/маркеры/шапки уехали) — импорт обязан падать ошибкой до `Send`, а не писать «0 строк» или частичный батч. Тесты: Task 2 (`TestParseSurveyMissingSheet/MissingMarker/MissingColumn`), запись — после полного разбора.
2. **Пустая ячейка значения внутри ряда** (конец ряда, вопрос не задавался) — точка пропускается, соседние ряды не страдают, прогон не падает. Тест: Task 2 (`TestParseSurveySkipsEmptyCells`).
3. **Точность значений** — excelize `GetRows` отдаёт форматированные строки («67.02», «январь 2002»); читать даты и числа только raw-режимом, иначе потеря точности. Тест: Task 2 (spot-значение `capacity_utilization_sa` 2к26 = 76.9190833585828, не 76.92).
4. **Даты** — отчётный период vs месяц волны ЦБ; квартал → следующий месяц; иначе джойны с ВВП/ИПЦ разъедутся. Тесты: Task 2 (точные даты: 2002-01-01, 2026-08-01, 2к26 → 2026-07-01, 1к02 → 2002-04-01).
5. **Идемпотентность повторного прогона** (ключ `(source, series, date)`, `ReplacingMergeTree`) — счётчик витрины (FINAL) не растёт. Проверка: Task 6; сырой `count()` без FINAL может расти незамёрженными частями — это ожидаемо.

---

### Task 1: Определения рядов и каталог

**Files:**
- Create: `surveys/series_meta.go`
- Test: `surveys/series_meta_test.go`

**Interfaces:**
- Produces: `const mpSurveySource = "cbr_survey"`; `type seriesDef struct { Name, Sheet, Header string; SA bool; Exclude []string }`; `var mpSurveySeries []seriesDef` (13 шт.); `var mpSurveySeriesMeta []util.SeriesMeta` (13 шт.). Всё — пакет `surveys`.

- [ ] **Step 1: Написать падающий тест полноты каталога**

`surveys/series_meta_test.go` — двусторонняя сверка в духе `TestSeriesMetaCoversAllLines` (`bea/bea_test.go`): множества `seriesDef.Name` и `SeriesMeta.Series` совпадают; длина каждого — 13; у каждой записи непустые `Title/Unit/Origin/Description`, `Source == mpSurveySource`, `Frequency` ∈ {M, Q}; у серий с суффиксом `_sa` поле `SA == true`, без суффикса — `false`; у `credit_conditions` — ровно одна запись (без `_sa`).

Run: `go test ./surveys/ -run TestSeriesMetaCompleteness -v`
Expected: FAIL (package не определён).

- [ ] **Step 2: Реализовать `surveys/series_meta.go`**

Определения (13):

```go
var mpSurveySeries = []seriesDef{
	{Name: "business_climate", Sheet: "Экономика всего", Header: "Индикатор бизнес-климата Банка России", Exclude: []string{"(факт)", "(ожид)"}},
	{Name: "business_climate_sa", Sheet: "Экономика всего", Header: "Индикатор бизнес-климата Банка России", SA: true, Exclude: []string{"(факт)", "(ожид)"}},
	{Name: "price_expectations", Sheet: "Экономика всего", Header: "Ценовые ожидания предприятий"},
	{Name: "price_expectations_sa", Sheet: "Экономика всего", Header: "Ценовые ожидания предприятий", SA: true},
	{Name: "credit_conditions", Sheet: "Экономика всего", Header: "условия кредитования"},
	{Name: "price_growth_expected", Sheet: "Экономика всего", Header: "Средний темп прироста отпускных цен"},
	{Name: "price_growth_expected_sa", Sheet: "Экономика всего", Header: "Средний темп прироста отпускных цен", SA: true},
	{Name: "capacity_utilization", Sheet: "Загрузка произв мощностей", Header: "Экономика всего"},
	{Name: "capacity_utilization_sa", Sheet: "Загрузка произв мощностей", Header: "Экономика всего", SA: true},
	{Name: "staff_availability", Sheet: "Обеспеченность персоналом", Header: "Экономика всего"},
	{Name: "staff_availability_sa", Sheet: "Обеспеченность персоналом", Header: "Экономика всего", SA: true},
	{Name: "headcount_expectations", Sheet: "Ожидания изменения численности", Header: "Экономика всего"},
	{Name: "headcount_expectations_sa", Sheet: "Ожидания изменения численности", Header: "Экономика всего", SA: true},
}
```

`mpSurveySeriesMeta` — 13 `util.SeriesMeta` со значениями (Source везде `mpSurveySource`; Origin месячных: `ЦБ, мониторинг предприятий (mp_survey_data.xlsx, id 135603), лист «Экономика всего», колонка «<полный заголовок>» (исходные данные|сезонно скорректированные данные)`; квартальных: `…, лист «<лист>», колонка «Экономика всего*» (исходные данные|сезонно скорректированные данные)`):

| Series | Title | Unit | Freq | Description (суть; формулировку сохранить) |
|---|---|---|---|---|
| business_climate | `ИБК Банка России (индикатор бизнес-климата), п.` | пункты (баланс ответов) | M | Индикатор бизнес-климата = среднее текущих оценок и ожиданий предприятий (баланс ответов, п.); рост = улучшение делового климата. Прокси разрыва выпуска: числового ряда разрыва ЦБ не публикует. Дата — отчётный период (1-е число месяца); волна анкет — первая декада следующего месяца, публикация ~10–15-го, справки ЦБ нумеруются по месяцу волны |
| business_climate_sa | `ИБК Банка России, сезонно сглаженный, п.` | пункты | M | ИБК, сезонно сглаженный ряд (R seasonal); ЦБ в комментариях цитирует именно с.к. значения (например, −2,3 п. в справке «сентябрь 2026» = точке отчётного периода август). Дата — отчётный период; публикация ~10–15-го следующего месяца |
| price_expectations | `Ценовые ожидания предприятий на 3 месяца вперёд, п.` | пункты | M | Баланс ответов об ожидаемом изменении отпускных цен на следующие 3 месяца, п.; положительный = преобладает ожидание роста цен. Главный канал решения ЦБ — называются в каждом резюме; уровень читать по с.к. ряду (price_expectations_sa), как цитирует ЦБ. Дата — отчётный период; публикация ~10–15-го следующего месяца |
| price_expectations_sa | `Ценовые ожидания предприятий на 3 месяца вперёд, с.к., п.` | пункты | M | Ценовые ожидания предприятий, сезонно сглаженный ряд (цитируемое ЦБ значение: «выросли до 22,7 п.» в справке «сентябрь 2026»). Дата — отчётный период; публикация ~10–15-го следующего месяца |
| credit_conditions | `Изменение условий кредитования предприятий, п.` | пункты | M | Индекс изменения условий кредитования (баланс ответов, п.): отрицательные значения = преобладает ужесточение (соотносит долю компаний, для которых условия стали жёстче, с долей отметивших смягчение); рост значения = смягчение. Исторически смещён в минус (среднее 2016–2025 ≈ −11,6 п.). Сезонной корректировки в источнике нет. Дата — отчётный период; публикация ~10–15-го следующего месяца |
| price_growth_expected | `Ожидаемый прирост отпускных цен (годовое выражение), %` | % годовых | M | Средний ожидаемый прирост отпускных цен на следующие 3 месяца в годовом выражении — аннуализированный средневзвешенный темп, расчёт ЦБ. Количественная мера ценовых ожиданий бизнеса. Вопрос в анкете с 2020 г. (ряд короче остальных). Дата — отчётный период; публикация ~10–15-го следующего месяца |
| price_growth_expected_sa | `Ожидаемый прирост отпускных цен (годовое выражение), с.к., %` | % годовых | M | Средний ожидаемый прирост отпускных цен, сезонно сглаженный ряд (цитируемое ЦБ значение: «до 7,0%» в справке «сентябрь 2026»). Ряд с 2020 г. |
| capacity_utilization | `Загрузка производственных мощностей, %` | % | Q | Уровень использования производственных мощностей, % («экономика всего» — взвешено по структуре ВДС). Квартальный ряд; дата — 1-е число месяца, следующего за отчётным кварталом. Прокси загрузки мощностей/напряжённости спроса |
| capacity_utilization_sa | `Загрузка производственных мощностей, с.к., %` | % | Q | То же, сезонно сглаженный ряд; дата — 1-е число месяца, следующего за отчётным кварталом |
| staff_availability | `Обеспеченность предприятия персоналом, п.` | пункты | Q | Баланс ответов «как Вы оцениваете обеспеченность предприятия персоналом», п.: отрицательные значения = недоукомплектованность преобладает (дефицит кадров), рост = улучшение обеспеченности. Квартальный ряд (с 1к20) |
| staff_availability_sa | `Обеспеченность предприятия персоналом, с.к., п.` | пункты | Q | То же, сезонно сглаженный ряд (цитируемое ЦБ значение: «−19,7 п. в 2к26»). Квартальный ряд (с 1к20) |
| headcount_expectations | `Ожидания изменения численности работников, п.` | пункты | Q | Баланс ответов об ожидаемом изменении численности работников в следующем квартале, п.: положительный = преобладают планы по набору. Квартальный ряд |
| headcount_expectations_sa | `Ожидания изменения численности работников, с.к., п.` | пункты | Q | То же, сезонно сглаженный ряд. Квартальный ряд |

- [ ] **Step 3: Прогнать тест — PASS**

Run: `go test ./surveys/ -run TestSeriesMetaCompleteness -v` → PASS.
Также: `go build ./...` (пакет собирается).

- [ ] **Step 4: Commit**

```bash
git add surveys/series_meta.go surveys/series_meta_test.go
git commit -m "surveys: series definitions and catalog for CBR business monitoring"
```

---

### Task 2: Парсер XLSX и фикстура

**Files:**
- Create: `surveys/parse.go`, `surveys/parse_test.go`, `surveys/testdata/mp_survey_data.xlsx`
- Test: `surveys/parse_test.go`

**Interfaces:**
- Consumes: `seriesDef`, `mpSurveySeries` (Task 1).
- Produces: `type surveyPoint struct { Series string; Date time.Time; Value float64 }`; `func parseSurvey(xlsx *excelize.File, defs []seriesDef) ([]surveyPoint, error)`.

- [ ] **Step 1: Скачать источник и собрать фикстуру (урезанная копия, 4 листа)**

```bash
curl -sS --cacert russian_trusted_root_ca_pem.crt --max-time 60 \
  -o /tmp/mp_survey_data.xlsx \
  "https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx"
mkdir -p surveys/testdata
python3 - <<'PY'
import zipfile, re
src='/tmp/mp_survey_data.xlsx'; dst='surveys/testdata/mp_survey_data.xlsx'
keep_names={'Экономика всего','Загрузка произв мощностей','Обеспеченность персоналом','Ожидания изменения численности'}
zin=zipfile.ZipFile(src)
wb=zin.read('xl/workbook.xml').decode('utf-8')
rels=zin.read('xl/_rels/workbook.xml.rels').decode('utf-8')
relmap=dict(re.findall(r'<Relationship [^>]*Id="([^"]+)"[^>]*Target="([^"]+)"', rels))
sheets=re.findall(r'<sheet [^>]*name="([^"]+)"[^>]*r:id="([^"]+)"[^>]*/>', wb)
keep_rids={rid for name,rid in sheets if name in keep_names}
drop_targets={relmap[rid] for name,rid in sheets if name not in keep_names}
drop_parts={'xl/'+t for t in drop_targets}
for t in drop_targets:
    drop_parts.add('xl/worksheets/_rels/'+t.split('/')[-1]+'.rels')
wb2=re.sub(r'<sheet [^>]*name="([^"]+)"[^>]*r:id="([^"]+)"[^>]*/>',
           lambda m: m.group(0) if m.group(2) in keep_rids else '', wb)
rels2=re.sub(r'<Relationship [^>]*Id="([^"]+)"[^>]*Target="([^"]+)"[^>]*/>',
             lambda m: m.group(0) if m.group(1) in keep_rids else '', rels)
ct=zin.read('[Content_Types].xml').decode('utf-8')
for t in sorted(drop_targets):
    ct=ct.replace('<Override PartName="/xl/%s" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' % t,'')
zout=zipfile.ZipFile(dst,'w',zipfile.ZIP_DEFLATED)
for item in zin.infolist():
    if item.filename in drop_parts: continue
    if item.filename=='xl/workbook.xml': data=wb2.encode()
    elif item.filename=='xl/_rels/workbook.xml.rels': data=rels2.encode()
    elif item.filename=='[Content_Types].xml': data=ct.encode()
    else: data=zin.read(item.filename)
    zout.writestr(item, data)
zout.close()
PY
shasum -a 256 surveys/testdata/mp_survey_data.xlsx
```

Expected: файл 225 437 Б, sha256 `221d9d85ff3893288b397381474296740742fcbc087d2df41f58e4d594b3cd7b`; 4 листа (`Экономика всего`, `Загрузка произв мощностей`, `Обеспеченность персоналом`, `Ожидания изменения численности`). Если sha не совпал — проверить число листов и что данные читаются (sha — ориентир); ломаный результат не коммитить.

- [ ] **Step 2: Написать падающие тесты**

`surveys/parse_test.go`:

1. `TestParseSurveyFixture` — `excelize.OpenFile("testdata/mp_survey_data.xlsx")`, `parseSurvey(f, mpSurveySeries)`:
   - ровно 13 рядов и 2084 точки; по рядам: `business_climate`/`_sa` 296, `price_expectations`/`_sa` 296, `credit_conditions` 296, `price_growth_expected`/`_sa` 80, `capacity_utilization`/`_sa` 98, `staff_availability`/`_sa` 26, `headcount_expectations`/`_sa` 98;
   - крайние даты: месячные 2002-01-01 … 2026-08-01; `price_growth_expected` с 2020-01-01; квартальные 2002-04-01 … 2026-07-01, у `staff_availability` с 2020-04-01;
   - spot-значения (точность 1e-6): `price_expectations_sa` 2026-08-01 = 22.655674, 2026-07-01 = 21.100159; `business_climate_sa` 2026-08-01 = −2.260690; `price_growth_expected_sa` 2026-08-01 = 6.984156; `capacity_utilization_sa` 2026-07-01 = **76.9190833585828** (полная точность — доказывает raw-чтение); `capacity_utilization` 2026-07-01 = 77.025394; `staff_availability_sa` 2026-07-01 = −19.712443; `headcount_expectations_sa` 2026-07-01 = 1.055521.
2. `TestParseSurveySkipsEmptyCells` — фикстура, `f.SetCellValue("Экономика всего", "J101", "")`: `price_expectations` = 295, `price_expectations_sa` = 296, всего 2083; прогон без ошибки.
3. Fail-fast (по подтесту, каждый — ошибка):
   - `MissingSheet`: `f.DeleteSheet("Загрузка произв мощностей")`;
   - `MissingMarker`: `f.SetCellValue("Экономика всего", "C3", "")`;
   - `MissingColumn`: `f.SetCellValue("Экономика всего", "J4", "Другой заголовок")`;
   - `AmbiguousColumn`: `f.SetCellValue("Загрузка произв мощностей", "C4", "Экономика всего (дубль)")`;
   - `TooFewRows`: синтетическая книга (`excelize.NewFile()`): лист «Экономика всего» с маркерами строки 3 («Исходные данные» в C3, «Сезонно скорректированные данные» в P3), шапкой строки 4 (J4 «Ценовые ожидания предприятий на следующие 3 месяца, баланс ответов») и одной строкой данных → ошибка «слишком мало строк» (порог 200).

Run: `go test ./surveys/ -run TestParseSurvey -v`
Expected: FAIL (`parseSurvey` не определён).

- [ ] **Step 3: Реализовать `surveys/parse.go`**

Алгоритм (тела — на усмотрение исполнителя, поведение пинуют тесты):

- Для каждого листа (кэшировать по имени, не перечитывать): существование — через `f.GetSheetIndex(sheet)` (ошибка/0 — «нет листа»); строки — `f.GetRows(sheet)`.
- Маркеры блоков — строка 3 (индекс 2): точные значения «Исходные данные» (raw) и «Сезонно скорректированные данные» (SA); нет любого — ошибка. Границы: raw = `[rawIdx, saIdx)`, SA = `[saIdx, len(headerRow))`; шапка — строка 4 (индекс 3).
- Колонка ряда: в блоке (`def.SA` → SA) ищем ячейки шапки, где `strings.Contains(header, def.Header)` и ни одна из `def.Exclude` не содержится; **ровно одна** — иначе ошибка.
- Числа и даты — только raw: `f.GetCellValue(sheet, cell, excelize.Options{RawCellValue: true})` (буква колонки — `excelize.ColumnNumberToName(col+1)`); форматированные строки (`GetRows`) для числовых значений не использовать — теряется точность («67.02»).
- Месячный лист («Экономика всего»): строки с 5-й; raw col1: пусто — пропустить; не парсится в число (сноски после данных) — пропустить; число → `excelize.ExcelDateToTime(serial, false)` (ошибка — ошибка разбора); значение целевой ячейки: пусто — точку пропустить, не парсится — ошибка.
- Квартальные листы: col1 из `GetRows` по шаблону `^[1-4]к([0-9]{2})$` (кириллическая «к»); не совпало — пропустить; дата = `time.Date(2000+yy, time.Month(3*q+1), 1, 0, 0, 0, 0, time.UTC)` (q = 1..4; месяц 13 нормализуется в январь следующего года).
- Валидация: порядок обработки листа — существование листа → маркеры блоков → порог строк (до поиска колонок) → по каждому def поиск колонки → извлечение точек. Пороги: датированных строк в «Экономика всего» ≥ 200; распознанных кварталов в каждом квартальном листе ≥ 20; итог ≥ 1 точки; иначе ошибка.
- Парсинг завершается ПОЛНОСТЬЮ до записи (импортёр вызывает `parseSurvey` внутри `ImportFunc` и при ошибке батч не отправляется).

- [ ] **Step 4: Прогнать тесты — PASS**

Run: `go test ./surveys/ -v` → PASS (все тесты Task 1–2).
Также: `gofmt -l surveys/` — пусто; `go build ./...`.

- [ ] **Step 5: Commit**

```bash
git add surveys/parse.go surveys/parse_test.go surveys/testdata/mp_survey_data.xlsx
git commit -m "surveys: parse CBR mp_survey_data.xlsx into 13 series + fixture tests"
```

---

### Task 3: Импортёр, витрина, регистрация

**Files:**
- Create: `surveys/mp_survey.go`, `surveys/views.go`, `surveys/mp_survey_test.go`
- Modify: `main.go` (blank-import `surveys`)

**Interfaces:**
- Consumes: `parseSurvey`, `mpSurveySeries`, `mpSurveySeriesMeta`, `mpSurveySource` (Tasks 1–2).
- Produces: `var importer *mpSurveyStat` (`Name()` = `cbr_mp_survey`, регистрируется в `chimport.Stats`); `var cbrSurveyView util.View` (`v_cbr_survey`).

- [ ] **Step 1: Написать падающий тест**

`surveys/mp_survey_test.go`:
- `TestImporterName` — `importer.Name() == "cbr_mp_survey"`;
- `TestSurveyViewDefinition` — `cbrSurveyView.Name == "v_cbr_survey"`, `Tables == ["macro_series"]`, `Select` содержит `macro_series FINAL` и `source = 'cbr_survey'`, в `Columns` есть ключи `source/series/date/value`, `Comment` непустой;
- `TestSurveyDdlPinsSource` — `mpSurveyDdl` содержит `CREATE TABLE IF NOT EXISTS macro_series` и `'cbr_survey'` (комментарий списка source).

Run: `go test ./surveys/ -run 'TestImporterName|TestSurveyView|TestSurveyDdl' -v` → FAIL.

- [ ] **Step 2: Реализовать `surveys/mp_survey.go`**

```go
const (
	mpSurveyName   = "cbr_mp_survey"
	mpSurveyURL    = "https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx"
	mpSurveySource = "cbr_survey"
	mpSurveyTable  = "macro_series"
)

const mpSurveyDdl = `CREATE TABLE IF NOT EXISTS macro_series (
    source LowCardinality(String),   -- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold','cbr_survey'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);`

type mpSurveyStat struct {
	util.ClickHouseImport
}

func (s *mpSurveyStat) Name() string { return mpSurveyName }

func (s *mpSurveyStat) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	count, err := s.ClickHouseImport.Import(ctx, conn)
	if err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, mpSurveySeriesMeta); err != nil {
		return count, err
	}
	_, err = util.CreateView(ctx, conn, cbrSurveyView)
	return count, err
}

var importer = &mpSurveyStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   mpSurveyTable,
	CreateTable: []string{mpSurveyDdl},
	DataUrl:     mpSurveyURL,
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := parseSurvey(xlsx, mpSurveySeries)
		if err != nil {
			return err
		}
		log.Infof("Собрано %d точек мониторинга предприятий", len(points))
		for _, p := range points {
			if err = batch.Append(mpSurveySource, p.Series, p.Date, p.Value); err != nil {
				return fmt.Errorf("append %s %s: %w", p.Series, p.Date.Format("2006-01-02"), err)
			}
		}
		return nil
	},
}}

func init() {
	chimport.Stats = append(chimport.Stats, importer)
}
```

`surveys/views.go` — витрина (комментарии публикует `util.CreateView`):

```go
var cbrSurveyView = util.View{
	Name:   "v_cbr_survey",
	Tables: []string{"macro_series"},
	Select: `SELECT source, series, date, value FROM macro_series FINAL WHERE source = 'cbr_survey'`,
	Comment: "Мониторинг предприятий Банка России (опросы нефинансовых компаний): ценовые ожидания бизнеса, ИБК и условия кредитования (месячные); загрузка мощностей, обеспеченность персоналом, ожидания численности (квартальные). Дата — отчётный период: 1-е число месяца, у квартальных — 1-е число месяца, следующего за кварталом; ЦБ публикует волну в первую декаду следующего месяца и нумерует справки по месяцу волны (справка «сентябрь 2026» цитирует точку отчётного периода август 2026). Перед использованием читать v_series_catalog по (source='cbr_survey', series): единицы и знаки различаются. ЦБ в комментариях цитирует сезонно сглаженные ряды (…_sa)",
	Columns: map[string]string{
		"source": "Всегда cbr_survey",
		"series": "Код ряда; 13 рядов — описания в v_series_catalog (source='cbr_survey')",
		"date":   "Отчётный период: 1-е число месяца; у квартальных рядов — 1-е число месяца, следующего за отчётным кварталом (конвенция vvp_kvartal)",
		"value":  "Значение в единицах ряда: пункты баланса ответов, % (загрузка мощностей), % годовых (ожидаемый прирост цен) — см. unit и description в v_series_catalog",
	},
}
```

`main.go`: добавить в блок blank-import `_ "github.com/kmlebedev/clickhouse-import-rosstat/surveys"` (между `rosstat` и `views`).

- [ ] **Step 3: Прогнать тесты и сборку — PASS**

Run: `go test ./surveys/ -v && go build ./... && gofmt -l surveys/ main.go`
Expected: тесты PASS; сборка без ошибок; gofmt пусто.

- [ ] **Step 4: Commit**

```bash
git add surveys/mp_survey.go surveys/views.go surveys/mp_survey_test.go main.go
git commit -m "surveys: cbr_mp_survey importer, v_cbr_survey view, registration"
```

---

### Task 4: Расписание, грант, проверки MCP

**Files:**
- Create: `dagu/surveys.yaml`
- Modify: `sql/mcp_kimi_reader.sql` (в конец), `scripts/mcp_check.py` (перед `return failed`)

**Interfaces:**
- Consumes: имя импортёра `cbr_mp_survey` (Task 3), витрина `v_cbr_survey`, каталог `source='cbr_survey'` (13 записей).

- [ ] **Step 1: Создать `dagu/surveys.yaml`**

```yaml
type: chain
schedule: "47 9 * * *"
overlap_policy: skip

steps:
  - id: cbr_mp_survey
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=cbr_mp_survey
    retry_policy:
      limit: 3
      interval_sec: 300
```

Run: `dagu validate dagu/surveys.yaml` → exit 0.

- [ ] **Step 2: Добавить грант в `sql/mcp_kimi_reader.sql`**

В конец файла: `GRANT SELECT ON default.v_cbr_survey TO kimi_reader;`

- [ ] **Step 3: Добавить блок проверок в `scripts/mcp_check.py`**

Перед `return failed` (после блока keyrate), имена переменных — с префиксом `survey_` (проверить, что не конфликтуют с существующими):

```python
            # Контур мониторинга предприятий ЦБ (правило 11, пункт 1.3 роадмапа): витрина
            # v_cbr_survey читается агентом, сырая macro_series закрыта (проверка выше).
            # Существование витрины подтверждается запросом к ней, а не поиском имени в
            # list_tables (урок bold: в тексте list_tables имя попадает из COMMENT соседки).
            survey_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_survey"}))
            survey_view_rows = result_rows(survey_view)
            if survey_view_rows is None:
                survey_view_detail = f"витрина не читается: {survey_view[:200]}"
            elif survey_view_rows:
                survey_view_detail = f"строк: {survey_view_rows[0].get('count()')}"
            else:
                survey_view_detail = "в ответе нет строк"
            failed += not expect("витрина v_cbr_survey существует и читается", bool(survey_view_rows), survey_view_detail)

            survey_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr_survey'"}))
            survey_catalog_rows = result_rows(survey_catalog)
            survey_catalog_n = survey_catalog_rows[0].get("count()") if survey_catalog_rows else None
            failed += not expect("каталог cbr_survey описывает 13 рядов", survey_catalog_n == 13,
                                 f"записей: {survey_catalog_n}" if survey_catalog_rows is not None else f"каталог не читается: {survey_catalog[:200]}")
```

Run: `python3 -c "import ast; ast.parse(open('scripts/mcp_check.py').read())"` → без ошибок.
(Реальный запуск `make mcp-check` — в Task 6, после импорта и гранта.)

- [ ] **Step 4: Commit**

```bash
git add dagu/surveys.yaml sql/mcp_kimi_reader.sql scripts/mcp_check.py
git commit -m "surveys: dagu schedule, kimi_reader grant, mcp_check coverage"
```

---

### Task 5: Документация (ARCHITECTURE / README / ingest)

**Files:**
- Modify: `ingest/schema.go` (только комментарий `source` в `macroSeriesDdl`), `ARCHITECTURE.md`, `README.md`

**Interfaces:**
- Consumes: всё из Tasks 1–4; дополнительные факты для текста — из спеки (§Источник/Ряды) и из приёмки Task 6 (числа).

- [ ] **Step 1: Синхронизировать копию DDL**

`ingest/schema.go`, `macroSeriesDdl`: комментарий `source` → добавить `,'cbr_survey'` (итог: `-- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold','cbr_survey'`). Убедиться, что ровно та же строка в `surveys/mp_survey.go` (`mpSurveyDdl`) и в блоке §6.2 `ARCHITECTURE.md`.

- [ ] **Step 2: ARCHITECTURE.md**

- строка 4 (changelog «Последнее обновление»): добавить в начало запись 2026-10-13 о контуре мониторинга предприятий (в стиле существующих записей): импортёр `cbr_mp_survey` (`surveys/`) из `mp_survey_data.xlsx` → 13 рядов `macro_series` (`source='cbr_survey'`), каталог, витрина `v_cbr_survey`, расписание `dagu/surveys.yaml`; §2/§6.1/§6.2/§6.3/§6.5/§7 обновлены; приёмка — в роадмапе;
- §2 «Структура репозитория»: строка `surveys/` после `news/`: «ЦБ РФ, мониторинг предприятий: импортёр `cbr_mp_survey` (`mp_survey_data.xlsx`, id 135603) → 13 рядов `macro_series` (`source='cbr_survey'`: ценовые ожидания бизнеса (±с.к.), ИБК (±с.к.), условия кредитования, ожидаемый прирост цен (±с.к.), загрузка мощностей (±с.к.), обеспеченность персоналом (±с.к.), ожидания численности (±с.к.)), каталог `series_catalog`, витрина `v_cbr_survey`; расписание `dagu/surveys.yaml` (ежедневно)»;
- §6.1: новая строка таблицы `| \`cbr_mp_survey\` | \`surveys/\` | реализован; \`Name()\` = \`cbr_mp_survey\`. Закрывает каналы «ценовые ожидания бизнеса» (главный канал ЦБ), прокси разрыва выпуска, рынка труда и условий кредитования | … |` — колонки как у строки `eia` (что/откуда/как работает), включить: URL и id 135603, 4 листа из 31, маркеры блоков и raw-чтение, fail-fast-пороги, 13 рядов и ожидаемое число точек, «ЦБ нумерует справки по месяцу волны», конвенцию даты кварталов, тесты на фикстуре, расписание;
- §6.2: комментарий DDL (см. Step 1) + в прозе «Значения `source`, используемые в коде `macro_series`: …» добавить: `cbr_survey` (импортёр `cbr_mp_survey`: 13 рядов мониторинга предприятий ЦБ, витрина `v_cbr_survey`);
- §6.3: новый пункт про `v_cbr_survey` (long-формат `source, series, date, value` из `macro_series FINAL WHERE source='cbr_survey'`, создаётся импортёром с комментариями, грант — `sql/mcp_kimi_reader.sql`, оговорки о датах/волнах и с.к.);
- §6.5: в перечень контуров, удовлетворяющих правилу 11, добавить абзац про `surveys/` (витрина читается, сырая `macro_series` закрыта, проверки — `scripts/mcp_check.py`; результат живой проверки — дата и «ПРОВЕРКА ПРОЙДЕНА»);
- §7: строка расписания `dagu/surveys.yaml` — ежедневно 09:47 МСК, `cbr_mp_survey` (мониторинг предприятий; файл обновляется раз в месяц ~10-го числа).

- [ ] **Step 3: README.md**

- таблица импортёров: строка `cbr_mp_survey` (что/куда/расписание), по образцу строк `eia`/`moex`;
- прозаический абзац про контур (после абзаца `moex`): источник, id 135603, 13 рядов и что они закрывают (главный канал ожиданий, прокси разрыва выпуска, труд, условия кредитования), оговорки (отчётный период vs волна; с.к. цитируется ЦБ; условия кредитования без с.к.; квартальные даты), фикстура, тесты, запуск `make import STAT=cbr_mp_survey`;
- дерево пакетов в README (строка после `eia/`/`moex/`): `surveys/`;
- список витрин (там, где перечислены `v_*`): `v_cbr_survey`;
- строку «тесты есть у `bls`, `bea`, …» дополнить `surveys`.

Пройти чек-лист скила `sync-readme-architecture` (проектный скил) и убедиться, что не осталось рассинхронов.

- [ ] **Step 4: Проверки и Commit**

Run: `grep -n "cbr_survey" ARCHITECTURE.md ingest/schema.go surveys/mp_survey.go | wc -l` (≥ 5 — все три копии и тексты), `go build ./...`.
```bash
git add ingest/schema.go ARCHITECTURE.md README.md
git commit -m "docs: sync ARCHITECTURE/README for cbr_mp_survey importer"
```

---

### Task 6: Живая приёмка и статус в роадмапе

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md`, `docs/KEY_RATE_CHANNELS.md`, `docs/KEY_RATE_PUBLICATIONS.md`

**Interfaces:**
- Consumes: всё предыдущее; dev-ClickHouse пользователя (поднят), `make`-цели с env из Makefile, MCP `clickhouse` (пользователь `kimi_reader`).

- [ ] **Step 1: `make all`**

Run: `make all`
Expected: gofmt пусто; golangci-lint `0 issues.`; `go vet` без замечаний; `go test -race ./...` — все пакеты `ok`; обе сборки (`build/clickhouse-import-rosstat`, `build/ingest`).

- [ ] **Step 2: Живой прогон**

Run: `make ch-status` → «ClickHouse отвечает…» (если нет — остановиться и попросить владельца поднять `make ch-up`).
Run: `make import STAT=cbr_mp_survey`
Expected в логе: `Imported 2084 rows of cbr_mp_survey` (допустимо иное число — зафиксировать точное; месячных 1 640, квартальных 444).

- [ ] **Step 3: Повторный прогон — идемпотентность**

Повторить `make import STAT=cbr_mp_survey` → `Imported 2084 rows of cbr_mp_survey`; счётчики витрины не изменятся (Step 5). Оговорка для записи: сырой `count()` без FINAL может вырасти незамёрженными частями — ожидаемо для `ReplacingMergeTree`.

- [ ] **Step 4: Грант и MCP-проверки**

Run: `make mcp-user` → в выводе грант на `v_cbr_survey` применён (ожидается строка вида `OK GRANT SELECT ON default.v_cbr_survey TO kimi_reader`).
Run: `make mcp-check` → **ПРОВЕРКА ПРОЙДЕНА** (включая новые строки: витрина `v_cbr_survey` читается; каталог `cbr_survey` = 13; сырая `macro_series` закрыта).

- [ ] **Step 5: Запросы к витрине (MCP `clickhouse`, `kimi_reader`)**

```sql
SELECT count() FROM v_cbr_survey;                                    -- 2084
SELECT series, count() AS n, min(date) AS d0, max(date) AS d1
FROM v_cbr_survey GROUP BY series ORDER BY series;                   -- 13 строк: 296/296/296/296/296/80/80 (M), 98/98/26/26/98/98 (Q); d0 2002-01-01 (price_growth 2020-01-01; staff 2020-04-01), d1 2026-08-01 (M) / 2026-07-01 (Q)
SELECT date, value FROM v_cbr_survey WHERE series = 'price_expectations_sa' ORDER BY date DESC LIMIT 2;  -- 2026-08-01 ≈ 22.655674; 2026-07-01 ≈ 21.100159
SELECT date, value FROM v_cbr_survey WHERE series = 'capacity_utilization_sa' ORDER BY date DESC LIMIT 2; -- 2026-07-01 ≈ 76.919083; 2026-04-01 ≈ 77.553879
```

Сверка с ЦБ: SA-«ценовые ожидания» 2026-08 = 22,7 п. (+1,6 п.) — как в справке ЦБ по волне сентября (15.09.2026).

- [ ] **Step 6: Расписание**

Run: `dagu validate dagu/surveys.yaml` → exit 0.

- [ ] **Step 7: Статусы в трёх документах**

- `docs/ROADMAP_KEY_RATE.md`:
  - §4.2, строка «Ценовые ожидания бизнеса»: в колонке «Откуда» добавить `✅ фаза 1.3, этим PR` (по образцу строки KEYRATE);
  - §5, строка `macro_series (source='cbr_survey')` / «мониторинг предприятий ЦБ»: статус → `✅ фаза 1.3, этим PR`;
  - §6, строка шага 1.3: в колонку критерия — `✅ **сделан этим PR** — приёмка ниже`;
  - после блока «Живая приёмка … шаг 1.1» добавить блок `#### Живая приёмка 2026-10-13 — шаг 1.3 (мониторинг предприятий ЦБ) — ✅ выполнен`: ссылки на спеку/план, файлы кода, и нумерованные факты с командами и результатами из Step 1–6 (включая точное число строк, крайние даты, сверку 22,7 п., строки `make mcp-check`).
- `docs/KEY_RATE_CHANNELS.md` §8: строка «Ценовые ожидания бизнеса» → `✅ есть (source='cbr_survey', витрина v_cbr_survey) — фаза 1.3`.
- `docs/KEY_RATE_PUBLICATIONS.md` §6: строка «Ценовые ожидания бизнеса, бизнес-климат» → `✅ есть (v_cbr_survey) — фаза 1.3`.

- [ ] **Step 8: Commit**

```bash
git add docs/ROADMAP_KEY_RATE.md docs/KEY_RATE_CHANNELS.md docs/KEY_RATE_PUBLICATIONS.md
git commit -m "roadmap key rate: mark step 1.3 done (CBR business monitoring acceptance)"
```

---

## После задач (вне нумерации)

Финальное ревью всей ветки — скил `superpowers:requesting-code-review` (сильнейшая модель), затем `superpowers:verification-before-completion`; после — один PR (`feat/key-rate-mp-survey` → `main`): источник, схема/ряды, `CLICKHOUSE_IMPORT_STAT=cbr_mp_survey`, пример `SELECT … FROM v_cbr_survey`.
