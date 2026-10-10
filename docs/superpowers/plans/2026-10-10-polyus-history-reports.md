# Включение пресс-релизов 4Q/FY 2019–2024 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собирать шапку таблицы по смене роли строки, а не по вертикальному зазору, и включить шесть пресс-релизов Полюса 4Q/FY за 2019–2024.

**Architecture:** Границы шапки определяются ролями строк: шапка начинается со строки с маркером единиц и продолжается, пока строки несут метки колонок (периоды или изменения); первая строка с опознанной метрикой и числами закрывает шапку. Метки периода, разнесённые по нескольким строкам (например `4Q` в одной строке и `2019` в следующей), склеиваются по X-координате при построении колонок.

**Tech Stack:** Go 1.27, clickhouse-go/v2, logrus, `pdftotext -tsv` (Poppler 26.10), `go test` на TSV-снимках.

**Spec:** `docs/superpowers/specs/2026-10-10-polyus-history-reports-design.md`

## Global Constraints

- Секреты только через `os.Getenv`; никаких токенов/паролей в коде и комментариях.
- Вставка в ClickHouse только батчами (`PrepareBatch` → `Append` → `Send`); построчный `conn.Exec` в цикле запрещён.
- Ошибки не проглатывать: каждый `strconv.Parse*` и `batch.Append` — с проверкой `err`.
- DDL не меняется; ключ `(company, metric, period)` не трогается.
- Логирование — logrus/`log`; `fmt.Print*` в прод-коде запрещены.
- Новый код только в `polyus/`; в `financial/` ничего не добавлять.
- Тесты не вызывают `pdftotext` — работают на закоммиченных TSV-снимках.
- `make all` зелёный; коммиты кратко по-английски.
- Команды с ClickHouse — только через `make`; ClickHouse поднимает пользователь.

## Review Focus

1. **Разорванная шапка 2019–2021.** Шапка занимает 3–4 строки с зазорами 5.04 и 5.28pt (текущий `headerContinuationGap` = 2pt). Если граница останется по зазору, квартальные периоды (`4Q 2019`, `3Q 2019`) не появятся. Тест: Task 2 (`TestHeaderBandSpansBrokenHeader`), Task 3 (`TestColumnsFromBrokenHeader2019`).
2. **Цельная шапка 2022–2024 не должна вырасти.** У этих отчётов шапка одной строкой и уже работает (5 периодов). Новая граница обязана сохранить ширину. Тест: Task 2 (`TestHeaderBandKeepsSingleLineHeader`).
3. **Строка данных закрывает шапку.** Метка с годом в тексте метрики (например `Average realised refined gold price` рядом со значениями) не должна удерживать шапку открытой и не должна попасть в колонки. Тест: Task 2 (`TestHeaderBandStopsAtDataRow`), Task 3 (`TestColumnsFromBrokenHeader2019`).
4. **Существующие отчёты не регрессируют.** RU 1H2026 (27 записей, 18 нераспределённых), FY2014 (32/8), FY2024 (53/30) — значения не меняются. Тест: Task 4 (`TestExistingReportsUnchanged`).
5. **Маркер единиц разорван по строкам.** `$ million (if not mentioned` и `otherwise)` в разных строках; маркер ищется по склейке слов строки. Тест: Task 1 (`TestUnitsMarkerSurvivesGlue`).

---

### Task 1: Фикстуры KPI-страниц шести отчётов

**Files:**
- Create: `polyus/testdata/press_release_fy2019_p3.tsv`
- Create: `polyus/testdata/press_release_fy2020_p4.tsv`
- Create: `polyus/testdata/press_release_fy2021_p4.tsv`
- Create: `polyus/testdata/press_release_fy2022_p4.tsv`
- Create: `polyus/testdata/press_release_fy2023_p4.tsv`
- Create: `polyus/testdata/press_release_fy2024_p4.tsv`
- Test: `polyus/words_test.go`

**Interfaces:**
- Consumes: —
- Produces: шесть TSV-снимков, по одному на KPI-страницу каждого отчёта; их читают Tasks 2–5.

- [ ] **Step 1: Скачать шесть PDF**

```bash
cd /tmp && mkdir -p pr && cd pr
curl -sfL -m 60 -o 2019.pdf "https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf"
curl -sfL -m 60 -o 2020.pdf "https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf"
curl -sfL -m 60 -o 2021.pdf "https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf"
curl -sfL -m 60 -o 2022.pdf "https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf"
curl -sfL -m 60 -o 2023.pdf "https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf"
curl -sfL -m 60 -o 2024.pdf "https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf"
```

- [ ] **Step 2: Сгенерировать TSV-снимки**

Номер страницы для каждого отчёта — тот, что содержит строку `Gold production (koz)`. Контролёр выверил: 2019 → стр. 3; 2020, 2021, 2022, 2023, 2024 → стр. 4. **Осторожно:** у 2020 строка `Gold production` встречается ещё и на стр. 11, но там это текст абзаца («Gold production is comprised of …»), а не таблица, — для фикстуры берётся стр. 4. Проверить каждый отчёт перед генерацией по наличию **числового** ряда:

```bash
cd /tmp/pr
for y in 2019 2020 2021 2022 2023 2024; do
  for p in $(seq 1 $(pdfinfo $y.pdf | awk '/^Pages/{print $2}')); do
    pdftotext -f $p -l $p -layout -nopgbrk -enc UTF-8 $y.pdf - 2>/dev/null | grep -q "Gold production (koz)" && echo "$y: KPI на стр $p"
  done
done
```

Если номер отличается от ожидаемого — использовать фактический и записать это в отчёт.

```bash
cd /Users/whitefox/GolandProjects/clickhouse-import-rosstat
pdftotext -f 3 -l 3 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2019.pdf polyus/testdata/press_release_fy2019_p3.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2020.pdf polyus/testdata/press_release_fy2020_p4.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2021.pdf polyus/testdata/press_release_fy2021_p4.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2022.pdf polyus/testdata/press_release_fy2022_p4.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2023.pdf polyus/testdata/press_release_fy2023_p4.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/pr/2024.pdf polyus/testdata/press_release_fy2024_p4.tsv
```

- [ ] **Step 3: Добавить тест на маркер единиц в разорванной строке**

В `polyus/words_test.go` (Review Focus #5):

```go
func TestUnitsMarkerSurvivesGlue(t *testing.T) {
	// шапка 2019 разорвана: «$ million (if not mentioned» в одной строке,
	// «otherwise)» — в следующей. Маркер ищется по склейке слов строки,
	// поэтому «$million» обязан найтись в первой из них.
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, l := range lines {
		if unitsMarker(l) {
			found = true
			break
		}
	}
	if !found {
		t.Error("unitsMarker не находит разорванный маркер «$ million (if not mentioned»")
	}
}
```

- [ ] **Step 4: Запустить тест**

Run: `go test ./polyus/ -run TestUnitsMarkerSurvivesGlue -v`
Expected: PASS (маркер найден). Если FAIL — сообщить: это значит, что проверенный контролёром факт о склейке не воспроизводится, и его надо перепроверить до продолжения.

- [ ] **Step 5: Commit**

```bash
git add polyus/testdata/ polyus/words_test.go
git commit -m "test: add fy2019-2024 kpi page snapshots for polyus"
```

---

### Task 2: Границы шапки по ролям строк

**Files:**
- Modify: `polyus/columns.go` (функция `headerBand`, удаление `headerContinuationGap`)
- Test: `polyus/columns_test.go`

**Interfaces:**
- Consumes: six TSV-снимков (Task 1), `unitsMarker`, `isPeriodHeaderLine` (`columns.go`), `lineText`/`gluedLineText` (`columns.go`), `matchMetric` (`kpi.go`)
- Produces: `func isDataLine(l Line) bool` — строка с опознанной метрикой и хотя бы одним числом; `headerBand` с новой границей.

- [ ] **Step 1: Написать failing-тесты границ**

В `polyus/columns_test.go`:

```go
func TestHeaderBandSpansBrokenHeader(t *testing.T) {
	// шапка 2019 занимает строки top=306.99, 312.03, 317.31; между ними
	// зазоры 5.04 и 5.28pt — больше прежнего критерия в 2pt.
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("шапка не найдена")
	}
	if got := len(headerBand(lines, start)); got < 3 {
		t.Errorf("band = %d строк, want >= 3: разорванная шапка не собрана целиком", got)
	}
}

func TestHeaderBandKeepsSingleLineHeader(t *testing.T) {
	// у 2022–2024 шапка одной строкой: граница не должна её вырастить
	lines, err := readTSVLines("testdata/press_release_fy2024_p4.tsv")
	if err != nil {
		t.Fatal(err)
	}

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("шапка не найдена")
	}
	if got := len(headerBand(lines, start)); got != 1 {
		t.Errorf("band = %d строк, want 1: цельная шапка не должна расти", got)
	}
}

func TestHeaderBandStopsAtDataRow(t *testing.T) {
	// строка данных (метрика + числа) закрывает шапку даже если несёт метку года
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}
	start := firstHeaderLine(lines)
	band := headerBand(lines, start)
	for _, l := range band {
		if isDataLine(l) {
			t.Errorf("строка данных попала в шапку: %q", lineText(l))
		}
	}
}
```

- [ ] **Step 2: Запустить — убедиться, что падают**

Run: `go test ./polyus/ -run TestHeaderBand -v`
Expected: FAIL — `undefined: isDataLine`.

- [ ] **Step 3: Реализовать новую границу в `polyus/columns.go`**

Добавить:

```go
// isDataLine сообщает, что строка — строка данных таблицы: в ней опознана
// метрика и есть хотя бы одно число.
func isDataLine(l Line) bool {
	if _, _, ok := matchMetric(lineText(l)); !ok {
		return false
	}

	for _, w := range l.Words {
		if _, err := parseNumericToken(w.Text); err == nil {
			return true
		}
	}

	return false
}
```

Переписать `headerBand`: начиная со строки с маркером единиц, добавлять строки, пока каждая следующая — **строка шапки** (`isPeriodHeaderLine`), и остановиться на первой, которая ею не является. Удалить переменную/константу `headerContinuationGap` и её обоснование в комментарии; в комментарии новой функции объяснить, что граница идёт по роли строки, а прежний критерий по зазору не различал разорванную шапку 2019–2021 (зазоры 5.04/5.28pt).

Проверить, что `isPeriodHeaderLine` не считает строкой шапки строку данных: если она опознаёт метрику — добавить проверку `isDataLine` **перед** ней в условии продолжения.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run TestHeaderBand -v`
Expected: PASS (3 теста). Затем весь пакет: `go test ./polyus/ -v` — существующие тесты не падают. Если падают тесты Task 3 из предыдущего пункта (`TestColumnsFromHeader*`), сообщить — возможно, потребовалась правка колонок, и это Task 3.

- [ ] **Step 5: Commit**

```bash
git add polyus/columns.go polyus/columns_test.go
git commit -m "derive polyus header band from line roles, not vertical gap"
```

---

### Task 3: Склейка меток периода по X-координате

**Files:**
- Modify: `polyus/columns.go` (функция `columnsFromHeader`)
- Test: `polyus/columns_test.go`

**Interfaces:**
- Consumes: `headerBand` (Task 2), `matchPeriodAt` (`kpi.go`), `parsePeriod` (`periods.go`)
- Produces: `columnsFromHeader([]Line) []Column` — колонки строятся из слов **всех** строк band, слитых в одно координатное пространство по X.

- [ ] **Step 1: Написать failing-тест на разорванную шапку**

В `polyus/columns_test.go`:

```go
func TestColumnsFromBrokenHeader2019(t *testing.T) {
	// «4Q» стоит на top=306.99, а год «2019» — на top=317.31. Период
	// «4Q 2019» обязан собраться из двух разных строк по X-координате.
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}

	cols := columnsFromHeader(headerBand(lines, firstHeaderLine(lines)))

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}
	for _, want := range []string{"2019Q4", "2019Q3", "2019FY", "2018FY"} {
		if !slices.Contains(periods, want) {
			t.Errorf("период %s не распознан; получено %v", want, periods)
		}
	}
	// колонка-изменение Q-o-Q не должна стать периодом
	if slices.Contains(periods, "Q-o-Q") {
		t.Error("Q-o-Q стал периодом")
	}
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `go test ./polyus/ -run TestColumnsFromBrokenHeader2019 -v`
Expected: FAIL — периоды `2019Q4`/`2019Q3` отсутствуют (сейчас распознаются только годовые).

- [ ] **Step 3: Доработать `columnsFromHeader`**

Сейчас функция собирает слова строк band в одно пространство функцией `bandWords`, но метка периода ищется в пределах одной строки. Требуется: искать окна периодов по **слитому по X** списку слов, а не по каждой строке отдельно, — тогда `4Q` (x=228.10, top=306.99) и `2019` (x=224.02, top=317.31) окажутся рядом и `parsePeriod("4Q 2019")` сработает.

Порядок проверок при этом сохраняется: `parsePeriod` вызывается на окнах до 3 токенов (`matchPeriodAt`), а слова-метки изменений (`Q-o-Q`, `Y-o-Y`, `Изм.`) исключаются из окон — как это сделано для строки 17 сейчас (`windowHasChangeLabel`).

Проверить на живых данных, что колонка `2019Q4` получает границы, накрывающие значение `804` (x=227.86), а `2019Q3` — значение `753` (x=270.34).

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run 'TestColumns|TestHeaderBand' -v`
Expected: PASS, включая существующие `TestColumnsFromHeaderRu`, `TestColumnsFromHeaderWide`, `TestColumnsPeriodsInvariantUnderBandWidth`.

- [ ] **Step 5: Commit**

```bash
git add polyus/columns.go polyus/columns_test.go
git commit -m "join polyus period labels split across header lines"
```

---

### Task 4: Сквозные значения шести отчётов

**Files:**
- Test: `polyus/kpi_test.go`

**Interfaces:**
- Consumes: `parseKPIPage` (Task 3), six TSV-снимков (Task 1)
- Produces: закреплённые значения шести отчётов.

- [ ] **Step 1: Написать тесты по каждому отчёту**

В `polyus/kpi_test.go`. Значения сверены контролёром с живыми PDF; перед записью **проверить каждое** по фикстуре и при расхождении использовать фактическое, записав это в отчёт.

```go
func TestKPIValuesHistoryReports(t *testing.T) {
	cases := []struct {
		file    string
		periods map[string]int // период -> gold_output
	}{
		{"testdata/press_release_fy2019_p3.tsv", map[string]int{
			"2019FY": 2841, "2018FY": 2440, "2019Q4": 804, "2019Q3": 753,
		}},
		{"testdata/press_release_fy2020_p4.tsv", map[string]int{
			"2020FY": 2766, "2019FY": 2841, "2020Q4": 710, "2019Q4": 804,
		}},
		{"testdata/press_release_fy2021_p4.tsv", map[string]int{
			"2021FY": 2717, "2020FY": 2766, "2021Q4": 684, "2020Q4": 710,
		}},
		{"testdata/press_release_fy2022_p4.tsv", map[string]int{
			"2022FY": 2541, "2021FY": 2717,
		}},
		{"testdata/press_release_fy2023_p4.tsv", map[string]int{
			"2023FY": 2902, "2022FY": 2541,
		}},
		{"testdata/press_release_fy2024_p4.tsv", map[string]int{
			"2024FY": 3002, "2023FY": 2799,
		}},
	}

	for _, c := range cases {
		t.Run(path.Base(c.file), func(t *testing.T) {
			records, err := parseKPIPage(c.file, "https://example.invalid/report.pdf", 1)
			if err != nil {
				t.Fatalf("parseKPIPage: %v", err)
			}
			got := map[string]float64{}
			for _, r := range records {
				if r.Metric == "gold_output" {
					got[r.Period] = r.Value
				}
			}
			for period, want := range c.periods {
				if got[period] != float64(want) {
					t.Errorf("gold_output %s = %v, want %d", period, got[period], want)
				}
			}
		})
	}
}
```

- [ ] **Step 2: Запустить**

Run: `go test ./polyus/ -run TestKPIValuesHistoryReports -v`
Expected: PASS. Периоды, которых нет в фикстуре, убрать из ожиданий **только** убедившись по TSV, что их там действительно нет, и записать это в отчёт.

- [ ] **Step 3: Тест на отсутствие регрессий существующих отчётов**

```go
func TestExistingReportsUnchanged(t *testing.T) {
	// RU 1H2026 и FY2014 — контроль того, что новая граница шапки их не тронула
	cases := []struct {
		file     string
		metric   string
		period   string
		value    float64
	}{
		{"testdata/press_reliz_1h26_p1.tsv", "gold_output", "2026H1", 1287},
		{"testdata/press_reliz_1h26_p1.tsv", "tcc_per_ounce", "2026H1", 1069},
		{"testdata/press_reliz_1h26_p1.tsv", "gold_output", "2025H2", 1218},
		{"testdata/press_release_hist_p1.tsv", "gold_output", "2014FY", 1696},
		{"testdata/press_release_hist_p1.tsv", "gold_output", "2014H2", 950},
	}
	for _, c := range cases {
		records, err := parseKPIPage(c.file, "u", 1)
		if err != nil {
			t.Fatalf("parseKPIPage(%s): %v", c.file, err)
		}
		var found bool
		for _, r := range records {
			if r.Metric == c.metric && r.Period == c.period {
				found = true
				if r.Value != c.value {
					t.Errorf("%s %s %s = %v, want %v", c.file, c.metric, c.period, r.Value, c.value)
				}
			}
		}
		if !found {
			t.Errorf("%s: %s %s не найдено", c.file, c.metric, c.period)
		}
	}
}
```

- [ ] **Step 4: Тесты зелёные, `make all`**

Run: `go test ./polyus/ -v && make all`
Expected: PASS, `make all` exit 0.

- [ ] **Step 5: Commit**

```bash
git add polyus/kpi_test.go
git commit -m "pin fy2019-2024 polyus values and existing report regressions"
```

---

### Task 5: Включение отчётов и живая приёмка

**Files:**
- Modify: `polyus/pages.go` (шесть отчётов `Enabled: true`)
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: результаты Tasks 1–4
- Produces: включённые отчёты; обновлённая документация.

- [ ] **Step 1: Включить шесть отчётов**

В `polyus/pages.go` для 2019FY…2024FY поставить `Enabled: true` и `Pages: []int{N}`, где N — выверенный номер KPI-страницы (2019 → 3, остальные → 4, либо фактический из Task 1, Step 2). К каждому добавить комментарий «Проверено pdftotext: страница N содержит "Gold production (koz)"».

Обновить вводный комментарий к отключённым: набор отключённых теперь только 2015–2018, и его причина — другой тип документа (MD&A и консолидированная МСФО), а не выверка страниц.

- [ ] **Step 2: Тесты и сборка**

Run: `go test ./polyus/ -v && make all`
Expected: PASS.

- [ ] **Step 3: Живая приёмка**

Run: `make import STAT=polyus_financial_metrics`
Expected: `0 failed` в сводке; число строк выросло против нынешних 66.

Затем через `make ch-sql` (SQL в файл, путь через `FILE=`):

```sql
SELECT period, count() FROM polyus_financial_metrics FINAL
 WHERE period IN ('2019FY','2018FY','2019Q4','2019Q3','2022FY','2024FY') GROUP BY period ORDER BY period;
SELECT metric, value FROM polyus_financial_metrics FINAL
 WHERE period='2019FY' AND metric IN ('gold_output','revenue') ORDER BY metric;
```

Ожидание: `gold_output` 2019FY = 2841, `2019Q4` = 804, `2019Q3` = 753; `revenue` 2019FY = 4005. Записать фактические числа в отчёт; расхождение — блокер.

- [ ] **Step 4: Документация**

Вызвать `Skill(sync-readme-architecture)` и обновить `ARCHITECTURE.md` (§6.6 — описание границы шапки по ролям строк; ограничения, которые остаются) и `README.md`. В `docs/ROADMAP_DCF_POLYUS.md` отметить пункт выполненным со ссылкой на спеку и фактические числа приёмки; зафиксировать, что 2015–2018 остаются отдельным пунктом.

- [ ] **Step 5: Commit**

```bash
git add polyus/pages.go ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "enable polyus fy2019-2024 press releases"
```
