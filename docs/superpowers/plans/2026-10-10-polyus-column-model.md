# Колоночная модель разбора PDF в `polyus/` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перевести разбор отчётности Полюса с позиционного сопоставления на колоночное: значения принадлежат колонке по X-координате, а колонки выводятся из шапки по координатам `pdftotext -tsv`.

**Architecture:** `pdftotext` вызывается с `-tsv` вместо `-layout`, отдавая координаты каждого слова. Слова группируются в строки по точному `Top` (с допуском как страховкой) и сортируются по `Left`. Шапка таблицы распознаётся как строка с меткой периода (`$ млн` / `$ mln`), из неё выводятся колонки с границами по X; каждое значение строки относится к колонке, накрывающей его X.

**Tech Stack:** Go 1.27, clickhouse-go/v2, logrus, `pdftotext` (Poppler 26.10, флаг `-tsv`), `go test` на TSV-снимках.

**Spec:** `docs/superpowers/specs/2026-10-10-polyus-column-model-design.md`

## Global Constraints

- Секреты только через `os.Getenv`; никаких токенов/паролей в коде и комментариях.
- Вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`.
- Никаких сетевых вызовов в `init()`.
- Ошибки не проглатывать: каждый `strconv.Parse*` и `batch.Append` — с проверкой `err`.
- DDL всегда `CREATE TABLE IF NOT EXISTS`; схемы `databook_polyus` и `polyus_financial_metrics` **не меняются** (ключ `(company, metric, period)` не трогается).
- Логирование — logrus/`log`; `fmt.Print*` в прод-коде запрещены.
- Новый код только в `polyus/`; в `financial/` ничего не добавлять.
- Тесты не вызывают `pdftotext` — работают на закоммиченных TSV-снимках (утилиты может не быть в CI).
- `make all` зелёный; коммиты кратко по-английски.
- Команды, которым нужен ClickHouse, — только через `make`.

## Review Focus

1. **Колонка-изменение не становится периодом.** Шапки содержат `Изм. за год`, `Y-o-Y`, `H-o-H`, `change` — метки без года. Если такая колонка получит период, значения сдвинутся, как сейчас. Тест: Task 4 (`TestColumnsFromHeaderRu`), Task 4 (`TestColumnsFromHeaderWide`).
2. **Значения раскладываются по X, а не по порядку.** Строка FY2014 `Total gold production … 1,696 1,652 3% 950 746` при 4 объявленных периодах: `3%` — это `change`. Тест: Task 5 (`TestKPIValuesByColumnFY2014`).
3. **Шапка привязывается к своей таблице по Y.** На странице может быть несколько таблиц; шапка действует только на строки ниже себя до следующей шапки. Тест: Task 4 (`TestHeaderAppliesToRowsBelow`).
4. **Все периоды строки читаются, ничего не теряется.** Русский релиз: TCC даёт 3 значения по периодам (`1 069`, `653`, `814`), а не 2 с потерей слота. Тест: Task 5 (`TestKPIValuesByColumnRuAllPeriods`).
5. **Сноска, слипшаяся с меткой, не попадает в значение.** `Производство золота (тыс. унций)2` — сноска `2` отделена от числа по X. Тест: Task 5 (`TestKPIFootnoteNotAValue`).

---

### Task 1: Удаление текстовых фикстур и создание TSV-снимков

Новые фикстуры — вход для всех последующих задач, поэтому создаются первыми.

**Files:**
- Delete: `polyus/testdata/press_reliz_1h26_p1.txt`
- Delete: `polyus/testdata/press_release_hist_p1.txt`
- Delete: `polyus/testdata/en_msfo_p6.txt`
- Delete: `polyus/testdata/en_msfo_p7.txt`
- Create: `polyus/testdata/press_reliz_1h26_p1.tsv`
- Create: `polyus/testdata/press_release_hist_p1.tsv`
- Create: `polyus/testdata/en_msfo_p6.tsv`
- Create: `polyus/testdata/en_msfo_p7.tsv`
- Create: `polyus/testdata/press_release_fy2024_p4.tsv`

**Interfaces:**
- Consumes: —
- Produces: пять TSV-файлов — выход `pdftotext -tsv` для конкретной страницы каждого отчёта; их читают все последующие задачи.

- [ ] **Step 1: Скачать исходные PDF**

Живые PDF уже лежат в `/tmp` (`polyus_1h26_ru.pdf`, `en_msfo.pdf`, `polyus_datapack` — не нужен). Для FY2014 и FY2024 скачать:

```bash
cd /tmp
curl -sfL -m 60 -o fy2014.pdf "https://polyus.com/upload/iblock/7fb/report_management-discussion-and-analysis_financial-statements-_fy2014.pdf"
curl -sfL -m 60 -o fy2024.pdf "https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf"
```

- [ ] **Step 2: Сгенерировать TSV-снимки**

Команда одна и та же, меняются файл и страница:

```bash
cd /Users/whitefox/GolandProjects/clickhouse-import-rosstat
pdftotext -f 1 -l 1 -tsv -nopgbrk -enc UTF-8 /tmp/polyus_1h26_ru.pdf   polyus/testdata/press_reliz_1h26_p1.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/fy2014.pdf            polyus/testdata/press_release_hist_p1.tsv
pdftotext -f 6 -l 6 -tsv -nopgbrk -enc UTF-8 /tmp/en_msfo.pdf           polyus/testdata/en_msfo_p6.tsv
pdftotext -f 7 -l 7 -tsv -nopgbrk -enc UTF-8 /tmp/en_msfo.pdf           polyus/testdata/en_msfo_p7.tsv
pdftotext -f 4 -l 4 -tsv -nopgbrk -enc UTF-8 /tmp/fy2024.pdf            polyus/testdata/press_release_fy2024_p4.tsv
```

- [ ] **Step 3: Убедиться, что снимки содержат ожидаемое**

Run: `Grep` по `polyus/testdata/press_reliz_1h26_p1.tsv` на `1 069` (по компонентам: `1` и `069` могут быть отдельными словами) и на `Производство`.
Expected: файл непустой, есть слова уровня `5`, присутствуют ключевые метки. Если при извлечении страница содержит не ту таблицу — исправить номер страницы и перегенерировать, зафиксировав причину в комментарии к `pages.go` (Task 8).

Проверить каждую из пяти: число строк уровня 5 больше нуля.

- [ ] **Step 4: Удалить текстовые фикстуры**

```bash
git rm polyus/testdata/press_reliz_1h26_p1.txt polyus/testdata/press_release_hist_p1.txt \
       polyus/testdata/en_msfo_p6.txt polyus/testdata/en_msfo_p7.txt
```

Тесты, которые их читают, будут падать — это ожидаемо и исправляется в Task 5–6. Чтобы `make all` оставался зелёным на промежуточных коммитах, тесты перевода парсеров идут в тех же задачах, что и замена фикстур (Task 5, 6).

- [ ] **Step 5: Commit**

```bash
git add polyus/testdata/
git commit -m "test: replace polyus text fixtures with pdftotext tsv snapshots"
```

---

### Task 2: Разбор TSV и группировка слов в строки

**Files:**
- Create: `polyus/words.go`
- Create: `polyus/words_test.go`

**Interfaces:**
- Consumes: TSV-снимки из Task 1
- Produces:
  - `type Word struct { Text string; Left, Top, Width, Height float64; Block, Line int }`
  - `type Line struct { Top float64; Words []Word }`
  - `func parseTSV(r io.Reader) ([]Word, error)` — только слова уровня 5, служебные маркеры отброшены
  - `func groupByLine(words []Word, deltaTop float64) []Line` — строки по возрастанию `Top`, слова внутри по возрастанию `Left`

- [ ] **Step 1: Написать failing-тесты**

`polyus/words_test.go`:

```go
func TestParseTSVReadsWordsOnly(t *testing.T) {
	f, err := os.Open("testdata/press_reliz_1h26_p1.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatalf("parseTSV: %v", err)
	}
	if len(words) == 0 {
		t.Fatal("no words parsed")
	}
	for _, w := range words {
		if w.Text == "" {
			t.Error("empty word text")
		}
		if strings.HasPrefix(w.Text, "###") {
			t.Errorf("service marker leaked into words: %q", w.Text)
		}
	}
	// «Производство» присутствует и имеет положительные координаты
	var found bool
	for _, w := range words {
		if w.Text == "Производство" {
			found = true
			if w.Left <= 0 || w.Top <= 0 {
				t.Errorf("Производство at non-positive coords: left=%v top=%v", w.Left, w.Top)
			}
		}
	}
	if !found {
		t.Error("Производство not found")
	}
}

func TestGroupByLineOrdersWordsByLeft(t *testing.T) {
	// три слова на одном Top, поданные в обратном порядке
	words := []Word{
		{Text: "674", Left: 296.21, Top: 434.80},
		{Text: "Выручка", Left: 56.64, Top: 434.80},
		{Text: "4", Left: 290.33, Top: 434.80},
	}
	lines := groupByLine(words, 0)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	got := []string{lines[0].Words[0].Text, lines[0].Words[1].Text, lines[0].Words[2].Text}
	want := []string{"Выручка", "4", "674"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("word %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGroupByLineSeparatesDifferentTops(t *testing.T) {
	words := []Word{
		{Text: "a", Left: 10, Top: 100},
		{Text: "b", Left: 10, Top: 120},
	}
	if got := groupByLine(words, 0); len(got) != 2 {
		t.Errorf("got %d lines, want 2", len(got))
	}
}

func TestGroupByLineDeltaJoinsNearbyTops(t *testing.T) {
	// дрейф базовой линии 1.5pt склеивается при delta=2, но не при delta=0
	words := []Word{
		{Text: "a", Left: 10, Top: 100.0},
		{Text: "b", Left: 20, Top: 101.5},
	}
	if got := groupByLine(words, 2); len(got) != 1 {
		t.Errorf("delta=2: got %d lines, want 1", len(got))
	}
	if got := groupByLine(words, 0); len(got) != 2 {
		t.Errorf("delta=0: got %d lines, want 2", len(got))
	}
}
```

- [ ] **Step 2: Запустить тесты — убедиться, что падают**

Run: `go test ./polyus/ -run 'TestParseTSV|TestGroupByLine' -v`
Expected: FAIL — `undefined: parseTSV`.

- [ ] **Step 3: Реализовать `polyus/words.go`**

Формат входа (проверено на живых файлах) — TSV с заголовком:

```
level	page_num	par_num	block_num	line_num	word_num	left	top	width	height	conf	text
5	1	0	0	0	0	50.52	28.41	41.76	20.04	100	ПАО
```

Уровни: `1` страница, `3` flow, `4` line, `5` word. Обрабатываются только `level == 5`; остальные пропускаются.

`parseTSV` читает через `bufio.Scanner` с увеличенным буфером, разбивает строку по `\t`, требует не менее 12 полей, разбирает `left`, `top`, `width`, `height` через `strconv.ParseFloat` (ошибка — с контекстом номера строки), кладёт `Text` как есть.

`groupByLine`:
- сортирует копию слов по `Top`, затем по `Left`;
- идёт по словам, накапливая группу, пока `|Top − опорный Top| ≤ deltaTop`, где опорный — `Top` первого слова группы;
- внутри группы сортирует по `Left`;
- возвращает группы в порядке возрастания `Top`.

При `deltaTop == 0` группировка строго по точному `Top` (штатный случай — у слов одной строки `Top` совпадает точно).

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run 'TestParseTSV|TestGroupByLine' -v`
Expected: PASS (4 теста).

- [ ] **Step 5: Commit**

```bash
git add polyus/words.go polyus/words_test.go
git commit -m "add tsv word parsing and line grouping to polyus"
```

---

### Task 3: Переключение `extractPage` на `-tsv`

**Files:**
- Modify: `polyus/source.go:113-155` (функция `extractPage`)

**Interfaces:**
- Consumes: —
- Produces: `extractPage` пишет TSV вместо размеченного текста; сигнатура не меняется.

- [ ] **Step 1: Заменить флаг**

В `extractPage` заменить `"-layout"` на `"-tsv"`. Остальные аргументы (`-f N -l N -nopgbrk -enc UTF-8`) и вся обработка ошибок (`CombinedOutput`, проверка `os.Stat`, проверка нулевого размера) остаются без изменений.

Обновить doc-комментарий функции: он говорит «в текстовый файл» — уточнить, что теперь это TSV с координатами слов, и назвать причину (позиционный разбор не различает колонки-изменения).

- [ ] **Step 2: Проверить, что живое извлечение даёт TSV**

Run (через уже существующую живую проверку из Task 1 — команду можно повторить):

```bash
cd /Users/whitefox/GolandProjects/clickhouse-import-rosstat
go build ./... && ./build/clickhouse-import-rosstat 2>/dev/null || true
```

Основная проверка здесь — что пакет собирается и `gofmt` чист:

Run: `gofmt -l polyus && go build ./polyus/`
Expected: пустой вывод, сборка без ошибок.

- [ ] **Step 3: Commit**

```bash
git add polyus/source.go
git commit -m "switch polyus pdf extraction to tsv coordinates"
```

---

### Task 4: Колоночная модель

**Files:**
- Create: `polyus/columns.go`
- Create: `polyus/columns_test.go`

**Interfaces:**
- Consumes: `Line`, `Word` (Task 2), `parsePeriod` (`polyus/periods.go`)
- Produces:
  - `type Column struct { Period, Type string; Left, Right float64 }` — `Period == ""` для непериодной колонки
  - `func columnsFromHeader(header Line) []Column` — колонки по X из строки шапки
  - `func (c Column) covers(left float64) bool` — попадает ли X значения в колонку

- [ ] **Step 1: Написать failing-тесты**

`polyus/columns_test.go`:

```go
// headerLineFrom открывает TSV-снимок и возвращает строку шапки таблицы,
// у которой в тексте есть метка «$ млн» (или «$ mln») — то есть финансовую.
func headerLineFrom(t *testing.T, path string) Line {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range groupByLine(words, 0) {
		text := lineText(l)
		if strings.Contains(text, "$ млн") || strings.Contains(text, "$ mln") {
			return l
		}
	}
	t.Fatalf("no header line with '$ млн'/'$ mln' in %s", path)
	return Line{}
}

func TestColumnsFromHeaderRu(t *testing.T) {
	// реальная шапка финансовой таблицы русского релиза 1H2026 (y=369.26)
	h := headerLineFrom(t, "testdata/press_reliz_1h26_p1.tsv")
	cols := columnsFromHeader(h)

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}
	want := []string{"2026H1", "2025H1", "2025H2"}
	if !slices.Equal(periods, want) {
		t.Errorf("periods = %v, want %v", periods, want)
	}
	// «Изм. за год» — колонка без периода, но она должна занимать место по X
	if len(cols) != len(periods)+2 {
		t.Errorf("got %d columns, want %d (3 periods + 2 change columns)", len(cols), len(periods)+2)
	}
}

func TestColumnsFromHeaderWide(t *testing.T) {
	// шапка 2024FY: 5 периодов + 3 изменения без года
	h := headerLineFrom(t, "testdata/press_release_fy2024_p4.tsv")
	cols := columnsFromHeader(h)

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}
	for _, want := range []string{"2024FY", "2023FY", "2024H2", "2024H1", "2023H2"} {
		if !slices.Contains(periods, want) {
			t.Errorf("period %s missing from %v", want, periods)
		}
	}
	// метки Y-o-Y и H-o-H не стали периодами
	for _, c := range cols {
		if c.Period == "Y-o-Y" || c.Period == "H-o-H" {
			t.Errorf("change label became a period: %q", c.Period)
		}
	}
}

func TestColumnCoversValueX(t *testing.T) {
	// значение 2026H1 стоит на x=290.33, метка периода — на x=282.05
	col := Column{Period: "2026H1", Left: 282.05, Right: 329.0}
	if !col.covers(290.33) {
		t.Error("value at 290.33 must fall into the 2026H1 column")
	}
	col2 := Column{Period: "2025H1", Left: 339.91, Right: 386.0}
	if col2.covers(290.33) {
		t.Error("value at 290.33 must not fall into the 2025H1 column")
	}
}

func TestHeaderAppliesToRowsBelow(t *testing.T) {
	// На странице русского релиза ДВЕ шапки: производственная (y=206.51)
	// и финансовая с «$ млн» (y=369.26). Финансовые строки (y>369.26)
	// обязаны раскладываться по колонкам финансовой шапки, а не первой.
	f, err := os.Open("testdata/press_reliz_1h26_p1.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}
	lines := groupByLine(words, 0)

	// шапки — строки, где matchMetric опознаёт определение period
	var headerTops []float64
	for _, l := range lines {
		if _, _, ok := matchMetric(lineText(l)); ok && isPeriodHeaderLine(l) {
			headerTops = append(headerTops, l.Top)
		}
	}
	if len(headerTops) < 2 {
		t.Fatalf("expected at least 2 header lines on the page, got %v", headerTops)
	}
	// строка «Выручка» (y=434.80) обслуживается ближайшей шапкой сверху
	revenue := findLineContaining(t, lines, "Выручка")
	wantHeader := headerTops[len(headerTops)-1]
	for _, top := range headerTops {
		if top < revenue.Top && top > wantHeader-1e-9 {
			wantHeader = top
		}
	}
	if wantHeader != 369.26 {
		t.Errorf("Выручка served by header at y=%v, want 369.26", wantHeader)
	}
}
```

`lineText` и `findLineContaining` — существующий/новый тестовый помощник этого файла; `isPeriodHeaderLine` — проверка, что строка содержит метку «$ млн»/«$ mln» (та же, что использует `parseKPIPage` при поиске шапки, Task 5). Правило «шапка обслуживает строки ниже себя до следующей шапки» проверяется здесь; сам `parseKPIPage` реализует его в Task 5.

- [ ] **Step 2: Запустить тесты — убедиться, что падают**

Run: `go test ./polyus/ -run TestColumn -v`
Expected: FAIL — `undefined: columnsFromHeader`.

- [ ] **Step 3: Реализовать `polyus/columns.go`**

Алгоритм `columnsFromHeader`:
1. Идти по словам шапки слева направо, пробуя `parsePeriod` на окнах длиной до 3 слов (как `matchPeriodAt` в `kpi.go` — переиспользовать его, если он экспортируем в рамках пакета: он уже существует и решает именно эту задачу).
2. Распознанный период задаёт `Column{Period, Type}`; его `Left` — `Left` первого слова окна, `Right` — `Left + Width` последнего слова окна.
3. Слова, не вошедшие ни в один период, формируют непериодные колонки (`Period: ""`) — каждая как отдельная колонка со своими границами. **Они обязаны попадать в результат**, иначе значения справа сдвинутся влево.
4. Границы для `covers`: между соседними колонками граница проходит по середине промежутка между `Right` левой и `Left` правой; у крайних колонок внешняя граница — плюс-минус бесконечность. Реализуется как расширение каждой колонки до середины промежутка — так значение `290.33` при метке `282.05` попадает в свою колонку, хотя формально правее её `Right`.

`covers(left float64) bool` проверяет попадание в расширенный диапазон.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run TestColumn -v`
Expected: PASS (3 теста).

- [ ] **Step 5: Commit**

```bash
git add polyus/columns.go polyus/columns_test.go
git commit -m "add column model derived from header coordinates"
```

---

### Task 5: Перевод KPI-парсера на колоночную модель

**Files:**
- Modify: `polyus/kpi.go` (функция `parseKPIPage`, вспомогательные разборы)
- Modify: `polyus/kpi_test.go`
- Modify: `polyus/guard.go` (удаление `untrustworthyPeriod`)

**Interfaces:**
- Consumes: `parseTSV`, `groupByLine` (Task 2), `columnsFromHeader`, `Column` (Task 4)
- Produces: `parseKPIPage(textPath, sourceURL string, page int) ([]MetricRecord, error)` — сигнатура **не меняется**, меняется только тело: вместо позиционного сопоставления значений строится колоночная модель. Плюс извлечённая единица разбора строки, которую используют и тесты guard (Task 7):
  - `func recordsFromLine(line Line, cols []Column, company, sourceURL string, page int) ([]MetricRecord, int)` — возвращает записи строки и число нераспределённых значений

- [ ] **Step 1: Переписать `kpi_test.go` на TSV-фикстуры и новые проверки**

Существующие тесты перевести на `.tsv` (имя файла меняется, ожидаемые значения — нет). Добавить:

```go
func TestKPIValuesByColumnRuAllPeriods(t *testing.T) {
	recs, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	byMetric := map[string]map[string]float64{}
	for _, r := range recs {
		if byMetric[r.Metric] == nil {
			byMetric[r.Metric] = map[string]float64{}
		}
		byMetric[r.Metric][r.Period] = r.Value
	}
	// TCC: все три периода прочитаны, включая слот 2 п/г 2025
	tcc := byMetric["tcc_per_ounce"]
	if tcc["2026H1"] != 1069 || tcc["2025H1"] != 653 || tcc["2025H2"] != 814 {
		t.Errorf("tcc_per_ounce = %v, want 2026H1=1069 2025H1=653 2025H2=814", tcc)
	}
	// производство золота по всем периодам
	gold := byMetric["gold_output"]
	if gold["2026H1"] != 1287 || gold["2025H1"] != 1311 || gold["2025H2"] != 1218 {
		t.Errorf("gold_output = %v, want 2026H1=1287 2025H1=1311 2025H2=1218", gold)
	}
	// колонка «Изм. за год» не породила значение: -2 больше не существует
	for _, r := range recs {
		if r.Metric == "gold_output" && r.Value == -2 {
			t.Errorf("change column value leaked as a period value: %+v", r)
		}
	}
}

func TestKPIValuesByColumnFY2014(t *testing.T) {
	recs, err := parseKPIPage("testdata/press_release_hist_p1.tsv", "https://example.invalid/fy2014.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	byPeriod := map[string]float64{}
	for _, r := range recs {
		if r.Metric == "gold_output" {
			byPeriod[r.Period] = r.Value
		}
	}
	// 1,696 (FY2014) 1,652 (FY2013) 3% (change) 950 (2H2014) 746 (1H2014)
	want := map[string]float64{"2014FY": 1696, "2013FY": 1652, "2014H2": 950, "2014H1": 746}
	for period, v := range want {
		if byPeriod[period] != v {
			t.Errorf("gold_output %s = %v, want %v", period, byPeriod[period], v)
		}
	}
	if v, ok := byPeriod["2014H2"]; ok && v == 1652 {
		t.Error("2014H2 carries FY2013's value — column shift not fixed")
	}
}

func TestKPIFootnoteNotAValue(t *testing.T) {
	// метка «Производство золота (тыс. унций)2»: сноска 2 не должна стать значением
	recs, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	for _, r := range recs {
		if r.Metric == "gold_output" && r.Period == "2026H1" && r.Value != 1287 {
			t.Errorf("gold_output 2026H1 = %v, want 1287 (footnote likely parsed as value)", r.Value)
		}
	}
}
```

- [ ] **Step 2: Запустить тесты — убедиться, что падают**

Run: `go test ./polyus/ -run TestKPI -v`
Expected: FAIL — тесты читают `.tsv`, а парсер всё ещё построен на токенах строки.

- [ ] **Step 3: Переписать тело `parseKPIPage`**

Новый поток обработки для каждой страницы:
1. `parseTSV` из файла → `[]Word`;
2. `groupByLine(words, 0)` → `[]Line`, отсортированные по `Top`;
3. найти строки-шапки: строка, в которой `matchMetric` опознаёт определение `period` (метка `$ млн` / `$ mln`); их может быть несколько на странице;
4. для каждой шапки `columnsFromHeader` даёт набор колонок; строки между этой шапкой и следующей обрабатываются против её колонок;
5. в строке метрика опознаётся по `matchMetric` по склеенному тексту слов строки **начиная с самого левого слова**; остаток строки — числа;
6. каждое число относится к колонке через `Column.covers(число.Left)`; числа, не накрытые ни одной колонкой, считаются нераспределёнными (см. guard, Task 7);
7. значение разбирается `parseNumericToken` (существующая логика: пробелы как разделители тысяч, запятая как десятичный разделитель, скобки как минус);
8. запись формируется с `Period` и `Type` из колонки.

`scanPeriodColumns`, `metricsSortedByPrefixLength` и `matchPeriodAt` переиспользуются там, где применимы; `normalizeLine` больше не нужен для склейки строк (слова приходят раздельно) — но нужен для очистки неразрывных пробелов внутри слов, поэтому сохраняется для обработки текста слова.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -v`
Expected: PASS, включая `TestKPIValuesByColumnRuAllPeriods`, `TestKPIValuesByColumnFY2014`, `TestKPIFootnoteNotAValue` и существующие проверки значений `2026H1` (1287 / 1069 / 4674 / 829).

- [ ] **Step 5: Живой прогон после обеих задач 5 и 6 не делается** — он в Task 7. Здесь ограничиться тестами.

- [ ] **Step 6: Commit**

```bash
git add polyus/kpi.go polyus/kpi_test.go
git commit -m "parse polyus kpi reports by column coordinates"
```

---

### Task 6: Перевод МСФО-парсера на колоночную модель

**Files:**
- Modify: `polyus/ifrs.go` (функция `parseIFRSPage`)
- Modify: `polyus/ifrs_test.go`

**Interfaces:**
- Consumes: `parseTSV`, `groupByLine` (Task 2), `Column` (Task 4)
- Produces: `parseIFRSPage(textPath, sourceURL string, page int, period string) ([]MetricRecord, error)` — сигнатура **не меняется**.

- [ ] **Step 1: Перевести тесты на TSV и добавить проверку раскладки**

Существующие проверки (`total_revenue` 4674, `profit_for_period` 829, `eps_basic` 0.87) сохраняются со сменой имени фикстуры на `.tsv`. Добавить:

```go
func TestIFRSValuesByColumn(t *testing.T) {
	// МСФО-страница: метка слева, два числовых столбца (отчётный и прошлый).
	// Отчётный — первый по X после метки; прошлый — следующий.
	recs, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	got := map[string]float64{}
	for _, r := range recs {
		got[r.Metric] = r.Value
	}
	if got["gold_sales"] != 4569 {
		t.Errorf("gold_sales = %v, want 4569", got["gold_sales"])
	}
	if got["total_revenue"] != 4674 {
		t.Errorf("total_revenue = %v, want 4674", got["total_revenue"])
	}
	if got["operating_expenses"] != -3405 {
		t.Errorf("operating_expenses = %v, want -3405 (parenthesised negative)", got["operating_expenses"])
	}
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `go test ./polyus/ -run TestIFRS -v`
Expected: FAIL — фикстура `.txt` удалена, парсер читает текстовый формат.

- [ ] **Step 3: Переписать тело `parseIFRSPage`**

Поток тот же, что в Task 5, с двумя отличиями:
1. период приходит параметром (МСФО-страница не несёт периодов в шапке), поэтому `columnsFromHeader` не применяется — колонки определяются позиционно по X: метка занимает левую часть строки, числа идут правее;
2. отчётный период — **первое** число строки (крайнее левое), прошлый период — второе; берётся только первое, как и сейчас.

Метка опознаётся по существующему `matchIFRSMetric` по склеенному тексту слов левой части строки.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -v`
Expected: PASS, включая `TestIFRSValuesByColumn` и `TestParseIFRSOnKPIPage` (перекрёстный случай — МСФО-парсер на KPI-странице даёт 0 записей).

- [ ] **Step 5: Commit**

```bash
git add polyus/ifrs.go polyus/ifrs_test.go
git commit -m "parse polyus ifrs statements by column coordinates"
```

---

### Task 7: Guard как резерв и живая приёмка

**Files:**
- Modify: `polyus/guard.go`
- Modify: `polyus/guard_test.go`, `polyus/guard_annual_test.go`
- Modify: `polyus/import.go` (вызов guard)

**Interfaces:**
- Consumes: `Column`, записи с координатами (Task 5, 6)
- Produces: guard, отбрасывающий только нераспределённые значения; `untrustworthyPeriod` удалён.

- [ ] **Step 1: Переписать тесты guard**

Тесты, опирающиеся на `untrustworthyPeriod` и на «отбросить последнюю колонку», заменяются:

```go
func TestGuardDropsUnassignedValues(t *testing.T) {
	// Синтетическая строка, значение которой не накрыто ни одной колонкой,
	// не должна породить запись. Строку собираем вручную, потому что на живых
	// фикстурах нераспределённых значений нет (0) — это и есть проверяемое
	// свойство разбора: он раскладывает всё.
	//
	// Колонки: 2026H1 при x∈[282,329], 2025H1 при x∈[340,386].
	// Значение на x=900 — вне обеих колонок.
	words := []Word{
		{Text: "Выручка", Left: 56.64, Top: 434.80},
		{Text: "4", Left: 290.33, Top: 434.80},   // внутри 2026H1
		{Text: "674", Left: 296.21, Top: 434.80}, // внутри 2026H1
		{Text: "999", Left: 900.0, Top: 434.80},  // вне колонок
	}
	lines := groupByLine(words, 0)
	cols := []Column{
		{Period: "2026H1", Type: "H", Left: 282.05, Right: 329.0},
		{Period: "2025H1", Type: "H", Left: 339.91, Right: 386.0},
	}

	recs, unassigned := recordsFromLine(lines[0], cols, "PLZL", "u", 1)
	if unassigned == 0 {
		t.Error("expected the out-of-column value to be counted as unassigned")
	}
	for _, r := range recs {
		if r.Value == 999 {
			t.Errorf("unassigned value became a record: %+v", r)
		}
		if r.Period == "" {
			t.Errorf("record with empty period produced: %+v", r)
		}
	}
}

func TestGuardKeepsAnnualPeriods(t *testing.T) {
	// годовые периоды больше не отбрасываются: 2014FY и 2013FY присутствуют
	recs, err := parseKPIPage("testdata/press_release_hist_p1.tsv", "https://example.invalid/fy2014.pdf", 4)
	if err != nil {
		t.Fatal(err)
	}
	var annual int
	for _, r := range recs {
		if r.Period == "2014FY" || r.Period == "2013FY" {
			annual++
		}
	}
	if annual == 0 {
		t.Fatal("annual periods were dropped — guard regressed to whitelist behaviour")
	}
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `go test ./polyus/ -run TestGuard -v`
Expected: FAIL — `untrustworthyPeriod` всё ещё существует и отбрасывает период.

- [ ] **Step 3: Переписать `polyus/guard.go`**

Удалить `untrustworthyPeriod` и `knownKPIPeriods` (последний уже удалён ранее; проверить). Новый guard принимает записи вместе со счётчиком нераспределённых значений, полученным от парсера, и:
- пропускает записи с пустым `Period` (они не должны появляться — если появились, это ошибка разбора, и о ней пишется `Warnf`);
- логирует `Warnf` при ненулевых нераспределённых значениях с их числом;
- пишет счётчик в итоговую строку импортёра.

В `import.go` вызов guard обновляется под новую сигнатуру; счётчик идёт в summary рядом с `dropped`/`skipped`.

- [ ] **Step 4: Тесты зелёные, `make all`**

Run: `go test ./polyus/ -v && make all`
Expected: PASS, `make all` exit 0.

- [ ] **Step 5: Живая приёмка**

Run: `make import STAT=polyus_financial_metrics`
Expected: число импортированных записей; затем через `make ch-sql`:

```sql
SELECT period, count() FROM polyus_financial_metrics FINAL GROUP BY period ORDER BY period;
SELECT metric, value FROM polyus_financial_metrics FINAL
 WHERE period = '2026H1' AND metric IN ('gold_output','tcc_per_ounce','total_revenue','profit_for_period','stripping_capex') ORDER BY metric;
SELECT metric, value FROM polyus_financial_metrics FINAL WHERE period = '2014H2' ORDER BY metric;
```

Ожидание:
- `2026H1`: `gold_output` 1287, `tcc_per_ounce` 1069, `total_revenue` 4674, `profit_for_period` 829, `stripping_capex` 397;
- слот `2025H2` русского релиза присутствует со значением `1 218` (по крайней мере для `gold_output`), а не `-2`;
- записи `2014H2` не содержат значений FY2013 (`revenue` ≠ 2329).

Записать фактические числа в отчёт: расхождение с ожиданием — блокер, а не повод объявить успех.

- [ ] **Step 6: Commit**

```bash
git add polyus/guard.go polyus/guard_test.go polyus/guard_annual_test.go polyus/import.go
git commit -m "reduce polyus guard to unassigned-value reporting"
```

---

### Task 8: Документация

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `README.md`
- Modify: `docs/ROADMAP_DCF_POLYUS.md`
- Modify: `polyus/pages.go` (комментарии о причинах отключения)

**Interfaces:**
- Consumes: результаты Task 1–7
- Produces: документация, отражающая колоночную модель.

- [ ] **Step 1: Синхронизировать документы**

Вызвать `Skill(sync-readme-architecture)` и следовать чек-листу. Отразить:
- способ извлечения: `pdftotext -tsv` и колоночная модель вместо `-layout` и позиционного сопоставления (ARCHITECTURE §6.6 и §6.2-комментарии);
- что три дефекта из ROADMAP §8.1 закрыты (сдвиг колонок, `2014H2`, потеря слота) и что остаётся открытым (`revenue` ≡ `total_revenue`, ключ без источника);
- что 11 отчётов 2015–2025 по-прежнему отключены и вынесены в следующий пункт (причина: выверка страниц и разные типы документов, см. спеку §3.1).

- [ ] **Step 2: Обновить комментарии в `pages.go`**

Причину отключения сформулировать точно: не «номера страниц не выверены» как единственную, а добавить вторую — широкая шапка (до 8 колонок) с метками-изменениями без года, и третью — часть набора это IFRS-формы, а не KPI-релизы.

- [ ] **Step 3: Commit**

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md polyus/pages.go
git commit -m "sync docs with polyus column model"
```
