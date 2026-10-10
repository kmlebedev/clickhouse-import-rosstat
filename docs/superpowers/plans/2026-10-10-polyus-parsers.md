# Пакет `polyus/` — парсеры отчётности Полюса — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Вынести парсеры отчётности Полюса из legacy-пакета `financial/` в самостоятельный пакет `polyus/`, сделав их двуязычными (EN/RU) и добавив разбор МСФО-форм, чтобы закрыть отчётность 1H2026.

**Architecture:** Два независимых режима разбора PDF: KPI-страницы пресс-релизов (таблица «метрика + строки периодов») и МСФО-формы (статья + два столбца периодов). Оба пишут в одну таблицу `polyus_financial_metrics`. xlsx-датапак переносится без изменения схемы. Список отчётов становится данными (`pages.go`), а не закомментированными строками в `init()`.

**Tech Stack:** Go 1.27, clickhouse-go/v2, logrus, `github.com/xuri/excelize/v2` (xlsx), внешняя утилита `pdftotext` (Poppler) для извлечения текста PDF, `go test` для тестов на локальных фикстурах.

**Spec:** `docs/superpowers/specs/2026-10-10-polyus-parsers-design.md`

## Global Constraints

- Секреты только через `os.Getenv`; никаких токенов/паролей в коде и комментариях.
- Вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`; построчный `conn.Exec` в цикле запрещён.
- Никаких сетевых вызовов в `init()` — URL и соединения вычисляются внутри `Import()`.
- Ошибки не проглатывать: каждый `strconv.Parse*` и `batch.Append` — с проверкой `err`.
- DDL всегда `CREATE TABLE IF NOT EXISTS`; схемы `databook_polyus` и `polyus_financial_metrics` переносятся **как есть** (таблицы уже существуют в ClickHouse — переименование сломало бы накопленные ряды и `dashboard/finance-polyus.json`).
- Логирование — logrus; `fmt.Print*`/`fmt.Fprintf(os.Stderr, ...)` в прод-коде запрещены.
- Новый код в пакет `financial/` не добавлять (AGENTS.md, правило 7) — только удаление кода Полюса.
- Команды, которым нужен ClickHouse или env-ключи, запускать только через `make`; ClickHouse поднимает пользователь (`make ch-up`).
- `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) должен быть зелёным.
- Коммиты: кратко, по-английски, в духе истории (`add fred importer`).

## Review Focus

1. **Русский пресс-релиз 1H2026 читается.** Английские `Prefix`-строки и регулярка периода `([1-4][HQ])\s+(\d{4})` на русском тексте не срабатывают; при неверном словаре парсер вернёт 0 записей **без ошибки** — тихий отказ. Тест: Task 5 (`TestParseKPIReportRU`).
2. **МСФО-режим не ломает KPI-режим.** МСФО-страница, поданная KPI-парсеру, и наоборот — перекрёстный случай; парсер не должен падать и не должен выдавать мусорные метрики. Тест: Task 6 (`TestParseIFRSOnKPIPage`).
3. **Смешение единиц.** Английская МСФО в долларах (`Total revenue 4 674`), русская — в рублях (`352 667`); датапак англоязычный. Парсим английскую; тест фиксирует именно долларовые значения. Тест: Task 6 (`TestParseIFRSReport`).
4. **Пропуск отключённого отчёта не роняет импорт.** `Enabled: false` пропускается с логом, а не с ошибкой и не молча. Тест: Task 7 (`TestEnabledReports`).
5. **Битая ячейка датапака не роняет весь импорт.** Одно нечисловое значение — пропуск строки с warn и счётчиком, а не `return err`. Тест: Task 3 (`TestParseDatapackToleratesBadCell`).

---

### Task 1: Удаление кода Полюса из `financial/`

Перенос начинается с удаления — так компилятор сразу покажет все места, которые зависят от старого кода, и ни один файл не будет существовать в двух пакетах.

**Files:**
- Delete: `financial/gold_polyus.go`
- Delete: `financial/gold_polyus_finance.go`
- Modify: `main.go`

**Interfaces:**
- Consumes: —
- Produces: пакет `financial` без кода Полюса; `chimport.Stats` содержит одну запись из `financial` (`databook_ugk`).

- [ ] **Step 1: Удалить два файла**

```bash
git rm financial/gold_polyus.go financial/gold_polyus_finance.go
```

- [ ] **Step 2: Убедиться, что удаление видно компилятору**

Run: `go build ./... && make all`
Expected: PASS. Если сборка падает с `undefined: FinDataBook` или похожим — значит `financial/databook.go` используется только этими двумя файлами; в таком случае НЕ удалять `databook.go` (он остаётся по спеке §4.1), а зафиксировать в отчёте текстом ошибки, что именно сломалось.

- [ ] **Step 3: Проверить, что регистрации Полюса исчезли**

Run: `Grep` по репозиторию: `databook_polyus|polyus_financial_metrics`
Expected: совпадения только в `docs/`, `ARCHITECTURE.md`, `README.md`, `dashboard/finance-polyus.json` — ни одного в `*.go`.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove polyus parsers from legacy financial package"
```

---

### Task 2: Пакет `polyus/` — типы, источник и извлечение PDF

**Files:**
- Create: `polyus/polyus.go`
- Create: `polyus/source.go`
- Create: `polyus/source_test.go`

**Interfaces:**
- Consumes: —
- Produces:
  - `type Report struct { URL string; Period string; Kind string; Lang string; Pages []int; Enabled bool }` — `Kind` ∈ {`"kpi"`,`"ifrs"`}, `Lang` ∈ {`"en"`,`"ru"`}
  - `func downloadPDF(ctx context.Context, url, destination string) error` — скачивает с проверкой `%PDF-` заголовка
  - `func extractPage(ctx context.Context, pdfPath, textPath string, page int) error` — вызывает `pdftotext`
  - `func joinFiles(paths []string, dst string) error` — чистая склейка текстовых файлов (то, что тестируется)
  - `func extractPDF(ctx context.Context, pdfPath, textPath string, pages []int) error` — извлекает указанные страницы и склеивает их через `joinFiles`

- [ ] **Step 1: Написать failing-тест склейки страниц**

`polyus/source_test.go`. Тестируется чистая функция склейки — `pdftotext` в тестах не вызывается (в CI утилиты может не быть):

```go
func TestJoinFiles(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.txt")
	b := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(a, []byte("Gold sales\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("Total revenue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "joined.txt")
	if err := joinFiles([]string{a, b}, out); err != nil {
		t.Fatalf("joinFiles: %v", err)
	}
	text, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "Gold sales\nTotal revenue\n" {
		t.Errorf("joined text = %q", text)
	}
}

func TestJoinFilesMissingFileErrors(t *testing.T) {
	dir := t.TempDir()
	if err := joinFiles([]string{filepath.Join(dir, "absent.txt")}, filepath.Join(dir, "out.txt")); err == nil {
		t.Error("expected error for missing input file")
	}
}
```

- [ ] **Step 2: Запустить тест — убедиться, что падает**

Run: `go test ./polyus/ -run TestJoinFiles -v`
Expected: FAIL — `undefined: joinFiles` (пакет ещё не существует).

- [ ] **Step 3: Реализовать `polyus/source.go`**

Перенести из `financial/databook.go` две функции: `downloadFile` → `downloadPDF` (без изменений логики: проверка `Content-Type`, размера ≥1024 байт, заголовка `%PDF-`) и `extractPage` (та же команда `pdftotext -f N -l N -layout -nopgbrk -enc UTF-8`). Заменить `fmt.Fprintf(os.Stderr, ...)` на `log.Infof`.

Третья функция — `joinFiles`, чистая склейка (её и проверяет тест):

```go
func joinFiles(paths []string, dst string) error {
	var buf []byte
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		buf = append(buf, data...)
	}
	if err := os.WriteFile(dst, buf, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}
```

`extractPDF` — обёртка, извлекающая страницы во временные файлы и склеивающая их `joinFiles`:

```go
func extractPDF(ctx context.Context, pdfPath, textPath string, pages []int) error {
	dir := filepath.Dir(textPath)
	var extracted []string
	for _, page := range pages {
		tmp := filepath.Join(dir, fmt.Sprintf("%s.p%d", filepath.Base(textPath), page))
		if err := extractPage(ctx, pdfPath, tmp, page); err != nil {
			return err
		}
		extracted = append(extracted, tmp)
		defer func() { _ = os.Remove(tmp) }()
	}
	return joinFiles(extracted, textPath)
}
```

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run TestJoinFiles -v`
Expected: PASS (2 теста).

- [ ] **Step 5: Реализовать `polyus/polyus.go`**

```go
package polyus

type Report struct {
	URL     string
	Period  string // "FY2025", "2026H1"
	Kind    string // "kpi" | "ifrs"
	Lang    string // "en" | "ru"
	Pages   []int
	Enabled bool
}
```

- [ ] **Step 6: Тесты зелёные**

Run: `go test ./polyus/ -v && gofmt -l polyus`
Expected: PASS, `gofmt` не выводит файлов.

- [ ] **Step 7: Commit**

```bash
git add polyus/
git commit -m "add polyus package with pdf source helpers"
```

---

### Task 3: xlsx-парсер датапака (перенос + устойчивость к битым ячейкам)

**Files:**
- Create: `polyus/datapack.go`
- Create: `polyus/datapack_test.go`
- Create: `polyus/testdata/polyus_datapack_fy2025_new.xlsx` (копия)
- Modify: `main.go` (blank-import `polyus`)

**Interfaces:**
- Consumes: — (самодостаточный импортёр)
- Produces: `type datapackImport struct{}` с `Name() string` → `"databook_polyus"` и `Import(ctx context.Context, conn driver.Conn) (int64, error)`; таблица `databook_polyus` — схема из `financial/gold_polyus.go` без изменений.

- [ ] **Step 1: Скопировать фикстуру**

```bash
cp financial/data/polyus_datapack_fy2025_new.xlsx polyus/testdata/
```

- [ ] **Step 2: Написать failing-тесты**

`polyus/datapack_test.go`:

```go
func TestParseDatapackReadsAssetRows(t *testing.T) {
	rows, stats, err := parseDatapack("testdata/polyus_datapack_fy2025_new.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("no rows parsed")
	}
	// OLIMPIADA присутствует как таблица
	var found bool
	for _, r := range rows {
		if r.Table == "OLIMPIADA" {
			found = true
			break
		}
	}
	if !found {
		t.Error("OLIMPIADA table not found in parsed rows")
	}
	if stats.Rows == 0 {
		t.Error("stats.Rows is zero")
	}
}

func TestParseDatapackToleratesBadCell(t *testing.T) {
	// фикстура с ячейкой "n/a" вместо числа не должна ронять разбор
	rows, stats, err := parseDatapack("testdata/datapack_bad_cell.xlsx")
	if err != nil {
		t.Fatalf("parseDatapack must not fail on a bad cell: %v", err)
	}
	if stats.Skipped == 0 {
		t.Error("expected at least one skipped cell to be counted")
	}
	if len(rows) == 0 {
		t.Error("good rows must still be parsed")
	}
}
```

- [ ] **Step 3: Запустить тесты — убедиться, что падают**

Run: `go test ./polyus/ -run TestParseDatapack -v`
Expected: FAIL — `undefined: parseDatapack`.

- [ ] **Step 4: Реализовать `polyus/datapack.go`**

Перенести логику `polyusTableImport` из `financial/gold_polyus.go` дословно, изменив три вещи:

1. Сигнатура становится `parseDatapack(path string) (rows []datapackRow, stats parseStats, err error)` — чистая функция без `driver.Batch`, чтобы её можно было тестировать; `Import()` отдельно вызывает её и делает `PrepareBatch`/`Append`/`Send`.

```go
type datapackRow struct {
	Table string
	Name  string
	Data  string
	Date  time.Time
	Value float32
}

type parseStats struct {
	Rows    int
	Skipped int
}
```

2. Нечисловая ячейка → `stats.Skipped++` и `log.Warnf("skip non-numeric cell %q in %s/%s", cell, table, name)` вместо `return count, err` (Review Focus #5).

3. `fmt.Printf` заменяется на `log.Debugf`.

Схема таблицы и `Name()` переносятся без изменений:

```go
createTable: `CREATE TABLE IF NOT EXISTS %s (
   table LowCardinality(String),
   name LowCardinality(String),
   data LowCardinality(String),
   date Date,
   value Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (table, name, data, date)`
```

Таблицы (список из `init()` старого файла, переносится в переменную уровня пакета):

```go
var datapackTables = []string{
	"CONSOLIDATED OPERATING RESULTS", "OLIMPIADA", "BLAGODATNOYE", "TITIMUKHTA",
	"VERNINSKOYE2", "ALLUVIALS", "KURANAKH", "ZAPADNOYE", "NATALKA", "Sukhoi Log",
}
```

- [ ] **Step 5: Подготовить фикстуру с битой ячейкой**

```bash
python3 - <<'PY'
import zipfile, shutil, re
src, dst = "polyus/testdata/polyus_datapack_fy2025_new.xlsx", "polyus/testdata/datapack_bad_cell.xlsx"
shutil.copy(src, dst)
z = zipfile.ZipFile(src)
sheet = z.read("xl/worksheets/sheet1.xml").decode()
# первое числовое значение меняем на текст "n/a"
sheet = re.sub(r'(<c r="F13"[^>]*>)<v>[^<]*</v>', r'\1<v>n/a</v>', sheet, count=1)
out = zipfile.ZipFile(dst, "w")
for item in z.infolist():
    data = sheet.encode() if item.filename == "xl/worksheets/sheet1.xml" else z.read(item.filename)
    out.writestr(item, data)
out.close()
PY
```

- [ ] **Step 6: Тесты зелёные**

Run: `go test ./polyus/ -v`
Expected: PASS.

- [ ] **Step 7: Подключить пакет в `main.go`**

Добавить `_ "github.com/kmlebedev/clickhouse-import-rosstat/polyus"` в блок blank-импортов (модуль — проверить в `go.mod`).

- [ ] **Step 8: Живой прогон**

Run: `make import STAT=databook_polyus`
Expected: лог `Imported N rows of databook_polyus`, N > 0; затем `make ch-sql FILE=...` с `SELECT count() FROM databook_polyus` — ненулевой.

- [ ] **Step 9: Commit**

```bash
git add polyus/ main.go
git commit -m "add polyus datapack parser"
```

---

### Task 4: Разбор периода на трёх формах

**Files:**
- Create: `polyus/periods.go`
- Create: `polyus/periods_test.go`

**Interfaces:**
- Consumes: —
- Produces: `func parsePeriod(input string) (PeriodInfo, error)`; `type PeriodInfo struct { Period string; Type string }` — `Period` ∈ {`2026H1`,`2025H2`,`2022Q4`,`2025FY`}, `Type` ∈ {`H`,`Q`,`FY`}

- [ ] **Step 1: Написать failing-тест таблицей**

`polyus/periods_test.go`:

```go
func TestParsePeriodAllForms(t *testing.T) {
	cases := []struct {
		in     string
		period string
		typ    string
	}{
		{"1H 2026", "2026H1", "H"},   // англ. пресс-релиз
		{"1 п/г 2026", "2026H1", "H"}, // рус. пресс-релиз
		{"2 п/г 2025", "2025H2", "H"},
		{"2H2025", "2025H2", "H"},
		{"2025", "2025FY", "FY"},
		{"4Q 2022", "2022Q4", "Q"},
		{"4Q2022", "2022Q4", "Q"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parsePeriod(c.in)
			if err != nil {
				t.Fatalf("parsePeriod(%q): %v", c.in, err)
			}
			if got.Period != c.period || got.Type != c.typ {
				t.Errorf("parsePeriod(%q) = %+v, want {%s %s}", c.in, got, c.period, c.typ)
			}
		})
	}
}

func TestParsePeriodUnknownFormErrors(t *testing.T) {
	if _, err := parsePeriod("какой-то текст"); err == nil {
		t.Error("expected error for unknown period form")
	}
}
```

- [ ] **Step 2: Запустить тест — убедиться, что падает**

Run: `go test ./polyus/ -run TestParsePeriod -v`
Expected: FAIL — `undefined: parsePeriod`.

- [ ] **Step 3: Реализовать `polyus/periods.go`**

Перенести `parsePeriod` из `financial/gold_polyus_finance.go` и добавить русские формы. Регулярки:

```go
var (
	reFY     = regexp.MustCompile(`^(\d{4})$`)
	reH      = regexp.MustCompile(`^([12])H(\d{4})$`)          // 2H2025
	reQ      = regexp.MustCompile(`^([1-4])Q(\d{4})$`)         // 4Q2022
	rePeriod = regexp.MustCompile(`([1-4])\s*[HQ]\s+(\d{4})`)  // 1H 2026
	reRuHalf = regexp.MustCompile(`([12])\s*п/г\s+(\d{4})`)    // 1 п/г 2026
)
```

Порядок проверок: `reFY` → `reH` → `reQ` → `reRuHalf` → `rePeriod`. Русская проверка **до** общей `rePeriod`, иначе `п/г` не совпадёт. Для русских форм `Type` всегда `"H"`. Неизвестный формат — `fmt.Errorf("unknown period format: %s", input)`.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./polyus/ -run TestParsePeriod -v`
Expected: PASS (8 подтестов).

- [ ] **Step 5: Commit**

```bash
git add polyus/periods.go polyus/periods_test.go
git commit -m "add bilingual period parsing to polyus package"
```

---

### Task 5: KPI-парсер пресс-релизов (перенос + русский словарь)

**Files:**
- Create: `polyus/kpi.go`
- Create: `polyus/metrics.go`
- Create: `polyus/kpi_test.go`
- Create: `polyus/testdata/press_reliz_1h26_p1.txt`
- Create: `polyus/testdata/press_release_hist_p1.txt`

**Interfaces:**
- Consumes: `parsePeriod` (Task 4), `Report` (Task 2)
- Produces: `type MetricDefinition struct { Name, Unit string; Prefix []string }`; `type MetricRecord struct { Company, Metric, Period, PeriodType string; Value float64; Unit, SourceURL string; SourcePage int }`; `func parseKPIPage(textPath, sourceURL string, page int) ([]MetricRecord, error)`; `var metrics []MetricDefinition`

- [ ] **Step 1: Подготовить фикстуры**

```bash
mkdir -p polyus/testdata
pdftotext -f 1 -l 1 -layout -nopgbrk -enc UTF-8 /tmp/polyus_1h26_ru.pdf polyus/testdata/press_reliz_1h26_p1.txt
```

Вторую фикстуру (английский исторический релиз) взять из существующего списка URL в спеке; если файл недоступен — использовать любой английский релиз из `financial/gold_polyus_finance.go` и зафиксировать в отчёте, какой именно выбран.

- [ ] **Step 2: Написать failing-тесты**

`polyus/kpi_test.go`:

```go
func TestParseKPIReportRU(t *testing.T) {
	records, err := parseKPIPage("testdata/press_reliz_1h26_p1.txt", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no records parsed from russian report — english prefixes likely still in use")
	}
	byMetric := map[string]float64{}
	for _, r := range records {
		if r.Period == "2026H1" {
			byMetric[r.Metric] = r.Value
		}
	}
	if got, ok := byMetric["gold_output"]; !ok || got != 1287 {
		t.Errorf("gold_output for 2026H1 = %v (present=%v), want 1287", got, ok)
	}
	if got, ok := byMetric["tcc_per_ounce"]; !ok || got != 1069 {
		t.Errorf("tcc_per_ounce for 2026H1 = %v (present=%v), want 1069", got, ok)
	}
}
```

Значения `1287` (производство золота, тыс. унций) и `1069` (TCC на проданную унцию, $) проверены на живом файле `press_reliz-1h26-_tu_mda_2.pdf`, страница 1.

- [ ] **Step 3: Запустить тест — убедиться, что падает**

Run: `go test ./polyus/ -run TestParseKPIReportRU -v`
Expected: FAIL — `undefined: parseKPIPage`.

- [ ] **Step 4: Перенести словарь и парсер**

`polyus/metrics.go` — перенос `MetricDefinition`, `MetricRecord`, `metrics` из `financial/gold_polyus_finance.go` **без изменений**, плюс русские префиксы в существующие записи:

```go
{
	Name: "gold_output", Unit: "koz",
	Prefix: []string{"Gold output, koz", "Gold production (koz)", "Производство золота (тыс. унций)"},
},
{
	Name: "tcc_per_ounce", Unit: "USD/oz",
	Prefix: []string{
		"Total cash cost (TCC) per ounce sold, $/oz",
		"Total cash cost (TCC) per ounce\nsold ($/oz)",
		"Общие денежные затраты (TCC) на проданную унцию ($)",
	},
},
```

Существующие английские префиксы `tcc_per_ounce` скопированы из `financial/gold_polyus_finance.go` дословно (включая разрыв строки во втором варианте) — не переформулировать.

**Ловушка со сносками.** В живом русском релизе строка выглядит так (проверено на `press_reliz-1h26-_tu_mda_2.pdf`, стр. 1):

```
    Общие денежные затраты (TCC) на проданную унцию ($)4                                1 069 ...
    Производство золота (тыс. унций)2                                                   1 287 ...
```

Сноска-цифра (`4`, `2`) стоит **вплотную** к последней скобке. `matchMetric` ищет `strings.Index(line, prefix) == 0`, то есть требует, чтобы префикс начинался с нулевой позиции строки, — но строка начинается с пробелов. При переносе нормализация (`normalizeLine`) должна обрезать ведущие пробелы **до** сопоставления, иначе ни одна метрика не опознается и тест `TestParseKPIReportRU` даст 0 записей.

Причина в старом коде: английские релизы, для которых он писался, содержат метку и значения в одной строке без ведущих отступов, а русские — с отступом. Если после нормализации сопоставление всё ещё не срабатывает — сверить фактическую строку `Grep`-ом по фикстуре и править нормализацию, а не подгонять префиксы.

Остальные метрики: русские префиксы добавляются к `revenue`, `operating_profit`, `profit_for_period`, `eps_basic`, `eps_diluted`, `adjusted_ebitda`, `capex`, `adjusted_net_profit` по тому же принципу. Полный русский текст строк брать из фикстуры `testdata/press_reliz_1h26_p1.txt` (Step 1), а не додумывать.

`polyus/kpi.go` — перенос `parseKPIPage`, `matchMetric`, `metricsSortedByPrefixLength`, `normalizeLine`, `extractValueTokens`, `parseNumericToken` из `financial/gold_polyus_finance.go`. Логика не меняется; `parsePeriod` вызывается из `polyus/periods.go` (Task 4). Удалить локальный `exitf` — вместо него `return nil, fmt.Errorf(...)`.

**Важно:** `matchMetric` сейчас завершает работу при `spaceIndex == -1` и может ронять весь разбор — при переносе заменить на `continue` (пропуск строки) и вести счётчик пропусков.

- [ ] **Step 5: Тесты зелёные**

Run: `go test ./polyus/ -run 'TestParseKPI|TestParsePeriod' -v`
Expected: PASS.

- [ ] **Step 6: Тест на английском релизе (регрессия переноса)**

`polyus/kpi_test.go`:

```go
func TestParseKPIReportEN(t *testing.T) {
	records, err := parseKPIPage("testdata/press_release_hist_p1.txt", "https://example.invalid/hist.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no records parsed from english report — transfer regressed")
	}
}
```

Run: `go test ./polyus/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add polyus/
git commit -m "add polyus KPI report parser with bilingual metric dictionary"
```

---

### Task 6: МСФО-парсер

**Files:**
- Create: `polyus/ifrs.go`
- Create: `polyus/ifrs_metrics.go`
- Create: `polyus/ifrs_test.go`
- Create: `polyus/testdata/en_msfo_p6.txt`
- Create: `polyus/testdata/en_msfo_p7.txt`

**Interfaces:**
- Consumes: `MetricRecord` (Task 5)
- Produces: `func parseIFRSPage(textPath, sourceURL string, page int, period string) ([]MetricRecord, error)`; `var ifrsMetrics []MetricDefinition`

- [ ] **Step 1: Подготовить фикстуры**

```bash
pdftotext -f 6 -l 6 -layout -nopgbrk -enc UTF-8 /tmp/en_msfo.pdf polyus/testdata/en_msfo_p6.txt
pdftotext -f 7 -l 7 -layout -nopgbrk -enc UTF-8 /tmp/en_msfo.pdf polyus/testdata/en_msfo_p7.txt
```

- [ ] **Step 2: Написать failing-тесты**

`polyus/ifrs_test.go`:

```go
func TestParseIFRSReport(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.txt", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
	}
	// значения из otchetnost-6m2026_eng.pdf, страница 6 (в млн долларов США)
	if got["total_revenue"] != 4674 {
		t.Errorf("total_revenue = %v, want 4674", got["total_revenue"])
	}
	if got["profit_for_period"] != 829 {
		t.Errorf("profit_for_period = %v, want 829", got["profit_for_period"])
	}
	if got["eps_basic"] != 0.87 {
		t.Errorf("eps_basic = %v, want 0.87", got["eps_basic"])
	}
}

func TestParseIFRSOnKPIPage(t *testing.T) {
	// перекрёстный случай: KPI-страница, поданная МСФО-парсеру
	records, err := parseIFRSPage("testdata/press_reliz_1h26_p1.txt", "https://example.invalid/x.pdf", 1, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage must not fail on a KPI page: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected no IFRS metrics on a KPI page, got %d", len(records))
	}
}
```

- [ ] **Step 3: Запустить тесты — убедиться, что падают**

Run: `go test ./polyus/ -run TestParseIFRS -v`
Expected: FAIL — `undefined: parseIFRSPage`.

- [ ] **Step 4: Реализовать `polyus/ifrs_metrics.go`**

Словарь статей МСФО (английский, по страницам 6–7 живого файла):

```go
var ifrsMetrics = []MetricDefinition{
	{Name: "gold_sales", Unit: "USD million", Prefix: []string{"Gold sales"}},
	{Name: "other_sales", Unit: "USD million", Prefix: []string{"Other sales"}},
	{Name: "total_revenue", Unit: "USD million", Prefix: []string{"Total revenue"}},
	{Name: "operating_expenses", Unit: "USD million", Prefix: []string{"Operating expenses and other income"}},
	{Name: "profit_before_tax", Unit: "USD million", Prefix: []string{"Profit before income tax"}},
	{Name: "income_tax_expense", Unit: "USD million", Prefix: []string{"Income tax expense"}},
	{Name: "profit_for_period", Unit: "USD million", Prefix: []string{"Profit for the period"}},
	{Name: "eps_basic", Unit: "USD/share", Prefix: []string{"-    basic"}},
	{Name: "eps_diluted", Unit: "USD/share", Prefix: []string{"-    diluted"}},
	{Name: "total_assets", Unit: "USD million", Prefix: []string{"Total assets"}},
}
```

- [ ] **Step 5: Реализовать `polyus/ifrs.go`**

Схема МСФО-страницы отличается от KPI: в строке сначала числовые столбцы, потом текст статьи («значения, затем метка»), а не «метка, затем значения». Поэтому `parseIFRSPage` — отдельная функция:

- строка разбивается на токены; статья опознаётся по вхождению префикса из `ifrsMetrics` в **конец** строки;
- значение — последний числовой токен **перед** меткой (первый столбец = отчётный период);
- если ни одна статья не опознана — возвращается пустой слайс **без ошибки** (Review Focus #2);
- скобки означают отрицательное значение (как в `operating_expenses`: `(3,405)` → `-3405`).

Метрики, которых нет на странице, не выдумываются: отсутствие ключа в результате — нормально.

- [ ] **Step 6: Тесты зелёные**

Run: `go test ./polyus/ -v`
Expected: PASS, включая `TestParseIFRSOnKPIPage` (0 записей).

- [ ] **Step 7: Commit**

```bash
git add polyus/
git commit -m "add polyus IFRS statement parser"
```

---

### Task 7: Таблица отчётов, импортёр и живой прогон

**Files:**
- Create: `polyus/pages.go`
- Create: `polyus/import.go`
- Modify: `sql/mcp_kimi_reader.sql` (проверить, нужен ли грант)

**Interfaces:**
- Consumes: `Report` (Task 2), `parseKPIPage` (Task 5), `parseIFRSPage` (Task 6), `downloadPDF`/`extractPDF` (Task 2)
- Produces: `var reports []Report`; `type financialMetrics struct{}` с `Name()` → `"polyus_financial_metrics"`

- [ ] **Step 1: Написать failing-тест таблицы отчётов**

`polyus/pages_test.go`:

```go
func TestEnabledReports(t *testing.T) {
	var enabled int
	for _, r := range reports {
		if r.Enabled {
			enabled++
			if r.Kind != "kpi" && r.Kind != "ifrs" {
				t.Errorf("report %s has invalid Kind %q", r.URL, r.Kind)
			}
			if r.Lang != "en" && r.Lang != "ru" {
				t.Errorf("report %s has invalid Lang %q", r.URL, r.Lang)
			}
			if len(r.Pages) == 0 {
				t.Errorf("report %s is enabled but has no pages", r.URL)
			}
		}
	}
	if enabled == 0 {
		t.Fatal("no enabled reports")
	}
}

func TestDisabledReportsHavePeriods(t *testing.T) {
	for _, r := range reports {
		if r.Period == "" {
			t.Errorf("report %s has empty Period", r.URL)
		}
	}
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `go test ./polyus/ -run TestEnabledReports -v`
Expected: FAIL — `undefined: reports`.

- [ ] **Step 3: Реализовать `polyus/pages.go`**

Перенести все URL из `init()` `financial/gold_polyus_finance.go` (включая закомментированные — они становятся записями с `Enabled: false` и причиной в комментарии). Добавить два новых отчёта:

```go
{
	URL:     "https://polyus.com/upload/iblock/80e/press_reliz-1h26-_tu_mda_2.pdf",
	Period:  "2026H1",
	Kind:    "kpi",
	Lang:    "ru",
	Pages:   []int{1}, // выверить прогоном pdftotext перед включением
	Enabled: true,
},
{
	URL:     "https://polyus.com/upload/iblock/2c4/otchetnost-6m2026_eng.pdf",
	Period:  "2026H1",
	Kind:    "ifrs",
	Lang:    "en",
	Pages:   []int{6, 7}, // P&L и баланс; выверить прогоном
	Enabled: true,
},
```

- [ ] **Step 4: Выверить номера страниц на живых файлах**

```bash
cd /tmp && pdftotext -f 1 -l 1 -layout -nopgbrk -enc UTF-8 polyus_1h26_ru.pdf - | head -40
pdftotext -f 6 -l 6 -layout -nopgbrk -enc UTF-8 en_msfo.pdf - | head -20
```

Expected: страница 1 рус. релиза содержит «Производство золота»; страница 6 EN МСФО содержит `Total revenue`. Если номера иные — исправить `Pages` в `pages.go`.

- [ ] **Step 5: Реализовать `polyus/import.go`**

Импортёр `polyus_financial_metrics`:

- DDL переносится из `financial/gold_polyus_finance.go` без изменений;
- цикл по `reports`: `Enabled: false` → `log.Infof("skip report %s (%s): disabled", r.URL, r.Period)` и `continue` (Review Focus #4);
- для каждого включённого отчёта: `os.MkdirTemp` → `downloadPDF` → `extractPDF(ctx, pdf, txt, r.Pages)` → разбор (`parseKPIPage` при `Kind == "kpi"`, `parseIFRSPage` при `Kind == "ifrs"`) → `batch.Append`;
- ошибка скачивания одного отчёта **не** роняет импорт целиком: `log.Warnf` и продолжение, счётчик ошибок в итоговом логе;
- `Company` в записях — `"PLZL"`.

- [ ] **Step 6: Тесты зелёные**

Run: `go test ./polyus/ -v && gofmt -l polyus`
Expected: PASS.

- [ ] **Step 7: `make all` зелёный**

Run: `make all`
Expected: lint (0 issues), vet, `go test -race`, build — всё PASS.

- [ ] **Step 8: Живой прогон**

Run: `make import STAT=polyus_financial_metrics`
Expected: лог с числом импортированных записей > 0.

Проверка данных (через `make ch-sql`):

```sql
SELECT period, metric, value FROM polyus_financial_metrics FINAL
WHERE period LIKE '%2026%' ORDER BY metric LIMIT 20
```

Expected: присутствуют и операционные метрики (`gold_output` = 1287, `tcc_per_ounce` = 1069), и финансовые (`total_revenue` = 4674, `profit_for_period` = 829) за период `2026H1`.

- [ ] **Step 9: Commit**

```bash
git add polyus/
git commit -m "add polyus report importer with 1H2026 data"
```

---

### Task 8: Документация и статус роадмапа

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `README.md`
- Modify: `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: результаты Task 1–7
- Produces: документация, отражающая пакет `polyus/`

- [ ] **Step 1: Синхронизировать документы**

Вызвать `Skill(sync-readme-architecture)` и следовать чек-листу: структура репозитория (новый пакет `polyus/`), таблица импортёров §6.1 (`databook_polyus` и `polyus_financial_metrics` теперь в `polyus/`, а не в `financial/`), состав таблиц, команда запуска.

- [ ] **Step 2: Отметить пункт в роадмапе**

В `docs/ROADMAP_DCF_POLYUS.md` пометить пункт о парсерах Полюса как выполненный со ссылкой на спеку `docs/superpowers/specs/2026-10-10-polyus-parsers-design.md` и результатом живой приёмки (число строк за `2026H1`).

- [ ] **Step 3: Commit**

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "sync docs with polyus package"
```
