# edisclosure-documents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать документы эмитентов PLZL/SBER/CHMF с e-disclosure.ru в `manifest.json` + `documents.jsonl`, импортировать их в ClickHouse-таблицу `edisclosure_documents` и сохранить каждую версию файла в винтажном архиве.

**Architecture:** Python-коллектор (`scripts/edisclosure_collect.py`) работает через сохранённый Chrome-профиль Playwright (капчу решает владелец вручную) и пишет снапшот; Go-импортёр пакета `edisclosure/` читает снапшот, валидирует его до сети, скачивает документы через `util.GetFileBytes`, гейтит повторный прогон по метаданным карточки и `sha256`, пишет строки одним батчем и версионирует файлы через `archive.Registry`. Витрина `v_edisclosure_documents` — единственная точка чтения для MCP-агента.

**Tech Stack:** Go 1.x (существующие зависимости: `clickhouse-go/v2`, `logrus`), Python 3.12 + Playwright + BeautifulSoup + lxml + requests (через `uv run --with …`), ClickHouse, `archive/`, `util/`.

**Spec:** [../specs/2026-10-18-edisclosure-documents-design.md](../specs/2026-10-18-edisclosure-documents-design.md)

## Global Constraints

- Новых Go-зависимостей нет: `go.mod` / `go.sum` не меняются.
- HTTP в Go — только через `util.GetFileBytes` (национальные TLS-сертификаты, ротация User-Agent, дедлайн `fileTimeout`).
- Секреты — только `os.Getenv`; cookies, профиль браузера и токены никогда не печатаются и не коммитятся. Профиль коллектора — `build/edisclosure-profile/` (gitignored, каталог 0700).
- Вставка в ClickHouse — только `PrepareBatch` → `Append` → `Send`; каждый `Append` с проверкой `err`; на пути ошибки — `Abort`.
- DDL — `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, измерения `LowCardinality(String)`.
- Сетевых вызовов в `init()` нет; окружение читается в момент действия.
- Новый источник виден агенту только через витрину: `GRANT SELECT` в `sql/mcp_kimi_reader.sql`, сырая таблица закрыта, комментарии таблицы и колонок — через `util.View`.
- Коллектор не обходит капчу: только ждёт ручного решения владельцем в headed Chrome.
- Документация и статус роадмапа — в том же PR; `make all` — до объявления работы законченной.
- Комментарии в коде — по-русски, в стиле репозитория; `fmt.Print*` и `colly` в Go не использовать.

## Review Focus

1. **Повторный прогон не должен ходить в сеть.** Гейт по метаданным карточки и `sha256` обязан вернуть `Imported 0` без единого `GetFileBytes`; регрессия здесь — самая дорогая (290 скачиваний на каждом прогоне).
2. **HTML-заглушка с HTTP 200 не должна попасть в архив и таблицу.** `util.GetFile` статус не проверяет; тело, начинающееся с `<`, обязано быть ошибкой.
3. **Частичный снапшот коллектора не должен импортироваться.** `document_count` ≠ числу строк или отсутствующий/битый `manifest.json` — ошибка до сети и БД, а не импорт подмножества.
4. **Строка с непустым `archive_key` обязана иметь объект и строку реестра.** Сбой `Put`/`Ensure` отменяет батч; обратное — строка объявляет версию, которой в архиве нет.
5. **URL скачивания — только `e-disclosure.ru/portal/FileLoad.ashx` с совпадающим `Fileid`.** Иначе импортёр скачает чужой артефакт или сходит на внутренний адрес.

---

### Task 1: Коллектор документов и Makefile

**Files:**
- Create: `scripts/edisclosure_collect.py`
- Create: `scripts/edisclosure_collect_test.py`
- Create: `scripts/testdata/edisclosure/PLZL_IFRS.html` (реальная страница из пилота)
- Modify: `Makefile` (цели `edisclosure-login`, `edisclosure-collect`)

**Interfaces:**
- Consumes: реальный Chrome-профиль `build/edisclosure-profile/`, сохранённый режимом `login`.
- Produces:
  - `COMPANIES: dict[str, str] = {"PLZL": "7832", "SBER": "3043", "CHMF": "30"}`
  - `SECTIONS: dict[str, int] = {"IFRS": 4, "RAS": 3, "AFFILIATES": 6, "AGM": 16}`
  - `parse_documents_table(html: str, section: str, ticker: str) -> list[dict[str, str]]`
  - `validate_document(row: dict[str, str], companies: dict[str, str]) -> None`
  - `document_key(row: dict[str, str]) -> tuple[str, str, str]`
  - `dedupe_documents(rows: list[dict[str, str]]) -> list[dict[str, str]]`
  - `build_manifest(companies: list[dict[str, str]], sections: list[str], documents: list[dict[str, str]], generated_at: str) -> dict`
  - `write_output(out_dir: pathlib.Path, manifest: dict, documents: list[dict[str, str]]) -> None`
  - CLI: `login` и `collect` (флаги `--companies`, `--sections`, `--out`, `--wait`).

- [ ] **Step 1: Скопировать реальную страницу в фикстуру**

```bash
mkdir -p scripts/testdata/edisclosure
cp build/edisclosure-probe/raw/collect/PLZL_IFRS.html scripts/testdata/edisclosure/PLZL_IFRS.html
```

Фикстура — настоящая страница раздела IFRS (112 КБ), не синтетическая.

- [ ] **Step 2: Написать падающий тест разбора**

```python
# scripts/edisclosure_collect_test.py
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import edisclosure_collect as ec

FIXTURES = pathlib.Path(__file__).resolve().parent / "testdata" / "edisclosure"


class ParseDocumentsTableTest(unittest.TestCase):
    def test_real_page(self):
        html = (FIXTURES / "PLZL_IFRS.html").read_text(encoding="utf-8")
        rows = ec.parse_documents_table(html, "IFRS", "PLZL")
        self.assertGreater(len(rows), 0)
        row = rows[0]
        self.assertEqual(row["ticker"], "PLZL")
        self.assertEqual(row["section"], "IFRS")
        self.assertTrue(row["fileid"].isdigit())
        self.assertEqual(
            row["url"],
            f"https://e-disclosure.ru/portal/FileLoad.ashx?Fileid={row['fileid']}",
        )
```

- [ ] **Step 3: Убедиться, что тест падает**

Run: `python3 scripts/edisclosure_collect_test.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'edisclosure_collect'`.

- [ ] **Step 4: Реализовать разбор в `scripts/edisclosure_collect.py`**

Повторить разбор пилота: таблица `table.files-table`, ячейки строки, ссылка в 6-й ячейке (`cells[5]`), `Fileid` из query; `urljoin` к `https://e-disclosure.ru`. Поля строки — ровно контракт спеки §4.2 (`ticker`, `company_id`, `section`, `doc_type`, `period`, `basis_date`, `publication_date`, `size`, `fileid`, `url`, `table_num`).

- [ ] **Step 5: Убедиться, что тест проходит**

Run: `python3 scripts/edisclosure_collect_test.py -v`
Expected: PASS.

- [ ] **Step 6: Написать падающие тесты валидации, дедупликации и манифеста**

```python
class ValidationTest(unittest.TestCase):
    def test_wrong_host_rejected(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1", "doc_type": "x",
               "period": "", "basis_date": "", "publication_date": "",
               "url": "https://example.com/portal/FileLoad.ashx?Fileid=1"}
        with self.assertRaisesRegex(ValueError, "e-disclosure"):
            ec.validate_document(row, ec.COMPANIES)

    def test_fileid_mismatch_rejected(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1", "doc_type": "x",
               "period": "", "basis_date": "", "publication_date": "",
               "url": "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=2"}
        with self.assertRaisesRegex(ValueError, "Fileid"):
            ec.validate_document(row, ec.COMPANIES)

    def test_dedupe_rejects_conflict(self):
        a = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
             "doc_type": "A", "period": "2025", "basis_date": "",
             "publication_date": "", "url":
             "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        b = dict(a, doc_type="B")
        with self.assertRaisesRegex(ValueError, "1943701"):
            ec.dedupe_documents([a, b])

    def test_manifest_count(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
               "doc_type": "A", "period": "2025", "basis_date": "",
               "publication_date": "", "url":
               "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        m = ec.build_manifest(
            [{"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
              "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"}],
            list(ec.SECTIONS), [row], "2026-10-18T12:00:00+03:00")
        self.assertEqual(m["document_count"], 1)
        self.assertEqual(m["documents_file"], "documents.jsonl")
        self.assertEqual(m["schema_version"], 1)
```

- [ ] **Step 7: Реализовать валидацию, дедупликацию и сборку манифеста**

`validate_document` проверяет: ticker в `companies`; `section` из `SECTIONS`; `doc_type` непустой; `fileid` только цифры; URL — `https`, host `e-disclosure.ru`, path `/portal/FileLoad.ashx`, query `Fileid` совпадает. `dedupe_documents` схлопывает одинаковые ключи и падает на расхождении любого поля. `build_manifest` ставит `schema_version: 1`, `document_count = len(documents)`, `documents_file: "documents.jsonl"`.

- [ ] **Step 8: Реализовать CLI и атомарную запись**

- `login`: `sync_playwright().chromium.launch_persistent_context(user_data_dir="build/edisclosure-profile", channel="chrome", headless=False, args=["--disable-blink-features=AutomationControlled"])`, открыть карточку PLZL, ждать маркер контента, при капче ждать `--wait` секунд.
- `collect`: тот же профиль; для каждой компании открыть `https://e-disclosure.ru/portal/company.aspx?id=<id>`, взять `name` из `h1`/`table.company-table`; для каждого раздела открыть `https://e-disclosure.ru/portal/files.aspx?id=<id>&type=<type_id>`, ждать `files-table`, разобрать, добавить `ticker` и `company_id`; дедуплицировать; `write_output`.
- `write_output`: `documents.jsonl.tmp` → `documents.jsonl`, затем `manifest.json.tmp` → `manifest.json` (манифест последним). Каталог выхода создаётся, профиль — `build/edisclosure-profile` с режимом 0700.
- Капча: если маркер капчи на странице — писать инструкцию и ждать; по таймауту — `SystemExit` с ошибкой, манифест не перезаписывать.

- [ ] **Step 9: Добавить цели Makefile**

```make
edisclosure-login:
	uv run --with playwright --with beautifulsoup4 --with lxml --with requests --python 3.12 \
		python3 scripts/edisclosure_collect.py login

edisclosure-collect:
	uv run --with playwright --with beautifulsoup4 --with lxml --with requests --python 3.12 \
		python3 scripts/edisclosure_collect.py collect $(ARGS)
```

- [ ] **Step 10: Прогнать тесты и закоммитить**

Run: `python3 scripts/edisclosure_collect_test.py -v`
Expected: PASS (все тесты).

```bash
git add scripts/edisclosure_collect.py scripts/edisclosure_collect_test.py \
  scripts/testdata/edisclosure/PLZL_IFRS.html Makefile
git commit -m "add edisclosure collector"
```

---

### Task 2: Каркас пакета, DDL, витрина, реестр, dagu

**Files:**
- Create: `edisclosure/schema.go`
- Create: `edisclosure/documents.go` (заглушка: `documentsImporter` и `Import`, возвращающий ошибку)
- Create: `edisclosure/importers.go`
- Create: `edisclosure/views.go`
- Create: `edisclosure/importers_test.go`
- Create: `edisclosure/views_test.go`
- Create: `dagu/edisclosure.yaml`
- Modify: `main.go` (добавить `_ "github.com/kmlebedev/clickhouse-import-rosstat/edisclosure"`)

**Interfaces:**
- Consumes: `chimport.Stats`, `util.View`, `util.CreateView`, `driver.Conn`.
- Produces:
  - `const edisclosureTable = "edisclosure_documents"`
  - `const edisclosureArchiveSource = "edisclosure_documents"`
  - `const edisclosureDdl = \`CREATE TABLE IF NOT EXISTS edisclosure_documents (...)\`` — дословно из спеки §5.2
  - `type documentsImporter struct{}`; `func (documentsImporter) Name() string`; `func (documentsImporter) Import(ctx context.Context, conn driver.Conn) (int64, error)` — в Task 2 заглушка с ошибкой, тело заменяется в Task 5
  - `var edisclosureImporter = documentsImporter{}`
  - `var edisclosureImporters = []chimport.ImportStat{edisclosureImporter}`
  - `var edisclosureView = util.View{...}`
  - `func publishEdisclosureView(ctx context.Context, conn driver.Conn) error`

- [ ] **Step 1: Написать падающий тест реестра и dagu**

```go
// edisclosure/importers_test.go
package edisclosure

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

func TestImportersRegistry(t *testing.T) {
	if got := edisclosureImporter.Name(); got != "edisclosure_documents" {
		t.Fatalf("Name() = %q, want edisclosure_documents", got)
	}
	found := false
	for _, stat := range chimport.Stats {
		if stat.Name() == "edisclosure_documents" {
			found = true
		}
	}
	if !found {
		t.Fatal("edisclosure_documents не зарегистрирован в chimport.Stats")
	}
	data, err := os.ReadFile(filepath.Join("..", "dagu", "edisclosure.yaml"))
	if err != nil {
		t.Fatalf("чтение dagu/edisclosure.yaml: %v", err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) != 1 || steps[0][1] != "edisclosure_documents" {
		t.Fatalf("шаги dagu = %v, want один edisclosure_documents", steps)
	}
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./edisclosure/ -run TestImportersRegistry -v`
Expected: FAIL — пакет/символы не существуют.

- [ ] **Step 3: Создать `schema.go`, `importers.go`, `dagu/edisclosure.yaml`, добавить импорт в `main.go`**

DDL — дословно из спеки §5.2 (14 колонок, `ENGINE = ReplacingMergeTree(loaded_at) ORDER BY (ticker, section, fileid)`). `importers.go`:

```go
var edisclosureImporters = []chimport.ImportStat{edisclosureImporter}

func init() {
	for _, imp := range edisclosureImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
```

`dagu/edisclosure.yaml` — из спеки §5.7 (`type: chain`, `schedule: "17 21 5 * *"`, шаг `edisclosure_documents`, env `CLICKHOUSE_IMPORT_STAT` и `EDISCLOSURE_ALLOW_MISSING=1`).

- [ ] **Step 4: Убедиться, что тест проходит**

Run: `go test ./edisclosure/ -run TestImportersRegistry -v`
Expected: PASS.

- [ ] **Step 5: Написать падающие тесты DDL и витрины**

```go
func TestEdisclosureDdlShape(t *testing.T) {
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS edisclosure_documents",
		"ENGINE = ReplacingMergeTree(loaded_at)",
		"ORDER BY (ticker, section, fileid)",
		"archive_key String", "sha256 String", "first_seen DateTime",
	} {
		if !strings.Contains(edisclosureDdl, want) {
			t.Errorf("DDL не содержит %q", want)
		}
	}
}

func TestEdisclosureViewShape(t *testing.T) {
	if edisclosureView.Name != "v_edisclosure_documents" {
		t.Fatalf("view = %q", edisclosureView.Name)
	}
	if len(edisclosureView.Tables) != 1 || edisclosureView.Tables[0] != "edisclosure_documents" {
		t.Fatalf("Tables = %v", edisclosureView.Tables)
	}
	if !strings.Contains(edisclosureView.Select, "FROM edisclosure_documents FINAL") {
		t.Fatalf("Select = %q", edisclosureView.Select)
	}
	for _, col := range []string{"ticker", "company_id", "section", "doc_type", "period",
		"basis_date", "publication_date", "fileid", "url", "size_bytes", "sha256",
		"archive_key", "first_seen", "loaded_at"} {
		if _, ok := edisclosureView.Columns[col]; !ok {
			t.Errorf("нет комментария колонки %q", col)
		}
	}
}
```

- [ ] **Step 6: Реализовать `views.go`**

`util.View` с `Name: "v_edisclosure_documents"`, `Tables: []string{edisclosureTable}`, `Select` из спеки §5.6, `Comment` — что это за документы и как читать версию; `Columns` — 14 записей. `publishEdisclosureView` вызывает `util.CreateView`.

- [ ] **Step 7: Прогнать тесты пакета**

Run: `go test ./edisclosure/ -v`
Expected: PASS.

- [ ] **Step 8: Закоммитить**

```bash
git add edisclosure/schema.go edisclosure/importers.go edisclosure/views.go \
  edisclosure/importers_test.go edisclosure/views_test.go dagu/edisclosure.yaml main.go
git commit -m "add edisclosure importer skeleton"
```

---

### Task 3: Разбор и валидация входа

**Files:**
- Create: `edisclosure/documents_parse.go`
- Create: `edisclosure/testdata/manifest.json`
- Create: `edisclosure/testdata/documents.jsonl`
- Create: `edisclosure/documents_parse_test.go`

**Interfaces:**
- Consumes: `manifest`/`document` JSON, `time.Parse("02.01.2006", …)`.
- Produces:
  - `type manifest struct { SchemaVersion int; GeneratedAt string; Collector struct{Name,Version string}; Sections []string; Companies []company; DocumentCount int; DocumentsFile string }`
  - `type company struct { Ticker, CompanyID, Name, URL string }`
  - `type document struct { Ticker, CompanyID, Section, DocType, Period, BasisDate, PublicationDate, Size, FileID, URL, TableNum string }`
  - `type doc struct { Ticker, CompanyID, Section, DocType, Period string; BasisDate, PublicationDate time.Time; FileID, URL, TableNum string }`
  - `func readInput(dir string) (manifest, []doc, error)`
  - `func parseManifest(data []byte) (manifest, error)`
  - `func parseDocuments(data []byte, m manifest) ([]doc, error)`
  - `func parseDate(s string) (time.Time, error)`
  - `func validateDocumentURL(fileID, rawURL string) error`
  - `func docKey(d doc) string`

- [ ] **Step 1: Собрать фикстуры из реальных строк пилота**

```bash
mkdir -p edisclosure/testdata
python3 - <<'PY'
import json, pathlib
need = ["1943701", "1914520", "1940333", "1910980", "1851694", "1912839"]
src = pathlib.Path("build/edisclosure-probe/parsed/documents.jsonl")
rows = [json.loads(l) for l in src.read_text(encoding="utf-8").splitlines() if l.strip()]
by_id = {r["fileid"]: r for r in rows}
out = [by_id[i] for i in need]
pathlib.Path("edisclosure/testdata/documents.jsonl").write_text(
    "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in out), encoding="utf-8")
print("fixture rows:", len(out))
PY
```

`manifest.json` — вручную, ровно по контракту спеки §4.2:

```json
{
  "schema_version": 1,
  "generated_at": "2026-10-18T12:00:00+03:00",
  "collector": {"name": "edisclosure_collect.py", "version": "1"},
  "sections": ["IFRS", "RAS", "AFFILIATES", "AGM"],
  "companies": [
    {"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
     "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"},
    {"ticker": "SBER", "company_id": "3043", "name": "Сбербанк",
     "url": "https://e-disclosure.ru/portal/company.aspx?id=3043"},
    {"ticker": "CHMF", "company_id": "30", "name": "Северсталь",
     "url": "https://e-disclosure.ru/portal/company.aspx?id=30"}
  ],
  "document_count": 6,
  "documents_file": "documents.jsonl"
}
```

- [ ] **Step 2: Написать падающий тест успешного разбора**

```go
func TestReadInputFixture(t *testing.T) {
	m, docs, err := readInput("testdata")
	if err != nil {
		t.Fatalf("readInput: %v", err)
	}
	if m.DocumentCount != 6 || len(docs) != 6 {
		t.Fatalf("manifest=%d docs=%d, want 6/6", m.DocumentCount, len(docs))
	}
	first := docs[0]
	if first.Ticker != "PLZL" || first.Section != "IFRS" || first.FileID != "1943701" {
		t.Fatalf("first = %+v", first)
	}
	if got := first.PublicationDate.Format("2006-01-02"); got != "2026-08-31" {
		t.Fatalf("publication_date = %s", got)
	}
}
```

- [ ] **Step 3: Убедиться, что тест падает**

Run: `go test ./edisclosure/ -run TestReadInputFixture -v`
Expected: FAIL — `readInput` не определён.

- [ ] **Step 4: Реализовать разбор и валидацию**

Правила — спека §5.3 п. 1 и таблица полей §4.2. Обязательно: `schema_version == 1`; `documents_file == "documents.jsonl"`; `len(companies) > 0`; `document_count == len(docs)`; ticker в manifest; `company_id` строки (если есть) совпадает с manifest; `section` из набора; `fileid` только цифры; URL проверяется `validateDocumentURL`; даты `02.01.2006`, пустая строка — zero time; дубликат `(ticker, section, fileid)` с расхождением — ошибка, идентичный — схлопывается. Ошибки — с номером строки JSONL.

- [ ] **Step 5: Убедиться, что тест проходит**

Run: `go test ./edisclosure/ -run TestReadInputFixture -v`
Expected: PASS.

- [ ] **Step 6: Написать падающие тесты отказов**

```go
func TestParseDocumentsRejects(t *testing.T) {
	base := `{"ticker":"PLZL","section":"IFRS","doc_type":"x","period":"2025",` +
		`"basis_date":"","publication_date":"","fileid":"1943701",` +
		`"url":"https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}`
	cases := []struct{ name, row, want string }{
		{"bad ticker", strings.Replace(base, `"PLZL"`, `"GAZP"`, 1), "manifest"},
		{"bad section", strings.Replace(base, `"IFRS"`, `"OTHER"`, 1), "section"},
		{"empty doc_type", strings.Replace(base, `"x"`, `""`, 1), "doc_type"},
		{"bad fileid", strings.Replace(base, `"1943701"`, `"abc"`, 2), "fileid"},
		{"wrong host", strings.Replace(base, "e-disclosure.ru", "example.com", 1), "e-disclosure"},
		{"wrong path", strings.Replace(base, "/portal/FileLoad.ashx", "/portal/other", 1), "FileLoad"},
		{"fileid mismatch", strings.Replace(base, "Fileid=1943701", "Fileid=2", 1), "Fileid"},
		{"bad date", strings.Replace(base, `"publication_date":""`, `"publication_date":"31/08/2026"`, 1), "publication_date"},
	}
	for _, tc := range cases { /* parseDocuments с валидным manifest; err должен содержать want */ }
}
```

Плюс тесты: `document_count` mismatch, `schema_version: 2`, пустой `companies`, конфликтующий дубликат, пустые даты → `IsZero`.

- [ ] **Step 7: Добиться зелёных тестов**

Run: `go test ./edisclosure/ -v`
Expected: PASS.

- [ ] **Step 8: Закоммитить**

```bash
git add edisclosure/documents_parse.go edisclosure/documents_parse_test.go \
  edisclosure/testdata/manifest.json edisclosure/testdata/documents.jsonl
git commit -m "add edisclosure input parsing"
```

---

### Task 4: Гейт, имя архива, проверка тела

**Files:**
- Create: `edisclosure/documents_archive.go`
- Modify: `edisclosure/documents.go` (добавить типы и чистые функции; заглушку `Import` не трогать)
- Create: `edisclosure/documents_test.go`
- Create: `edisclosure/documents_archive_test.go`

**Interfaces:**
- Consumes: `doc`, `time.Time`.
- Produces:
  - `type existingDoc struct { DocType, Period string; PublicationDate time.Time; URL, SHA256, ArchiveKey string; FirstSeen time.Time }`
  - `func (d doc) metadataMatches(e existingDoc) bool`
  - `func planFetches(docs []doc, existing map[string]existingDoc, recheck, archiveEnabled bool) []doc`
  - `func needsWrite(d doc, e existingDoc, sha, archiveKey string, archiveEnabled bool) bool`
  - `func validateBody(body []byte) error`
  - `func archiveName(d doc, ext string) string`
  - `func bodyExt(body []byte) string`
  - `func sanitizePeriod(s string) string`

- [ ] **Step 1: Написать падающий тест гейта**

```go
func TestPlanFetches(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", FileID: "1943701", DocType: "x",
		Period: "2025", URL: "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
	key := docKey(d)
	cur := existingDoc{DocType: "x", Period: "2025", URL: d.URL,
		SHA256: "abc", ArchiveKey: "edisclosure_documents/abc_x.zip"}
	cases := []struct{ name string; existing map[string]existingDoc; recheck, archive bool; want int }{
		{"no row", nil, false, false, 1},
		{"same metadata and archive", map[string]existingDoc{key: cur}, false, false, 0},
		{"changed period", map[string]existingDoc{key: func() existingDoc { e := cur; e.Period = "2024"; return e }()}, false, false, 1},
		{"archive enabled but key empty", map[string]existingDoc{key: func() existingDoc { e := cur; e.ArchiveKey = ""; return e }()}, false, true, 1},
		{"archive disabled and key empty", map[string]existingDoc{key: func() existingDoc { e := cur; e.ArchiveKey = ""; return e }()}, false, false, 0},
		{"recheck", map[string]existingDoc{key: cur}, true, false, 1},
	}
	for _, tc := range cases { /* len(planFetches([]doc{d}, tc.existing, tc.recheck, tc.archive)) == tc.want */ }
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./edisclosure/ -run TestPlanFetches -v`
Expected: FAIL — `planFetches` не определён.

- [ ] **Step 3: Реализовать `metadataMatches`, `planFetches`, `needsWrite`**

`metadataMatches`: `DocType`, `Period`, `URL` равны, `PublicationDate.Equal`. `planFetches`: skip, если строка есть, `!recheck`, метаданные совпали и (`!archiveEnabled` или `ArchiveKey != ""`). `needsWrite`: true для новой строки, смены метаданных, смены `sha256`, и для `archiveEnabled && archiveKey != "" && e.ArchiveKey == ""`; иначе false.

- [ ] **Step 4: Убедиться, что тест проходит**

Run: `go test ./edisclosure/ -run TestPlanFetches -v`
Expected: PASS.

- [ ] **Step 5: Написать падающие тесты тела и имени архива**

```go
func TestValidateBody(t *testing.T) {
	if err := validateBody(nil); err == nil { t.Fatal("пустое тело принято") }
	if err := validateBody([]byte("  <!DOCTYPE html><html>")); err == nil { t.Fatal("HTML принят") }
	if err := validateBody([]byte("PK\x03\x04data")); err != nil { t.Fatalf("zip отвергнут: %v", err) }
	if err := validateBody([]byte("Rar!\x1a\x07data")); err != nil { t.Fatalf("rar отвергнут: %v", err) }
	if err := validateBody([]byte("%PDF-1.7")); err != nil { t.Fatalf("pdf отвергнут: %v", err) }
}

func TestArchiveNameAndExt(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", Period: "2026, 6 месяцев", FileID: "1943701"}
	if got := archiveName(d, ".zip"); got != "PLZL_IFRS_2026_6_месяцев_1943701.zip" {
		t.Fatalf("archiveName = %q", got)
	}
	if got := bodyExt([]byte("PK\x03\x04")); got != ".zip" { t.Fatalf("ext = %q", got) }
	if got := bodyExt([]byte("Rar!\x1a\x07")); got != ".rar" { t.Fatalf("ext = %q", got) }
	if got := bodyExt([]byte("%PDF-1.7")); got != ".pdf" { t.Fatalf("ext = %q", got) }
	if got := bodyExt([]byte("other")); got != ".bin" { t.Fatalf("ext = %q", got) }
}
```

- [ ] **Step 6: Реализовать `validateBody`, `bodyExt`, `sanitizePeriod`, `archiveName`**

`validateBody`: пустое — ошибка; первый непробельный байт `<` — ошибка «HTML вместо файла». `bodyExt` — по сигнатурам выше. `sanitizePeriod` заменяет пробелы/запятые/слэши на `_`, пустую строку — на `na`; `archiveName` — `<ticker>_<section>_<period>_<fileid><ext>`.

- [ ] **Step 7: Прогнать тесты и закоммитить**

Run: `go test ./edisclosure/ -v`
Expected: PASS.

```bash
git add edisclosure/documents.go edisclosure/documents_test.go \
  edisclosure/documents_archive.go edisclosure/documents_archive_test.go
git commit -m "add edisclosure import gate"
```

---

### Task 5: `Import()` — скачивание, батч, архив

**Files:**
- Modify: `edisclosure/documents.go` (реализовать `Import`, `loadExisting`, `openArchive`, `appendDocument`, `abortBatch`)
- Create: `edisclosure/documents_import_test.go` (фейковый `driver.Conn`, `driver.Rows`, `driver.Batch`)

**Interfaces:**
- Consumes: `readInput`, `planFetches`, `needsWrite`, `validateBody`, `archiveName`, `bodyExt`, `util.GetFileBytes`, `archive.FromEnv`, `archive.NewRegistry`.
- Produces:
  - `var getFileBytes = util.GetFileBytes` (подменяется тестом)
  - `var archiveFromEnv = archive.FromEnv` (подменяется тестом)
  - `const envInputDir = "EDISCLOSURE_INPUT_DIR"`, `envRecheck = "EDISCLOSURE_RECHECK"`, `envAllowMissing = "EDISCLOSURE_ALLOW_MISSING"`, `defaultInputDir = "build/edisclosure"`
  - `func (documentsImporter) Import(ctx context.Context, conn driver.Conn) (int64, error)`
  - `func loadExisting(ctx context.Context, conn driver.Conn) (map[string]existingDoc, error)`
  - `func openArchive() (archive.Store, *archive.Registry)`
  - `func appendDocument(batch driver.Batch, d doc, size int64, sha, key string, firstSeen time.Time) error`

- [ ] **Step 1: Написать падающий тест полного офлайн-импорта**

Фейковое соединение (в тестовом файле):

```go
type fakeExisting struct {
	ticker, section, fileid string
	existingDoc
}

type fakeConn struct {
	driver.Conn
	execs    []string
	existing []fakeExisting
	batch    *appendRecorder
}
func (c *fakeConn) Exec(_ context.Context, query string, _ ...any) error { c.execs = append(c.execs, query); return nil }
func (c *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) { return &fakeRows{rows: c.existing}, nil }
func (c *fakeConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.batch = &appendRecorder{}
	return c.batch, nil
}
```

`appendRecorder` встраивает `driver.Batch`, реализует `Append` (пишет `[]any`), `Send`, `Abort` (флаг), `Rows`. `fakeRows` встраивает `driver.Rows`, реализует `Next`/`Scan`/`Err`/`Close`; `Scan` раскладывает `fakeExisting` по 10 приёмникам: ticker, section, fileid, doc_type, period, publication_date, url, sha256, archive_key, first_seen (порядок — как в запросе `loadExisting`).

Тест:

```go
func TestImportOfflineWritesRowsAndArchive(t *testing.T) {
	t.Setenv("EDISCLOSURE_INPUT_DIR", "testdata")
	t.Setenv("ARCHIVE_BACKEND", "local")
	t.Setenv("ARCHIVE_LOCAL_DIR", t.TempDir())
	t.Setenv("EDISCLOSURE_RECHECK", "")
	old := getFileBytes
	getFileBytes = func(url string) ([]byte, error) { return []byte("PK\x03\x04" + url), nil }
	defer func() { getFileBytes = old }()

	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err != nil { t.Fatalf("Import: %v", err) }
	if n != 6 || len(conn.batch.rows) != 6 { t.Fatalf("n=%d rows=%d", n, len(conn.batch.rows)) }
	// DDL + витрина выполнены; в архиве 6 объектов и 6 строк registry.jsonl
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./edisclosure/ -run TestImportOfflineWritesRowsAndArchive -v`
Expected: FAIL — `Import` возвращает заглушку/символы не определены.

- [ ] **Step 3: Реализовать `Import` по шагам спеки §5.3**

Порядок: прочитать env → `readInput` (при `os.IsNotExist` и `EDISCLOSURE_ALLOW_MISSING=1` — warn и `0, nil`) → DDL → `publishEdisclosureView` → `loadExisting` (`SELECT ticker, section, fileid, doc_type, period, publication_date, url, sha256, archive_key, first_seen FROM edisclosure_documents FINAL`) → `openArchive` → `planFetches` → no-op при пустом плане → `PrepareBatch("INSERT INTO edisclosure_documents (ticker, company_id, section, doc_type, period, basis_date, publication_date, fileid, url, size_bytes, sha256, archive_key, first_seen)")` → цикл: `getFileBytes`, `validateBody`, `sha256`, архив (`store.Put`, затем `registry.Ensure`), `needsWrite`, `appendDocument` → `Send`. Ошибка на любом шаге — `abortBatch` и возврат ошибки. `first_seen` — из существующей строки, иначе `time.Now()`.

`openArchive`: `archiveFromEnv(util.HttpClient)`; ошибка конфигурации — `log.Errorf` и архив выключен; `store == nil` — выключен. `appendDocument` передаёт `nil` для нулевых дат.

- [ ] **Step 4: Убедиться, что тест проходит**

Run: `go test ./edisclosure/ -run TestImportOfflineWritesRowsAndArchive -v`
Expected: PASS.

- [ ] **Step 5: Написать остальные тесты импорта**

- `TestImportNoopMakesNoCalls` — `existing` заполнен 6 строками с метаданными фикстуры, `archive_key` непустой, `getFileBytes` вызывает `t.Fatal`; `Import` → `0`, `conn.batch == nil`.
- `TestImportRecheckRewritesChangedHash` — `EDISCLOSURE_RECHECK=1`, существующие строки со старым `sha256`; после импорта 6 строк с новым `sha256`.
- `TestImportArchiveDisabledWritesEmptyKey` — `ARCHIVE_BACKEND=""`; 6 строк, у всех `archive_key == ""`.
- `TestImportMissingInputHonoursAllowMissing` — пустой temp-каталог; без флага — ошибка; с `EDISCLOSURE_ALLOW_MISSING=1` — `0, nil`.
- `TestImportAbortsBatchOnDownloadError` — `getFileBytes` возвращает ошибку; `Import` → ошибка, `conn.batch.aborted == true`, `conn.batch.sent == false`.
- `TestImportAbortsBatchOnArchiveError` — подменить `archiveFromEnv` на `Store`, чей `Put` возвращает ошибку; `Import` → ошибка, батч абортнут, строк в таблице нет (строка с `archive_key` не должна появиться без объекта).
- `TestImportAbortsBatchOnRegistryError` — `Store.Put` успешен, `Registry.Ensure` падает (например, store, чей `Get` для `registry.jsonl` возвращает ошибку); `Import` → ошибка, батч абортнут.

- [ ] **Step 6: Прогнать все тесты пакета и `go vet`**

Run: `go test -race -count=1 ./edisclosure/ && go vet ./edisclosure/`
Expected: PASS.

- [ ] **Step 7: Закоммитить**

```bash
git add edisclosure/documents.go edisclosure/documents_import_test.go
git commit -m "add edisclosure import and archive"
```

---

### Task 6: Документация, MCP-грант и проверка

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`
- Modify: `ARCHITECTURE.md` (§6.2 — таблица; §6.3 — витрина; §7 — расписание; §8 п. 13 — точка контроля/архив)
- Modify: `README.md` (пакет, источник, запуск, переменные)
- Modify: `docs/CONFIGURATION.md` (переменные `EDISCLOSURE_*`)

**Interfaces:**
- Consumes: имя витрины `v_edisclosure_documents`, имя сырой таблицы `edisclosure_documents`, env-переменные из спеки §5.3/§5.7.
- Produces: документация и MCP-проверки; кода не добавляет.

- [ ] **Step 1: Добавить грант**

В `sql/mcp_kimi_reader.sql` рядом с остальными витринами:

```sql
GRANT SELECT ON default.v_edisclosure_documents TO kimi_reader;
```

- [ ] **Step 2: Добавить блок в `scripts/mcp_check.py`**

Ориентир — блок `v_cbr_dip_bulletin` (около строк 1275–1371). Проверки:

1. `SELECT count() AS n FROM v_edisclosure_documents` — витрина читается, `n > 0`.
2. `SELECT count() AS n FROM edisclosure_documents` — `ACCESS_DENIED`/`Not enough privileges` (сырая закрыта).
3. `list_tables` по `default`: `v_edisclosure_documents` присутствует, в тексте есть комментарий с `e-disclosure` и комментарии ключевых колонок (`sha256`, `archive_key`, `publication_date`).
4. `SELECT ticker, section, fileid, sha256, archive_key FROM v_edisclosure_documents WHERE ticker = 'PLZL' AND section = 'IFRS' LIMIT 1` — строка есть, `sha256` и `archive_key` непустые.

- [ ] **Step 3: Обновить `ARCHITECTURE.md`**

- §6.2 — строка таблицы `edisclosure_documents`: ключ `(ticker, section, fileid)`, `ReplacingMergeTree(loaded_at)`, назначение `archive_key`/`sha256`.
- §6.3 — витрина `v_edisclosure_documents` и её грант.
- §7 — ежемесячное расписание `dagu/edisclosure.yaml` и ручной характер коллектора.
- §8 п. 13 — отступления О1–О3 из спеки §8: свой `Import()`, нет HEAD-префлайта/`source_publications`, архив через `archive.Registry`.

- [ ] **Step 4: Обновить `README.md` и `docs/CONFIGURATION.md`**

README: пакет `edisclosure/`, источник, `make edisclosure-login` / `make edisclosure-collect`, `make import STAT=edisclosure_documents`, витрина. CONFIGURATION: `EDISCLOSURE_INPUT_DIR` (по умолчанию `build/edisclosure`), `EDISCLOSURE_RECHECK`, `EDISCLOSURE_ALLOW_MISSING`, `EDISCLOSURE_OUT` — с точными значениями из спеки.

- [ ] **Step 5: Прогнать проверки и закоммитить**

Run: `make all && dagu validate dagu/edisclosure.yaml`
Expected: `make all` зелёный; `dagu validate` — OK.

```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py ARCHITECTURE.md README.md docs/CONFIGURATION.md
git commit -m "document edisclosure source"
```

---

### Task 7: Живая приёмка, статус ✅ и PR

**Files:**
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (статус пункта и протокол)
- Modify: `docs/superpowers/plans/2026-10-18-edisclosure-documents.md` (протокол приёмки)

**Interfaces:**
- Consumes: все задачи 1–6; поднятый dev-ClickHouse; Chrome-профиль владельца.
- Produces: протокол живой приёмки, коммит статуса, PR с `Closes #182`.

**Исполняется главным агентом вместе с владельцем** (капчу решает владелец; `make ch-up` запускает владелец).

- [ ] **Step 1: Собрать снапшот коллектором**

```bash
make edisclosure-login
make edisclosure-collect
```

Владелец решает капчу в открытом окне Chrome. Ожидаемо: `manifest.json` + `documents.jsonl`, число строк = `document_count`, в логе нет cookies/секретов.

- [ ] **Step 2: Живой импорт**

```bash
ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/edisclosure-archive \
  make import STAT=edisclosure_documents
```

Записать: команду, `Imported N rows`, число строк по тикерам, крайние `publication_date`.

- [ ] **Step 3: Повторный прогон — no-op**

Повторить команду шага 2. Ожидаемо: `Imported 0 rows`, в логе no-op, ни одного скачивания; `SELECT count() FROM edisclosure_documents FINAL` не изменился.

- [ ] **Step 4: Глубокий прогон**

```bash
EDISCLOSURE_RECHECK=1 ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/edisclosure-archive \
  make import STAT=edisclosure_documents
```

Ожидаемо: `sha256` не меняются, строки не переписываются (`Imported 0 rows`).

- [ ] **Step 5: Архив и MCP**

```bash
make archive-list ARCHIVE_LIST_SOURCE=edisclosure_documents
make mcp-check
```

Ожидаемо: версии в списке, строки `registry.jsonl` с `source=edisclosure_documents`; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 6: Записать протокол и статус**

Обновить пункт в `docs/ROADMAP_DCF_POLYUS.md`: `🔶` → `✅`, добавить команды, числа строк, крайние даты, результат no-op/recheck, `make mcp-check`, `dagu validate`. Протокол — в тело коммита и в план (раздел «Протокол живой приёмки»).

```bash
git add docs/ROADMAP_DCF_POLYUS.md docs/superpowers/plans/2026-10-18-edisclosure-documents.md
git commit -m "accept edisclosure documents"
```

- [ ] **Step 7: Push и PR (только после подтверждения владельца)**

```bash
git push -u origin feat/edisclosure-documents
gh pr create --title "add edisclosure documents importer" \
  --body "Source: e-disclosure.ru documents (PLZL/SBER/CHMF). Table edisclosure_documents, view v_edisclosure_documents, vintage archive. Closes #182"
```

После открытия PR — комментарий с протоколом живой приёмки (спека §9, стандарт §16.1 п. 8). Merge решает владелец.
