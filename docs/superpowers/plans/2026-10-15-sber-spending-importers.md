# Починка пустых рядов Сбера — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Оба импортёра Сбера (`sber_consumper_spending_index`, `sber_izmenenie_trat`) наполняют свои таблицы живым прогоном, а ряды становятся описаны в `series_catalog` и читаемы MCP-агентом через витрину `v_bank_sber_spending`.

**Architecture:** Транспорт-фикс: мёртвый путь `sberindex.ru/proxy/services/...` заменяется на живую выгрузку `/api/dataset/deprecated/csv/ru/<dataset>.csv`; импортёры переводятся на шаблон `util.ClickHouseImport` со случаем `CrawFunc` (CSV), общий разбор — в отдельном файле; производные (каталог 51 ряда + витрина) публикует `AfterImport`.

**Tech Stack:** Go 1.27, `util.GetCSV`/`util.ClickHouseImport`/`util.UpsertSeriesCatalog`/`util.CreateView`, ClickHouse `ReplacingMergeTree`, logrus, mcp-clickhouse.

**Spec:** `docs/superpowers/specs/2026-10-15-sber-spending-importers-design.md` (обязательна к прочтению; ссылки «спека §N» ниже — на неё, «стандарт §N» — на `docs/IMPORTER_PACKAGE_STANDARD.md`).

## Global Constraints

- Секреты только из окружения, в код/логи не попадают (правило 1 AGENTS.md); HTTP — только через `util` (правило 4); вставка — один `PrepareBatch` (правило 2); ошибки не проглатывать (правило 6); логирование — logrus, `fmt.Print*` в пакете запрещён (стандарт §5.6).
- Имена `Name()`/таблиц не меняются: `sber_consumper_spending_index`, `sber_izmenenie_trat` (шаги `dagu/bank.yaml`, `CLICKHOUSE_IMPORT_STAT` — те же).
- Схема таблиц — legacy `Float32`, не меняется (ARCHITECTURE §6.2).
- Точка контроля публикации **не** объявляется (HEAD источника — 403; у недельного темпа файла = темп шага): `PublicationCountPoints` пуст, `source_publications` не пополняется.
- Разбор — чистая функция без сети (стандарт §6.1); пустой/неразобранный результат — ошибка с контекстом, не «0 строк» (§6.4–6.5).
- Незнакомая категория недельного файла — warn и импорт, не ошибка (§6.6).
- Каталог: `Source` — `sber_consumper_spending_index` и `sber_izmenenie_trat` (разные ключи); витрина — long-форма с комментариями (стандарт §10.2–10.3).
- Тесты пакета — офлайн, на урезанных живых файлах в `bank/testdata/` (§11.1–11.3).
- Перед завершением: `make all` зелёный; живая приёмка и `make mcp-check` — по протоколу задачи 6.

## Review Focus

1. **Источник отдаёт не CSV** (HTML-заглушка SPA, страница ошибки, пустое тело, HTML WAF) — прогон обязан упасть с понятным текстом и **не записать ни строки**; молчаливый «0 строк» — дефект, ради которого заведён пункт.
2. **Новая или пропавшая категория** в недельном файле — прогон не падает, точки пишутся, в журнале warn; каталог при этом отстаёт, и это осознанно (стандарт §6.6).
3. **Категория с пробелами** («Все категории », пустая, лишние пробелы) — нормализация `TrimSpace`, пустое имя — ошибка.
4. **Край файла**: последняя строка без перевода строки; дата — первое число месяца (месячный) и воскресенье ISO-недели (недельный); идемпотентность повтора (`count() FINAL` не растёт).
5. **Значения**: индекс (100 = декабрь 2018) против % г/г — не перепутаны местами между наборами; в витрине `source` различает наборы.

---

### Task 1: Общий разбор CSV и фикстуры

**Files:**
- Create: `bank/sber_csi_parse.go`
- Create: `bank/testdata/sber_consumper_spending_index.csv` (урезанный живой файл)
- Create: `bank/testdata/sber_izmenenie-trat-po-kategoriyam.csv` (урезанный живой файл)
- Create: `bank/sber_csi_test.go`

**Interfaces:**
- Produces: `type sberPoint struct{ Series string; Date time.Time; Value float64 }`; `func parseSberRows(records [][]string) ([]sberPoint, error)`; `func sberUndescribed(points []sberPoint, meta []util.SeriesMeta) []string`; константы `sberCSVTimeLayout = "2006-01-02"`.
- Consumes: `util.SeriesMeta` (существующий тип).

- [ ] **Step 1: Снять фикстуры с живых файлов источника**

```bash
curl -sS -o /tmp/sber_mon.csv 'https://sberindex.ru/api/dataset/deprecated/csv/ru/consumper-spending-index-sa.csv'
curl -sS -o /tmp/sber_week.csv 'https://sberindex.ru/api/dataset/deprecated/csv/ru/ver-izmenenie-trat-po-kategoriyam.csv'
mkdir -p bank/testdata
{ head -1 /tmp/sber_mon.csv; grep -E '^"(2018-12-01|2026-09-01)";' /tmp/sber_mon.csv; } > bank/testdata/sber_consumper_spending_index.csv
{ head -1 /tmp/sber_week.csv; grep -E '^"2026-(09-27|10-04|10-11)";' /tmp/sber_week.csv; } > bank/testdata/sber_izmenenie-trat-po-kategoriyam.csv
wc -l bank/testdata/*.csv                 # ожидается 11 и 139
tail -n +2 bank/testdata/sber_izmenenie-trat-po-kategoriyam.csv | cut -d';' -f2 | sort -u | wc -l   # 46
head -c 3 bank/testdata/sber_consumper_spending_index.csv | od -An -tx1   # ef bb bf — BOM сохранён
```

- [ ] **Step 2: Написать падающие тесты (`bank/sber_csi_test.go`)**

Хелпер загрузки фикстуры (офлайн, разделитель `;`, как в `util.GetCSV`):

```go
func loadFixture(t *testing.T, name string) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil { t.Fatalf("открыть фикстуру: %v", err) }
	defer func() { _ = f.Close() }()
	r := csv.NewReader(f)
	r.Comma = ';'
	records, err := r.ReadAll()
	if err != nil { t.Fatalf("разобрать фикстуру: %v", err) }
	return records
}
```

Тесты и точные утверждения:

- `TestSberCSIParse` (месячная фикстура): точек ровно 10; `Всего` 2018-12-01 = 100; `Всего` 2026-09-01 = 128.99; `Услуги` 2026-09-01 = 125.23; `Общественное питание` 2026-09-01 = 194.21.
- `TestSberCSIWeekParse` (недельная): точек ровно 138; `Все категории` 2026-10-11 = 7.46266724; `Продовольственные товары` 2026-09-27 = 6.11935367; `Ювелирные изделия и часы` 2026-10-11 = 6.48375314; пробельных имён нет (нормализация «Все категории » → «Все категории»).
- `TestParseSberRowsFailFast` (таблица случаев, каждый обязан дать ошибку): пустой `[][]string`; только шапка без данных; шапка от HTML-заглушки (`[][]string{{"<!doctype html><html lang=\"ru\">..."}}`); битая дата в строке; нечисловое значение; строка из двух полей.
- `TestSberUndescribed`: точка с именем, которого нет в мете, возвращается; описанная — нет.

- [ ] **Step 3: Убедиться, что тесты падают**

Run: `go test -race -count=1 ./bank/`
Expected: FAIL — `undefined: parseSberRows` (или `undefined: sberPoint`).

- [ ] **Step 4: Реализовать разбор (`bank/sber_csi_parse.go`)**

```go
// sberPoint — одна точка выгрузки раздела «Потребители» СберИндекса.
type sberPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

const sberCSVTimeLayout = "2006-01-02"

var sberCSVHeader = []string{"date", "category", "value"}

// parseSberRows — чистый разбор выгрузки (сеть — в CrawFunc импортёра): шапка сверяется
// целиком (BOM снимается), счёт — с контекстом строки, пустой результат — ошибка.
func parseSberRows(records [][]string) ([]sberPoint, error)

// sberUndescribed — имена точек, отсутствующие в каталоге: вызывающий журналирует warn и
// импортирует точку (стандарт §6.6 — новый товар источника не валит цепочку).
func sberUndescribed(points []sberPoint, meta []util.SeriesMeta) []string
```

Требования к телу: `len(records) == 0` → ошибка «пустая выгрузка»; сверка шапки после `strings.TrimPrefix(records[0][0], "\ufeff")` со `sberCSVHeader`, при расхождении — ошибка с полученным и ожидаемым значением и подсказкой «вероятно, источник отдал HTML-заглушку»; на каждую строку — проверка `len(row) == 3`, `TrimSpace` категории (пустая — ошибка), разбор даты и `strconv.ParseFloat(row[2], 64)`, ошибка включает номер строки и её содержимое; итоговый пустой список — ошибка.

- [ ] **Step 5: Прогнать тесты задачи**

Run: `go test -race -count=1 ./bank/ -run 'Sber|ParseSber' -v`
Expected: PASS (все четыре теста).

- [ ] **Step 6: gofmt + коммит**

```bash
gofmt -l bank/ && git add bank/sber_csi_parse.go bank/sber_csi_test.go bank/testdata/ && git commit -m "bank: add sber csv parser with fixtures and tests"
```

---

### Task 2: Месячный импортёр на шаблоне (`bank/sber_csi.go`)

**Files:**
- Modify (переписывается): `bank/sber_csi.go`

**Interfaces:**
- Consumes: `parseSberRows`, `sberUndescribed` (Task 1); `afterSberMacro`, `sberSeriesMeta` (Task 4 — до её выполнения используйте заглушку `AfterImport` не ставить вовсе и добавьте его в Task 4; в этой задаче поле отсутствует).
- Produces: импортёр `SberCSI` с встроенным `util.ClickHouseImport`; `sberCSIDataUrl`, `sberCSITable`, `sberCSIDdl` (`%s`-форма), `sberCSICraw(dataUrl string, conn driver.Conn) (int64, error)`.

- [ ] **Step 1: Переписать `bank/sber_csi.go`**

Форма (образцы: `bank/sber_fin_rez.go` — `CrawFunc` на шаблоне; `cbr/currency_usd.go` — пакетный `ctx`):

```go
const (
	// Старый путь /proxy/services/node-services/v1/... мёртв: отдаёт HTML-оболочку SPA (200 text/html),
	// из-за чего таблица годами оставалась пустой. Живая выгрузка — пункт «…в .csv» кнопки «Скачать»
	// дашборда sberindex.ru/ru/dashboards/consumper-spending-index-sa (проверено 2026-10-15).
	// Резерв при исчезновении пути: /api/dataset/v1/download/consumper-spending-index-sa/csv (ZIP с CSV).
	sberCSIDataUrl = "https://sberindex.ru/api/dataset/deprecated/csv/ru/consumper-spending-index-sa.csv"
	sberCSITable   = "sber_consumper_spending_index"
	sberCSIDdl     = `CREATE TABLE IF NOT EXISTS %s (...)` // прежние колонки: name LowCardinality(String), date Date, value Float32
)

type SberCSI struct {
	util.ClickHouseImport
}

func sberCSICraw(dataUrl string, conn driver.Conn) (count int64, err error) {
	var records [][]string
	if records, err = util.GetCSV(dataUrl); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSITable, err)
	}
	var points []sberPoint
	if points, err = parseSberRows(records); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSITable, err)
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", sberCSITable))
	if err != nil { return 0, err }
	for _, p := range points {
		if err = batch.Append(p.Series, p.Date, p.Value); err != nil {
			_ = batch.Abort()
			return count, fmt.Errorf("sber %s: %w", sberCSITable, err)
		}
		count++
	}
	if err = batch.Send(); err != nil { return count, err }
	return count, nil
}

var sberCSIImporter = &SberCSI{ClickHouseImport: util.ClickHouseImport{
	TableName:   sberCSITable,
	CreateTable: []string{sberCSIDdl},
	DataUrl:     sberCSIDataUrl,
	CrawFunc:    sberCSICraw,
}}

func init() { chimport.Stats = append(chimport.Stats, sberCSIImporter) }
```

`ctx` — пакетная переменная из `bank/sber_fin_rez.go` (не объявлять заново). Метод `Name()` убрать — его даёт встроенный `ClickHouseImport` (`TableName`). Удалить: `sberCSIInsert`, `fmt.Printf`, `charmap`, рукописный `Import`, `export()`.

- [ ] **Step 2: Сборка и тесты пакета**

Run: `go build ./... && go vet ./bank/ && go test -race -count=1 ./bank/`
Expected: сборка без ошибок, тесты PASS (фикстуры Task 1 не ходят в сеть).

- [ ] **Step 3: Коммит**

```bash
git add bank/sber_csi.go && git commit -m "bank: fix sber spending index importer (live csv endpoint, template)"
```

---

### Task 3: Недельный импортёр на шаблоне (`bank/sber_csi_week.go`)

**Files:**
- Modify (переписывается): `bank/sber_csi_week.go`

**Interfaces:**
- Consumes: `parseSberRows`, `sberUndescribed` (Task 1); форма из Task 2.
- Produces: импортёр `SberCSIWeek`; `sberCSIWeekDataUrl`, `sberCSIWeekTable`, `sberCSIWeekDdl` (`%s`-форма), `sberCSIWeekCraw`.

- [ ] **Step 1: Переписать `bank/sber_csi_week.go`**

По образцу Task 2 с отличиями: URL `https://sberindex.ru/api/dataset/deprecated/csv/ru/ver-izmenenie-trat-po-kategoriyam.csv`; таблица `sber_izmenenie_trat`; колонки `category, date, value`; при непустом `sberUndescribed(points, sberWeekSeriesMeta)` — `log.Warnf("sber %s: категория %q не описана в series_catalog — импортирую, описание догоняется", ...)` на каждое имя (сам импорт не прерывается); удалить битые константы `sberCSIWeekInsert`/`sberCSIWeekUrl` и `fmt.Printf`.

- [ ] **Step 2: Сборка и тесты пакета**

Run: `go build ./... && go vet ./bank/ && go test -race -count=1 ./bank/`
Expected: без ошибок, PASS.

- [ ] **Step 3: Коммит**

```bash
git add bank/sber_csi_week.go && git commit -m "bank: fix weekly sber spending importer (live csv endpoint, template)"
```

---

### Task 4: Каталог рядов и витрина

**Files:**
- Create: `bank/series_meta.go`
- Create: `bank/views.go`
- Modify: `bank/sber_csi.go`, `bank/sber_csi_week.go` (добавить `AfterImport`)
- Modify: `bank/sber_csi_test.go` (тест контракта каталога)

**Interfaces:**
- Consumes: `sberPoint`, `parseSberRows` (Task 1); импортёры (Task 2–3); `util.SeriesMeta`, `util.View`, `util.UpsertSeriesCatalog`, `util.CreateView`.
- Produces: `sberSourceIndex`, `sberSourceWeek`; `sberIndexSeriesMeta` (5), `sberWeekSeriesMeta` (46), `sberSeriesMeta`; `afterSberMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error`; `sberSpendingView util.View`.

- [ ] **Step 1: Тест контракта каталога (падающий)**

В `bank/sber_csi_test.go` добавить `TestSberSeriesMetaCoversFixtures`: каждая разобранная из месячной фикстуры категория есть в `sberIndexSeriesMeta` (по `Series`), каждая из недельной — в `sberWeekSeriesMeta`; имена уникальны внутри каждого источника.

Run: `go test -race -count=1 ./bank/ -run TestSberSeriesMetaCoversFixtures`
Expected: FAIL — `undefined: sberIndexSeriesMeta`.

- [ ] **Step 2: `bank/series_meta.go` — 51 описание**

```go
const (
	sberSourceIndex = "sber_consumper_spending_index"
	sberSourceWeek  = "sber_izmenenie_trat"
)
```

Месячные (5): `Всего`, `Непродовольственные товары`, `Общественное питание`, `Продовольственные товары`, `Услуги`.
`Title` — «Индекс реальных потребительских расходов, <категория>, помесячно»; `Unit` — «индекс, 100 = декабрь 2018»; `Frequency` — `M`; `Origin` — «sberindex.ru, дашборд consumper-spending-index-sa, файл consumper-spending-index-sa.csv, category из файла»; `Description` — что измеряет (реальные расходы по категории, ML-наукаст СберИндекса с корректировкой на прогнозный ИПЦ Росстата), дата — первое число месяца, опережение публикации Росстата на месяц, последний месяц — оценка СберИндекса и может ревизоваться; для «Всего» — сумма видов расходов.

Недельные (46) — ровно имена, встречающиеся в фикстуре (включая `Все категории` после нормализации, `Авиабилеты`, `Ювелирные изделия и часы`, …; список берётся из `cut -d';' -f2 | sort -u` по фикстуре, описание каждого — по шаблону): `Title` — «Изменение потребительских расходов, <категория>, недельно, % г/г»; `Unit` — «% к сопоставимой ISO-неделе прошлого года (положительное — рост)»; `Frequency` — `W`; `Origin` — «sberindex.ru, дашборд ver-izmenenie-trat-po-kategoriyam, файл ver-izmenenie-trat-po-kategoriyam.csv, category из файла (TrimSpace)»; `Description` — что измеряет (изменение объёма расходов по транзакциям карт Сбера), дата — воскресенье ISO-недели, методология пересчитана в июне и декабре 2025, последние недели могут пересматриваться; для `Все категории` — агрегат по всем категориям.

`var sberSeriesMeta = slices.Concat(sberIndexSeriesMeta, sberWeekSeriesMeta)`

```go
// afterSberMacro — производные импортёров Сбера (образец rosstat/series_meta.go): каталог рядов
// и общая витрина публикуются после Send точек и до записи строки версии.
func afterSberMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error
```

- [ ] **Step 3: `bank/views.go` — витрина long-формы**

```go
var sberSpendingView = util.View{
	Name:   "v_bank_sber_spending",
	Tables: []string{sberCSITable, sberCSIWeekTable},
	Select: `SELECT 'sber_consumper_spending_index' AS source, name AS series, date, toFloat64(value) AS value FROM sber_consumper_spending_index FINAL
UNION ALL SELECT 'sber_izmenenie_trat', category, date, toFloat64(value) FROM sber_izmenenie_trat FINAL`,
	Comment: "Потребительские расходы по данным СберИндекса: месячный индекс реальных потребительских расходов (100 = декабрь 2018) и недельное изменение расходов по категориям (% к сопоставимой ISO-неделе прошлого года). Единицы у наборов разные — сверяться с v_series_catalog по (source, series)",
	Columns: map[string]string{
		"source": "Набор данных: sber_consumper_spending_index (месячный индекс) или sber_izmenenie_trat (недельный темп)",
		"series": "Категория расходов из файла источника; у месячного набора — Всего, Продовольственные товары, Непродовольственные товары, Общественное питание, Услуги; у недельного — 46 категорий, включая агрегат «Все категории». Категория, появившаяся в источнике и ещё не описанная в v_series_catalog, импортируется с предупреждением в журнале",
		"date":   "Дата точки: у месячного набора — первое число месяца, у недельного — воскресенье ISO-недели",
		"value":  "Значение: индекс (месячный набор, 100 = декабрь 2018) или процент г/г (недельный набор). Читать по v_series_catalog",
	},
}
```

- [ ] **Step 4: Подключить производные в импортёрах**

В `bank/sber_csi.go` и `bank/sber_csi_week.go` добавить в `util.ClickHouseImport` поле
`AfterImport: afterSberMacro(sberSeriesMeta)`.

- [ ] **Step 5: Тесты и сборка**

Run: `go build ./... && go test -race -count=1 ./bank/`
Expected: PASS, включая `TestSberSeriesMetaCoversFixtures`.

- [ ] **Step 6: Коммит**

```bash
git add bank/series_meta.go bank/views.go bank/sber_csi.go bank/sber_csi_week.go bank/sber_csi_test.go && git commit -m "bank: sber series catalog and consumer spending view"
```

---

### Task 5: Грант агенту, проверки MCP и снимок покрытия

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`
- Modify: `sql/key_rate_coverage.sql`

**Interfaces:**
- Consumes: имя витрины `v_bank_sber_spending`, источники каталога `sber_consumper_spending_index` / `sber_izmenenie_trat` (Task 4); таблицы (Task 2–3).

- [ ] **Step 1: Грант**

В `sql/mcp_kimi_reader.sql` добавить строку `GRANT SELECT ON default.v_bank_sber_spending TO kimi_reader;` рядом с прочими грантами витрин (сырые таблицы не выдаются).

- [ ] **Step 2: Проверки `scripts/mcp_check.py`**

По образцу соседних блоков (функции `text`, `result_rows`, `expect`) добавить:
- `SELECT count() FROM v_bank_sber_spending` — запрос успешен и число > 0 (витрина видна и непуста);
- `SELECT count() AS n FROM v_series_catalog WHERE source = 'sber_consumper_spending_index'` == 5;
- `SELECT count() AS n FROM v_series_catalog WHERE source = 'sber_izmenenie_trat'` == 46;
- `SELECT count() FROM sber_consumper_spending_index` и `... sber_izmenenie_trat` — `ACCESS_DENIED`/`Not enough privileges` (сырые закрыты).

Проверка синтаксиса: `python3 -m py_compile scripts/mcp_check.py`.

- [ ] **Step 3: Снимок покрытия**

В `sql/key_rate_coverage.sql` добавить два `UNION ALL`:

```sql
UNION ALL SELECT 'sber_consumper_spending_index', name, count(), min(date), max(date) FROM sber_consumper_spending_index FINAL GROUP BY name
UNION ALL SELECT 'sber_izmenenie_trat', category, count(), min(date), max(date) FROM sber_izmenenie_trat FINAL GROUP BY category
```

- [ ] **Step 4: Коммит**

```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py sql/key_rate_coverage.sql && git commit -m "bank: grant sber spending view to mcp reader and cover with checks"
```

---

### Task 6: Живая приёмка

**Files:**
- Modify: `docs/superpowers/plans/2026-10-15-sber-spending-importers.md` (протокол приёмки — этот же файл)

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: протокол (команды, числа, крайние даты, spot-сверка) для роадмапа и PR.

- [ ] **Step 1: Полный прогон проверок**

Run: `make all`
Expected: gofmt чисто, golangci-lint без замечаний, `go vet`/`go test -race` зелёные, обе сборки собраны.

- [ ] **Step 2: Живые прогоны**

```bash
make import STAT=sber_consumper_spending_index   # ожидается Imported 470 rows
make import STAT=sber_izmenenie_trat             # ожидается Imported 7038 rows
```

- [ ] **Step 3: Проверка покрытия и идемпотентности**

```bash
make ch-sql FILE=sql/key_rate_coverage.sql
```
Ожидание: `sber_consumper_spending_index` — 5 строк по 94 точки, 2018-12-01 … 2026-09-01;
`sber_izmenenie_trat` — 46 строк по 153 точки, 2023-11-12 … 2026-10-11. Затем повторить оба
`make import` — числа через `count() FINAL` не меняются (числа повторов записать в протокол).

- [ ] **Step 4: Spot-сверка с живым файлом**

```bash
curl -sS 'https://sberindex.ru/api/dataset/deprecated/csv/ru/consumper-spending-index-sa.csv' | grep -E '^"2026-09-01";"Всего";'
curl -sS 'https://sberindex.ru/api/dataset/deprecated/csv/ru/ver-izmenenie-trat-po-kategoriyam.csv' | grep -E '^"2026-10-11";"Все категории '
```
Ожидание: `128.99` и `7.46266724` — совпадают с точками в БД (запрос по витрине).

- [ ] **Step 5: MCP end-to-end и расписание**

```bash
make mcp-user
make mcp-check          # ПРОВЕРКА ПРОЙДЕНА, включая новые проверки
dagu validate dagu/bank.yaml
```

- [ ] **Step 6: Записать протокол приёмки в план-файл и закоммитить**

```bash
git add docs/superpowers/plans/2026-10-15-sber-spending-importers.md && git commit -m "docs: sber importers live acceptance protocol"
```

---

### Task 7: Документация и статусы

**Files:**
- Modify: `ARCHITECTURE.md` (§2 строка `bank/`, §6.1 — две строки импортёров, §6.3 — витрина, запись в шапке-changelog)
- Modify: `README.md` (строка пакета `bank` в таблице импортёров; при необходимости — раздел про источники с новыми URL и риском «deprecated»-пути)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.1 — строки двух рядов со снимком; дефект № 2 — закрыт с датой; §6 фаза 2 — остаток «починки пустых рядов» закрыт; §8 при необходимости)
- Modify: `docs/KEY_RATE_PUBLICATIONS.md` §6 (❌ → ✅ с числами)
- Modify: `docs/KEY_RATE_MODEL.md` §4 (снять оговорку «таблицы пусты» применительно к этим рядам)

**Interfaces:**
- Consumes: числа приёмки из Task 6.

- [ ] **Step 1: Обновить `ARCHITECTURE.md`**

В §6.1 добавить строки: `sber_consumper_spending_index` (`bank/sber_csi.go`; транспорт — `util.GetCSV` по `/api/dataset/deprecated/csv/ru/consumper-spending-index-sa.csv`; 470 точек, 5 категорий, месячно с 2018-12; дата — первое число месяца; точка контроля публикации не объявлена: HEAD источника — 403) и `sber_izmenenie_trat` (то же; `.../ver-izmenenie-trat-po-kategoriyam.csv`; 7 038 точек, 46 категорий, недельно с 2023-11; дата — воскресенье ISO-недели). В §6.3 — витрина `v_bank_sber_spending`. В §2 — расширить строку `bank/`. Добавить запись в changelog шапки.

- [ ] **Step 2: Обновить `README.md`**

В строке пакета `bank` — новые эндпоинты и оговорка про «deprecated»-путь; числа прогона — в стиле соседних записей.

- [ ] **Step 3: Обновить роадмап и связанные документы**

`ROADMAP_KEY_RATE.md`: в §4.1 добавить строки «Высокочастотные потребительские расходы» для обеих таблиц с числами снимка; дефект № 2 пометить закрытым (дата, команда, числа); в §6 (фаза 2) отметить, что «починка пустых рядов» закрыта полностью. `KEY_RATE_PUBLICATIONS.md` §6 — статус строки. `KEY_RATE_MODEL.md` §4 — актуализировать оговорку.

- [ ] **Step 4: Проверить синхронность и закоммитить**

Run: `make all` (документация не влияет, но правило репозитория — зелёный билд перед коммитом)

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_KEY_RATE.md docs/KEY_RATE_PUBLICATIONS.md docs/KEY_RATE_MODEL.md && git commit -m "docs: sync architecture, readme and roadmaps for sber spending fix"
```

---

## Self-Review (выполнено автором плана)

- **Покрытие спеки:** §3 (источник/форматы) → Task 1–3; §4.1 (шаблон) → Task 2–3; §4.2 (нет префлайта) → Global Constraints; §4.3 (каталог/витрина/грант/проверки) → Task 4–5; §4.4 (TrimSpace, warn) → Task 1/3; §4.5 (legacy Float32) → Global Constraints; §4.6 (снимок) → Task 5–6; §6 (тесты) → Task 1/4; §7 (приёмка) → Task 6; §8 (риски) — комментарии в коде и README.
- **Review Focus:** каждый пункт покрыт тестом или шагом верификации (1 → fail-fast тесты Task 1; 2 → `sberUndescribed` тест и warn Task 3; 3 → нормализация в тестах Task 1; 4 → фикстуры без финального `\n` и spot-даты, повтор в Task 6; 5 → spot-значения по обоим наборам + `source` в витрине).
- **Имена/типы:** `parseSberRows`, `sberPoint`, `sberUndescribed`, `afterSberMacro`, `sberSeriesMeta`, `sberSpendingView`, `sber*Craw` — согласованы между задачами.
- **Пропорция:** план описывает решения, не тела функций; единственный крупный литерал — описания каталога, они данные, а не логика.

---

## Протокол живой приёмки (2026-10-15, dev-ClickHouse)

Ветка `fix/sber-spending-importers`, HEAD `7dd2b20` (до документации), команды запускались через `make` (env-файл подключает Makefile).

| Проверка | Команда | Результат |
|---|---|---|
| Формат/линт | `gofmt -l .` / `golangci-lint run ./...` | вывод пуст / `0 issues.` |
| Вет-тест | `go vet ./...` | exit 0 |
| Тесты | `go test -race ./...` (внутри `make all`) | все пакеты `ok` |
| Сборки | `make all` | `build/clickhouse-import-rosstat`, `build/ingest` |
| Месячный импортёр | `make import STAT=sber_consumper_spending_index` | `Imported 470 rows` (5 категорий × 94 мес., 2018-12-01 … 2026-09-01) |
| Недельный импортёр | `make import STAT=sber_izmenenie_trat` | `Imported 7038 rows` (46 категорий × 153 ISO-недели, 2023-11-12 … 2026-10-11) |
| Идемпотентность | повтор обоих `make import` | те же числа; `count() FINAL` = 470 / 7 038 (не изменились) |
| Снимок покрытия | `make ch-sql FILE=sql/key_rate_coverage.sql` | 51 строка: 5 × 94 точки и 46 × 153 точки, крайние даты совпали с ожиданием |
| Витрина и каталог | `count() FROM v_bank_sber_spending`; `v_series_catalog` | 7 508 строк (= 470 + 7 038); каталог 5 и 46 рядов; имён с хвостовым пробелом — 0 |
| Spot-сверка с живым файлом | `curl … \| grep '^"2026-09-01";"Всего";'` и `'^"2026-10-11";"Все категории '` | месяц: источник `128.99` = БД `128.99`; неделя: источник `7.46266724` → БД `7.4626675` — расхождение только в точности legacy-колонки `Float32` (Architecture §6.2) |
| Грант агенту | `make mcp-user` | `OK GRANT SELECT ON default.v_bank_sber_spending TO kimi_reader` |
| MCP end-to-end | `make mcp-check` | **ПРОВЕРКА ПРОЙДЕНА**: витрина непуста (7 508), каталог 5 + 46, обе сырые таблицы закрыты (497) |
| Расписание | `dagu validate dagu/bank.yaml` | exit 0 |

Замечания приёмки: (1) расхождение недельного spot-значения — не ошибка импорта, а округление legacy-`Float32` (осознанно, спека §4.5); (2) `v_series_catalog`/`series_catalog` — ReplacingMergeTree, счёт каталога проверяется через витрину (её `Select` содержит `FINAL`), прямой `count()` без `FINAL` считает несмёрженные копии; (3) повторные прогоны пишут те же ключи `(name|category, date)` — число строк `FINAL` стабильно, витрина не растёт.
