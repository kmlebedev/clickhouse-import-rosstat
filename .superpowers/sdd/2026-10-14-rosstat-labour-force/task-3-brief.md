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

- [ ] **Step 5b: Добавить шаг в расписание**

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

Шаг входит в эту задачу, а не в следующую, потому что тест-энфорсер двусторонний (§8.3(г)): запись в реестре без шага в расписании роняет `TestRosstatImportersPrefixedAndRegistered`, и без шага гейт этой задачи зелёным не станет.

- [ ] **Step 6: Прогнать тесты пакета**

Run: `go test -race -count=1 ./rosstat/`
Expected: PASS, включая `TestRosstatImportersPrefixedAndRegistered` (двусторонняя сверка реестра и `dagu/rosstat.yaml`).

- [ ] **Step 7: Коммит**

```bash
git add rosstat/labour_force.go rosstat/labour_force_test.go rosstat/series_meta.go rosstat/importers.go rosstat/importers_test.go dagu/rosstat.yaml
git commit -m "register rosstat_labour_force_mes importer with catalog, view and schedule"
```

---

