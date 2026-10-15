### Task 2: Реестр `rosstat/importers.go`, префикс `rosstat_*`, тест-энфорсер

**Files:**
- Create: `rosstat/importers.go`
- Create: `rosstat/importers_test.go`
- Modify: `rosstat/ipc_mes.go`, `rosstat/ipc_weeks.go`, `rosstat/salaries_mes.go`, `rosstat/vvp_kvartal.go`, `rosstat/series_meta.go`
- Modify: `dagu/rosstat.yaml`, `views/model_inputs.go`, `sql/key_rate_coverage.sql`, `util/publications.go` (комментарий), `cbr/views.go` (комментарий)

**Interfaces:**
- Consumes: ничего из T1.
- Produces: `rosstatImporters` (список записей реестра), константы `rosstatIPCMesTable = "rosstat_ipc_mes"`, `rosstatIPCWeeksTable = "rosstat_ipc_weeks"`, `rosstatSalariesMesTable = "rosstat_salaries_mes"`, `rosstatVVPKvartalTable = "rosstat_vvp_kvartal"`; переменные импортёров `ipcMesImporter`, `ipcWeeksImporter`, `salariesMesImporter`, `vvpKvartalImporter`. Эти имена используют все последующие задачи.

- [ ] **Step 1: Переименовать константы таблиц и DDL в четырёх файлах импортёров**

В `rosstat/ipc_mes.go`: `ipcMesTable` → `rosstatIPCMesTable = "rosstat_ipc_mes"`, DDL и `ipcMesCountPoints` — на новое имя (`SELECT count() FROM rosstat_ipc_mes FINAL`). Аналогично в трёх остальных файлах (имена констант: `rosstatIPCWeeksTable`, `rosstatSalariesMesTable`, `rosstatVVPKvartalTable`; запросы чека — `rosstat_ipc_mes`/`rosstat_salaries_mes`/`rosstat_vvp_kvartal`, у `ipc_weeks` чека нет). DDL привести к `%s`-форме (`CREATE TABLE IF NOT EXISTS %s (`), убрав склейку `"CREATE TABLE IF NOT EXISTS " + ipcWeeksTable` и `ipcWeeksInsert`.

- [ ] **Step 2: Добавить именованную переменную импортёра там, где её нет**

В `rosstat/ipc_weeks.go` завести `var ipcWeeksImporter = &IpcWeeksStat{}` и **удалить** персональный `init()`; в остальных трёх файлах удалить персональные `init()`, оставив именованные переменные.

- [ ] **Step 3: Создать `rosstat/importers.go`**

Форма — по образцу `cbr/importers.go` (тип, список, один `init()`); литералы таблиц и запросов чека обязательны (стандарт §8.2), комментарий объясняет, почему литералы, а не константы:

```go
type rosstatImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

var rosstatImporters = []rosstatImporter{
	{ipcMesImporter, "rosstat_ipc_mes", "SELECT count() FROM rosstat_ipc_mes FINAL"},
	{ipcWeeksImporter, "rosstat_ipc_weeks", ""},
	{salariesMesImporter, "rosstat_salaries_mes", "SELECT count() FROM rosstat_salaries_mes FINAL"},
	{vvpKvartalImporter, "rosstat_vvp_kvartal", "SELECT count() FROM rosstat_vvp_kvartal FINAL"},
}

func init() {
	for _, imp := range rosstatImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
```

- [ ] **Step 4: Обновить имя витрины-владельца и её `Select` в `rosstat/series_meta.go`**

`rosstatMacroView.Tables` — новые константы; `Select` — литералы `rosstat_ipc_mes`, `rosstat_ipc_weeks`, `rosstat_vvp_kvartal`, `rosstat_salaries_mes`; имя витрины `v_rosstat_macro` и `Comment`/`Columns` не меняются.

- [ ] **Step 5: Обновить расписание и внешние ссылки**

`dagu/rosstat.yaml` — четыре `id` и четыре `CLICKHOUSE_IMPORT_STAT` на новые имена (порядок шагов и расписание `17 12 * * 4` не меняются); `views/model_inputs.go` — `ipc_mes`→`rosstat_ipc_mes` и `ipc_weeks`→`rosstat_ipc_weeks` в `Tables` и во всех местах `Select` (`FROM ipc_mes FINAL`, `FROM ipc_weeks FINAL`, подзапрос года); `sql/key_rate_coverage.sql` — те же четыре строки; комментарии-примеры в `util/publications.go:30` и `cbr/views.go:68`.

- [ ] **Step 6: Создать `rosstat/importers_test.go` — энфорсер `TestRosstatImportersPrefixedAndRegistered`**

Копия структуры `cbr/importers_test.go` со своими литералами: `rosstatNamePrefix = "rosstat_"`, пустой `sharedMacroSeriesTable` (у пакета общей таблицы нет — проверка (б) выполняется как «таблица с префиксом, `Name() == table`»), ожидаемый набор точек контроля:

```go
var expectedPublicationCountPoints = map[string]string{
	"rosstat_ipc_mes":       "SELECT count() FROM rosstat_ipc_mes FINAL",
	"rosstat_salaries_mes":  "SELECT count() FROM rosstat_salaries_mes FINAL",
	"rosstat_vvp_kvartal":   "SELECT count() FROM rosstat_vvp_kvartal FINAL",
}
```

плюс инвариант числа (`число rosstat_-имён в chimport.Stats == len(rosstatImporters)`) и проверка полноты `../dagu/rosstat.yaml` в обе стороны (обратное направление — шаг `rosstat_ipc_weeks` обязан быть в расписании, у него нет объявления, которое могло бы его «защитить»).

- [ ] **Step 7: Прогнать тесты пакета**

Run: `go test ./rosstat/ -count=1 -v -run 'Importers|SeriesMeta'`
Expected: PASS (старые фикстурные тесты каталога ещё живы и должны остаться зелёными).

- [ ] **Step 8: Проверить, что старых имён в коде не осталось**

Run: `grep -rn '\bipc_mes\b\|\bipc_weeks\b\|\bsalaries_mes\b\|\bvvp_kvartal\b' --include=*.go rosstat/ views/ | grep -v rosstat_`
Expected: пусто (допустимы только строки внутри `rosstat_...`).

- [ ] **Step 9: Коммит**

```bash
git add -A rosstat/ dagu/rosstat.yaml views/model_inputs.go sql/key_rate_coverage.sql util/publications.go cbr/views.go
git commit -m "rosstat: single importer registry and rosstat_ table prefix"
```

---

