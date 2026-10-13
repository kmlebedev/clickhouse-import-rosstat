package cbr

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// Ожидаемые значения ниже — литералами, а не константами пакета. Тест, сверяющий
// cbrKeyRateTable с самим собой, прошёл бы и после переименования таблицы в key_rate,
// то есть ровно на той правке, от которой он и должен падать.
const (
	// cbrNamePrefix — пространство имён импортёров и таблиц пакета cbr. Имя
	// импортёра — контракт расписания (CLICKHOUSE_IMPORT_STAT и id шага
	// dagu/cbr.yaml), имя таблицы — контракт данных (v_cbr_macro и агент MCP).
	cbrNamePrefix = "cbr_"
	// sharedMacroSeriesTable — единственная таблица вне cbr_-пространства, которую
	// занимает импортёр ЦБ: ряды мониторинга предприятий лежат в общей macro_series
	// (source='cbr_survey'), рядом с fred/bls/bea.
	sharedMacroSeriesTable = "macro_series"
	// sharedMacroSeriesOwner — импортёр, которому разрешено писать в общую таблицу.
	sharedMacroSeriesOwner = "cbr_mp_survey"
)

// daguCbrYAML — расписание cbr: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT. Путь от
// каталога пакета (тесты идут с cwd=cbr/): так же читают фикстуры другие пакеты.
var daguCbrYAML = filepath.Join("..", "dagu", "cbr.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
// Разбор построчный и регуляркой, без YAML-зависимости: тест не должен тянуть в
// пакетный go.mod парсер ради одного ключа.
var daguStatRef = regexp.MustCompile(`CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)`)

// TestCbrImportersPrefixedAndRegistered держит инварианты реестра cbrImporters —
// единственной точки, из которой импортёры пакета попадают в chimport.Stats.
//
// Проверяется именно реестр, а не Import напрямую: остальные тесты пакета зовут
// парсеры и Import, и потому проходили бы, даже если импортёр не зарегистрирован
// и недостижим ни из `make import`, ни из расписания. Регистрация — единственное,
// что видит оператор.
//
//   - (а) Name() каждого импортёра начинается с cbr_ — иначе ручной запуск
//     CLICKHOUSE_IMPORT_STAT=<table> попадёт в чужой шаг конвейера (префикс cbr_
//     в репозитории уникален за пределами пакета);
//   - (б) таблица — cbr_-префикс, либо общий macro_series и только для cbr_mp_survey:
//     смена таблицы под импортёром увела бы витрину v_cbr_macro к чужим данным;
//   - (в) имена уникальны и лежат в chimport.Stats: дубль имени в фильтре
//     `make import STAT=...` выбрал бы произвольный из двух импортёров;
//   - (г) каждый шаг dagu/cbr.yaml присутствует в реестре: расписание —
//     единственный продакшн-вызов, и его опечатка иначе не видна ничем.
func TestCbrImportersPrefixedAndRegistered(t *testing.T) {
	if len(cbrImporters) == 0 {
		t.Fatal("реестр cbrImporters пуст: ни один импортёр пакета не зарегистрирован")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(cbrImporters))
	for _, imp := range cbrImporters {
		name := imp.Name()

		// (а)
		if !strings.HasPrefix(name, cbrNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml", name, cbrNamePrefix)
		}

		// (в)
		if seen[name] {
			t.Errorf("имя импортёра %q встречается в реестре дважды", name)
		}
		seen[name] = true
		if !registered[name] {
			t.Errorf("импортёр %q не зарегистрирован в chimport.Stats: `make import STAT=%s` и шаг dagu той же строкой не найдут ничего", name, name)
		}

		// (б)
		if imp.table == sharedMacroSeriesTable {
			if name != sharedMacroSeriesOwner {
				t.Errorf("импортёр %q объявил общую таблицу %q: её занимает только %s (source='cbr_survey')", name, sharedMacroSeriesTable, sharedMacroSeriesOwner)
			}
			continue
		}
		if !strings.HasPrefix(imp.table, cbrNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q и не равна %q", imp.table, name, cbrNamePrefix, sharedMacroSeriesTable)
		}
		// У всех импортёров, кроме cbr_mp_survey, Name() — это TableName
		// встроенного util.ClickHouseImport. Значит, литерал в колонке table
		// обязан с ним совпадать: иначе реестр описывает не ту таблицу, в
		// которую импортёр пишет (переименовали константу — обновили литерал).
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() (TableName импортёра) — %q", name, imp.table, name)
		}
	}

	// «Ровно те же»: число cbr_-имён в Stats не может быть больше реестра
	// (кто-то вернул персональный init()-append в файл импортёра) или меньше
	// (запись забыта при консолидации) — ровно одна точка регистрации на пакет.
	cbrRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), cbrNamePrefix) {
			cbrRegistered++
		}
	}
	if cbrRegistered != len(cbrImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d: регистрация должна быть только в cbr/importers.go", cbrRegistered, cbrNamePrefix, len(cbrImporters))
	}

	// (г)
	data, err := os.ReadFile(daguCbrYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguCbrYAML, err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в dagu/cbr.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=: разбор сломан или шаги переписаны")
	}
	for _, step := range steps {
		if name := step[1]; !seen[name] {
			t.Errorf("шаг dagu/cbr.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре cbrImporters: расписание позовёт импортёр, которого нет", name)
		}
	}
}
