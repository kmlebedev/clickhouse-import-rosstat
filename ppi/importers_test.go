package ppi

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

const (
	// ppiNamePrefix — пространство имён импортёров и таблиц пакета.
	ppiNamePrefix = "ppi_"
	// ppiCountPointsLiteral — литерал ожидаемого чека версии; отдельно от
	// константы импортёра, чтобы тест падал на расхождении двух записей.
	ppiCountPointsLiteral = "SELECT count() FROM ppi_sa FINAL"
)

// daguPPIYAML — расписание пакета; тесты идут с cwd=ppi/.
var daguPPIYAML = filepath.Join("..", "dagu", "ppi.yaml")

// daguStatRef — имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
// Регулярка без YAML-зависимости; якоря ^…$ в (?m) отсекают закомментированный шаг.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

func TestPPIImportersPrefixedAndRegistered(t *testing.T) {
	if len(ppiImporters) == 0 {
		t.Fatal("реестр ppiImporters пуст")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(ppiImporters))
	for _, imp := range ppiImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, ppiNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q", name, ppiNamePrefix)
		}
		if seen[name] {
			t.Errorf("имя импортёра %q встречается дважды", name)
		}
		seen[name] = true
		if !registered[name] {
			t.Errorf("импортёр %q не зарегистрирован в chimport.Stats", name)
		}
		if !strings.HasPrefix(imp.table, ppiNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q", imp.table, name, ppiNamePrefix)
		}
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q", name, imp.table)
		}
		if imp.countPoints != ppiSACountPoints {
			t.Errorf("реестр записал чек %q, константа импортёра %q", imp.countPoints, ppiSACountPoints)
		}
		if imp.countPoints != ppiCountPointsLiteral {
			t.Errorf("реестр записал чек %q, ожидался литерал %q", imp.countPoints, ppiCountPointsLiteral)
		}
	}

	ppiRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), ppiNamePrefix) {
			ppiRegistered++
		}
	}
	if ppiRegistered != len(ppiImporters) {
		t.Errorf("в chimport.Stats %d ppi_-импортёров, в реестре %d", ppiRegistered, len(ppiImporters))
	}

	data, err := os.ReadFile(daguPPIYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguPPIYAML, err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в %s не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=", daguPPIYAML)
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		if !strings.HasPrefix(name, ppiNamePrefix) {
			// dagu/ppi.yaml общий с пакетом weeklysa: чужой шаг проверяет его энфорсер.
			continue
		}
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг %s не найден в реестре ppiImporters", name)
		}
	}
	for name := range seen {
		if !scheduled[name] {
			t.Errorf("в %s нет шага CLICKHOUSE_IMPORT_STAT=%s", daguPPIYAML, name)
		}
	}
}
