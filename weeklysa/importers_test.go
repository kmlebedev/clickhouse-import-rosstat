package weeklysa

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

const (
	// weeklyPrefix — пространство имён импортёров и таблиц пакета.
	weeklyPrefix = "weekly_cpi"
	// weeklyCountPointsLiteral — литерал ожидаемого чека версии; отдельно от
	// константы импортёра, чтобы тест падал на расхождении двух записей.
	weeklyCountPointsLiteral = "SELECT count() FROM weekly_cpi_sa FINAL"
)

// daguWeeklyYAML — расписание пакета; тесты идут с cwd=weeklysa/.
var daguWeeklyYAML = filepath.Join("..", "dagu", "ppi.yaml")

// daguStatRef — имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

func TestWeeklyImportersPrefixedAndRegistered(t *testing.T) {
	if len(weeklySAImporters) == 0 {
		t.Fatal("реестр weeklySAImporters пуст")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(weeklySAImporters))
	for _, imp := range weeklySAImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, weeklyPrefix) {
			t.Errorf("имя импортёра %q без префикса %q", name, weeklyPrefix)
		}
		if seen[name] {
			t.Errorf("имя импортёра %q встречается дважды", name)
		}
		seen[name] = true
		if !registered[name] {
			t.Errorf("импортёр %q не зарегистрирован в chimport.Stats", name)
		}
		if !strings.HasPrefix(imp.table, weeklyPrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q", imp.table, name, weeklyPrefix)
		}
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q", name, imp.table)
		}
		if imp.countPoints != weeklySACountPoints {
			t.Errorf("реестр записал чек %q, константа импортёра %q", imp.countPoints, weeklySACountPoints)
		}
		if imp.countPoints != weeklyCountPointsLiteral {
			t.Errorf("реестр записал чек %q, ожидался литерал %q", imp.countPoints, weeklyCountPointsLiteral)
		}
	}

	weeklyRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), weeklyPrefix) {
			weeklyRegistered++
		}
	}
	if weeklyRegistered != len(weeklySAImporters) {
		t.Errorf("в chimport.Stats %d weekly_cpi_-импортёров, в реестре %d", weeklyRegistered, len(weeklySAImporters))
	}

	data, err := os.ReadFile(daguWeeklyYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguWeeklyYAML, err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в %s не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=", daguWeeklyYAML)
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		if !strings.HasPrefix(name, weeklyPrefix) {
			// dagu/ppi.yaml общий с пакетом ppi: чужой шаг проверяет его энфорсер.
			continue
		}
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг %s не найден в реестре weeklySAImporters", name)
		}
	}
	for name := range seen {
		if !scheduled[name] {
			t.Errorf("в %s нет шага CLICKHOUSE_IMPORT_STAT=%s", daguWeeklyYAML, name)
		}
	}
}
