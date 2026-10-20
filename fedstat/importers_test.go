package fedstat

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// TestFedstatImportersPrefixedAndRegistered — энфорсер реестра (стандарт §8.3):
// префикс имени/таблицы, уникальность, регистрация в chimport.Stats, отсутствие
// точки контроля публикации и полное совпадение с dagu/fedstat.yaml в обе стороны.
func TestFedstatImportersPrefixedAndRegistered(t *testing.T) {
	if len(fedstatImporters) != 1 {
		t.Fatalf("fedstatImporters = %d, want 1", len(fedstatImporters))
	}
	seen := make(map[string]bool, len(fedstatImporters))
	for _, imp := range fedstatImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, "fedstat_") {
			t.Errorf("%q: Name() без префикса fedstat_", name)
		}
		if imp.table != name {
			t.Errorf("%q: литерал таблицы %q не совпадает с Name()", name, imp.table)
		}
		if !strings.HasPrefix(imp.table, "fedstat_") {
			t.Errorf("%q: таблица %q вне пространства fedstat_", name, imp.table)
		}
		if seen[name] {
			t.Errorf("%q: дубль имени в реестре", name)
		}
		seen[name] = true
		if imp.countPoints != "" {
			t.Errorf("%q: countPoints = %q, want empty (точки контроля нет)", name, imp.countPoints)
		}
		if pub := fedstatWeeklyPricesImporter.Publication(); pub != (util.PublicationDecl{}) {
			t.Errorf("%q: Publication() = %+v, want zero", name, pub)
		}
		registered := false
		for _, stat := range chimport.Stats {
			if stat.Name() == name {
				registered = true
				break
			}
		}
		if !registered {
			t.Errorf("%q: не зарегистрирован в chimport.Stats", name)
		}
	}

	statsCount := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), "fedstat_") {
			statsCount++
		}
	}
	if statsCount != len(fedstatImporters) {
		t.Errorf("chimport.Stats: %d импортёров fedstat_, реестр — %d", statsCount, len(fedstatImporters))
	}

	body, err := os.ReadFile("../dagu/fedstat.yaml")
	if err != nil {
		t.Fatalf("dagu/fedstat.yaml: %v", err)
	}
	matches := regexp.MustCompile(`CLICKHOUSE_IMPORT_STAT=([a-z0-9_]+)`).FindAllStringSubmatch(string(body), -1)
	if len(matches) != len(fedstatImporters) {
		t.Errorf("dagu/fedstat.yaml: %d шагов, реестр — %d", len(matches), len(fedstatImporters))
	}
	for _, match := range matches {
		if !seen[match[1]] {
			t.Errorf("dagu/fedstat.yaml: шаг %q отсутствует в реестре", match[1])
		}
	}
}
