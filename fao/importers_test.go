package fao

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const faoNamePrefix = "fao_"

type faoPublicationDeclarer interface {
	Publication() util.PublicationDecl
}

var daguFaoStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// TestFaoImportersPrefixedAndRegistered держит инварианты реестра faoImporters:
//   - имя и таблица начинаются с fao_, Name() == TableName;
//   - имя уникально и зарегистрировано в chimport.Stats;
//   - шаг dagu/fao.yaml совпадает с реестром в обе стороны;
//   - префлайта нет: файл обновляется с той же частотой, что и шаг (§9.3).
func TestFaoImportersPrefixedAndRegistered(t *testing.T) {
	if len(faoImporters) == 0 {
		t.Fatal("реестр faoImporters пуст")
	}
	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(faoImporters))
	for _, imp := range faoImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, faoNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q", name, faoNamePrefix)
		}
		if seen[name] {
			t.Errorf("имя импортёра %q встречается дважды", name)
		}
		seen[name] = true
		if !registered[name] {
			t.Errorf("импортёр %q не зарегистрирован в chimport.Stats", name)
		}
		if !strings.HasPrefix(imp.table, faoNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q", imp.table, name, faoNamePrefix)
		}
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() — %q", name, imp.table, name)
		}
		declarer, ok := imp.ImportStat.(faoPublicationDeclarer)
		if !ok {
			t.Errorf("импортёр %q не объявляет Publication()", name)
			continue
		}
		if decl := declarer.Publication(); decl.CountPoints != "" {
			t.Errorf("импортёр %q объявил префлайт %q, хотя темп файла равен шагу", name, decl.CountPoints)
		}
		if imp.countPoints != "" {
			t.Errorf("реестр записал импортёру %q префлайт %q, хотя его нет", name, imp.countPoints)
		}
	}

	faoRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), faoNamePrefix) {
			faoRegistered++
		}
	}
	if faoRegistered != len(faoImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d", faoRegistered, faoNamePrefix, len(faoImporters))
	}

	data, err := os.ReadFile(filepath.Join("..", "dagu", "fao.yaml"))
	if err != nil {
		t.Fatalf("чтение dagu/fao.yaml: %v", err)
	}
	steps := daguFaoStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatal("в dagu/fao.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=")
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг dagu/fao.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре", name)
		}
	}
	for _, imp := range faoImporters {
		if !scheduled[imp.Name()] {
			t.Errorf("в dagu/fao.yaml нет шага CLICKHOUSE_IMPORT_STAT=%s", imp.Name())
		}
	}
}
