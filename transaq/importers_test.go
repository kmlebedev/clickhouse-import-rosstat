package transaq

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// Энфорсер реестра (стандарт пакета §8.3): префикс пакета в имени, литеральное
// имя таблицы, уникальность, единственная регистрация в chimport.Stats и
// двусторонняя сверка с расписанием dagu. Ожидаемые имена — литералами: ссылка на
// константу пакета сделала бы проверку тавтологичной.
func TestTransaqImportersPrefixedAndRegistered(t *testing.T) {
	if len(transaqImporters) != 1 {
		t.Fatalf("importers = %d, want 1", len(transaqImporters))
	}
	names := make([]string, 0, len(transaqImporters))
	for _, imp := range transaqImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, "transaq_") {
			t.Errorf("импортёр %q вне пространства имён transaq_: ручной запуск "+
				"CLICKHOUSE_IMPORT_STAT попадёт в чужой шаг", name)
		}
		if name != "transaq_net_flows" {
			t.Errorf("Name() = %q, want transaq_net_flows: имя импортёра обязано совпадать с таблицей (§7.2)", name)
		}
		if slices.Contains(names, name) {
			t.Errorf("имя %q зарегистрировано дважды: фильтр CLICKHOUSE_IMPORT_STAT выбрал бы произвольный", name)
		}
		names = append(names, name)
	}

	registered := 0
	for _, stat := range chimport.Stats {
		if stat.Name() == "transaq_net_flows" {
			registered++
		}
	}
	if registered != 1 {
		t.Fatalf("transaq_net_flows в chimport.Stats: %d записей, want 1 (персональный init() в файле импортёра вернулся?)", registered)
	}
	if len(transaqImporters) != len(chimport.Stats) {
		t.Fatalf("реестр пакета = %d, а в chimport.Stats всего %d: пакет обязан регистрировать только свои импортёры",
			len(transaqImporters), len(chimport.Stats))
	}
}

// Расписание и реестр сверяются в обе стороны: опечатка в dagu оставила бы шаг без
// импортёра, а импортёр без шага молча не запускался бы по расписанию.
func TestTransaqDaguMatchesRegistry(t *testing.T) {
	raw, err := os.ReadFile("../dagu/transaq.yaml")
	if err != nil {
		t.Fatalf("читать dagu/transaq.yaml: %v", err)
	}
	pattern := regexp.MustCompile(`CLICKHOUSE_IMPORT_STAT=(\S+)`)
	var scheduled []string
	for _, match := range pattern.FindAllStringSubmatch(string(raw), -1) {
		scheduled = append(scheduled, match[1])
	}
	if len(scheduled) == 0 {
		t.Fatalf("в dagu/transaq.yaml нет шагов CLICKHOUSE_IMPORT_STAT")
	}

	var registered []string
	for _, imp := range transaqImporters {
		registered = append(registered, imp.Name())
	}
	for _, name := range scheduled {
		if !slices.Contains(registered, name) {
			t.Errorf("шаг dagu %q не зарегистрирован в пакете transaq", name)
		}
	}
	for _, name := range registered {
		if !slices.Contains(scheduled, name) {
			t.Errorf("импортёр %q не поставлен в расписание dagu/transaq.yaml", name)
		}
	}
}
