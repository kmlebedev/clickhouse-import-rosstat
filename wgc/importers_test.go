package wgc

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Ожидаемые значения ниже — литералами, а не константами пакета: тест, сверяющий
// etfStatName с самим собой, прошёл бы и после переименования импортёра, то есть ровно на
// той правке, от которой он и должен падать (стандарт §8.5).

// wgcPrefix — префикс пакета: Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/wgc.yaml.
const wgcPrefix = "wgc_"

// sharedMacroSeries — общая таблица длинных рядов вне пространства имён пакета.
const sharedMacroSeries = "macro_series"

// sharedMacroSeriesOwners — импортёры, которым разрешено писать в macro_series. Литеральный
// allowlist: незнакомец в общей таблице — ровно тот случай, ради которого проверка (б) есть.
var sharedMacroSeriesOwners = map[string]bool{
	"wgc_etf_flows":   true,
	"wgc_cb_reserves": true,
}

// expectedWgcImporters — ожидаемый литеральный набор имён реестра.
var expectedWgcImporters = []string{"wgc_etf_flows", "wgc_cb_reserves"}

// publicationDeclarer — как тест достаёт объявление точки контроля публикации: метод
// Publication() есть только у импортёров на util.ClickHouseImport.
type publicationDeclarer interface {
	Publication() util.PublicationDecl
}

// Страховка от пустой проверки (д): шаблонный импортёр обязан приводиться к publicationDeclarer.
var _ publicationDeclarer = (*util.ClickHouseImport)(nil)

var daguWgcYAML = filepath.Join("..", "dagu", "wgc.yaml")

// daguStatRef — строка шага `- CLICKHOUSE_IMPORT_STAT=<name>`. Разбор регуляркой, без
// YAML-зависимости (стандарт §8.4); якоря (?m) отсекают закомментированный шаг.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

func daguStats(data []byte) []string {
	var names []string
	for _, m := range daguStatRef.FindAllStringSubmatch(string(data), -1) {
		names = append(names, m[1])
	}
	return names
}

// TestDaguStatsIgnoreComments — закомментированный шаг не считается расписанием.
func TestDaguStatsIgnoreComments(t *testing.T) {
	data := []byte("- id: a\n  env:\n    - CLICKHOUSE_IMPORT_STAT=a\n# - CLICKHOUSE_IMPORT_STAT=commented\n")
	if got := daguStats(data); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("daguStats = %v, want [a]", got)
	}
}

// TestWgcImportersPrefixedAndRegistered держит инварианты реестра wgcImporters (стандарт §8.3):
//   - (а) Name() начинается с wgc_ — ручной запуск не попадёт в чужое пространство имён;
//   - (б) таблица — macro_series и только у владельцев из литерального allowlist'а; Name() != table
//     допустимо ровно для общей таблицы (§7.2);
//   - (в) имена уникальны, совпадают с ожидаемым набором и зарегистрированы в chimport.Stats;
//   - (г) каждая строка CLICKHOUSE_IMPORT_STAT из dagu/wgc.yaml есть в реестре и обратно;
//   - (д) точка контроля публикации не объявлена ни у кого (§9.3): «на всякий случай»
//     объявленный префлайт дал бы HEAD по вручную скачанному файлу или no-op ETF-API;
//   - (¬) число импортёров пакета в chimport.Stats равно len(wgcImporters): больше — кто-то вернул
//     персональный init() в файл импортёра.
func TestWgcImportersPrefixedAndRegistered(t *testing.T) {
	if len(wgcImporters) == 0 {
		t.Fatal("реестр wgcImporters пуст")
	}

	registered := make(map[string]int, len(chimport.Stats))
	inPackage := 0
	for _, stat := range chimport.Stats {
		registered[stat.Name()]++
		if strings.HasPrefix(stat.Name(), wgcPrefix) {
			inPackage++
		}
	}

	seen := make(map[string]bool, len(wgcImporters))
	var names []string
	for _, imp := range wgcImporters {
		name := imp.Name()
		names = append(names, name)

		if !strings.HasPrefix(name, wgcPrefix) {
			t.Errorf("имя импортёра %q без префикса %q", name, wgcPrefix)
		}
		if seen[name] {
			t.Errorf("имя импортёра %q встречается в реестре дважды", name)
		}
		seen[name] = true
		if registered[name] != 1 {
			t.Errorf("импортёр %q зарегистрирован в chimport.Stats %d раз, ожидалась ровно одна регистрация", name, registered[name])
		}

		if imp.table != sharedMacroSeries {
			t.Errorf("импортёр %q пишет в %q, ожидалась общая %q", name, imp.table, sharedMacroSeries)
		}
		if !sharedMacroSeriesOwners[name] {
			t.Errorf("импортёр %q не в allowlist владельцев %q", name, sharedMacroSeries)
		}
		if name == imp.table {
			t.Errorf("Name() импортёра %q совпал с общей таблицей", name)
		}

		if _, ok := imp.ImportStat.(publicationDeclarer); ok {
			t.Errorf("импортёр %q объявил точку контроля публикации: у ETF-API и ручных выгрузок no-op по HEAD недостижим (§9.3)", name)
		}
	}

	if want := slices.Clone(expectedWgcImporters); !slices.Equal(sorted(names), sorted(want)) {
		t.Errorf("набор реестра %v, ожидался %v", names, want)
	}
	if inPackage != len(wgcImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d: персональный init() вернулся в файл или запись забыта", inPackage, wgcPrefix, len(wgcImporters))
	}

	data, err := os.ReadFile(daguWgcYAML)
	if err != nil {
		t.Fatalf("читаю расписание %s: %v", daguWgcYAML, err)
	}
	steps := daguStats(data)
	for _, step := range steps {
		if !seen[step] {
			t.Errorf("шаг расписания %q отсутствует в реестре: опечатка в dagu/wgc.yaml", step)
		}
	}
	for name := range seen {
		if !slices.Contains(steps, name) {
			t.Errorf("импортёр %q есть в реестре, но не стоит в dagu/wgc.yaml: ряды перестанут обновляться", name)
		}
	}
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}
