package minec

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// minecNamePrefix — пространство имён пакета: Name() — это CLICKHOUSE_IMPORT_STAT
// и id шага dagu/minec.yaml.
const minecNamePrefix = "minec_"

// sharedMacroSeriesTable — общая таблица пакетов-владельцев macro_series; в
// minec она единственная, но проверка записана отдельно от minecTable, чтобы
// литерал теста не повторил константу кода.
const sharedMacroSeriesTable = "macro_series"

// minecSharedTableOwners — литеральный allowlist общей macro_series: таблица чужая
// для имени импортёра, поэтому пара «имя → таблица» записана явно. Литералы, а не
// ссылки на константы: сравнение константы с самой собой прошло бы и после
// переименования, от которого проверка обязана падать.
var minecSharedTableOwners = map[string]string{
	"minec_urals_price": "macro_series",
}

// daguMinecYAML — расписание пакета: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT.
var daguMinecYAML = filepath.Join("..", "dagu", "minec.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
// Регулярка, а не YAML-парсер: тест не должен тянуть зависимость ради одного
// ключа (стандарт §8.4), а закомментированный шаг `# - CLICKHOUSE_IMPORT_STAT=…`
// не считается шагом расписания.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// TestMinecImportersPrefixedAndRegistered — энфорсер реестра (стандарт §8.3):
// (а) префикс имени, (б) общая таблица только из allowlist, (в) уникальность и
// регистрация, (г) покрытие dagu в обе стороны, (д) отсутствие объявления
// префлайта. Каждая проверка закрывает правку, которая иначе прошла бы молча:
// импортёр в чужом пространстве имён, дубль имени, шаг расписания без импортёра и
// префлайт «на всякий случай», которого у набора страниц быть не может.
func TestMinecImportersPrefixedAndRegistered(t *testing.T) {
	if len(minecImporters) == 0 {
		t.Fatal("реестр minecImporters пуст: ни один импортёр пакета не зарегистрирован")
	}
	registered := make(map[string]int, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()]++
	}
	seen := make(map[string]bool, len(minecImporters))
	for _, imp := range minecImporters {
		name := imp.Name()
		// (а)
		if !strings.HasPrefix(name, minecNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: ручной `make import STAT=…` попадёт в чужое пространство имён", name, minecNamePrefix)
		}
		// (в)
		if seen[name] {
			t.Errorf("имя импортёра %q встречается в реестре дважды: фильтр выбрал бы произвольную запись", name)
		}
		seen[name] = true
		if registered[name] != 1 {
			t.Errorf("импортёр %q зарегистрирован в chimport.Stats %d раз: `make import STAT=%s` и шаг dagu той же строкой не найдут ровно один", name, registered[name], name)
		}
		// (б)
		if imp.table == sharedMacroSeriesTable {
			want, ok := minecSharedTableOwners[name]
			if !ok {
				t.Errorf("импортёр %q объявил общую таблицу %q, но её занимает только %s", name, imp.table, "minec_urals_price")
				continue
			}
			if imp.table != want {
				t.Errorf("импортёр %q объявил общую таблицу %q, а allowlist ждёт %q", name, imp.table, want)
			}
			continue
		}
		if !strings.HasPrefix(imp.table, minecNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q и не равна %q", imp.table, name, minecNamePrefix, sharedMacroSeriesTable)
		}
	}
	// (д) Префлайта у импортёра нет: артефакт — набор страниц, известность
	// считает гейт по max(date). Реестр обязан это отражать, иначе тест реестра
	// описывал бы чек, которого нет, или наоборот.
	for _, imp := range minecImporters {
		if imp.countPoints != "" {
			t.Errorf("реестр записал импортёру %q запрос чека %q, хотя точка контроля публикации не объявлена", imp.Name(), imp.countPoints)
		}
	}
	if decl := minecUralsImporter.Publication(); decl.CountPoints != "" || decl.Source != "" || decl.Url != "" {
		t.Errorf("импортёр объявил точку контроля публикации %+v: у набора из 104 страниц отпечаток одного URL не отвечает на вопрос «что уже разобрано»", decl)
	}
	// — число импортёров пакета в chimport.Stats равно len(реестр): возврат
	// персонального init() в файл импортёра поймала бы эта проверка.
	total := 0
	for name := range registered {
		if strings.HasPrefix(name, minecNamePrefix) {
			total++
		}
	}
	if total != len(minecImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, а в реестре %d: персональный init() вернулся в файл импортёра", total, minecNamePrefix, len(minecImporters))
	}
	// (г) Расписание в обе стороны: шаг без импортёра — опечатка, импортёр без
	// шага — пропавшая работа при зелёных тестах.
	data, err := os.ReadFile(daguMinecYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguMinecYAML, err)
	}
	scheduled := make(map[string]bool)
	for _, m := range daguStatRef.FindAllStringSubmatch(string(data), -1) {
		scheduled[m[1]] = true
		if !seen[m[1]] {
			t.Errorf("шаг %q из %s не зарегистрирован в реестре пакета", m[1], daguMinecYAML)
		}
	}
	for name := range seen {
		if !scheduled[name] {
			t.Errorf("импортёр %q не попал в %s: работа пропала из расписания при зелёных тестах", name, daguMinecYAML)
		}
	}
}

// TestMinecDaguStepId — id шага равен Name(): dagu показывает шаг оператору, и
// расхождение с именем импортёра затрудняет разбор упавшего прогона.
func TestMinecDaguStepId(t *testing.T) {
	data, err := os.ReadFile(daguMinecYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguMinecYAML, err)
	}
	stepID := regexp.MustCompile(`(?m)^[ \t]*-[ \t]+id:[ \t]*(\S+)[ \t]*$`).FindStringSubmatch(string(data))
	if stepID == nil {
		t.Fatalf("%s: id шага не найден", daguMinecYAML)
	}
	if stepID[1] != minecImporterName {
		t.Errorf("id шага %q, ожидался %q", stepID[1], minecImporterName)
	}
}

// TestMinecDdlAndView — DDL в %s-форме (шаблон подставляет таблицу) и витрина
// объявлена над macro_series литералом.
func TestMinecDdlAndView(t *testing.T) {
	if !strings.Contains(minecMacroDdl, "%s") {
		t.Error("DDL без подстановки имени: шаблон не подставит таблицу и получит «%!(EXTRA …)»")
	}
	if minecMacroView.Name != "v_minec_macro" {
		t.Errorf("имя витрины %q", minecMacroView.Name)
	}
	if len(minecMacroView.Tables) != 1 || minecMacroView.Tables[0] != "macro_series" {
		t.Errorf("витрина объявляет таблицы %v, ожидалась macro_series", minecMacroView.Tables)
	}
	if !strings.Contains(minecMacroView.Select, "macro_series") || !strings.Contains(minecMacroView.Select, "'minec'") {
		t.Errorf("витрина не фильтрует macro_series по source='minec': %s", minecMacroView.Select)
	}
	if minecMacroView.Comment == "" || len(minecMacroView.Columns) == 0 {
		t.Error("витрина без комментариев таблицы/колонок: агент не увидит единиц и привязки даты")
	}
}
