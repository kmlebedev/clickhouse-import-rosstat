package fns

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// fnsNamePrefix — пространство имён пакета: Name() он же CLICKHOUSE_IMPORT_STAT, он
// же id шага dagu/fns.yaml.
const fnsNamePrefix = "fns_"

// daguFnsYAML — расписание пакета.
var daguFnsYAML = filepath.Join("..", "dagu", "fns.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`;
// регулярка вместо YAML-парсера: тест не тянет зависимость ради одного ключа, а
// закомментированный шаг шагом не считается.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// TestFnsImportersPrefixedAndRegistered — энфорсер реестра (стандарт §8.3):
// префикс имени, таблица = имя импортёра (своя таблица, не общая), уникальность и
// регистрация, покрытие dagu в обе стороны, отсутствие объявления точки контроля
// публикации. Каждая проверка ловит правку, которая иначе прошла бы молча: импортёр
// в чужом пространстве имён, шаг расписания без импортёра, импортёр без шага и
// префлайт «на всякий случай», которого у набора архивов быть не может.
func TestFnsImportersPrefixedAndRegistered(t *testing.T) {
	if len(fnsImporters) == 0 {
		t.Fatal("реестр fnsImporters пуст: ни один импортёр пакета не зарегистрирован")
	}
	registered := make(map[string]int, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()]++
	}
	seen := make(map[string]bool, len(fnsImporters))
	for _, imp := range fnsImporters {
		name := imp.Name()
		if !strings.HasPrefix(name, fnsNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: `make import STAT=…` попало бы в чужое пространство имён", name, fnsNamePrefix)
		}
		if seen[name] {
			t.Errorf("имя %q в реестре дважды: фильтр выбрал бы произвольную запись", name)
		}
		seen[name] = true
		if registered[name] != 1 {
			t.Errorf("импортёр %q зарегистрирован в chimport.Stats %d раз", name, registered[name])
		}
		// Таблица — своя и равна имени: стандарт §7.2. Литерал реестра сверяется с
		// объявлением импортёра, а не с константой пакета.
		if imp.table != name {
			t.Errorf("таблица %q импортёра %q: у импортёра собственной таблицы Name() обязан совпадать с TableName", imp.table, name)
		}
		if !strings.HasPrefix(imp.table, fnsNamePrefix) {
			t.Errorf("таблица %q без префикса %q", imp.table, fnsNamePrefix)
		}
		if imp.countPoints != "" {
			t.Errorf("реестр записал импортёру %q запрос чека %q, хотя точка контроля публикации не объявлена", name, imp.countPoints)
		}
		bound, ok := imp.ImportStat.(*fnsBudgetRevenue)
		if !ok {
			t.Errorf("импортёр %q — не *fnsBudgetRevenue: реестр описывает другой объект", name)
			continue
		}
		if bound.TableName != imp.table {
			t.Errorf("импортёр пишет в таблицу %q, а реестр объявил %q", bound.TableName, imp.table)
		}
		if decl := bound.Publication(); decl.CountPoints != "" || decl.Source != "" || decl.Url != "" {
			t.Errorf("импортёр %q объявил точку контроля %+v: у набора месячных архивов отпечаток одного URL не отвечает на вопрос, что уже разобрано", name, decl)
		}
	}
	total := 0
	for name := range registered {
		if strings.HasPrefix(name, fnsNamePrefix) {
			total++
		}
	}
	if total != len(fnsImporters) {
		t.Errorf("в chimport.Stats %d имён с префиксом %q, а в реестре %d: персональный init() вернулся в файл импортёра",
			total, fnsNamePrefix, len(fnsImporters))
	}

	data, err := os.ReadFile(daguFnsYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguFnsYAML, err)
	}
	scheduled := make(map[string]bool)
	for _, match := range daguStatRef.FindAllStringSubmatch(string(data), -1) {
		scheduled[match[1]] = true
		if !seen[match[1]] {
			t.Errorf("шаг %q из %s не зарегистрирован в реестре пакета", match[1], daguFnsYAML)
		}
	}
	for name := range seen {
		if !scheduled[name] {
			t.Errorf("импортёр %q не попал в %s: работа пропала из расписания при зелёных тестах", name, daguFnsYAML)
		}
	}
}

// TestFnsDaguStepId — id шага равен Name(): dagu показывает шаг оператору, и
// расхождение с именем импортёра затрудняет разбор упавшего прогона.
func TestFnsDaguStepId(t *testing.T) {
	data, err := os.ReadFile(daguFnsYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguFnsYAML, err)
	}
	stepID := regexp.MustCompile(`(?m)^[ \t]*-[ \t]+id:[ \t]*(\S+)[ \t]*$`).FindStringSubmatch(string(data))
	if stepID == nil {
		t.Fatalf("%s: id шага не найден", daguFnsYAML)
	}
	if stepID[1] != fnsImporterName {
		t.Errorf("id шага %q, ожидался %q", stepID[1], fnsImporterName)
	}
	if !strings.Contains(string(data), "retry_policy") {
		t.Errorf("%s: у шага нет retry_policy: одна сетевая осечка ФНС выронила бы шаг из расписания", daguFnsYAML)
	}
}

// TestFnsDdlAndViews — DDL в %s-форме, канонические колонки и тип значения; витрины
// объявлены над своей таблицей, с комментариями таблицы и каждой колонки, а помесячная
// считает разность накопленных и отдаёт NULL на пропуске.
func TestFnsDdlAndViews(t *testing.T) {
	if len(fnsTableDdl) != 1 {
		t.Fatalf("DDL стейтментов %d, ожидался один", len(fnsTableDdl))
	}
	ddl := fnsTableDdl[0]
	if !strings.Contains(ddl, "%s") {
		t.Error("DDL без подстановки имени: шаблон подставил бы таблицу криво")
	}
	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS") || !strings.Contains(ddl, "ReplacingMergeTree") {
		t.Error("DDL вне канона правил: IF NOT EXISTS + ReplacingMergeTree")
	}
	for _, column := range []string{"date Date", "region LowCardinality(String)", "region_kind LowCardinality(String)",
		"series LowCardinality(String)", "value Float64"} {
		if !strings.Contains(ddl, column) {
			t.Errorf("DDL не содержит %q — расхождение с канонической схемой ARCHITECTURE.md §6.2", column)
		}
	}
	if strings.Contains(ddl, "Float32") {
		t.Error("значение в Float32: правило 5 AGENTS.md требует Float64 для новых денежных величин")
	}

	if fnsBudgetRevenueView.Name != "v_fns_budget_revenue" || fnsBudgetRevenueMonthlyView.Name != "v_fns_budget_revenue_monthly" {
		t.Errorf("имена витрин %q и %q: витрина обязана быть v_fns_*", fnsBudgetRevenueView.Name, fnsBudgetRevenueMonthlyView.Name)
	}
	for _, view := range []util.View{fnsBudgetRevenueView, fnsBudgetRevenueMonthlyView} {
		if len(view.Tables) != 1 || view.Tables[0] != fnsTable {
			t.Errorf("витрина %q объявляет таблицы %v, ожидалась %q", view.Name, view.Tables, fnsTable)
		}
		if view.Comment == "" || len(view.Columns) == 0 {
			t.Errorf("витрина %q без комментариев таблицы или колонок: агент не увидит единиц и привязки даты", view.Name)
		}
		if !strings.Contains(view.Select, fnsTable) || !strings.Contains(view.Select, "FINAL") {
			t.Errorf("витрина %q не читает %s FINAL: несмёрженные части ревизий попали бы в ответ", view.Name, fnsTable)
		}
	}
	if !strings.Contains(fnsBudgetRevenueView.Comment, "накопленным итогом") {
		t.Error("витрина как опубликовано не предупреждает, что значение накопленное")
	}
	monthly := fnsBudgetRevenueMonthlyView.Select
	if !strings.Contains(monthly, "lagInFrame") || !strings.Contains(monthly, "subtractMonths") {
		t.Error("помесячная витрина не считает разность накопленных оконной функцией")
	}
	if !strings.Contains(monthly, "NULL") {
		t.Error("помесячная витрина не отдаёт NULL на пропуске предыдущего месяца: разрыв стал бы нулём")
	}
	if strings.Contains(monthly, "toDate(0)") {
		t.Error("в определении витрины остался служебный столбец")
	}
	for _, column := range []string{"source", "series", "region", "region_kind", "date", "value"} {
		if _, ok := fnsBudgetRevenueMonthlyView.Columns[column]; !ok {
			t.Errorf("у витрины %s нет комментария колонки %q", fnsBudgetRevenueMonthlyView.Name, column)
		}
	}
}

// TestFnsSeriesNamesAreStableAndUnique — имена рядов не зависят от заголовков
// источника и не дублируются: один и тот же код+уровень встречается в реестре колонок
// ровно один раз.
func TestFnsSeriesNamesAreStableAndUnique(t *testing.T) {
	series := map[string]bool{}
	columns := map[string]bool{}
	for _, row := range fnsSeriesRows {
		name := row.series()
		if !strings.HasPrefix(name, row.stub+"_") {
			t.Errorf("ряд %q не начинается с основы %q", name, row.stub)
		}
		if series[name] {
			t.Errorf("имя ряда %q объявлено дважды", name)
		}
		series[name] = true
		key := fnsColumnKey(row.code, row.suffix)
		if columns[key] {
			t.Errorf("колонка %s объявлена дважды", key)
		}
		columns[key] = true
	}
	if len(fnsSeriesRows) != 47 {
		t.Errorf("объявлено %d рядов, ожидалось 47 (спека §3.4)", len(fnsSeriesRows))
	}
	if len(series) != len(fnsSeriesRows) {
		t.Errorf("уникальных имён %d, а рядов %d", len(series), len(fnsSeriesRows))
	}
}
