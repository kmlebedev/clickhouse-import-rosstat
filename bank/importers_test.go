package bank

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Ожидаемые значения ниже — литералами, а не константами пакета. Тест, сверяющий sberCSITable с
// самим собой, прошёл бы и после переименования таблицы, то есть ровно на той правке, от которой он
// и должен падать. Префикса пакета у банка нет (решение О1): имена шести таблиц утверждены владельцем
// как есть, поэтому роль префикса играют литеральные наборы ниже.

// approvedImporterNames — утверждённый литеральный набор имён импортёров пакета (спека §3.1). Имя
// импортёра — контракт расписания (CLICKHOUSE_IMPORT_STAT и id шага dagu/bank.yaml), и набор из семи
// имён держит тот же инвариант «имя не попадёт в чужой шаг конвейера», который у cbr/rosstat держит
// префикс. Литеральный, а не ссылка на константу пакета (bankCompanyMetricsName): та сделала бы
// проверку тавтологичной — имя сверялось бы с самим собой.
var approvedImporterNames = map[string]bool{
	"sber_finansovie_rezultaty":     true,
	"sber_consumper_spending_index": true,
	"sber_izmenenie_trat":           true,
	"rus_vtb_group_ifrs":            true,
	"domrf_mortgage":                true,
	"tbank_group_ifrs":              true,
	"bank_company_metrics":          true,
}

// approvedBankTables — таблицы пакета, утверждённые О1 (шесть). Имя таблицы — контракт данных
// (витрина v_bank_macro и агент MCP). Набор отдельный от набора имён: у шести импортёров имена и
// таблицы совпадают, а у bank_company_metrics таблица — общая company_financials. Литеральный, а не
// производный от approvedImporterNames: подмена колонки table на чужое имя из разрешённого набора
// имён (например, bank_company_metrics) обязана падать.
var approvedBankTables = map[string]bool{
	"sber_finansovie_rezultaty":     true,
	"sber_consumper_spending_index": true,
	"sber_izmenenie_trat":           true,
	"rus_vtb_group_ifrs":            true,
	"domrf_mortgage":                true,
	"tbank_group_ifrs":              true,
}

// sharedCompanyFinancialsTable — общая секторная таблица вне набора пакета: bank_company_metrics
// пишет курированные метрики Сбера и ВТБ в company_financials, рядом с метриками Полюса
// (polyus/import.go, views/company.go).
const sharedCompanyFinancialsTable = "company_financials"

// sharedCompanyFinancialsOwners — импортёры, которым разрешено писать в общую company_financials.
// Набор литеральный (не ссылка на константу импортёра: та сделала бы проверку тавтологичной), а
// незнакомец в этой таблице — ровно тот случай, ради которого проверка (б) и существует.
var sharedCompanyFinancialsOwners = map[string]bool{
	"bank_company_metrics": true,
}

// unscheduledTBank — импортёр реестра, которому шаг расписания не положен (О2): ссылка источника
// ведёт на PDF, и упавший шаг останавливал бы всю цепочку. Литерал: проверка «шага нет» ниже —
// явная, и она же страхует обратное направление (г), которое иначе пропускало бы этот импортёр.
const unscheduledTBank = "tbank_group_ifrs"

// foreignDaguSteps — шаги dagu/bank.yaml, которых нет в реестре bankImporters, но стоят в
// банк-цепочке осознанно. company_views принадлежит пакету views/ и пересоздаёт общие витрины
// v_company_* сразу после банковского маппера bank_company_metrics: ALTER period_type расширил
// таблицу, а CREATE VIEW фиксирует тип колонки на момент создания — без прогона company_views
// витрины отдают period_type старого enum. Импортёр в реестр пакета не входит и не должен,
// поэтому проверка (г) пропускает шаг по allowlist'у; в dagu/financial.yaml шаг остаётся
// (идемпотентен, нужен контуру Полюса). Набор литеральный и намеренно узкий: он разрешает
// ровно известный чужой шаг, а не любой шаг вне реестра (опечатка в имени по-прежнему падает).
var foreignDaguSteps = map[string]bool{
	"company_views": true,
}

// expectedPublicationCountPoints — запросы количественного чека (спека §3.1) для импортёров,
// объявивших точку контроля публикации. Литералами, а не ссылкой на константу импортёра: проверка
// «колонка countPoints реестра == объявление самого импортёра» падает ровно на расхождении запросов,
// а сравнение константы с самой собой прошло бы и после правки запроса в объявлении — то есть на том
// самом случае, от которого проверка и защищает.
//
// Набор неполный по построению: у sber_consumper_spending_index и sber_izmenenie_trat префлайта быть
// не должно — HEAD источника отвечает 403, отпечаток версии построить нельзя (спека §4.2); у
// tbank_group_ifrs объявлять чек не на чем (PDF, О2/О4); у bank_company_metrics внешнего артефакта
// нет вовсе.
var expectedPublicationCountPoints = map[string]string{
	"domrf_mortgage":            "SELECT count() FROM domrf_mortgage FINAL",
	"rus_vtb_group_ifrs":        "SELECT count() FROM rus_vtb_group_ifrs FINAL",
	"sber_finansovie_rezultaty": "SELECT count() FROM sber_finansovie_rezultaty FINAL",
}

// publicationDeclarer — то, как тест достаёт объявление импортёра из реестра: метод Publication()
// промотируется из встроенного util.ClickHouseImport, поэтому приведение работает у шести шаблонных
// импортёров. У bank_company_metrics встроенного шаблона нет, приведение не срабатывает — и это
// ожидаемо: внешнего артефакта у него нет, объявления быть не может. Именованный тип, а не литерал
// интерфейса в теле цикла: так проверка читается как контракт, а не как приём приведения.
type publicationDeclarer interface {
	Publication() util.PublicationDecl
}

// daguBankYAML — расписание bank: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT. Путь от каталога пакета
// (тесты идут с cwd=bank/): так же читают фикстуры другие пакеты.
var daguBankYAML = filepath.Join("..", "dagu", "bank.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`. Разбор —
// регуляркой по всему файлу, без YAML-зависимости: тест не должен тянуть в пакетный go.mod
// парсер ради одного ключа. Якоря ^…$ в режиме (?m) обязательны: без них закомментированный
// шаг `# - CLICKHOUSE_IMPORT_STAT=…` считался бы шагом расписания. Хвостовой
// inline-комментарий в строке шага не поддержан: таких строк в dagu/bank.yaml нет, а сам файл
// проверяется `dagu validate`.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// daguIDRef вытаскивает id шага из строки `- id: <name>` тем же разбором регуляркой по всему
// файлу: id — контракт dagu и энфорсера, и закомментированный пример шага не должен попадать
// в проверку уникальности.
var daguIDRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+id:[ \t]*([A-Za-z0-9_]+)[ \t]*$`)

// daguStats возвращает имена из строк `- CLICKHOUSE_IMPORT_STAT=…` в порядке файла.
func daguStats(data []byte) []string {
	matches := daguStatRef.FindAllStringSubmatch(string(data), -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, m[1])
	}
	return names
}

// daguStepIDs возвращает значения `- id: …` в порядке файла.
func daguStepIDs(data []byte) []string {
	matches := daguIDRef.FindAllStringSubmatch(string(data), -1)
	ids := make([]string, 0, len(matches))
	for _, m := range matches {
		ids = append(ids, m[1])
	}
	return ids
}

// duplicateDaguStepIDs возвращает уникальные id, встретившиеся больше одного раза, в порядке
// первого повтора: dagu не различает шаги с одинаковым id, а проверка (г) ищет шаг по имени.
// Один id — одна ошибка энфорсера, сколько бы раз он ни повторился.
func duplicateDaguStepIDs(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	duplicated := make(map[string]bool, len(ids))
	var dups []string
	for _, id := range ids {
		if seen[id] && !duplicated[id] {
			dups = append(dups, id)
			duplicated[id] = true
		}
		seen[id] = true
	}
	return dups
}

// TestDaguStatsIgnoreComments — закомментированный шаг не считается шагом расписания: разбор идёт
// регуляркой по всему файлу, а `# - CLICKHOUSE_IMPORT_STAT=…` — не шаг dagu.
func TestDaguStatsIgnoreComments(t *testing.T) {
	data := []byte("- id: a\n  env:\n    - CLICKHOUSE_IMPORT_STAT=a\n# - CLICKHOUSE_IMPORT_STAT=commented\n")
	if got := daguStats(data); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("daguStats(%q) = %v, want [a]: закомментированный шаг не должен считаться расписанием", data, got)
	}
}

// TestDaguStepIDsDuplicate — парсер отдаёт оба вхождения id в порядке файла, а duplicateDaguStepIDs
// находит дубль: id — контракт dagu и энфорсера, и одинаковые id делают шаги неразличимыми.
func TestDaguStepIDsDuplicate(t *testing.T) {
	data := []byte("- id: a\n- id: a\n")
	got := daguStepIDs(data)
	if !slices.Equal(got, []string{"a", "a"}) {
		t.Fatalf("daguStepIDs(%q) = %v, want [a a]", data, got)
	}
	if dups := duplicateDaguStepIDs(got); !slices.Equal(dups, []string{"a"}) {
		t.Fatalf("duplicateDaguStepIDs(%v) = %v, want [a]", got, dups)
	}
	// Тройной повтор — по-прежнему один элемент: энфорсер печатает одну ошибку на id.
	if dups := duplicateDaguStepIDs([]string{"b", "b", "b"}); !slices.Equal(dups, []string{"b"}) {
		t.Fatalf("duplicateDaguStepIDs([b b b]) = %v, want [b]: дубль должен возвращаться один раз", dups)
	}
}

// TestBankImportersNamedAndRegistered держит инварианты реестра bankImporters — единственной точки,
// из которой импортёры пакета попадают в chimport.Stats.
//
// Проверяется именно реестр, а не Import напрямую: остальные тесты пакета зовут парсеры и Import, и
// потому проходили бы, даже если импортёр не зарегистрирован и недостижим ни из `make import`, ни из
// расписания. Регистрация — единственное, что видит оператор.
//
//   - (а′) Name() каждого импортёра — из утверждённого набора семи имён (О1): иначе ручной запуск
//     CLICKHOUSE_IMPORT_STAT=<name> попадёт в чужой шаг конвейера (имена пакета уникальны в
//     репозитории);
//   - (б) таблица — из утверждённого набора шести, либо общая company_financials и только у
//     bank_company_metrics (литеральный allowlist владельцев, как macro_series у cbr_): смена таблицы
//     под импортёром увела бы витрину v_bank_macro к чужим данным, а чужой импортёр в
//     company_financials затёр бы метрики Полюса;
//   - (в) имена уникальны и лежат в chimport.Stats: дубль имени в фильтре `make import STAT=...`
//     выбрал бы произвольный из двух импортёров;
//   - (г) шесть банковских шагов dagu/bank.yaml (пять импортёров и bank_company_metrics) есть в
//     реестре, а каждый импортёр реестра — в расписании (кроме tbank_group_ifrs, О2); единственный
//     чужой шаг company_views пропускается литеральным allowlist'ом foreignDaguSteps: он пересоздаёт
//     общие витрины v_company_* сразу после банковского маппера; плюс явная проверка «шага
//     tbank_group_ifrs в расписании нет» (О2): пропавший шаг не заметил бы больше ни один тест —
//     реестр верен сам по себе, а витрина v_bank_macro и метрики банков в company_financials просто
//     перестали бы обновляться; вернувшийся же шаг Т-Банка уронил бы цепочку на PDF-ссылке до
//     починки источника; заодно проверяется уникальность id шагов: дубль делает шаги неразличимыми
//     для dagu; сама проверка (г) собирает множество шагов из CLICKHOUSE_IMPORT_STAT, а id
//     проверяются только на уникальность;
//   - (д) точка контроля публикации (префлайт) объявлена ровно у ожидаемых трёх импортёров и
//     запросом из expectedPublicationCountPoints: колонка countPoints реестра сверяется с
//     объявлением самого импортёра (метод Publication() встроенного util.ClickHouseImport), а у
//     импортёров вне ожидаемого набора объявления нет вовсе — забытое объявление молча выключило бы
//     префлайт, а лишнее у недельных наборов Сбера дало бы HEAD на каждом прогоне без шанса на
//     no-op;
//   - (¬) число импортёров пакета в chimport.Stats (по утверждённому набору имён) равно
//     len(bankImporters): больше — кто-то вернул персональный init()-append в файл импортёра,
//     меньше — запись забыта при консолидации; ровно одна точка регистрации на пакет.
func TestBankImportersNamedAndRegistered(t *testing.T) {
	if len(bankImporters) == 0 {
		t.Fatal("реестр bankImporters пуст: ни один импортёр пакета не зарегистрирован")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(bankImporters))
	for _, imp := range bankImporters {
		name := imp.Name()

		// (а′)
		if !approvedImporterNames[name] {
			t.Errorf("имя импортёра %q вне утверждённого набора семи (О1, спека §3.1): Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/bank.yaml", name)
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
		if imp.table == sharedCompanyFinancialsTable {
			if !sharedCompanyFinancialsOwners[name] {
				t.Errorf("импортёр %q объявил общую таблицу %q: её занимает только bank_company_metrics (метрики SBER/VTBR рядом с метриками Полюса)", name, sharedCompanyFinancialsTable)
			}
			continue
		}
		if !approvedBankTables[imp.table] {
			t.Errorf("таблица %q импортёра %q вне утверждённого набора шести и не равна %q", imp.table, name, sharedCompanyFinancialsTable)
		}
		// У шести шаблонных импортёров Name() — это TableName встроенного util.ClickHouseImport.
		// Значит, литерал в колонке table обязан с ним совпадать: иначе реестр описывает не ту
		// таблицу, в которую импортёр пишет (переименовали константу — обновили литерал).
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() (TableName импортёра) — %q", name, imp.table, name)
		}
	}

	// (д) Точка контроля публикации: колонка countPoints реестра — литерал, как и колонка table, и
	// сверяется с объявлением самого импортёра. Расхождение между ними и есть та правка, от которой
	// проверка должна падать (прецедент — table): реестр описывал бы префлайт, которого у импортёра
	// нет, или наоборот.
	declared := make(map[string]bool, len(expectedPublicationCountPoints))
	for _, imp := range bankImporters {
		name := imp.Name()
		decl, declares := imp.ImportStat.(publicationDeclarer)
		var pub util.PublicationDecl
		if declares {
			pub = decl.Publication()
		}
		want, expected := expectedPublicationCountPoints[name]
		if !expected {
			// Импортёр вне ожидаемого набора: префлайта нет ни в реестре, ни в объявлении. Он и не
			// должен появиться «на всякий случай»: у недельных наборов Сбера HEAD недоступен, у
			// tbank_group_ifrs импорт падает до разбора, а у bank_company_metrics внешнего артефакта
			// нет вовсе.
			if imp.countPoints != "" {
				t.Errorf("реестр записал импортёру %q запрос чека %q, хотя префлайт ему не положен", name, imp.countPoints)
			}
			if declares && pub.CountPoints != "" {
				t.Errorf("импортёр %q объявил точку контроля публикации (%q), хотя префлайт ему не положен", name, pub.CountPoints)
			}
			continue
		}
		declared[name] = true

		if !declares {
			t.Errorf("импортёр %q объявил префлайт в реестре, но не имеет метода Publication(): объявление живёт рядом с импортёром и не может быть только в реестре", name)
			continue
		}

		// (а) Литерал реестра против объявления импортёра — разные источники значения: реестр
		// правит человек, объявление живёт рядом с импортёром.
		if pub.CountPoints != imp.countPoints {
			t.Errorf("импортёр %q объявил запрос чека %q, а реестр записал %q", name, pub.CountPoints, imp.countPoints)
		}
		// (б) Оба обязаны быть дословно равны спеке §3.1: FINAL — часть утверждения «эти точки
		// мои» (без него count() считал бы несмёрженные части).
		if imp.countPoints != want {
			t.Errorf("реестр записал импортёру %q запрос чека %q, а спека §3.1 — %q", name, imp.countPoints, want)
		}
		// (в) Ключ версии — Source объявления: у импортёров с одной таблицей это TableName, и сдвиг
		// ключа развёл бы одну версию файла на две строки source_publications.
		if pub.Source != name {
			t.Errorf("объявление импортёра %q даёт ключ версии %q, а импортёр называется %q: `make import STAT=%s` и dagu-шаг ищут его по имени", name, pub.Source, name, name)
		}
	}
	// Обратное направление: карта не должна описывать импортёра, которого в реестре нет, — иначе
	// ожидание §3.1 осталось бы без предъявителя, а проверки (а)–(в) над ним не выполнялись бы вовсе.
	for name := range expectedPublicationCountPoints {
		if !declared[name] {
			t.Errorf("в реестре bankImporters нет импортёра %q, для которого спека §3.1 ожидает объявление префлайта", name)
		}
	}

	// (¬) «Ровно те же»: число имён утверждённого набора в Stats не может быть больше реестра
	// (кто-то вернул персональный init()-append в файл импортёра) или меньше (запись забыта при
	// консолидации) — ровно одна точка регистрации на пакет.
	bankRegistered := 0
	for _, stat := range chimport.Stats {
		if approvedImporterNames[stat.Name()] {
			bankRegistered++
		}
	}
	if bankRegistered != len(bankImporters) {
		t.Errorf("в chimport.Stats %d импортёров из утверждённого набора имён, в реестре %d: регистрация должна быть только в bank/importers.go", bankRegistered, len(bankImporters))
	}

	// (г)
	data, err := os.ReadFile(daguBankYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguBankYAML, err)
	}
	ids := daguStepIDs(data)
	if len(ids) == 0 {
		t.Fatalf("в dagu/bank.yaml не найдено ни одной строки `- id:`: разбор сломан или шаги переписаны")
	}
	for _, dup := range duplicateDaguStepIDs(ids) {
		t.Errorf("id шага dagu/bank.yaml %q встречается дважды: dagu не различает шаги с одинаковым id", dup)
	}
	stats := daguStats(data)
	if len(stats) == 0 {
		t.Fatalf("в dagu/bank.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=: разбор сломан или шаги переписаны")
	}
	scheduled := make(map[string]bool, len(stats))
	for _, name := range stats {
		scheduled[name] = true
		if !seen[name] && !foreignDaguSteps[name] {
			t.Errorf("шаг dagu/bank.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре bankImporters: расписание позовёт импортёр, которого нет", name)
		}
	}
	// Обратное направление цикл не ловит: реестр верен сам по себе и без шага расписания. Пропавший
	// шаг не заметил бы больше ни один тест: витрина v_bank_macro и метрики банков в
	// company_financials оставались бы необновляемыми при всех зелёных тестах. Единственное
	// исключение — tbank_group_ifrs (О2): его шага в расписании быть не должно, и это проверяется
	// явно ниже.
	for name := range seen {
		if name == unscheduledTBank {
			continue
		}
		if !scheduled[name] {
			t.Errorf("в dagu/bank.yaml нет шага CLICKHOUSE_IMPORT_STAT=%s: импортёр зарегистрирован, но по расписанию не запускается", name)
		}
	}
	// Отдельная явная проверка «шага tbank_group_ifrs в расписании нет» (О2): без неё возврат шага
	// прошёл бы незамеченным (обратное направление его пропускает), а цепочка снова упала бы на
	// первом же шаге Т-Банка. Условие возврата шага — в комментарии dagu/bank.yaml: источник починен
	// — шаг вернуть, эту проверку обновить.
	if scheduled[unscheduledTBank] {
		t.Errorf("в dagu/bank.yaml есть шаг CLICKHOUSE_IMPORT_STAT=%s: Т-Банк вне расписания до починки источника (О2); вернули шаг — обновите эту проверку", unscheduledTBank)
	}
}
