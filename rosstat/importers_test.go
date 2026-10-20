package rosstat

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Ожидаемые значения ниже — литералами, а не константами пакета. Тест, сверяющий
// rosstatIPCMesTable с самим собой, прошёл бы и после переименования таблицы в имя вне
// rosstat_-пространства, то есть ровно на той правке, от которой он и должен падать.
const (
	// rosstatNamePrefix — пространство имён импортёров и таблиц пакета rosstat. Имя
	// импортёра — контракт расписания (CLICKHOUSE_IMPORT_STAT и id шага
	// dagu/rosstat.yaml), имя таблицы — контракт данных (v_rosstat_macro,
	// v_model_inputs и агент MCP).
	rosstatNamePrefix = "rosstat_"
	// sharedMacroSeriesTable — общая таблица вне rosstat_-пространства. У пакета rosstat
	// её нет (в отличие от cbr/macro_series): все восемь импортёров владеют своей таблицей,
	// поэтому исключение пусто и проверка (б) сводится к «таблица с префиксом» и
	// «Name() совпадает с именем таблицы».
	sharedMacroSeriesTable = ""
)

// expectedPublicationCountPoints — запросы количественного чека (спека §3.5) для
// импортёров, объявивших точку контроля публикации. Литералами, а не ссылкой на
// константу импортёра: проверка «колонка countPoints реестра == объявление самого
// импортёра» падает ровно на расхождении запросов, а сравнение константы с самой
// собой прошло бы и после правки запроса в объявлении — то есть на том самом
// случае, от которого проверка и защищает.
//
// Набор неполный по построению: у rosstat_ipc_weeks префлайта нет и быть не должно
// (Nedel_ipc.xlsx перезаписывается по тому же URL в недельном темпе, равном темпу
// шага расписания, спека §3.7), а у остальных семи файл обновляется реже прогонов.
var expectedPublicationCountPoints = map[string]string{
	"rosstat_ipc_mes":             "SELECT count() FROM rosstat_ipc_mes FINAL",
	"rosstat_ipc_weeks_bulletins": "SELECT count() FROM rosstat_ipc_weeks_bulletins FINAL",
	"rosstat_labour_force_mes":    "SELECT count() FROM rosstat_labour_force_mes FINAL",
	"rosstat_okved2":              "SELECT count() FROM rosstat_okved2 FINAL",
	"rosstat_ppi_ved":             "SELECT count() FROM rosstat_ppi_ved FINAL",
	"rosstat_salaries_mes":        "SELECT count() FROM rosstat_salaries_mes FINAL",
	"rosstat_vvp_kvartal":         "SELECT count() FROM rosstat_vvp_kvartal FINAL",
}

// publicationDeclarer — то, как тест достаёт объявление импортёра из реестра: метод
// Publication() промотируется из встроенного util.ClickHouseImport, поэтому приведение
// работает для всех восьми импортёров пакета. У семи (rosstat_ipc_mes,
// rosstat_salaries_mes, rosstat_vvp_kvartal, rosstat_labour_force_mes,
// rosstat_ipc_weeks_bulletins, rosstat_okved2, rosstat_ppi_ved) объявление непустое,
// у rosstat_ipc_weeks — нулевое: точке
// контроля публикации у недельного файла места нет (§9.3). Именованный тип, а не литерал
// интерфейса в теле цикла: так проверка читается как контракт, а не как приём приведения.
type publicationDeclarer interface {
	Publication() util.PublicationDecl
}

// daguRosstatYAML — расписание rosstat: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT. Путь от
// каталога пакета (тесты идут с cwd=rosstat/): так же читают фикстуры другие пакеты.
var daguRosstatYAML = filepath.Join("..", "dagu", "rosstat.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`. Разбор —
// регуляркой по всему файлу, без YAML-зависимости: тест не должен тянуть в пакетный go.mod
// парсер ради одного ключа. Якоря ^…$ в режиме (?m) обязательны: без них закомментированный
// шаг `# - CLICKHOUSE_IMPORT_STAT=…` считался бы шагом расписания.
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// TestDaguStatsIgnoreComments — закомментированный шаг не считается шагом расписания: разбор идёт
// регуляркой по всему файлу, а `# - CLICKHOUSE_IMPORT_STAT=…` — не шаг dagu.
func TestDaguStatsIgnoreComments(t *testing.T) {
	data := []byte("- id: a\n  env:\n    - CLICKHOUSE_IMPORT_STAT=a\n# - CLICKHOUSE_IMPORT_STAT=commented\n")
	got := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(got) != 1 || got[0][1] != "a" {
		t.Fatalf("разбор %q дал %v, want один шаг [a]: закомментированный шаг не должен считаться расписанием", data, got)
	}
}

// TestRosstatImportersPrefixedAndRegistered держит инварианты реестра rosstatImporters —
// единственной точки, из которой импортёры пакета попадают в chimport.Stats.
//
// Проверяется именно реестр, а не Import напрямую: остальные тесты пакета зовут
// парсеры, и потому проходили бы, даже если импортёр не зарегистрирован и
// недостижим ни из `make import`, ни из расписания. Регистрация — единственное,
// что видит оператор.
//
//   - (а) Name() каждого импортёра начинается с rosstat_ — иначе ручной запуск
//     CLICKHOUSE_IMPORT_STAT=<table> попадёт в чужой шаг конвейера (префикс rosstat_
//     в репозитории уникален за пределами пакета);
//   - (б) таблица — rosstat_-префикс и совпадает с Name(): общих таблиц вне префикса
//     пакет не занимает, а смена таблицы под импортёром увела бы витрины
//     v_rosstat_macro и v_model_inputs к чужим данным;
//   - (в) имена уникальны и лежат в chimport.Stats: дубль имени в фильтре
//     `make import STAT=...` выбрал бы произвольный из двух импортёров;
//   - (г) состав шагов dagu/rosstat.yaml и состав реестра совпадают в обе стороны:
//     расписание — единственный продакшн-вызов, и его опечатка или пропавший шаг
//     иначе не видны ничем; у rosstat_ipc_weeks нет даже объявления префлайта,
//     которое могло бы заметить исчезнувший шаг;
//   - (д) точка контроля публикации (префлайт) объявлена ровно у ожидаемых семи
//     импортёров и запросом из expectedPublicationCountPoints: колонка countPoints
//     реестра сверяется с объявлением самого импортёра (метод Publication()
//     встроенного util.ClickHouseImport), а у rosstat_ipc_weeks объявления нет ни
//     в реестре, ни у импортёра.
func TestRosstatImportersPrefixedAndRegistered(t *testing.T) {
	if len(rosstatImporters) == 0 {
		t.Fatal("реестр rosstatImporters пуст: ни один импортёр пакета не зарегистрирован")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(rosstatImporters))
	for _, imp := range rosstatImporters {
		name := imp.Name()

		// (а)
		if !strings.HasPrefix(name, rosstatNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/rosstat.yaml", name, rosstatNamePrefix)
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
			t.Errorf("импортёр %q не объявил таблицу: общей таблицы вне префикса %q у пакета нет — все восемь импортёров владеют своей", name, rosstatNamePrefix)
			continue
		}
		if !strings.HasPrefix(imp.table, rosstatNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q", imp.table, name, rosstatNamePrefix)
		}
		// У всех восьми импортёров Name() — это TableName встроенного
		// util.ClickHouseImport. Значит, литерал в колонке table обязан совпадать с
		// Name(): иначе реестр описывает не ту таблицу, в которую импортёр пишет
		// (переименовали константу — обновили литерал).
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() (TableName импортёра) — %q", name, imp.table, name)
		}
	}

	// (д) Точка контроля публикации: колонка countPoints реестра — литерал, как и
	// колонка table, и сверяется с объявлением самого импортёра. Расхождение между
	// ними и есть та правка, от которой проверка должна падать (прецедент — table):
	// реестр описывал бы префлайт, которого у импортёра нет, или наоборот.
	declared := make(map[string]bool, len(expectedPublicationCountPoints))
	for _, imp := range rosstatImporters {
		name := imp.Name()
		decl, declares := imp.ImportStat.(publicationDeclarer)
		var pub util.PublicationDecl
		if declares {
			pub = decl.Publication()
		}
		want, expected := expectedPublicationCountPoints[name]
		if !expected {
			// Импортёр вне ожидаемого набора: префлайта нет ни в реестре, ни в
			// объявлении. rosstat_ipc_weeks не должен обзавестись им «на всякий
			// случай»: его файл перезаписывается по тому же URL в недельном темпе,
			// равном темпу шага, — HEAD был бы чистыми накладными расходами.
			if imp.countPoints != "" {
				t.Errorf("реестр записал импортёру %q запрос чека %q, хотя префлайт ему не положен", name, imp.countPoints)
			}
			// Нулевое объявление rosstat_ipc_weeks — не отсутствие метода, а осознанно
			// незаполненные PublicationCountPoints/PublicationSource: приведение к
			// publicationDeclarer срабатывает (метод промотируется встроенным шаблоном), а
			// Publication() даёт нулевое значение. Импортёр вне набора не должен обзавестись
			// префлайтом «на всякий случай».
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

		// (а) Литерал реестра против объявления импортёра — разные источники
		// значения: реестр правит человек, объявление живёт рядом с импортёром.
		if pub.CountPoints != imp.countPoints {
			t.Errorf("импортёр %q объявил запрос чека %q, а реестр записал %q", name, pub.CountPoints, imp.countPoints)
		}
		// (б) Оба обязаны быть дословно равны спеке §3.5: FINAL — часть утверждения
		// «эти точки мои» (без него count() считал бы несмёрженные части).
		if imp.countPoints != want {
			t.Errorf("реестр записал импортёру %q запрос чека %q, а спека §3.5 — %q", name, imp.countPoints, want)
		}
		// (в) Ключ версии — Source объявления: у импортёров с одной таблицей это
		// TableName, и сдвиг ключа развёл бы одну версию файла на две строки
		// source_publications.
		if pub.Source != name {
			t.Errorf("объявление импортёра %q даёт ключ версии %q, а импортёр называется %q: `make import STAT=%s` и dagu-шаг ищут его по имени", name, pub.Source, name, name)
		}
	}
	// Обратное направление: карта не должна описывать импортёра, которого в реестре
	// нет, — иначе ожидание §3.5 осталось бы без предъявителя, а проверки (а)–(в)
	// над ним не выполнялись бы вовсе.
	for name := range expectedPublicationCountPoints {
		if !declared[name] {
			t.Errorf("в реестре rosstatImporters нет импортёра %q, для которого спека §3.5 ожидает объявление префлайта", name)
		}
	}

	// «Ровно те же»: число rosstat_-имён в Stats не может быть больше реестра
	// (кто-то вернул персональный init()-append в файл импортёра) или меньше
	// (запись забыта при консолидации) — ровно одна точка регистрации на пакет.
	rosstatRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), rosstatNamePrefix) {
			rosstatRegistered++
		}
	}
	if rosstatRegistered != len(rosstatImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d: регистрация должна быть только в rosstat/importers.go", rosstatRegistered, rosstatNamePrefix, len(rosstatImporters))
	}

	// (г)
	data, err := os.ReadFile(daguRosstatYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguRosstatYAML, err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в dagu/rosstat.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=: разбор сломан или шаги переписаны")
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг dagu/rosstat.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре rosstatImporters: расписание позовёт импортёр, которого нет", name)
		}
	}
	// Обратное направление: каждый импортёр реестра обязан быть шагом расписания.
	// Пропавший шаг не заметил бы больше ни один тест: реестр верен сам по себе, а
	// витрины v_rosstat_macro и v_model_inputs оставались бы пустыми при зелёном
	// пакете. rosstat_ipc_weeks здесь не исключение, а худший случай: у него и колонка
	// countPoints в реестре пуста, и объявления префлайта у импортёра нет, с которыми
	// состав расписания можно было бы сверить отдельно.
	for name := range seen {
		if !scheduled[name] {
			t.Errorf("в dagu/rosstat.yaml нет шага CLICKHOUSE_IMPORT_STAT=%s: импортёр зарегистрирован, но по расписанию не запускается", name)
		}
	}
}
