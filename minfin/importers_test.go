package minfin

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
// minfinOilGasSource с самим собой, прошёл бы и после переименования source, то есть
// ровно на той правке, от которой он и должен падать.
const (
	// minfinNamePrefix — пространство имён импортёров и таблиц пакета minfin. Имя
	// импортёра — контракт расписания (CLICKHOUSE_IMPORT_STAT и id шага
	// dagu/minfin.yaml), имя таблицы — контракт данных (v_minfin_budget и агент MCP).
	minfinNamePrefix = "minfin_"
)

// expectedMinfinPublicationCountPoints — запросы количественного чека (стандарт §9)
// для импортёров, объявивших точку контроля публикации. Литералами, а не ссылкой на
// константу импортёра: проверка «колонка countPoints реестра == объявление самого
// импортёра» падает ровно на расхождении запросов, а сравнение константы с самой
// собой прошло бы и после правки запроса в объявлении.
//
// Набор неполный по построению: у двух legacy-импортёров префлайта нет — файл
// обновляется с той же частотой, что и шаг, а адрес вычисляется legacy-транспортом.
var expectedMinfinPublicationCountPoints = map[string]string{
	"minfin_oil_gas_mes": "SELECT count() FROM minfin_oil_gas_mes FINAL",
}

// minfinPublicationDeclarer — то, как тест достаёт объявление импортёра из реестра:
// метод Publication() промотируется из встроенного util.ClickHouseImport, поэтому
// приведение работает и для голого *util.ClickHouseImport, и для обёрток со
// встроенным значением.
type minfinPublicationDeclarer interface {
	Publication() util.PublicationDecl
}

// daguMinfinYAML — расписание Минфина: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT. Путь
// от каталога пакета (тесты идут с cwd=minfin/): так же читают фикстуры другие пакеты.
var daguMinfinYAML = filepath.Join("..", "dagu", "minfin.yaml")

// daguMinfinStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
// Разбор — регуляркой по всему файлу, без YAML-зависимости: тест не должен тянуть в
// пакетный go.mod парсер ради одного ключа (стандарт §8.4). Якоря ^…$ в режиме (?m)
// обязательны: без них закомментированный шаг считался бы шагом расписания.
var daguMinfinStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

// TestMinfinImportersPrefixedAndRegistered держит инварианты реестра minfinImporters —
// единственной точки, из которой импортёры пакета попадают в chimport.Stats.
//
//   - (а) Name() каждого импортёра начинается с minfin_ — иначе ручной запуск
//     CLICKHOUSE_IMPORT_STAT=<table> попадёт в чужой шаг конвейера;
//   - (б) таблица — minfin_-префикс и совпадает с Name(): смена таблицы под
//     импортёром увела бы витрину v_minfin_budget к чужим данным;
//   - (в) имена уникальны и лежат в chimport.Stats: дубль имени в фильтре
//     `make import STAT=...` выбрал бы произвольный из двух импортёров;
//   - (г) каждая строка CLICKHOUSE_IMPORT_STAT= из dagu/minfin.yaml есть в реестре
//     (опечатка расписания иначе не видна ничем), и обратно — каждый импортёр
//     реестра есть в расписании (пропавший шаг оставил бы витрину без обновления
//     при всех зелёных тестах);
//   - (д) точка контроля публикации объявлена ровно у minfin_oil_gas_mes: её запрос
//     совпадает с литералом реестра и с ожиданием выше, а у остальных импортёров
//     объявления нет вовсе.
func TestMinfinImportersPrefixedAndRegistered(t *testing.T) {
	if len(minfinImporters) == 0 {
		t.Fatal("реестр minfinImporters пуст: ни один импортёр пакета не зарегистрирован")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(minfinImporters))
	for _, imp := range minfinImporters {
		name := imp.Name()

		// (а)
		if !strings.HasPrefix(name, minfinNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/minfin.yaml", name, minfinNamePrefix)
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
		if !strings.HasPrefix(imp.table, minfinNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q", imp.table, name, minfinNamePrefix)
		}
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() (TableName импортёра) — %q", name, imp.table, name)
		}

		// (д)
		declarer, ok := imp.ImportStat.(minfinPublicationDeclarer)
		if !ok {
			t.Errorf("импортёр %q не объявляет Publication(): реестр не может сверить точку контроля публикации", name)
			continue
		}
		decl := declarer.Publication()
		want, expected := expectedMinfinPublicationCountPoints[name]
		if !expected {
			// Импортёр вне ожидаемого набора: префлайта нет ни в реестре, ни в
			// объявлении. Лишнее объявление дало бы HEAD на каждом прогоне без шанса
			// на no-op.
			if imp.countPoints != "" {
				t.Errorf("реестр записал импортёру %q запрос чека %q, хотя префлайт ему не положен", name, imp.countPoints)
			}
			if decl.CountPoints != "" {
				t.Errorf("импортёр %q объявил точку контроля публикации (%q), хотя префлайт ему не положен", name, decl.CountPoints)
			}
			continue
		}

		// Литерал реестра против объявления импортёра — разные источники значения:
		// реестр правит человек, объявление живёт рядом с импортёром.
		if decl.CountPoints != imp.countPoints {
			t.Errorf("импортёр %q объявил запрос чека %q, а реестр записал %q", name, decl.CountPoints, imp.countPoints)
		}
		if imp.countPoints != want {
			t.Errorf("реестр записал импортёру %q запрос чека %q, а ожидание — %q", name, imp.countPoints, want)
		}
		// Ключ версии — Source объявления, а не TableName: только так строка
		// source_publications совпадает с именем шага расписания.
		if decl.Source != name {
			t.Errorf("объявление импортёра %q даёт ключ версии %q, а импортёр называется %q", name, decl.Source, name)
		}
	}
	// Обратное направление (д): карта не должна описывать импортёра, которого в
	// реестре нет, — иначе ожидание осталось бы без предъявителя.
	declared := make(map[string]bool, len(expectedMinfinPublicationCountPoints))
	for _, imp := range minfinImporters {
		if _, expected := expectedMinfinPublicationCountPoints[imp.Name()]; expected {
			declared[imp.Name()] = true
		}
	}
	for name := range expectedMinfinPublicationCountPoints {
		if !declared[name] {
			t.Errorf("в реестре minfinImporters нет импортёра %q, для которого ожидается объявление префлайта", name)
		}
	}

	// «Ровно те же»: число minfin_-имён в Stats не может быть больше реестра (кто-то
	// вернул персональный init()-append в файл импортёра) или меньше (запись забыта) —
	// ровно одна точка регистрации на пакет.
	minfinRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), minfinNamePrefix) {
			minfinRegistered++
		}
	}
	if minfinRegistered != len(minfinImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d: регистрация должна быть только в minfin/importers.go", minfinRegistered, minfinNamePrefix, len(minfinImporters))
	}

	// (г)
	data, err := os.ReadFile(daguMinfinYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguMinfinYAML, err)
	}
	steps := daguMinfinStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в dagu/minfin.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=: разбор сломан или шаги переписаны")
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг dagu/minfin.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре minfinImporters: расписание позовёт импортёр, которого нет", name)
		}
	}
	// Обратное направление цикл не ловит: реестр верен сам по себе и без шага
	// расписания. Пропавший шаг minfin_oil_gas_mes оставил бы source='minfin_oil_gas'
	// в витрине без обновления при всех зелёных тестах.
	for _, imp := range minfinImporters {
		if !scheduled[imp.Name()] {
			t.Errorf("в dagu/minfin.yaml нет шага CLICKHOUSE_IMPORT_STAT=%s: импортёр реестра не запускается по расписанию", imp.Name())
		}
	}
}
