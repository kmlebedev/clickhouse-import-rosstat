package cbr

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
// cbrKeyRateTable с самим собой, прошёл бы и после переименования таблицы в key_rate,
// то есть ровно на той правке, от которой он и должен падать.
const (
	// cbrNamePrefix — пространство имён импортёров и таблиц пакета cbr. Имя
	// импортёра — контракт расписания (CLICKHOUSE_IMPORT_STAT и id шага
	// dagu/cbr.yaml), имя таблицы — контракт данных (v_cbr_macro и агент MCP).
	cbrNamePrefix = "cbr_"
	// sharedMacroSeriesTable — таблица вне cbr_-пространства, которую занимают
	// импортёры ЦБ: ряды мониторинга предприятий (source='cbr_survey') и
	// макроэкономического опроса (source='cbr_mo_br') лежат в общей macro_series,
	// рядом с fred/bls/bea.
	sharedMacroSeriesTable = "macro_series"
)

// sharedMacroSeriesOwners — импортёры, которым разрешено писать в общую macro_series.
// Набор литеральный (не ссылка на константу импортёра: та сделала бы проверку
// тавтологичной — имя сверялось бы с самим собой), а незнакомец в этой таблице —
// ровно тот случай, ради которого проверка (б) и существует.
var sharedMacroSeriesOwners = map[string]bool{
	"cbr_mp_survey": true, // source='cbr_survey'
	"cbr_mo_br":     true, // source='cbr_mo_br'
}

// expectedPublicationCountPoints — запросы количественного чека (спека §3.5) для
// импортёров, объявивших точку контроля публикации. Литералами, а не ссылкой на
// константу импортёра: проверка «колонка countPoints реестра == объявление самого
// импортёра» падает ровно на расхождении запросов, а сравнение константы с самой
// собой прошло бы и после правки запроса в объявлении — то есть на том самом
// случае, от которого проверка и защищает.
//
// Набор неполный по построению: у остальных импортёров префлайта нет и быть не
// должно (у cbr_key_rate/cbr_ruania/cbr_currency_usd/cbr_gold URL содержит
// сегодняшнюю дату, отпечаток меняется каждый прогон и no-op недостижим).
var expectedPublicationCountPoints = map[string]string{
	"cbr_mp_survey":             "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'",
	"cbr_households_b_mes":      "SELECT count() FROM cbr_households_b_mes FINAL",
	"cbr_m2":                    "SELECT count() FROM cbr_m2 FINAL",
	"cbr_credit_m2x":            "SELECT count() FROM cbr_credit_m2x FINAL",
	"cbr_loans_to_corporations": "SELECT count() FROM cbr_loans_to_corporations FINAL",
	"cbr_loans_to_individuals":  "SELECT count() FROM cbr_loans_to_individuals FINAL",
	"cbr_bank_int_rate":         "SELECT count() FROM cbr_bank_int_rate FINAL",
	"cbr_int_rate_loans":        "SELECT count() FROM cbr_int_rate_loans FINAL",
	"cbr_indicators_cpd":        "SELECT count() FROM cbr_indicators_cpd FINAL",
	"cbr_infl_exp":              "SELECT count() FROM cbr_infl_exp FINAL",
	"cbr_mo_br":                 "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'",
}

// publicationDeclarer — то, как тест достаёт объявление импортёра из реестра:
// метод Publication() промотируется из встроенного util.ClickHouseImport, поэтому
// приведение работает и для голого *util.ClickHouseImport (cbr_bank_int_rate,
// cbr_credit_m2x зарегистрированы так), и для обёрток со встроенным значением
// (cbr_mo_br, cbr_mp_survey). Именованный тип, а не литерал интерфейса в теле
// цикла: так проверка читается как контракт, а не как приём приведения.
type publicationDeclarer interface {
	Publication() util.PublicationDecl
}

// daguCbrYAML — расписание cbr: каждый шаг задаёт CLICKHOUSE_IMPORT_STAT. Путь от
// каталога пакета (тесты идут с cwd=cbr/): так же читают фикстуры другие пакеты.
var daguCbrYAML = filepath.Join("..", "dagu", "cbr.yaml")

// daguStatRef вытаскивает имя из строки шага `- CLICKHOUSE_IMPORT_STAT=<name>`.
// Разбор построчный и регуляркой, без YAML-зависимости: тест не должен тянуть в
// пакетный go.mod парсер ради одного ключа.
var daguStatRef = regexp.MustCompile(`CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)`)

// TestCbrImportersPrefixedAndRegistered держит инварианты реестра cbrImporters —
// единственной точки, из которой импортёры пакета попадают в chimport.Stats.
//
// Проверяется именно реестр, а не Import напрямую: остальные тесты пакета зовут
// парсеры и Import, и потому проходили бы, даже если импортёр не зарегистрирован
// и недостижим ни из `make import`, ни из расписания. Регистрация — единственное,
// что видит оператор.
//
//   - (а) Name() каждого импортёра начинается с cbr_ — иначе ручной запуск
//     CLICKHOUSE_IMPORT_STAT=<table> попадёт в чужой шаг конвейера (префикс cbr_
//     в репозитории уникален за пределами пакета);
//   - (б) таблица — cbr_-префикс, либо общий macro_series и только для его
//     известных владельцев (cbr_mp_survey, cbr_mo_br): смена таблицы под
//     импортёром увела бы витрину v_cbr_macro к чужим данным;
//   - (в) имена уникальны и лежат в chimport.Stats: дубль имени в фильтре
//     `make import STAT=...` выбрал бы произвольный из двух импортёров;
//   - (г) каждый шаг dagu/cbr.yaml присутствует в реестре: расписание —
//     единственный продакшн-вызов, и его опечатка иначе не видна ничем; и
//     обратно — шаг контура макроопроса в расписании есть (иначе пропавший шаг
//     dagu не заметил бы ни один тест: реестр остаётся верным сам по себе), как и
//     шаг контура резюме обсуждений (cbr_mp_summary: пропавший шаг оставил бы
//     витрины v_cbr_mp_summary* пустыми при всех зелёных тестах);
//   - (д) точка контроля публикации (префлайт) объявлена ровно у ожидаемых одиннадцати
//     импортёров и запросом из expectedPublicationCountPoints: колонка countPoints
//     реестра сверяется с объявлением самого импортёра (метод Publication()
//     встроенного util.ClickHouseImport), а у импортёров вне ожидаемого набора
//     объявления нет вовсе — забытое объявление молча выключило бы префлайт, а
//     лишнее у дневных рядов дало бы HEAD на каждом прогоне без шанса на no-op.
func TestCbrImportersPrefixedAndRegistered(t *testing.T) {
	if len(cbrImporters) == 0 {
		t.Fatal("реестр cbrImporters пуст: ни один импортёр пакета не зарегистрирован")
	}

	registered := make(map[string]bool, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		registered[stat.Name()] = true
	}

	seen := make(map[string]bool, len(cbrImporters))
	for _, imp := range cbrImporters {
		name := imp.Name()

		// (а)
		if !strings.HasPrefix(name, cbrNamePrefix) {
			t.Errorf("имя импортёра %q без префикса %q: Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml", name, cbrNamePrefix)
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
			if !sharedMacroSeriesOwners[name] {
				t.Errorf("импортёр %q объявил общую таблицу %q: её занимают только cbr_mp_survey (source='cbr_survey') и cbr_mo_br (source='cbr_mo_br')", name, sharedMacroSeriesTable)
			}
			continue
		}
		if !strings.HasPrefix(imp.table, cbrNamePrefix) {
			t.Errorf("таблица %q импортёра %q без префикса %q и не равна %q", imp.table, name, cbrNamePrefix, sharedMacroSeriesTable)
		}
		// У всех импортёров, кроме владельцев общей macro_series, Name() — это
		// TableName встроенного util.ClickHouseImport. Значит, литерал в колонке
		// table обязан с ним совпадать: иначе реестр описывает не ту таблицу, в
		// которую импортёр пишет (переименовали константу — обновили литерал).
		if name != imp.table {
			t.Errorf("реестр записал импортёру %q таблицу %q, а Name() (TableName импортёра) — %q", name, imp.table, name)
		}
	}

	// (д) Точка контроля публикации: колонка countPoints реестра — литерал, как и
	// колонка table, и сверяется с объявлением самого импортёра. Расхождение между
	// ними и есть та правка, от которой проверка должна падать (прецедент — table):
	// реестр описывал бы префлайт, которого у импортёра нет, или наоборот.
	declared := make(map[string]bool, len(expectedPublicationCountPoints))
	for _, imp := range cbrImporters {
		name := imp.Name()
		decl := imp.ImportStat.(publicationDeclarer).Publication()
		want, expected := expectedPublicationCountPoints[name]
		if !expected {
			// Импортёр вне ожидаемого набора: префлайта нет ни в реестре, ни в
			// объявлении. Дневные ряды (cbr_key_rate, cbr_ruania, cbr_currency_usd,
			// cbr_gold) не должны обзавестись им «на всякий случай»: их URL содержит
			// сегодняшнюю дату, отпечаток меняется каждый прогон, no-op недостижим —
			// HEAD стал бы чистыми накладными расходами.
			if imp.countPoints != "" {
				t.Errorf("реестр записал импортёру %q запрос чека %q, хотя префлайт ему не положен", name, imp.countPoints)
			}
			if decl.CountPoints != "" {
				t.Errorf("импортёр %q объявил точку контроля публикации (%q), хотя префлайт ему не положен", name, decl.CountPoints)
			}
			continue
		}
		declared[name] = true

		// (а) Литерал реестра против объявления импортёра — разные источники
		// значения: реестр правит человек, объявление живёт рядом с импортёром.
		if decl.CountPoints != imp.countPoints {
			t.Errorf("импортёр %q объявил запрос чека %q, а реестр записал %q", name, decl.CountPoints, imp.countPoints)
		}
		// (б) Оба обязаны быть дословно равны спеке §3.5: FINAL и (у владельцев
		// общей macro_series) WHERE source — часть утверждения «эти точки мои».
		if imp.countPoints != want {
			t.Errorf("реестр записал импортёру %q запрос чека %q, а спека §3.5 — %q", name, imp.countPoints, want)
		}
		// (в) Ключ версии — Source объявления, а не TableName: у владельцев общей
		// macro_series TableName чужой для ряда, и версии двух импортёров слились бы
		// в одну строку source_publications (проверяется и пустое PublicationSource).
		if decl.Source != name {
			t.Errorf("объявление импортёра %q даёт ключ версии %q, а импортёр называется %q: `make import STAT=%s` и dagu-шаг ищут его по имени", name, decl.Source, name, name)
		}
	}
	// Обратное направление: карта не должна описывать импортёра, которого в реестре
	// нет, — иначе ожидание §3.5 осталось бы без предъявителя, а проверки (а)–(в)
	// над ним не выполнялись бы вовсе.
	for name := range expectedPublicationCountPoints {
		if !declared[name] {
			t.Errorf("в реестре cbrImporters нет импортёра %q, для которого спека §3.5 ожидает объявление префлайта", name)
		}
	}

	// «Ровно те же»: число cbr_-имён в Stats не может быть больше реестра
	// (кто-то вернул персональный init()-append в файл импортёра) или меньше
	// (запись забыта при консолидации) — ровно одна точка регистрации на пакет.
	cbrRegistered := 0
	for _, stat := range chimport.Stats {
		if strings.HasPrefix(stat.Name(), cbrNamePrefix) {
			cbrRegistered++
		}
	}
	if cbrRegistered != len(cbrImporters) {
		t.Errorf("в chimport.Stats %d импортёров с префиксом %q, в реестре %d: регистрация должна быть только в cbr/importers.go", cbrRegistered, cbrNamePrefix, len(cbrImporters))
	}

	// (г)
	data, err := os.ReadFile(daguCbrYAML)
	if err != nil {
		t.Fatalf("чтение %s: %v", daguCbrYAML, err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) == 0 {
		t.Fatalf("в dagu/cbr.yaml не найдено ни одной строки CLICKHOUSE_IMPORT_STAT=: разбор сломан или шаги переписаны")
	}
	scheduled := make(map[string]bool, len(steps))
	for _, step := range steps {
		name := step[1]
		scheduled[name] = true
		if !seen[name] {
			t.Errorf("шаг dagu/cbr.yaml CLICKHOUSE_IMPORT_STAT=%s не найден в реестре cbrImporters: расписание позовёт импортёр, которого нет", name)
		}
	}
	// Обратное направление цикл не ловит: реестр верен сам по себе и без шага
	// расписания. Пропавший шаг cbr_mo_br означал бы, что волны макроопроса не
	// импортируются вовсе, а витрина v_cbr_mo_br пустует при всех зелёных тестах.
	for owner := range sharedMacroSeriesOwners {
		if !scheduled[owner] {
			t.Errorf("в dagu/cbr.yaml нет шага CLICKHOUSE_IMPORT_STAT=%s: владелец общей %s не запускается по расписанию", owner, sharedMacroSeriesTable)
		}
	}
	// Отдельно от владельцев общей macro_series: cbr_mp_summary пишет в свои три
	// таблицы, и обратное направление (г) для него иначе не выполнялось бы вовсе —
	// шаг расписания, пропавший из dagu/cbr.yaml, не ломает ни реестр, ни парсеры,
	// а витрины v_cbr_mp_summary* остались бы пустыми при зелёных тестах.
	if !scheduled["cbr_mp_summary"] {
		t.Errorf("в dagu/cbr.yaml нет шага CLICKHOUSE_IMPORT_STAT=cbr_mp_summary: контур резюме обсуждений не запускается по расписанию")
	}
}
