package views

import (
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

// datapack fixture — тот же файл, из которого импортёр наполняет databook_polyus.
// Он лежит в polyus/data/ и читается здесь напрямую: список рядов датапака нельзя
// ни выписать по памяти, ни вывести из релизного словаря — он существует только
// в файле, и тест, который берёт имена из него, падает ровно тогда, когда каталог
// расходится с данными.
const datapackFixture = "../polyus/data/polyus_datapack_fy2025_new.xlsx"

// Скопированы из polyus/datapack.go: лист, колонка подписи и список таблиц-
// месторождений, по которым разбор узнаёт начало очередной таблицы. Скопированы
// намеренно: polyus их не экспортирует, а тест обязан разбирать файл по тем же
// правилам, что и импортёр, — иначе он проверял бы каталог против другого разбора.
const (
	datapackFixtureSheet = "Sheet1"
	datapackLabelCol     = 1 // колонка B: имя таблицы или подпись показателя
	datapackUnitCol      = 2 // колонка C: подпись единицы измерения
	datapackFirstDataCol = 4 // колонка E: первое значение периода
)

var datapackFixtureTables = []string{
	"CONSOLIDATED OPERATING RESULTS", "OLIMPIADA", "BLAGODATNOYE", "TITIMUKHTA",
	"VERNINSKOYE2", "ALLUVIALS", "KURANAKH", "ZAPADNOYE", "NATALKA", "Sukhoi Log",
}

// datapackMetricNames возвращает подписи-ПОКАЗАТЕЛИ листа: те, напротив которых
// в строках данных стоят числа. Заголовки блоков (Mining, Processing, Mill,
// Heap-leach) и заголовок листа (OPERATING RESULTS1) стоят в той же колонке B, но
// значений не несут, поэтому в набор не попадают — и именно поэтому тест на них
// не ругается, хотя они физически лежат в файле.
//
// Разбор повторяет правило импортёра: переменная table заполняется, только когда
// подпись совпала с именем месторождения, и строки до первого месторождения
// значения не дают (в parseDatapack это пустая table у первой таблицы листа).
func datapackMetricNames(t *testing.T, path string) (names map[string]bool, units map[string]string) {
	t.Helper()

	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = xlsx.Close() }()

	rows, err := xlsx.GetRows(datapackFixtureSheet)
	if err != nil {
		t.Fatalf("sheet %s: %v", datapackFixtureSheet, err)
	}

	names = make(map[string]bool)
	units = make(map[string]string)

	table := ""
	for _, row := range rows {
		if len(row) <= datapackLabelCol {
			continue
		}

		label := row[datapackLabelCol]
		if label == "" {
			continue
		}

		if isDatapackTable(label) {
			table = label

			continue
		}

		if table == "" {
			// Подпись выше первого месторождения: у импортёра table здесь ещё
			// пуста, и значения из такой строки в витрину не попадают.
			continue
		}

		if !rowHasNumber(row) {
			continue
		}

		names[label] = true

		if unit := row[datapackUnitCol]; unit != "" && units[label] == "" {
			units[label] = unit
		}
	}

	if len(names) == 0 {
		t.Fatalf("%s: no datapack metric names found — the fixture or the parsing rule changed", path)
	}

	return names, units
}

func isDatapackTable(label string) bool {
	for _, table := range datapackFixtureTables {
		if label == table {
			return true
		}
	}

	return false
}

// rowHasNumber сообщает, есть ли в строке хоть одно числовое значение периода.
// Проверяются только колонки данных: в колонке единицы (C) стоит подпись вроде
// '000 m3', и принимать её за значение нельзя. Короткие строки (в excelize
// хвостовые пустые ячейки обрезаются) безопасно пропускаются — значений в них нет.
func rowHasNumber(row []string) bool {
	if len(row) <= datapackFirstDataCol {
		return false
	}

	for _, cell := range row[datapackFirstDataCol:] {
		if isDatapackNumber(cell) {
			return true
		}
	}

	return false
}

// isDatapackNumber повторяет допущения разбора polyus/datapack.go: числа бывают с
// запятой-разделителем тысяч, отрицательные пишутся в скобках, проценты — со
// знаком %, а '-' и 'N/A' означают пропуск.
func isDatapackNumber(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || strings.EqualFold(s, "N/A") {
		return false
	}

	cleaned := strings.NewReplacer(",", "", "%", "", "(", "", ")", "").Replace(s)
	if cleaned == "" {
		return false
	}

	_, err := strconv.ParseFloat(cleaned, 64)

	return err == nil
}

// Каталог обязан описывать ровно те ряды датапака, которые в датапаке есть:
// набор имён берётся из самого файла, а не из второго списка в коде. Первая
// версия каталога собирала датапак-ряды циклом по polyus.MetricNames() — словарю
// релизов, — и все 18 строк описывали имена, которых датапак не эмитит (gold_output,
// revenue, ...), а 19 настоящих подписей отсутствовали. Этот тест падает на обоих
// концах: и на выдуманном имени в каталоге, и на пропущенном имени из файла.
func TestDatapackSeriesMetaCoversFixtureMetricNames(t *testing.T) {
	expected, unitOf := datapackMetricNames(t, datapackFixture)

	inCatalog := make(map[string]bool)
	for _, m := range polyusSeriesMeta() {
		if m.Source == polyusDatapackSource {
			inCatalog[m.Series] = true
		}
	}

	if len(inCatalog) == 0 {
		t.Fatal("series catalog has no polyus_datapack rows")
	}

	for name := range expected {
		if !inCatalog[name] {
			t.Errorf("datapack metric %q from %s has no series_catalog entry", name, datapackFixture)
		}
	}

	for name := range inCatalog {
		if !expected[name] {
			t.Errorf("series_catalog describes datapack metric %q, which %s never emits a value for", name, datapackFixture)
		}
	}

	if t.Failed() {
		t.Logf("fixture metrics (%d): %s", len(expected), strings.Join(sortedKeys(expected), " | "))
		t.Logf("catalog metrics (%d): %s", len(inCatalog), strings.Join(sortedKeys(inCatalog), " | "))
	}

	// Единица у каждого ряда должна быть той, что стоит в подписи строки листа:
	// в схеме databook_polyus колонки единиц нет, и взять её больше неоткуда.
	for _, m := range polyusSeriesMeta() {
		if m.Source != polyusDatapackSource {
			continue
		}

		unit, ok := unitOf[m.Series]
		if !ok {
			continue
		}

		if !strings.HasPrefix(m.Unit, unit+" ") && m.Unit != unit {
			t.Errorf(
				"datapack metric %q: Unit = %q, but the sheet labels it %q",
				m.Series, m.Unit, unit,
			)
		}
	}
}

// У датапак-рядов не должно быть юнита из релизного словаря: единицы двух
// источников описывают разные величины, и совпасть они могут только случайно.
// Проверка ловит возврат к byName[name].Unit — то есть к той самой ошибке, из-за
// которой в каталоге оказались релизные единицы у датапак-имён.
func TestDatapackSeriesMetaUnitsAreNotReleaseUnits(t *testing.T) {
	for _, m := range polyusSeriesMeta() {
		if m.Source != polyusDatapackSource {
			continue
		}

		if m.Unit == "" {
			t.Errorf("datapack metric %q has no unit note", m.Series)

			continue
		}

		if !strings.Contains(m.Unit, "подписи строки") {
			t.Errorf(
				"datapack metric %q: Unit = %q must say the unit comes from the sheet row, not from a schema column",
				m.Series, m.Unit,
			)
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// Четыре словаря датапака (имена, заголовки, единицы, описания) обязаны
// покрывать друг друга: имя без описания дало бы агенту ряд с пустой строкой, а
// имя без единицы — ряд, у которого в unit стоит одна оговорка вместо ответа.
// Тест держит словари вместе с polyusDatapackMetricNames и потому срабатывает
// при добавлении подписи в один список без остальных — в том числе когда новая
// подпись появится в xlsx и её внесёт datapack_names_test.go.
func TestDatapackSeriesMetaListsAreAligned(t *testing.T) {
	for _, name := range polyusDatapackMetricNames {
		if polyusDatapackMetricDescription[name] == "" {
			t.Errorf("datapack metric %q has no description", name)
		}

		if polyusDatapackTitles[name] == "" {
			t.Errorf("datapack metric %q has no title", name)
		}

		if _, ok := polyusDatapackUnits[name]; !ok {
			t.Errorf("datapack metric %q has no unit entry (use \"\" when the sheet label carries none)", name)
		}
	}

	named := make(map[string]bool, len(polyusDatapackMetricNames))
	for _, name := range polyusDatapackMetricNames {
		if named[name] {
			t.Errorf("datapack metric %q is listed twice", name)
		}

		named[name] = true
	}

	for name := range polyusDatapackMetricDescription {
		if !named[name] {
			t.Errorf("description for %q, which is not in polyusDatapackMetricNames", name)
		}
	}

	for name := range polyusDatapackTitles {
		if !named[name] {
			t.Errorf("title for %q, which is not in polyusDatapackMetricNames", name)
		}
	}

	for name := range polyusDatapackUnits {
		if !named[name] {
			t.Errorf("unit for %q, which is not in polyusDatapackMetricNames", name)
		}
	}
}
