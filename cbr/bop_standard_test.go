package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// bopStandardFixture — живой файл источника целиком: книга 462 206 байт с двумя листами
// («Кварталы» 419×131 и «Годы» 419×33), коммитится как есть (стандарт §11.2). Винтаж снимка —
// выпуск 30.09.2026: ЦБ перезаписывает файл по тому же адресу, поэтому прежние значения фикстуры
// не воспроизводятся.
const bopStandardFixture = "testdata/bal_of_payments_standart.xlsx"

// Разметка живой фикстуры, нужная мутациям: строка 4 — строка единиц (ячейка EA4 на листе
// «Кварталы», AG4 на листе «Годы»), строка 5 — подписи периодов (1 квартал 1994 … 2 квартал 2026
// в 130 колонках и 1994 … 2025 в 32), строки 6…413 — 408 компонентов, из них импортируются 17
// верхних агрегатов, колонка A — их метки. Числа взяты из разметки листа, а не из кода разбора:
// мутация обязана попадать в те же ячейки, что читает разбор.
const (
	bopStandardQuartersSheet   = "Кварталы"
	bopStandardYearsSheet      = "Годы"
	bopStandardQuarterUnitCell = "EA4"
	bopStandardYearUnitCell    = "AG4"
	bopStandardFirstDataRow    = 6
	bopStandardLastDataRow     = 413
	bopStandardQuarterColumns  = 130
	bopStandardYearColumns     = 32
)

// bopStandardWantCodes — 17 верхних агрегатов стандартной презентации литералами, в порядке строк
// листа (спека §3.3). Литералы, а не ссылка на cbrBopStandardAggregates: сверка со списком спеки —
// единственное место, где видно переименование агрегата или перестановку строк, а не код,
// читающий сам себя.
var bopStandardWantCodes = []string{
	"Счет текущих операций",
	"Экспорт товаров, услуг, первичных и вторичных доходов",
	"Импорт товаров, услуг, первичных и вторичных доходов",
	"Товары и услуги",
	"Товары",
	"Товары: экспорт",
	"Товары: импорт",
	"Услуги",
	"Услуги: экспорт",
	"Услуги: импорт",
	"Первичные доходы",
	"Вторичные доходы",
	"Счет операций с капиталом",
	"Сальдо счета текущих операций и счета операций с капиталом",
	"Сальдо финансового счета",
	"Резервные активы",
	"Чистые ошибки и пропуски",
}

// bopStandardWantRows — строка каждого агрегата в листах фикстуры: состав и порядок строк у обоих
// листов одинаковы (спека §3.5), и по этим номерам мутации попадают в ту же строку, что читает
// разбор.
var bopStandardWantRows = []int{6, 7, 8, 9, 12, 13, 14, 25, 26, 27, 225, 246, 249, 258, 259, 412, 413}

// bopStandardLabels — подпись строки источника у каждого агрегата (спека §3.3). У «Товары» подпись
// кончается пробелом — так она и стоит в листе («Товары »), и объявлена ожидаемая строка origin
// ровно с ним: сверка идёт с текстом источника, а не с его нормализованным видом.
var bopStandardLabels = map[string]string{
	"Счет текущих операций":                                 "Счет текущих операций",
	"Экспорт товаров, услуг, первичных и вторичных доходов": "Экспорт товаров, услуг, полученные первичные и вторичные доходы",
	"Импорт товаров, услуг, первичных и вторичных доходов":  "Импорт товаров, услуг, выплаченные первичные и вторичные доходы",
	"Товары и услуги":                                       "Товары и услуги",
	"Товары":                                                "Товары ",
	"Товары: экспорт":                                       "Экспорт",
	"Товары: импорт":                                        "Импорт",
	"Услуги":                                                "Услуги",
	"Услуги: экспорт":                                       "Экспорт",
	"Услуги: импорт":                                        "Импорт",
	"Первичные доходы":                                      "Первичные доходы",
	"Вторичные доходы":                                      "Вторичные доходы",
	"Счет операций с капиталом":                             "Счет операций с капиталом",
	"Сальдо счета текущих операций и счета операций с капиталом": "Сальдо счета текущих операций и счета операций с капиталом",
	"Сальдо финансового счета":                                   "Сальдо финансового счета",
	"Резервные активы":                                           "Резервные активы",
	"Чистые ошибки и пропуски":                                   "Чистые ошибки и пропуски",
}

// bopStandardRepeatedLabels — ряды, чья подпись источника повторяется в листе: origin обязан
// назвать блок, иначе «строка «Экспорт»» не отличима между товарами и услугами (стандарт §6.8).
var bopStandardRepeatedLabels = map[string]struct{ label, block string }{
	"Товары: экспорт": {"Экспорт", "Товары"},
	"Товары: импорт":  {"Импорт", "Товары"},
	"Услуги: экспорт": {"Экспорт", "Услуги"},
	"Услуги: импорт":  {"Импорт", "Услуги"},
}

// bopStandardWantSheets — листы фикстуры в порядке обхода разбора и вид периода их колонок
// (спека §3.5): «Кварталы» → квартал, «Годы» → год.
var bopStandardWantSheets = []struct{ sheet, kind string }{
	{bopStandardQuartersSheet, "квартал"},
	{bopStandardYearsSheet, "год"},
}

// bopStandardWantName — имя ряда по спеке §3.2: «<агрегат> (<вид>)».
func bopStandardWantName(code, kind string) string {
	return code + " (" + kind + ")"
}

// bopStandardWantNames — 34 имени ряда стандартной презентации: лист — внешний цикл, агрегат —
// внутренний, то есть в том порядке, в каком точки пишет разбор (сначала весь лист «Кварталы»,
// потом «Годы»).
func bopStandardWantNames() []string {
	names := make([]string, 0, len(bopStandardWantCodes)*len(bopStandardWantSheets))
	for _, sheet := range bopStandardWantSheets {
		for _, code := range bopStandardWantCodes {
			names = append(names, bopStandardWantName(code, sheet.kind))
		}
	}
	return names
}

// bopStandardLastDay — последний день месяца по правилу привязки даты (спека §3.4): сутки назад от
// первого числа следующего месяца. Правило повторено в тесте намеренно: ожидания не должны
// считаться тем же кодом, что разбирает подписи.
func bopStandardLastDay(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
}

// bopStandardExpectedDates — даты точек ряда по виду периода: 130 кварталов с 1 квартала 1994 года
// по 2 квартал 2026-го (последний день квартала) и 32 года 1994–2025 (31 декабря). Даты считаются
// правилом, а не списком дней: список пришлось бы переписывать при каждом выпуске, а правило
// проверяет ту же конвенцию, что и разбор (спека §3.4).
func bopStandardExpectedDates(t *testing.T, kind string) []time.Time {
	t.Helper()
	dates := make([]time.Time, 0, bopStandardQuarterColumns)
	switch kind {
	case "квартал":
		for i := 0; i < bopStandardQuarterColumns; i++ {
			dates = append(dates, bopStandardLastDay(1994+i/4, time.Month((i%4+1)*3)))
		}
	case "год":
		for year := 1994; year <= 2025; year++ {
			dates = append(dates, time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC))
		}
	default:
		t.Fatalf("неизвестный вид периода %q: ожидание дат посчитать нечем", kind)
	}
	return dates
}

// bopStandardTotalPoints — число точек выпуска: 17 агрегатов × (130 кварталов + 32 года) = 2 754
// (спека §3.3).
func bopStandardTotalPoints() int {
	return len(bopStandardWantCodes) * (bopStandardQuarterColumns + bopStandardYearColumns)
}

// bopStandardOpenFixture — копия живой фикстуры, загруженная в память под мутацию: ветки fail-fast
// и семантика ячеек проверяются на ней, а не на книге, собранной с нуля (стандарт §11.1). Правки
// идут по копии в памяти — файл на диске не меняется. Мутация получает книгу целиком: листов два,
// и часть веток проверяется на втором.
func bopStandardOpenFixture(t *testing.T, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(bopStandardFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", bopStandardFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if sheets := f.GetSheetList(); len(sheets) != 2 {
		t.Fatalf("в фикстуре %d листов, want 2 (%v)", len(sheets), sheets)
	}
	mutate(t, f)
	return f
}

// bopStandardSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодном адресе
// ошибкой, и проглотить её значило бы проверять немодифицированный лист.
func bopStandardSetCell(t *testing.T, f *excelize.File, sheet, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s, %v): %v", sheet, cell, value, err)
	}
}

// bopStandardCell — адрес ячейки листа по колонке и строке (1-based) — так же, как адресует ячейки
// разбор: колонка 1 — метка агрегата, 2…131 — колонки данных листа «Кварталы».
func bopStandardCell(t *testing.T, column, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(column, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", column, row, err)
	}
	return cell
}

// bopStandardClearDataRows — очистка всех колонок данных перечисленных строк листа: так проверяется
// ветка «разбор не дал ни одной точки» на обеих листах сразу.
func bopStandardClearDataRows(t *testing.T, f *excelize.File, sheet string, rows []int, columns int) {
	t.Helper()
	for _, row := range rows {
		for column := 2; column < 2+columns; column++ {
			bopStandardSetCell(t, f, sheet, bopStandardCell(t, column, row), "")
		}
	}
}

// bopStandardHasPoint — есть ли в разборе точка (ряд, дата).
func bopStandardHasPoint(rows []appendedRow, name string, date time.Time) bool {
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return true
		}
	}
	return false
}

// TestCbrBopStandardSeriesMetaNames: имена 34 рядов каталога совпадают со списком спеки §3.3
// дословно — и агрегаты, и виды, и их порядок, и форма «<агрегат> (<вид>)»; вместе с ними
// проверяются поля, от которых зависит чтение ряда агентом MCP: единицы (млн, не млрд), частота
// Q/A, origin с файлом источника (включая опечатку «standart»), листом, подписью строки и блоком
// для повторяющихся подписей, и description, где есть привязка даты, ноль как значение и
// методология 2022 года.
func TestCbrBopStandardSeriesMetaNames(t *testing.T) {
	want := bopStandardWantNames()
	if len(cbrBopStandardSeriesMeta) != len(want) {
		t.Fatalf("записей каталога %d, want %d (17 агрегатов × 2 листа)", len(cbrBopStandardSeriesMeta), len(want))
	}
	frequency := map[string]string{"квартал": "Q", "год": "A"}
	dates := map[string]string{"квартал": "toStartOfQuarter(date)", "год": "toYear(date)"}
	codes := len(bopStandardWantCodes)
	for i, name := range want {
		meta := cbrBopStandardSeriesMeta[i]
		if meta.Series != name {
			t.Errorf("ряд %d каталога = %q, want %q", i, meta.Series, name)
			continue
		}
		sheet := bopStandardWantSheets[i/codes]
		code := bopStandardWantCodes[i%codes]
		if meta.Source != "cbr_bop_standard" {
			t.Errorf("ряд %q: source %q, want cbr_bop_standard (источник каталога = имя таблицы)", name, meta.Source)
		}
		if meta.Unit != "млн долларов США" {
			t.Errorf("ряд %q: unit %q, want млн долларов США", name, meta.Unit)
		}
		if meta.Frequency != frequency[sheet.kind] {
			t.Errorf("ряд %q: frequency %q, want %q", name, meta.Frequency, frequency[sheet.kind])
		}
		if meta.Title != name {
			t.Errorf("ряд %q: title %q, want имя ряда", name, meta.Title)
		}
		label := bopStandardLabels[code]
		for _, need := range []string{"bal_of_payments_standart.xlsx", "лист «" + sheet.sheet + "»", "строка «" + label + "»"} {
			if !strings.Contains(meta.Origin, need) {
				t.Errorf("ряд %q: origin %q не содержит %q", name, meta.Origin, need)
			}
		}
		if repeated, ok := bopStandardRepeatedLabels[code]; ok {
			if !strings.Contains(meta.Origin, "блока «"+repeated.block+"»") {
				t.Errorf("ряд %q: origin %q не называет блок «%s» — повторяющаяся подпись «%s» не различима",
					name, meta.Origin, repeated.block, repeated.label)
			}
		}
		for _, need := range []string{"млн долларов США", "Ноль", "2022", dates[sheet.kind]} {
			if !strings.Contains(meta.Description, need) {
				t.Errorf("ряд %q: description %q не содержит %q (правило 11 AGENTS.md)", name, meta.Description, need)
			}
		}
	}
}

// TestCbrBopStandardImporterName: Name() — контракт фильтра `make import STAT=…` и id шага
// dagu/cbr.yaml; сверяется литералом, а не таблицей кода (стандарт §8.5). Тест же держит
// cbrBopStandardImporter «использованным» для линтера, пока реестр не завёл его сам (следующая
// задача пункта).
func TestCbrBopStandardImporterName(t *testing.T) {
	if got := cbrBopStandardImporter.Name(); got != "cbr_bop_standard" {
		t.Errorf("Name() = %q, want %q", got, "cbr_bop_standard")
	}
}

// TestCbrBopStandardDdl: DDL — контракт данных (правило 5 AGENTS.md: значения — Float64; движок и
// ключ общие для таблиц пакета). Проверяется литералами: DDL в %s-форме, имя таблицы подставляет
// шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestCbrBopStandardDdl(t *testing.T) {
	if len(cbrBopStandard.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrBopStandard.CreateTable))
	}
	ddl := fmt.Sprintf(cbrBopStandard.CreateTable[0], "cbr_bop_standard")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_bop_standard",
		"name LowCardinality(String)",
		"date Date",
		"value Float64",
		"ENGINE = ReplacingMergeTree ORDER BY (name, date)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL %q не содержит %q", ddl, want)
		}
	}
	if strings.Contains(ddl, "Float32") {
		t.Errorf("DDL %q объявляет Float32: правило 5 AGENTS.md требует Float64", ddl)
	}
}

// TestCbrBopStandardSeriesMetaCoversParsedRows: каталог cbr_bop_standard (34 ряда) и разбор живой
// фикстуры совпадают в обе стороны (стандарт §11.4): имена рядов объявлены кодом, поэтому
// проверяются и забытое имя ряда в каталоге, и забытая колонка в разборе.
func TestCbrBopStandardSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, "cbr_bop_standard", cbrBopStandardSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(bopStandardFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrBopStandard.ImportFunc(f, rec)
	})
}

// TestCbrBopStandardFixtureRows: разбор живой фикстуры даёт 2 754 точки (17 агрегатов × 130
// кварталов + 17 × 32 года) и по каждому ряду — 130 или 32 точки с датами по правилу привязки
// (спека §3.4), а не списком дней.
func TestCbrBopStandardFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, bopStandardFixture, cbrBopStandard.ImportFunc)
	if want := bopStandardTotalPoints(); len(rows) != want {
		t.Errorf("строк разбора %d, want %d (17 агрегатов × (130 кварталов + 32 года))", len(rows), want)
	}
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	for _, sheet := range bopStandardWantSheets {
		want := bopStandardExpectedDates(t, sheet.kind)
		for _, code := range bopStandardWantCodes {
			name := bopStandardWantName(code, sheet.kind)
			got := dates[name]
			if len(got) != len(want) {
				t.Errorf("ряд %q: точек %d, want %d (вид «%s»)", name, len(got), len(want), sheet.kind)
				continue
			}
			for i := range want {
				if !got[i].Equal(want[i]) {
					t.Errorf("ряд %q: точка %d = %s, want %s (привязка даты вида «%s»)",
						name, i, got[i].Format("2006-01-02"), want[i].Format("2006-01-02"), sheet.kind)
					break
				}
			}
		}
	}
}

// TestCbrBopStandardQuarterLabelCanonicalized: подписи периода листов ЦБ пишут номер квартала
// арабской цифрой («1 квартал 1994 г.»), а общий разбор подписи периода знает только латинскую
// римскую (контракт Task 1, закреплённый bop_parse_test.go). Приведение к канонической форме
// проверяется здесь отдельно от разбора фикстуры: переменная форма (пропал год, чужой хвост)
// обязана остаться исходным текстом, чтобы ошибка разбора называла текст источника, а не его
// переписанный вид (стандарт §6.4). Проверка «первый квартал 1994 года даёт 1996,33» в
// TestCbrBopStandardSpotValues — сквозное доказательство, что арабская цифра из живого листа
// доходит до даты 1994-03-31.
func TestCbrBopStandardQuarterLabelCanonicalized(t *testing.T) {
	for label, want := range map[string]string{
		"1 квартал 1994 г.":     "I квартал 1994 г.",
		"4 квартал 2025 г.":     "IV квартал 2025 г.",
		"2 квартал 2026":        "II квартал 2026",
		"I квартал 1994 г.":     "I квартал 1994 г.",
		"5 квартал 1994 г.":     "5 квартал 1994 г.",
		"2 квартал 2026 года":   "2 квартал 2026 года",
		"1 квартал":             "1 квартал",
		"1994 г.":               "1994 г.",
		"Счет текущих операций": "Счет текущих операций",
	} {
		if got := cbrBopStandardCanonicalLabel(label); got != want {
			t.Errorf("cbrBopStandardCanonicalLabel(%q) = %q, want %q", label, got, want)
		}
	}
	for _, label := range []string{"1 квартал 1994 г.", "4 квартал 2025 г.", "2 квартал 2026"} {
		got, err := cbrBopParsePeriod(cbrBopStandardCanonicalLabel(label))
		if err != nil {
			t.Errorf("cbrBopParsePeriod(%q) после приведения: %v", label, err)
			continue
		}
		if got.Kind != "квартал" {
			t.Errorf("подпись %q дала вид %q, want квартал", label, got.Kind)
		}
	}
}

// TestCbrBopStandardSpotValues: значения из известных ячеек живой фикстуры (спека §3.9) — края
// квартального и годового рядов, включая девятнадцатитысячный счёт текущих операций последнего
// квартала и отрицательные резервные активы первого. Значения литералами: сверка идёт с разбором,
// а не с константой кода.
func TestCbrBopStandardSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, bopStandardFixture, cbrBopStandard.ImportFunc)
	firstQuarter := time.Date(1994, time.March, 31, 0, 0, 0, 0, time.UTC)
	lastQuarter := time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC)
	firstYear := time.Date(1994, time.December, 31, 0, 0, 0, 0, time.UTC)
	lastYear := time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "Счет текущих операций (квартал)", firstQuarter, 1996.33)
	cbrCheckSpot(t, rows, "Счет текущих операций (квартал)", lastQuarter, 16949.24)
	cbrCheckSpot(t, rows, "Счет текущих операций (год)", firstYear, 8941.89)
	cbrCheckSpot(t, rows, "Счет текущих операций (год)", lastYear, 38588.14)
	cbrCheckSpot(t, rows, "Резервные активы (квартал)", firstQuarter, -1261)
	cbrCheckSpot(t, rows, "Резервные активы (квартал)", lastQuarter, 7251.98)
}

// TestCbrBopStandardAppendsFloat64: разбор пишет значение как float64 — тип колонки value в DDL
// (правило 5 AGENTS.md). Запись float32 прошла бы в колонку Float64 молча, потеряв точность на
// шаге представления, и расхождение заметила бы только живая БД.
func TestCbrBopStandardAppendsFloat64(t *testing.T) {
	f, err := excelize.OpenFile(bopStandardFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", bopStandardFixture, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = cbrBopStandard.ImportFunc(f, rec); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("разбор фикстуры: схема строк не совпала с разбором: %v", rec.err)
	}
	if len(rec.raw) != bopStandardTotalPoints() {
		t.Fatalf("разбор дал %d строк, want %d", len(rec.raw), bopStandardTotalPoints())
	}
	for i, raw := range rec.raw {
		if len(raw) != 3 {
			t.Fatalf("строка %d: %d значений, ожидалась форма (name, date, value)", i, len(raw))
		}
		if _, ok := raw[2].(float64); !ok {
			t.Fatalf("строка %d: значение записано как %T, ожидался float64 (колонка value — Float64)", i, raw[2])
		}
	}
}

// TestCbrBopStandardMissingSheetFails: пропажа любого из двух листов — ошибка с именем таблицы,
// именем листа и списком листов книги, а не «0 rows» (стандарт §6.4, спека §3.5). Оба листа
// обязательны: годовой ряд иначе оборвался бы молча. Рекордер при ошибке обязан остаться пустым —
// разбор размечает оба листа до первой записи.
func TestCbrBopStandardMissingSheetFails(t *testing.T) {
	for name, sheet := range map[string]string{"нет листа «Кварталы»": bopStandardQuartersSheet, "нет листа «Годы»": bopStandardYearsSheet} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrBopStandard.ImportFunc(bopStandardOpenFixture(t, func(t *testing.T, f *excelize.File) {
				if err := f.DeleteSheet(sheet); err != nil {
					t.Fatalf("DeleteSheet(%q): %v", sheet, err)
				}
			}), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_bop_standard") {
				t.Errorf("ошибка %q не называет таблицу cbr_bop_standard", err)
			}
			if !strings.Contains(err.Error(), "листа «"+sheet+"»") {
				t.Errorf("ошибка %q не называет пропавший лист %q", err, sheet)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0 (разметка идёт до записи)", len(rec.raw))
			}
		})
	}
}

// TestCbrBopStandardEmptyBookFails: книга без листов «Кварталы»/«Годы» — та же ветка «листа нет»;
// сюда попадает и пустая книга excelize (лист «Sheet1»), и лист с другим именем: лист ищется по
// точному имени, а не по префиксу, и «Кварталы 2026» листом «Кварталы» не считается.
func TestCbrBopStandardEmptyBookFails(t *testing.T) {
	for name, sheet := range map[string]string{
		"пустая книга":           "",
		"чужое имя листа":        "Платежный баланс",
		"имя с дописанным годом": "Кварталы 2026",
	} {
		t.Run(name, func(t *testing.T) {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if sheet != "" {
				if err := f.SetSheetName("Sheet1", sheet); err != nil {
					t.Fatalf("SetSheetName(%q): %v", sheet, err)
				}
			}
			rec := newCbrRecorder()
			err := cbrBopStandard.ImportFunc(f, rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_bop_standard") {
				t.Errorf("ошибка %q не называет таблицу cbr_bop_standard", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrBopStandardFailFastOnFixture: ветки, где разбор обязан вернуть ошибку, а не «0 строк»
// (стандарт §6.4, спека §3.9), — на мутированной копии живой фикстуры (стандарт §11.1). Копия, а
// не синтетическая книга: якоря, шапка и ячейки данных в ней те же, что у источника, поэтому
// ошибка приходит от мутации, а не от сборки книги. Батч уходит в БД только по Send, поэтому при
// ошибке разбора в рекордере обязано быть 0 строк — и это же ловит запись до проверки второго
// листа.
func TestCbrBopStandardFailFastOnFixture(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate  func(t *testing.T, f *excelize.File)
		wantErr string
	}{
		"нет строки единиц": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardQuarterUnitCell, "")
			},
			wantErr: "не найдена строка единиц",
		},
		"нет первой подписи агрегата": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 1, bopStandardFirstDataRow), "Итого")
			},
			wantErr: "нет первой ожидаемой подписи агрегата",
		},
		"пропала подпись в середине списка": {
			// Строка 225 — «Первичные доходы» (одиннадцатая в списке, после счёта текущих операций и
			// двух десятков подстатей услуг): разбор ищет подписи последовательно, поэтому пропавшая
			// в середине — ошибка, а не пропуск ряда.
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 1, 225), "Доходы")
			},
			wantErr: "не найдена ожидаемая подпись агрегата",
		},
		"пропала подпись на втором листе": {
			// Та же проверка на листе «Годы»: строки у листов одинаковые, и подмена подписи на втором
			// листе обязана валить прогон, а не оставлять 17 годовых рядов без обновления.
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardYearsSheet, bopStandardCell(t, 1, 412), "Резервы")
			},
			wantErr: "лист «Годы»: после строки",
		},
		"подпись периода не разбирается": {
			// «2 квартал 2026 года» — хвост, которого разбор не знает: подпись есть, период из неё не
			// выводится, и колонка осталась бы без даты, а её точки молча потерялись бы. Ошибка обязана
			// назвать исходный текст ячейки (приведение цифры квартала такую подпись не трогает).
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, "EA5", "2 квартал 2026 года")
			},
			wantErr: "не разобрана как период",
		},
		"вид колонок не совпал с видом листа": {
			// На листе «Годы» стоит квартальная подпись: имя ряда — часть ключа (name, date), и
			// подмена вида переписала бы чужие точки молча.
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardYearsSheet, "B5", "1 квартал 1994 г.")
			},
			wantErr: "источник изменил шапку листа",
		},
		"колонка с данными без подписи": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, "B5", "")
			},
			wantErr: "подпись периода не разобрана",
		},
		"значение не число": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 2, bopStandardFirstDataRow), "н/д")
			},
			wantErr: "значение \"н/д\"",
		},
		"значение NaN": {
			// ParseFloat принимает «NaN» молча: точка с NaN отравила бы ряд (суммы — NaN).
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 2, bopStandardFirstDataRow), "NaN")
			},
			wantErr: "не конечно",
		},
		"все ячейки данных пусты": {
			// Шапка, строка единиц и подписи агрегатов целы, а точек нет: «0 rows» с зелёным dagu
			// оставил бы все 34 ряда без обновления.
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardClearDataRows(t, f, bopStandardQuartersSheet, bopStandardWantRows, bopStandardQuarterColumns)
				bopStandardClearDataRows(t, f, bopStandardYearsSheet, bopStandardWantRows, bopStandardYearColumns)
			},
			wantErr: "не дал ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrBopStandard.ImportFunc(bopStandardOpenFixture(t, tc.mutate), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_bop_standard") {
				t.Errorf("ошибка %q не называет таблицу cbr_bop_standard", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ошибка %q не содержит %q", err, tc.wantErr)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrBopStandardFixtureSemantics — разбор мутированной копии живой фикстуры при ожидаемом
// изменении источника: пустая ячейка точки не даёт, а литеральный ноль даёт (стандарт §6.7,
// спека §3.5: в платёжном балансе ноль — настоящее значение, в отличие от cbr_int_rate_loans).
func TestCbrBopStandardFixtureSemantics(t *testing.T) {
	total := bopStandardTotalPoints()
	firstQuarter := time.Date(1994, time.March, 31, 0, 0, 0, 0, time.UTC)
	series := "Счет текущих операций (квартал)"
	for name, tc := range map[string]struct {
		mutate   func(t *testing.T, f *excelize.File)
		wantRows int
		value    float64
		isPoint  bool
	}{
		"пустая ячейка точки не даёт": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 2, bopStandardFirstDataRow), "")
			},
			wantRows: total - 1,
			isPoint:  false,
		},
		"литеральный ноль — точка": {
			mutate: func(t *testing.T, f *excelize.File) {
				bopStandardSetCell(t, f, bopStandardQuartersSheet, bopStandardCell(t, 2, bopStandardFirstDataRow), 0)
			},
			wantRows: total,
			value:    0,
			isPoint:  true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrBopStandard.ImportFunc(bopStandardOpenFixture(t, tc.mutate), rec); err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if rec.err != nil {
				t.Fatalf("схема строк не совпала с разбором: %v", rec.err)
			}
			if len(rec.rows) != tc.wantRows {
				t.Errorf("точек %d, want %d: пустая ячейка точки не даёт, а ноль — даёт", len(rec.rows), tc.wantRows)
			}
			if got := bopStandardHasPoint(rec.rows, series, firstQuarter); got != tc.isPoint {
				t.Errorf("точка (%q, %s): наличие %v, want %v", series, firstQuarter.Format("2006-01-02"), got, tc.isPoint)
			}
			if !tc.isPoint {
				return
			}
			cbrCheckSpot(t, rec.rows, series, firstQuarter, tc.value)
		})
	}
}
