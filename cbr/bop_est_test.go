package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// Оценка ключевых агрегатов проверяется на двух живых винтажах: ЦБ перезаписывает файл по тому же
// адресу и меняет в нём и колонки, и подписи строк, поэтому старый выпуск остаётся в фикстуре
// отдельным файлом (спека §3.3, §3.9).
//
// bopEstFixture — текущий винтаж (лист «Оценка ПБ (15.10.2026)», январь – август, 10 колонок,
// 114 411 байт); bopEstFixturePrev — прежний (лист «Оценка ПБ (14.09.2026)», январь – июль,
// 14 колонок, 115 017 байт) — на нём проверяется приём прежнего набора подписей строк. Обе книги
// коммитятся целиком, zip-хирургии нет (стандарт §11.2).
const (
	bopEstFixture     = "testdata/bop_est.xlsx"
	bopEstFixturePrev = "testdata/bop_est_prev.xlsx"
)

// bopEstFirstDataRow — номер первой строки данных в обоих винтажах: строки 1–5 заняты заголовком
// файла, строкой единиц и двумя строками шапки, строки 6…22 — 17 агрегатов (спека §2). Число
// берётся из разметки листа, а не из кода разбора: мутация обязана попадать в те же ячейки, что
// читает разбор.
const bopEstFirstDataRow = 6

// bopEstWantLabels — подписи строк источника по позициям: варианты в порядке «текущий винтаж,
// прежний» (спека §3.3). Литералы, а не ссылка на cbrBopEstAggregates: сверка со списком спеки —
// единственное место, где видно переименование подписи или перестановку строк, а не код, читающий
// сам себя.
var bopEstWantLabels = [][]string{
	{"Счет текущих операций"},
	{"Товары", "Торговый баланс"},
	{"Экспорт"},
	{"Импорт"},
	{"Услуги", "Баланс услуг"},
	{"Экспорт"},
	{"Импорт"},
	{"Первичные и вторичные доходы", "Баланс первичных и вторичных доходов"},
	{"К получению", "Доходы к получению"},
	{"К выплате", "Доходы к выплате"},
	{"Счет операций с капиталом"},
	{"Счет текущих операций и счет операций с капиталом", "Сальдо счета текущих операций и счета операций с капиталом"},
	{"Финансовый счет, исключая резервные активы", "Сальдо финансового счета, исключая резервные активы"},
	{"Чистое приобретение финансовых активов, исключая резервные активы"},
	{"Чистое принятие обязательств"},
	{"Резервные активы", "Изменение резервных активов"},
	{"Чистые ошибки и пропуски"},
}

// bopEstWantCodes — имена кода 17 агрегатов в порядке строк листа (спека §3.3). У агрегатов,
// которые есть и в стандартной презентации, имя кода то же, что там («Товары», «Услуги»): агент
// сравнивает оценку с финальными данными по имени, а различает по source.
var bopEstWantCodes = []string{
	"Счет текущих операций",
	"Товары",
	"Товары: экспорт",
	"Товары: импорт",
	"Услуги",
	"Услуги: экспорт",
	"Услуги: импорт",
	"Баланс первичных и вторичных доходов",
	"Доходы к получению",
	"Доходы к выплате",
	"Счет операций с капиталом",
	"Сальдо счета текущих операций и счета операций с капиталом",
	"Сальдо финансового счета, исключая резервные активы",
	"Чистое приобретение финансовых активов, исключая резервные активы",
	"Чистое принятие обязательств",
	"Изменение резервных активов",
	"Чистые ошибки и пропуски",
}

// bopEstWantKinds — три вида периода листа оценки в порядке имён ряда (спека §3.2).
var bopEstWantKinds = []string{"месяц", "квартал", "накопленным итогом"}

// bopEstWantName — имя ряда по спеке §3.2: «<агрегат> (<вид>)».
func bopEstWantName(code, kind string) string {
	return code + " (" + kind + ")"
}

// bopEstWantNames — 51 имя ряда листа оценки: 17 агрегатов × 3 вида, агрегат — внешний цикл
// (порядок строк листа), вид — внутренний.
func bopEstWantNames() []string {
	names := make([]string, 0, len(bopEstWantCodes)*len(bopEstWantKinds))
	for _, code := range bopEstWantCodes {
		for _, kind := range bopEstWantKinds {
			names = append(names, bopEstWantName(code, kind))
		}
	}
	return names
}

// bopEstRepeatedLabels — ряды, чья подпись источника повторяется в листе: origin обязан назвать
// блок, иначе «строка «Экспорт»» не отличима между товарами и услугами (стандарт §6.8).
var bopEstRepeatedLabels = map[string]struct{ label, block string }{
	"Товары: экспорт": {"Экспорт", "Товары"},
	"Товары: импорт":  {"Импорт", "Товары"},
	"Услуги: экспорт": {"Экспорт", "Услуги"},
	"Услуги: импорт":  {"Импорт", "Услуги"},
}

// bopEstLastDay — последний день месяца по правилу привязки даты (спека §3.4): сутки назад от
// первого числа следующего месяца. Правило повторено в тесте намеренно: ожидания не должны
// считаться тем же кодом, что разбирает подписи.
func bopEstLastDay(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
}

// bopEstLastQuarter — последний день квартала: его третий месяц, затем последний день месяца.
func bopEstLastQuarter(year, quarter int) time.Time {
	return bopEstLastDay(year, time.Month(quarter*3))
}

// bopEstSpot — спот-значение выпуска: точка, которую тест сверяет с разбором.
type bopEstSpot struct {
	series string
	date   time.Time
	value  float64
}

// bopEstVintage — живой выпуск оценки: файл фикстуры, разметка его листа и ожидания тестов.
type bopEstVintage struct {
	release  string                 // дата выпуска: она же в имени листа и в сообщениях теста
	fixture  string                 // файл фикстуры
	unitCell string                 // ячейка строки единиц: переезжает вместе с числом колонок
	columns  int                    // колонок данных (B…): 10 у текущего винтажа, 14 у прежнего
	dates    map[string][]time.Time // даты точек по видам периода в порядке колонок листа
	spots    []bopEstSpot
}

// total — число точек выпуска: 17 агрегатов × число колонок (170 у 15.10.2026, 238 у 14.09.2026).
func (v bopEstVintage) total() int {
	return len(bopEstWantCodes) * v.columns
}

// bopEstVintages — два живых выпуска оценки: текущий и прежний. У прежнего другое число колонок
// (январь – июль) и другой набор подписей строк — оба разбираются одним импортёром.
var bopEstVintages = []bopEstVintage{
	{
		release: "15.10.2026", fixture: bopEstFixture, unitCell: "K3", columns: 10,
		// Колонки листа: итог января – августа, I и II кварталы, июль и август текущего года, затем
		// та же пятёрка «Справочно:» за прошлый год — дат 2/4/4, всего 10 на ряд.
		dates: map[string][]time.Time{
			"месяц": {
				bopEstLastDay(2026, time.July), bopEstLastDay(2026, time.August),
				bopEstLastDay(2025, time.July), bopEstLastDay(2025, time.August),
			},
			"квартал": {
				bopEstLastQuarter(2026, 1), bopEstLastQuarter(2026, 2),
				bopEstLastQuarter(2025, 1), bopEstLastQuarter(2025, 2),
			},
			"накопленным итогом": {
				bopEstLastDay(2026, time.August), bopEstLastDay(2025, time.August),
			},
		},
		spots: []bopEstSpot{
			{"Счет текущих операций (накопленным итогом)", bopEstLastDay(2026, time.August), 35},
			{"Счет текущих операций (месяц)", bopEstLastDay(2026, time.August), 3.3},
			{"Товары: экспорт (месяц)", bopEstLastDay(2026, time.August), 40.9},
			{"Услуги: импорт (квартал)", bopEstLastQuarter(2026, 2), 25.1},
			{"Счет операций с капиталом (квартал)", bopEstLastQuarter(2026, 2), 0},
			{"Счет текущих операций (накопленным итогом)", bopEstLastDay(2025, time.August), 21},
		},
	},
	{
		release: "14.09.2026", fixture: bopEstFixturePrev, unitCell: "O3", columns: 14,
		// Колонки листа: итог января – июля, два квартала и четыре месяца текущего года, затем та же
		// семёрка «Справочно:» за прошлый год — дат 8/4/2, всего 14 на ряд.
		dates: map[string][]time.Time{
			"месяц": {
				bopEstLastDay(2026, time.April), bopEstLastDay(2026, time.May),
				bopEstLastDay(2026, time.June), bopEstLastDay(2026, time.July),
				bopEstLastDay(2025, time.April), bopEstLastDay(2025, time.May),
				bopEstLastDay(2025, time.June), bopEstLastDay(2025, time.July),
			},
			"квартал": {
				bopEstLastQuarter(2026, 1), bopEstLastQuarter(2026, 2),
				bopEstLastQuarter(2025, 1), bopEstLastQuarter(2025, 2),
			},
			"накопленным итогом": {
				bopEstLastDay(2026, time.July), bopEstLastDay(2025, time.July),
			},
		},
		spots: []bopEstSpot{
			{"Счет текущих операций (накопленным итогом)", bopEstLastDay(2026, time.July), 33.9},
			{"Счет текущих операций (месяц)", bopEstLastDay(2026, time.July), 1.7},
			{"Товары: экспорт (месяц)", bopEstLastDay(2025, time.July), 37.7},
			{"Услуги: импорт (квартал)", bopEstLastQuarter(2026, 2), 25.2},
			{"Счет операций с капиталом (квартал)", bopEstLastQuarter(2026, 2), 0},
		},
	},
}

// bopEstOpenFixture — копия живой фикстуры, загруженная в память под мутацию: ветки fail-fast и
// семантика ячеек проверяются на ней, а не на книге, собранной с нуля (стандарт §11.1). Правки
// идут по копии в памяти — файл на диске не меняется. Мутируется текущий винтаж: его разметка и
// нужна подтестам.
func bopEstOpenFixture(t *testing.T, mutate func(t *testing.T, f *excelize.File, sheet string)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(bopEstFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", bopEstFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	sheets := f.GetSheetList()
	if len(sheets) != 1 {
		t.Fatalf("в фикстуре %d листов, want 1", len(sheets))
	}
	mutate(t, f, sheets[0])
	return f
}

// bopEstSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодном адресе
// ошибкой, и проглотить её значило бы проверять немодифицированный лист.
func bopEstSetCell(t *testing.T, f *excelize.File, sheet, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s, %v): %v", sheet, cell, value, err)
	}
}

// bopEstCell — адрес ячейки листа по колонке и строке (1-based) — так же, как адресует ячейки
// разбор: колонка 1 — метка агрегата, 2… — колонки данных.
func bopEstCell(t *testing.T, column, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(column, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", column, row, err)
	}
	return cell
}

// bopEstHasPoint — есть ли в разборе точка (ряд, дата).
func bopEstHasPoint(rows []appendedRow, name string, date time.Time) bool {
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return true
		}
	}
	return false
}

// TestCbrBopEstAggregateLabels: подписи строк по позициям совпадают со спекой §3.3 — оба набора,
// в порядке «текущий винтаж, прежний»: именно эта таблица решает, какой текст источника разбор
// считает подписью агрегата, и ошибка в ней читалась бы как пропавшая строка.
func TestCbrBopEstAggregateLabels(t *testing.T) {
	if len(cbrBopEstAggregates) != len(bopEstWantLabels) {
		t.Fatalf("агрегатов %d, want %d", len(cbrBopEstAggregates), len(bopEstWantLabels))
	}
	for i, want := range bopEstWantLabels {
		got := cbrBopEstAggregates[i].Labels
		if len(got) != len(want) {
			t.Errorf("агрегат %d (%s): вариантов подписи %d, want %d (%v)", i+1, bopEstWantCodes[i], len(got), len(want), want)
			continue
		}
		for j := range want {
			if got[j] != want[j] {
				t.Errorf("агрегат %d (%s): подпись %d = %q, want %q", i+1, bopEstWantCodes[i], j, got[j], want[j])
			}
		}
	}
}

// TestCbrBopEstSeriesMetaNames: имена 51 ряда каталога совпадают со списком спеки §3.3 дословно —
// и агрегаты, и виды периода, и их порядок, и форма «<агрегат> (<вид>)»; вместе с ними
// проверяются поля, от которых зависит чтение ряда агентом MCP: единицы, частота, origin с
// файлом, листом и подписями строки источника (всеми вариантами и блоком для повторяющихся
// подписей) и description, где есть привязка даты, ноль как значение и ревизия оценки.
func TestCbrBopEstSeriesMetaNames(t *testing.T) {
	want := bopEstWantNames()
	if len(cbrBopEstSeriesMeta) != len(want) {
		t.Fatalf("записей каталога %d, want %d (17 агрегатов × 3 вида)", len(cbrBopEstSeriesMeta), len(want))
	}
	frequency := map[string]string{"месяц": "M", "квартал": "Q", "накопленным итогом": "M"}
	for i, name := range want {
		meta := cbrBopEstSeriesMeta[i]
		if meta.Series != name {
			t.Errorf("ряд %d каталога = %q, want %q", i, meta.Series, name)
			continue
		}
		code := bopEstWantCodes[i/len(bopEstWantKinds)]
		kind := bopEstWantKinds[i%len(bopEstWantKinds)]
		if meta.Source != "cbr_bop_est" {
			t.Errorf("ряд %q: source %q, want cbr_bop_est (источник каталога = имя таблицы)", name, meta.Source)
		}
		if meta.Unit != "млрд долларов США" {
			t.Errorf("ряд %q: unit %q, want млрд долларов США", name, meta.Unit)
		}
		if meta.Frequency != frequency[kind] {
			t.Errorf("ряд %q: frequency %q, want %q", name, meta.Frequency, frequency[kind])
		}
		for _, want := range []string{"bop_est.xlsx", "Оценка ПБ"} {
			if !strings.Contains(meta.Origin, want) {
				t.Errorf("ряд %q: origin %q не содержит %q", name, meta.Origin, want)
			}
		}
		// origin называет все подписи строки — и текущую, и прежнюю: агент, сверяющий ряд со старым
		// выпуском файла, обязан видеть, какой строке он соответствует (спека §3.3).
		for _, label := range bopEstWantLabels[i/len(bopEstWantKinds)] {
			if !strings.Contains(meta.Origin, "«"+label+"»") {
				t.Errorf("ряд %q: origin %q не называет подпись строки «%s»", name, meta.Origin, label)
			}
		}
		if repeated, ok := bopEstRepeatedLabels[code]; ok {
			if !strings.Contains(meta.Origin, "блока «"+repeated.block+"»") {
				t.Errorf("ряд %q: origin %q не называет блок «%s» — повторяющаяся подпись «%s» не различима",
					name, meta.Origin, repeated.block, repeated.label)
			}
		}
		for _, want := range []string{"последний день", "Ноль", "ревизу"} {
			if !strings.Contains(meta.Description, want) {
				t.Errorf("ряд %q: description %q не содержит %q (правило 11 AGENTS.md)", name, meta.Description, want)
			}
		}
	}
}

// TestCbrBopEstImporterName: Name() — контракт фильтра `make import STAT=…` и id шага dagu/cbr.yaml;
// сверяется литералом, а не таблицей кода (стандарт §8.5).
func TestCbrBopEstImporterName(t *testing.T) {
	if got := cbrBopEstImporter.Name(); got != "cbr_bop_est" {
		t.Errorf("Name() = %q, want %q", got, "cbr_bop_est")
	}
}

// TestCbrBopEstDdl: DDL — контракт данных (правило 5 AGENTS.md: значения — Float64; движок и ключ
// общие для таблиц пакета). Проверяется литералами: DDL в %s-форме, имя таблицы подставляет
// шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestCbrBopEstDdl(t *testing.T) {
	if len(cbrBopEst.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrBopEst.CreateTable))
	}
	ddl := fmt.Sprintf(cbrBopEst.CreateTable[0], "cbr_bop_est")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_bop_est",
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

// TestCbrBopEstSeriesMetaCoversParsedRows: каталог cbr_bop_est (51 ряд) и разбор каждой фикстуры
// совпадают в обе стороны (стандарт §11.4): имена рядов объявлены кодом, поэтому проверяются и
// забытое имя ряда в каталоге, и забытая колонка в разборе — на обоих наборах подписей строк.
func TestCbrBopEstSeriesMetaCoversParsedRows(t *testing.T) {
	for _, vintage := range bopEstVintages {
		t.Run(vintage.release, func(t *testing.T) {
			checkCbrSeriesMeta(t, "cbr_bop_est", cbrBopEstSeriesMeta, func(rec *appendRecorder) error {
				f, err := excelize.OpenFile(vintage.fixture)
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				return cbrBopEst.ImportFunc(f, rec)
			})
		})
	}
}

// TestCbrBopEstFixtureRows: разбор каждой фикстуры даёт 17 × число её колонок точек (170 у текущего
// винтажа, 238 у прежнего) и по каждому ряду — точки всех видов периода с датами по правилу
// привязки (спека §3.4), а не списком дней.
func TestCbrBopEstFixtureRows(t *testing.T) {
	for _, vintage := range bopEstVintages {
		t.Run(vintage.release, func(t *testing.T) {
			rows := cbrFixtureRows(t, vintage.fixture, cbrBopEst.ImportFunc)
			if len(rows) != vintage.total() {
				t.Errorf("строк разбора %d, want %d (17 агрегатов × %d колонок)", len(rows), vintage.total(), vintage.columns)
			}
			dates := map[string][]time.Time{}
			for _, row := range rows {
				dates[row.name] = append(dates[row.name], row.date)
			}
			for _, code := range bopEstWantCodes {
				for _, kind := range bopEstWantKinds {
					name := bopEstWantName(code, kind)
					want := vintage.dates[kind]
					got := dates[name]
					if len(got) != len(want) {
						t.Errorf("ряд %q: точек %d, want %d (вид «%s»)", name, len(got), len(want), kind)
						continue
					}
					for i := range want {
						if !got[i].Equal(want[i]) {
							t.Errorf("ряд %q: точка %d = %s, want %s (привязка даты вида «%s»)",
								name, i, got[i].Format("2006-01-02"), want[i].Format("2006-01-02"), kind)
							break
						}
					}
				}
			}
		})
	}
}

// TestCbrBopEstSpotValues: значения из известных ячеек обоих винтажей (спека §3.9) — крайние
// агрегаты выпуска, месяцы текущего и прошлого года и ноль как настоящее значение (сальдо счёта
// операций с капиталом). Значения литералами: сверка идёт с разбором, а не с константой кода.
func TestCbrBopEstSpotValues(t *testing.T) {
	for _, vintage := range bopEstVintages {
		t.Run(vintage.release, func(t *testing.T) {
			rows := cbrFixtureRows(t, vintage.fixture, cbrBopEst.ImportFunc)
			for _, spot := range vintage.spots {
				cbrCheckSpotFloat64(t, rows, spot.series, spot.date, spot.value)
			}
		})
	}
}

// bopRowValue — значение точки (ряд, дата) в разборе фикстуры; отсутствие точки фатально: тест,
// сверяющий два источника, обязан упасть, если один из них точку не дал.
func bopRowValue(t *testing.T, rows []appendedRow, name string, date time.Time) float64 {
	t.Helper()
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return row.value
		}
	}
	t.Fatalf("в разборе нет точки (%q, %s)", name, date.Format("2006-01-02"))
	return 0
}

// TestCbrBopUnitsAgreeAcrossSources: единицы двух выгрузок согласованы — и связь закреплена числом,
// а не только текстом каталога. Одни и те же агрегаты у оценки идут в млрд долларов США, у
// стандартной презентации — в млн, поэтому значение оценки равно значению стандартной презентации,
// делённому на 1000 (спека §3.6, §3.7). Сверяется один ряд одной даты на двух живых фикстурах
// («Товары: экспорт» за II квартал 2026: 125,7 млрд против 125 716,38 млн). Споты внутри одной
// выгрузки такую рассинхронизацию не поймают — они не видят вторую; допуск 0,1 — точность
// публикации оценки (один знак после запятой по млрд), то есть уже прошедшее округление источника.
func TestCbrBopUnitsAgreeAcrossSources(t *testing.T) {
	const (
		series    = "Товары: экспорт (квартал)"
		tolerance = 0.1
	)
	date := bopEstLastQuarter(2026, 2)
	estimate := bopRowValue(t, cbrFixtureRows(t, bopEstFixture, cbrBopEst.ImportFunc), series, date)
	standard := bopRowValue(t, cbrFixtureRows(t, bopStandardFixture, cbrBopStandard.ImportFunc), series, date)
	want := standard / 1000
	if diff := estimate - want; diff > tolerance || diff < -tolerance {
		t.Errorf("ряд %q за %s: в оценке %v млрд долларов США, в стандартной презентации %v млн (= %v млрд): расхождение больше допуска %v — единицы двух выгрузок разошлись",
			series, date.Format("2006-01-02"), estimate, standard, want, tolerance)
	}
}

// TestCbrBopEstAppendsFloat64: разбор пишет значение как float64 — тип колонки value в DDL
// (правило 5 AGENTS.md). Запись float32 прошла бы в колонку Float64 молча, потеряв точность на
// шаге представления, и расхождение заметила бы только живая БД.
func TestCbrBopEstAppendsFloat64(t *testing.T) {
	for _, vintage := range bopEstVintages {
		t.Run(vintage.release, func(t *testing.T) {
			f, err := excelize.OpenFile(vintage.fixture)
			if err != nil {
				t.Fatalf("открытие фикстуры %s: %v", vintage.fixture, err)
			}
			defer func() { _ = f.Close() }()
			rec := newCbrRecorder()
			if err = cbrBopEst.ImportFunc(f, rec); err != nil {
				t.Fatalf("разбор фикстуры: %v", err)
			}
			if rec.err != nil {
				t.Fatalf("разбор фикстуры: схема строк не совпала с разбором: %v", rec.err)
			}
			if len(rec.raw) == 0 {
				t.Fatal("разбор не дал ни одной точки")
			}
			for i, raw := range rec.raw {
				if len(raw) != 3 {
					t.Fatalf("строка %d: %d значений, ожидалась форма (name, date, value)", i, len(raw))
				}
				if _, ok := raw[2].(float64); !ok {
					t.Fatalf("строка %d: значение записано как %T, ожидался float64 (колонка value — Float64)", i, raw[2])
				}
			}
		})
	}
}

// TestCbrBopEstEmptyBookFails: книги без листа «Оценка ПБ …» — ошибка с именем таблицы и списком
// листов, а не «0 rows» (стандарт §6.4). Пустая книга excelize — это лист «Sheet1», то есть та
// же ветка «лист по префиксу не найден»; вторая книга проверяет, что префикс не сравнивается со
// всем именем листа целиком.
func TestCbrBopEstEmptyBookFails(t *testing.T) {
	for name, sheet := range map[string]string{
		"пустая книга":          "",
		"лист с другим именем":  "Платёжный баланс",
		"лист с похожим именем": "Оценка платежного баланса",
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
			err := cbrBopEst.ImportFunc(f, rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_bop_est") {
				t.Errorf("ошибка %q не называет таблицу cbr_bop_est", err)
			}
			if !strings.Contains(err.Error(), "листа с префиксом") {
				t.Errorf("ошибка %q не называет причину (лист по префиксу не найден)", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrBopEstFailFastOnFixture: ветки, где разбор обязан вернуть ошибку, а не «0 строк»
// (стандарт §6.4, спека §3.9), — на мутированной копии живой фикстуры текущего винтажа
// (стандарт §11.1). Копия, а не синтетическая книга: якоря, шапка и ячейки данных в ней те же,
// что у источника, поэтому ошибка приходит от мутации, а не от сборки книги. Проверка «в рекордере
// 0 строк» — про гигиену парсера, а не про чистоту БД: при ошибке ImportFunc Send не вызывается
// вовсе, и незаписанный батч в БД не уходит, но парсер не должен записать ни одной строки до того,
// как поймал ошибку разбора (иначе частично разобранный выпуск выглядел бы успешным).
func TestCbrBopEstFailFastOnFixture(t *testing.T) {
	live := bopEstVintages[0]
	for name, tc := range map[string]struct {
		mutate  func(t *testing.T, f *excelize.File, sheet string)
		wantErr string
	}{
		"нет строки единиц": {
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, live.unitCell, "") },
			wantErr: "не найдена строка единиц",
		},
		"нет первой подписи агрегата": {
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "A6", "Итого") },
			wantErr: "нет первой ожидаемой подписи агрегата",
		},
		"подпись не совпала ни с одним вариантом": {
			// Строка 7 — «Товары»: разбор обязан назвать позицию и оба варианта её подписи, иначе по
			// прогону не видно, чего именно он не дождался (спека §3.3).
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "A7", "Баланс") },
			wantErr: "не найдена подпись агрегата «Товары» (позиция 2; варианты: «Товары» / «Торговый баланс»)",
		},
		"пропала подпись в середине списка": {
			// Строка 13 — «Первичные и вторичные доходы» (восьмая в списке): разбор ищет подписи
			// последовательно, поэтому пропавшая в середине — ошибка, а не пропуск ряда.
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "A13", "Доходы") },
			wantErr: "не найдена подпись агрегата «Баланс первичных и вторичных доходов» (позиция 8",
		},
		"подпись периода не разбирается": {
			// «II квартал» без года: подпись есть, период из неё не выводится — колонка осталась бы
			// без даты, а её точки молча потерялись бы.
			mutate: func(t *testing.T, f *excelize.File, sheet string) {
				bopEstSetCell(t, f, sheet, "D4", "II квартал")
			},
			wantErr: "не разобрана как период",
		},
		"колонка с данными без подписи": {
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "B4", "") },
			wantErr: "подпись периода не разобрана",
		},
		"значение не число": {
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "B6", "н/д") },
			wantErr: "значение \"н/д\"",
		},
		"значение NaN": {
			// ParseFloat принимает «NaN» молча: точка с NaN отравила бы ряд (суммы — NaN).
			mutate:  func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "B6", "NaN") },
			wantErr: "не конечно",
		},
		"все ячейки данных пусты": {
			// Шапка и подписи агрегатов целы, а точек нет: «0 rows» с зелёным dagu оставил бы все 51
			// ряд без обновления.
			mutate: func(t *testing.T, f *excelize.File, sheet string) {
				for row := bopEstFirstDataRow; row < bopEstFirstDataRow+len(bopEstWantCodes); row++ {
					for column := 2; column < 2+live.columns; column++ {
						bopEstSetCell(t, f, sheet, bopEstCell(t, column, row), "")
					}
				}
			},
			wantErr: "не дал ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrBopEst.ImportFunc(bopEstOpenFixture(t, tc.mutate), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_bop_est") {
				t.Errorf("ошибка %q не называет таблицу cbr_bop_est", err)
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

// TestCbrBopEstFixtureSemantics — разбор мутированной копии живой фикстуры текущего винтажа при
// ожидаемом изменении источника: дата выпуска в имени листа разбору не мешает (лист ищется по
// префиксу, спека §3.5), пустая ячейка точки не даёт, а литеральный ноль даёт (стандарт §6.7,
// спека §3.5: в платёжном балансе ноль — настоящее значение, в отличие от cbr_int_rate_loans).
func TestCbrBopEstFixtureSemantics(t *testing.T) {
	live := bopEstVintages[0]
	accumulated := live.spots[0].series
	august2026 := live.spots[0].date
	for name, tc := range map[string]struct {
		mutate   func(t *testing.T, f *excelize.File, sheet string)
		wantRows int
		value    float64
		isPoint  bool
	}{
		"дата выпуска в имени листа не мешает разбору": {
			mutate: func(t *testing.T, f *excelize.File, sheet string) {
				if err := f.SetSheetName(sheet, "Оценка ПБ (01.11.2026)"); err != nil {
					t.Fatalf("SetSheetName: %v", err)
				}
			},
			wantRows: live.total(),
			value:    live.spots[0].value,
			isPoint:  true,
		},
		"пустая ячейка точки не даёт": {
			// B6 — накопленный итог счёта текущих операций за январь - август 2026.
			mutate:   func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "B6", "") },
			wantRows: live.total() - 1,
			isPoint:  false,
		},
		"литеральный ноль — точка": {
			mutate:   func(t *testing.T, f *excelize.File, sheet string) { bopEstSetCell(t, f, sheet, "B6", 0) },
			wantRows: live.total(),
			value:    0,
			isPoint:  true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrBopEst.ImportFunc(bopEstOpenFixture(t, tc.mutate), rec); err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if rec.err != nil {
				t.Fatalf("схема строк не совпала с разбором: %v", rec.err)
			}
			if len(rec.rows) != tc.wantRows {
				t.Errorf("точек %d, want %d: ячейка без значения точки не даёт, а ноль и значение месяца — дают",
					len(rec.rows), tc.wantRows)
			}
			if got := bopEstHasPoint(rec.rows, accumulated, august2026); got != tc.isPoint {
				t.Errorf("точка (%q, %s): наличие %v, want %v", accumulated, august2026.Format("2006-01-02"), got, tc.isPoint)
			}
			if !tc.isPoint {
				return
			}
			cbrCheckSpotFloat64(t, rec.rows, accumulated, august2026, tc.value)
		})
	}
}
