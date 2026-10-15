package cbr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Стандартная презентация платёжного баланса Российской Федерации (ЦБ РФ):
// https://www.cbr.ru/statistics/macro_itm/external_sector/pb/ — прямая стабильная выгрузка
// bal_of_payments_standart.xlsx. Два листа («Кварталы» с 1 квартала 1994 года и «Годы» 1994–2025),
// у каждого те же 17 верхних агрегатов в одном порядке и та же строка единиц. Публикуется по
// методологии платёжного баланса 2022 года; файл обновляется раз в квартал. Канал «внешние
// условия и курс» контура ключевой ставки (пункт 4.2 №14 роадмапа).

const (
	cbrBopStandardTable = "cbr_bop_standard"
	// cbrBopStandardDataUrl — стабильная прямая ссылка на файл (стандарт §4.1, строка «XLSX»):
	// даты в ней нет, темп обновления — квартал, поэтому качает файл шаблон — те же байты уходят
	// и в разбор, и в винтажный архив версии (§9.6). «standart» — опечатка в адресе ЦБ, а не в
	// коде: таблица называется cbr_bop_standard (спека §3.1).
	cbrBopStandardDataUrl = "https://www.cbr.ru/vfs/statistics/credit_statistics/bop/bal_of_payments_standart.xlsx"
	// cbrBopStandardCountPoints — количественный чек версии (стандарт §9.2): число точек своей
	// таблицы. FINAL обязателен — иначе count() считал бы несмёрженные части.
	cbrBopStandardCountPoints = "SELECT count() FROM cbr_bop_standard FINAL"
	// cbrBopStandardUnit — единицы значения, как в строке единиц источника «млн долларов США»
	// (строка 4 листа; в файле оценки те же ряды идут в млрд — в 1000 раз крупнее).
	cbrBopStandardUnit = "млн долларов США"
)

// cbrBopStandardSheet — лист стандартной презентации и вид периода всех его колонок (спека §3.5):
// «Кварталы» → квартал, «Годы» → год. Вид — часть имени ряда, поэтому объявлен кодом, а лист лишь
// сверяется с ним: смена шапки листа обязана валить прогон, а не подменить вид у половины точек.
type cbrBopStandardSheet struct {
	Sheet string
	Kind  string
}

// cbrBopStandardSheets — обход листов в порядке книги. Имена листов здесь — константы, а не
// признак: даты выпуска в них нет (лист оценки ищется по префиксу, здесь — по точному имени),
// поэтому отсутствие любого из двух листов — ошибка (спека §3.5). Номер квартала эти листы пишут
// арабской цифрой («1 квартал 1994 г.»), тогда как оценка — римской: обе записи разбирает общий
// cbrBopQuarter, и переписывать подписи перед разбором не нужно.
var cbrBopStandardSheets = []cbrBopStandardSheet{
	{Sheet: "Кварталы", Kind: cbrBopKindQuarter},
	{Sheet: "Годы", Kind: cbrBopKindYear},
}

// cbrBopStandardAggregates — 17 верхних агрегатов стандартной презентации в порядке строк листа
// (спека §3.3). Label — подпись строки источника, по которой идёт последовательный поиск: в листе
// десятки строк и подписи плоские и повторяющиеся («Экспорт» и «Импорт» встречаются по нескольку
// десятков раз), поэтому поиск без порядка прочитал бы экспорт услуг как экспорт товаров
// (стандарт §6.8). Code — имя кода: повторяющуюся подпись имя различить не может, поэтому
// «Товары: экспорт» собирает код. Block — блок листа, к которому относится повторяющаяся подпись:
// origin каталога обязан его назвать, иначе «строка «Экспорт»» не говорит, о каком экспорте речь.
// Measures — что измеряет агрегат: description каталога собирается из него в cbr/bop_meta.go
// (правило 11 AGENTS.md — описание каждого ряда).
var cbrBopStandardAggregates = []struct {
	Label    string
	Code     string
	Block    string
	Measures string
}{
	{
		Label: "Счет текущих операций", Code: "Счет текущих операций",
		Measures: "Сальдо счёта текущих операций: экспорт товаров, услуг и полученные первичные и вторичные доходы минус импорт и выплаченные доходы. Ключевой агрегат внешних условий: профицит — приток средств в экономику, дефицит — отток.",
	},
	{
		Label:    "Экспорт товаров, услуг, полученные первичные и вторичные доходы",
		Code:     "Экспорт товаров, услуг, первичных и вторичных доходов",
		Measures: "Валовой экспорт счёта текущих операций: товары, услуги и полученные первичные и вторичные доходы вместе — знак у этого ряда всегда положительный.",
	},
	{
		Label:    "Импорт товаров, услуг, выплаченные первичные и вторичные доходы",
		Code:     "Импорт товаров, услуг, первичных и вторичных доходов",
		Measures: "Валовой импорт счёта текущих операций: товары, услуги и выплаченные первичные и вторичные доходы вместе — знак у этого ряда всегда положительный. Сальдо счёта текущих операций — разность соседних строк листа.",
	},
	{
		Label: "Товары и услуги", Code: "Товары и услуги",
		Measures: "Сальдо баланса товаров и услуг: экспорт товаров и услуг минус импорт товаров и услуг, без доходов. Промежуточный агрегат между счётом текущих операций и остальными двумя его составляющими.",
	},
	{
		// Подпись источника — «Товары » с пробелом на конце (одна из ловушек листа: строкой выше
		// стоит «Товары и услуги»). Пробел для поиска не значим: cbrBopNormalizeLabel сворачивает
		// пробелы, — но объявлена подпись как есть, чтобы сверка со спекой §3.3 шла по тексту.
		Label: "Товары ", Code: "Товары",
		Measures: "Сальдо торгового баланса: экспорт товаров минус импорт товаров, без услуг и доходов. Чувствителен к условиям внешней торговли — «ухудшение условий внешней торговли» в резюме обсуждения ключевой ставки читается по нему.",
	},
	{
		Label: "Экспорт", Code: "Товары: экспорт", Block: "Товары",
		Measures: "Стоимость экспорта товаров — без услуг и доходов (блок торгового баланса).",
	},
	{
		Label: "Импорт", Code: "Товары: импорт", Block: "Товары",
		Measures: "Стоимость импорта товаров — без услуг и доходов (блок торгового баланса).",
	},
	{
		Label: "Услуги", Code: "Услуги",
		Measures: "Сальдо баланса услуг: экспорт услуг минус импорт услуг; подстатьи услуг (транспорт, поездки и прочие) в таблицу не импортируются.",
	},
	{
		Label: "Экспорт", Code: "Услуги: экспорт", Block: "Услуги",
		Measures: "Стоимость экспорта услуг (блок баланса услуг).",
	},
	{
		Label: "Импорт", Code: "Услуги: импорт", Block: "Услуги",
		Measures: "Стоимость импорта услуг (блок баланса услуг).",
	},
	{
		Label: "Первичные доходы", Code: "Первичные доходы",
		Measures: "Сальдо первичных доходов: оплата труда, доходы от инвестиций (проценты, дивиденды, реинвестированные доходы) и прочие первичные доходы полученные минус выплаченные.",
	},
	{
		Label: "Вторичные доходы", Code: "Вторичные доходы",
		Measures: "Сальдо вторичных доходов: текущие трансферты (в том числе переводы работников и гуманитарная помощь) полученные минус выплаченные.",
	},
	{
		Label: "Счет операций с капиталом", Code: "Счет операций с капиталом",
		Measures: "Сальдо счёта операций с капиталом: трансферты капитала и операции с непроизведёнными нефинансовыми активами. Величина по определению мала по сравнению со счётом текущих операций и по знаку неустойчива.",
	},
	{
		Label:    "Сальдо счета текущих операций и счета операций с капиталом",
		Code:     "Сальдо счета текущих операций и счета операций с капиталом",
		Measures: "Сумма сальдо счёта текущих операций и счёта операций с капиталом — чистое кредитование (при положительном значении) или чистое заимствование экономики у внешнего мира. По методологии равен сальдо финансового счёта с обратным знаком (в пределах чистых ошибок и пропусков).",
	},
	{
		Label: "Сальдо финансового счета", Code: "Сальдо финансового счета",
		Measures: "Сальдо финансового счёта: чистое приобретение финансовых активов минус чистое принятие обязательств, включая операции с резервными активами; положительное значение отвечает профициту счёта текущих операций и капитала.",
	},
	{
		Label: "Резервные активы", Code: "Резервные активы",
		Measures: "Операции с резервными активами Банка России: положительное значение — чистое приобретение (резервы выросли), отрицательное — сокращение. Это потоки, а не изменение запаса: курсовая и прочая переоценка резервов в файл не входит.",
	},
	{
		Label: "Чистые ошибки и пропуски", Code: "Чистые ошибки и пропуски",
		Measures: "Балансирующая статья платёжного баланса: расхождение между счётом текущих операций с капиталом и финансовым счётом (статистическая погрешность). Знак значения сам по себе экономического смысла не несёт.",
	},
}

// cbrBopStandardName собирает имя ряда из имени кода и вида периода: «Товары: экспорт (квартал)» —
// вид идёт в имя, а не в колонку таблицы (спека §3.2). Имя собирает код, и его же видят каталог
// рядов (cbr/bop_meta.go) и разбор: второго источника правды для имён нет.
func cbrBopStandardName(code, kind string) string {
	return code + " (" + kind + ")"
}

// cbrBopStandardSheetPlan — разобранная разметка листа: его строки (сырые значения ячеек), разметка
// колонок из cbrBopSheetLayout и строка каждого из 17 агрегатов. Разметка обоих листов строится
// до первой записи: пропавшая подпись или лист на втором листе не должны оставить в таблице
// половину выпуска — частично разобранный выпуск хуже пустого, потому что выглядит успешным
// (стандарт §6.4).
type cbrBopStandardSheetPlan struct {
	Sheet      string
	Rows       [][]string
	Layout     cbrBopLayout
	RowIndexes []int
}

// cbrBopStandardReadSheet выводит план одного листа: лист по точному имени, якоря шапки через
// cbrBopSheetLayout, сверка вида колонок с видом листа и последовательный поиск 17 подписей
// агрегатов. Подпись i+1 ищется только ниже подписи i — этим и различаются повторяющиеся
// «Экспорт»/«Импорт»; ненайденная ожидаемая подпись — ошибка с её текстом (стандарт §6.4), а
// посторонние строки между агрегатами молча пропускаются (§6.6), поэтому 391 неимпортируемая
// подстатья листа разбору не мешает.
func cbrBopStandardReadSheet(xlsx *excelize.File, sheet cbrBopStandardSheet) (cbrBopStandardSheetPlan, error) {
	plan := cbrBopStandardSheetPlan{}
	name, err := cbrBopSheetByName(xlsx, cbrBopStandardTable, sheet.Sheet)
	if err != nil {
		return plan, err
	}
	plan.Sheet = name
	rows, err := xlsx.GetRows(name, excelize.Options{RawCellValue: true})
	if err != nil {
		return plan, fmt.Errorf("%s: лист «%s»: %w", cbrBopStandardTable, name, err)
	}
	plan.Rows = rows
	// Первая ожидаемая подпись — якорь первой строки данных; список агрегатов непуст по
	// объявлению, поэтому индекс 0 здесь есть всегда.
	layout, err := cbrBopSheetLayout(rows, cbrBopStandardTable, name, cbrBopStandardAggregates[0].Label)
	if err != nil {
		return plan, err
	}
	plan.Layout = layout
	// Вид периода каждой колонки обязан совпасть с видом листа (спека §3.5): колонка другого вида
	// означала бы, что ЦБ сменил шапку листа, а имя ряда — часть ключа (name, date), и подмена
	// вида переписала бы чужие точки молча.
	for _, column := range layout.Columns {
		if column.Period.Kind != sheet.Kind {
			return plan, fmt.Errorf("%s: лист «%s», колонка %s: подпись периода дала вид %q, а лист объявлен как %q — источник изменил шапку листа",
				cbrBopStandardTable, name, cbrBopColumnName(column.Index), column.Period.Kind, sheet.Kind)
		}
	}
	plan.RowIndexes = make([]int, len(cbrBopStandardAggregates))
	searchFrom := layout.FirstDataRow
	for i, agg := range cbrBopStandardAggregates {
		rowIndex := -1
		for j := searchFrom; j < len(rows); j++ {
			if len(rows[j]) > 0 && cbrBopSameLabel(rows[j][0], agg.Label) {
				rowIndex = j
				break
			}
		}
		if rowIndex < 0 {
			return plan, fmt.Errorf("%s: лист «%s»: после строки %d не найдена ожидаемая подпись агрегата %q — источник изменил состав листа",
				cbrBopStandardTable, name, searchFrom, agg.Label)
		}
		plan.RowIndexes[i] = rowIndex
		searchFrom = rowIndex + 1
	}
	return plan, nil
}

// cbrBopStandardImport разбирает оба листа стандартной презентации: 17 агрегатов × 130 кварталов
// и 17 × 32 года (спека §3.3). Сначала размечаются оба листа целиком, и только потом пишутся
// точки: ошибка на втором листе не оставляет в батче половину первого. Пустая ячейка — не точка
// (§6.7), литеральный ноль — точка: в платёжном балансе ноль означает «сальдо нулевое», а не
// «данных не имеется» (в этой выгрузке ноль встречается, например, в счёте операций с капиталом) —
// отличие от cbr_int_rate_loans, где ноль в ячейке означает пропуск. Нечисловая ячейка и NaN/Inf —
// ошибка: ParseFloat принимает «NaN» молча, а точка с NaN отравила бы ряд.
func cbrBopStandardImport(xlsx *excelize.File, batch driver.Batch) error {
	plans := make([]cbrBopStandardSheetPlan, 0, len(cbrBopStandardSheets))
	for _, sheet := range cbrBopStandardSheets {
		plan, err := cbrBopStandardReadSheet(xlsx, sheet)
		if err != nil {
			return err
		}
		plans = append(plans, plan)
	}
	count := 0
	for _, plan := range plans {
		for i, agg := range cbrBopStandardAggregates {
			row := plan.Rows[plan.RowIndexes[i]]
			for _, column := range plan.Layout.Columns {
				// Строка короче набора колонок данных — у остальных колонок ячеек нет; недостающая
				// ячейка равносильна пустой и точкой не становится (§6.7).
				if column.Index >= len(row) {
					continue
				}
				cell := strings.TrimSpace(row[column.Index])
				if cell == "" {
					continue
				}
				value, err := strconv.ParseFloat(cell, 64)
				if err != nil {
					return fmt.Errorf("%s: лист «%s», строка %q, колонка %s: значение %q: %w",
						cbrBopStandardTable, plan.Sheet, agg.Label, cbrBopColumnName(column.Index), cell, err)
				}
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return fmt.Errorf("%s: лист «%s», строка %q, колонка %s: значение %q не конечно (NaN/Inf)",
						cbrBopStandardTable, plan.Sheet, agg.Label, cbrBopColumnName(column.Index), cell)
				}
				name := cbrBopStandardName(agg.Code, column.Period.Kind)
				if err = batch.Append(name, column.Period.Date, value); err != nil {
					return fmt.Errorf("%s: ряд %q, %s: %w",
						cbrBopStandardTable, name, column.Period.Date.Format("2006-01-02"), err)
				}
				count++
			}
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор не дал ни одной точки", cbrBopStandardTable)
	}
	return nil
}

// cbrBopStandard — стандартная презентация платёжного баланса: таблица, DDL и разбор двух листов
// («Кварталы» → вид «квартал», «Годы» → вид «год»); каталог рядов и витрину v_cbr_bop публикует
// шаблон через AfterImport. ЦБ пересчитывает и историю, поэтому точка перезаписывается по ключу
// (name, date). Пропавший лист, сломанная шапка, пропавшая подпись агрегата и ноль точек —
// ошибка (fail-fast, стандарт §6.4): «0 rows» выглядели бы успешным прогоном, а 34 ряда остались
// бы без обновления молча.
var cbrBopStandard = util.ClickHouseImport{
	TableName: cbrBopStandardTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	DataUrl:                cbrBopStandardDataUrl,
	ImportFunc:             cbrBopStandardImport,
	AfterImport:            afterCbrBop(cbrBopStandardSeriesMeta),
	PublicationCountPoints: cbrBopStandardCountPoints,
}

// cbrBopStandardImporter — стандартные компоненты платёжного баланса; регистрируется реестром
// cbr/importers.go.
var cbrBopStandardImporter = &cbrBopStandard
