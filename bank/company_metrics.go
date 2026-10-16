package bank

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// bankCompanyMetricsName — имя импортёра в реестре chimport.Stats (и шаг dagu/bank.yaml).
// Имя и таблица у него разные: своей таблицы у импортёра нет — он публикует метрики в общую
// company_financials (прецедент cbr_mo_br, пишущего в macro_series).
const bankCompanyMetricsName = "bank_company_metrics"

// bankCompanyMetricsTable — целевая таблица импортёра: общая для сектора company_financials
// (её же ведут polyus/import.go и витрины views/company.go). Данные PLZL этот импортёр не
// трогает: он пишет только строки компаний 'SBER' и 'VTBR'.
const bankCompanyMetricsTable = "company_financials"

// Единицы словаря (спека §3.7): остатки и потоки — млрд ₽, коэффициенты и нормативы достаточности
// — проценты. Второй литерал совпадает с единицей процентных рядов PLZL в той же таблице.
const (
	bankCompanyUnitBln     = "млрд ₽"
	bankCompanyUnitPercent = "%"
)

// bankCompanyMetricsDdl — копия канонического DDL company_financials (ARCHITECTURE.md §6.2) с
// 'M' = 5 в period_type: месячную гранулярность пишет только этот импортёр (обе банковские
// отчётности помесячны). Имя таблицы подставлено конкатенацией, а не %s: оператор исполняется
// сырым conn.Exec (прецедент companyFinancialsCreateTable в views/company.go). Копий DDL теперь
// три (polyus/import.go, views/company.go, этот файл), и они обязаны совпадать колонка в колонку.
// CREATE TABLE IF NOT EXISTS существующую таблицу не меняет, поэтому enum на живой БД расширяет
// ALTER (build/company_financials_period_type_m.sql; прогон — задача 12).
const bankCompanyMetricsDdl = `CREATE TABLE IF NOT EXISTS ` + bankCompanyMetricsTable + `
(
    company     LowCardinality(String),
    metric      LowCardinality(String),
    period      String,
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4, 'M' = 5),
    source_kind LowCardinality(String),
    value       Nullable(Float64),
    unit        LowCardinality(String),
    source_url  LowCardinality(String),
    source_page UInt16,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period, source_kind, source_url)`

// bankCompanyTables — сырые таблицы, которые обходит Import, в порядке обхода: карта порядка не
// даёт, а он нужен детерминированному логу и каталогу. tbank_group_ifrs в списке намеренно — его
// пропуск должен читаться в логе прогона (источник не починен, спека §3.5); в словаре метрик его
// нет: разобранных рядов у него пока нет, а каталог без источника был бы обещанием.
var bankCompanyTables = []string{sberRpbuDataUrlTable, vtbIfrsTable, tbIfrsTable}

// bankCompanySourceKind — вид отчётности источника: 'ras' — РСБУ Сбербанка, 'ifrs' — МСФО группы
// ВТБ. Значения — те же литералы, что стоят в правиле разрешения v_company_financials
// (views/company.go): у каждой банковской компании вид ровно один, и он же — её ступень в правиле.
var bankCompanySourceKind = map[string]string{
	sberRpbuDataUrlTable: "ras",
	vtbIfrsTable:         "ifrs",
}

// bankCompanySourceCompany — тикер эмитента в колонке company.
var bankCompanySourceCompany = map[string]string{
	sberRpbuDataUrlTable: "SBER",
	vtbIfrsTable:         "VTBR",
}

// bankCompanySourceTitle — как эмитент называется в заголовке ряда каталога.
var bankCompanySourceTitle = map[string]string{
	sberRpbuDataUrlTable: "Сбербанк (РСБУ)",
	vtbIfrsTable:         "группа ВТБ (МСФО)",
}

// bankCompanySourceUrl — идентификатор документа-источника для колонки source_url. У Сбера это
// статичный адрес файла книги (страница источника закрыта JS-challenge, имя файла не меняется),
// у ВТБ — страница результатов: имена его пофайловых выпусков несут отчётную дату и меняются
// каждый месяц, поэтому документом-источником ряда названа страница, а не файл — иначе
// source_url у одного и того же ряда менялся бы каждый месяц.
var bankCompanySourceUrl = map[string]string{
	sberRpbuDataUrlTable: sberRpbuXlsDataUrl,
	vtbIfrsTable:         vtbIfrsResultsUrl,
}

// bankCompanyMetric — метрика словаря: имя метрики в company_financials и единица её значения
// (одна из двух единиц словаря). Это и есть значение карты маппинга «подпись источника → метрика».
type bankCompanyMetric struct {
	Metric string
	Unit   string
}

// bankCompanyMetricSpec — строка словаря: подпись источника (она же имя ряда сырой таблицы: у ВТБ
// подпись нормализуется до имени ряда разбором — vtbSeriesName, у Сбера разбор отдаёт подпись
// дословно, и подписи словаря пробелов не содержат), метрика, единица и место подписи в книге.
// Лист и оговорка нужны каталогу (origin и description); метрика и единица — контракт маппинга
// (TestBankCompanyMetricMapWellFormed).
type bankCompanyMetricSpec struct {
	Label  string
	Metric string
	Unit   string
	Sheet  string
	// Note — оговорка ряда, без которой метрику прочли бы не тем, чем она является: база
	// «до/после резервов» есть у кредитного портфеля обеих компаний, и она у них разная.
	Note string
}

// bankCompanyMetricSpecs — словарь метрик по сырым таблицам (спека §3.7): у Сбера — все 14, у ВТБ
// — 11. Подписи — дословно из разведки живых книг (отчёты задач 3 и 5, §4), и каждая обязана
// находиться разбором своей фикстуры (TestBankCompanyLabelsExistInFixtures): это контракт
// «словарь ↔ живые файлы», и при опечатке в подписи прогон молча терял бы метрику.
//
// Метрик ВТБ одиннадцать, а не двенадцать: достаточность капитала (Н20.0/Н20.1/Н20.2) источник
// публикует в блоке «Достаточность капитала», который лежит НИЖЕ границы разбора балансового
// листа («Итого собственные средства», рамка задачи 3), а лист «Показатели и коэффициенты» в
// живом файле — глоссарий без значений. Раздельных Н1.1/Н1.2 ВТБ не публикует вовсе, поэтому
// capital_adequacy_core и capital_adequacy_base есть только у Сбера. Расхождение с буквой спеки
// §3.7 («у ВТБ применимы 12») названо в отчёте задачи 3 (§7 п. 3) и решено контроллером: словарь
// ВТБ — 11 метрик, capital_adequacy у него отсутствует.
var bankCompanyMetricSpecs = map[string][]bankCompanyMetricSpec{
	sberRpbuDataUrlTable: {
		{Label: "Активы", Metric: "total_assets", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Собственные средства", Metric: "equity", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{
			Label: "Кредиты и авансы клиентам (нетто)", Metric: "customer_loans", Unit: bankCompanyUnitBln,
			Sheet: sberFinBalanceSheet,
			Note: "Ряд — «нетто»: резервы под кредитные убытки уже вычтены. Сплиты loans_corporations " +
				"и loans_individuals — гросс (до резервов), поэтому их сумма больше этого ряда на " +
				"величину резервов; у ВТБ, наоборот, ряд «до резервов» — базы двух компаний не сходятся.",
		},
		{Label: "Кредиты корпоративным клиентам (гросс)", Metric: "loans_corporations", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Кредиты физическим лицам (гросс)", Metric: "loans_individuals", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Средства клиентов", Metric: "customer_deposits", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Средства корпоративных клиентов", Metric: "deposits_corporations", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Средства физических лиц", Metric: "deposits_individuals", Unit: bankCompanyUnitBln, Sheet: sberFinBalanceSheet},
		{Label: "Прибыль за период", Metric: "net_profit", Unit: bankCompanyUnitBln, Sheet: sberFinPnlSheet},
		{Label: "Рентабельность капитала", Metric: "roe", Unit: bankCompanyUnitPercent, Sheet: sberFinRatiosSheet},
		{Label: "Рентабельность активов", Metric: "roa", Unit: bankCompanyUnitPercent, Sheet: sberFinRatiosSheet},
		{Label: "Достаточность общего капитала Н1.0", Metric: "capital_adequacy", Unit: bankCompanyUnitPercent, Sheet: sberFinCapitalSheet},
		{Label: "Достаточность основного капитала Н1.2", Metric: "capital_adequacy_core", Unit: bankCompanyUnitPercent, Sheet: sberFinCapitalSheet},
		{Label: "Достаточность базового капитала Н1.1", Metric: "capital_adequacy_base", Unit: bankCompanyUnitPercent, Sheet: sberFinCapitalSheet},
	},
	vtbIfrsTable: {
		{Label: "Итого активы", Metric: "total_assets", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Итого собственные средства", Metric: "equity", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{
			Label: "Кредиты и авансы клиентам (до резервов)", Metric: "customer_loans", Unit: bankCompanyUnitBln,
			Sheet: vtbBookSheet,
			Note: "Ряд — «до резервов»: остаток под резервы не уменьшен. Вариант «после резервов» " +
				"источник публикует только на листе «Сегментный анализ», который импортёр не " +
				"разбирает, поэтому этот ряд не равен чистому кредитному портфелю (у Сбера " +
				"соответствующий ряд — «нетто»).",
		},
		{Label: "Кредиты ЮЛ", Metric: "loans_corporations", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Кредиты ФЛ", Metric: "loans_individuals", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Средства клиентов", Metric: "customer_deposits", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Средства клиентов ЮЛ", Metric: "deposits_corporations", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Средства клиентов ФЛ", Metric: "deposits_individuals", Unit: bankCompanyUnitBln, Sheet: vtbBookSheet},
		{Label: "Чистая прибыль", Metric: "net_profit", Unit: bankCompanyUnitBln, Sheet: vtbPnlSheet},
		{Label: "Возврат на капитал (ROE)", Metric: "roe", Unit: bankCompanyUnitPercent, Sheet: vtbPnlSheet},
		{Label: "Возврат на активы (ROA)", Metric: "roa", Unit: bankCompanyUnitPercent, Sheet: vtbPnlSheet},
	},
}

// bankCompanyMetricMap — маппинг «имя сырой таблицы → подпись источника → метрика»: то, чем
// пользуется Import при чтении таблиц. Собирается из bankCompanyMetricSpecs, а не пишется вторым
// литералом: подпись, метрика и лист не должны жить в двух списках, которые разойдутся молча.
var bankCompanyMetricMap = buildBankCompanyMetricMap()

// buildBankCompanyMetricMap собирает карту маппинга из словаря спецификаций.
func buildBankCompanyMetricMap() map[string]map[string]bankCompanyMetric {
	byTable := make(map[string]map[string]bankCompanyMetric, len(bankCompanyMetricSpecs))
	for table, specs := range bankCompanyMetricSpecs {
		byLabel := make(map[string]bankCompanyMetric, len(specs))
		for _, spec := range specs {
			byLabel[spec.Label] = bankCompanyMetric{Metric: spec.Metric, Unit: spec.Unit}
		}
		byTable[table] = byLabel
	}
	return byTable
}

// bankCompanyMetricDoc — описание метрики для каталога: что она измеряет и как читать её темпы.
// Тексты общие для обеих компаний: различают ряды источник (bankCompanySourceNote) и оговорки
// самих рядов (Note спецификации).
type bankCompanyMetricDoc struct {
	Title    string
	Measures string
	Rates    string
}

// bankCompanyStockRates — как читать темпы балансового ряда-остатка: один текст на восемь метрик,
// чтобы восемь описаний не разошлись между собой.
const bankCompanyStockRates = "Остаток на конец отчётного месяца: месячный темп — значение / " +
	"значение предыдущего месяца − 1, годовой — значение / значение того же месяца годом раньше − 1; " +
	"прирост в млрд ₽ — разность двух значений ряда."

// bankCompanyFlowRates — как читать темпы потока (чистой прибыли за месяц).
const bankCompanyFlowRates = "Значение — за отчётный месяц, а не накопленным итогом с начала года: " +
	"накопительные колонки источника (8М'2026, 6М25 и колонки-годы) в ряд не входят. Месячный " +
	"темп — значение / значение предыдущего месяца − 1, годовой — значение / значение того же " +
	"месяца годом раньше − 1; месяц с убытком — отрицательное значение, и темп-отношение при смене " +
	"знака читать нельзя, смотреть сами значения."

// bankCompanyRatioRates — как читать темпы коэффициента (в процентах).
const bankCompanyRatioRates = "Коэффициент за отчётный месяц: месячный темп — разность с " +
	"предыдущим месяцем в п.п., годовой — разность с тем же месяцем годом раньше в п.п.; " +
	"отношение «значение / предыдущее значение» у коэффициента смысла не имеет."

// bankCompanyPercentNote — правило единиц процентных рядов: источник печатает доли, ряд хранит
// проценты.
const bankCompanyPercentNote = "Источник печатает долю (0.146), ряд хранит процент (14,6): " +
	"импортёр умножает значение на 100, единица ряда — '%'."

// bankCompanyCrossNote — оговорка сравнения двух компаний между собой: базы у них разные.
const bankCompanyCrossNote = "Базы компаний не совпадают: у Сбера — РСБУ и периметр банка, у ВТБ — " +
	"МСФО и периметр группы, поэтому уровни двух компаний сопоставимы только как порядок " +
	"величины, а приросты — с осторожностью; темпы считать внутри своего ряда, а не по разности " +
	"рядов разных компаний."

// bankCompanySourceNote — чей это ряд и из какой книги он собран.
var bankCompanySourceNote = map[string]string{
	sberRpbuDataUrlTable: "Ряд собран из отчётности ПАО Сбербанк по РСБУ (книга " +
		"sber_finansovie_rezultaty, листы «Баланс», «Отчет о прибылях и убытках», «Капитал», " +
		"«Показатели») — российские стандарты, периметр банка.",
	vtbIfrsTable: "Ряд собран из отчётности группы ВТБ по МСФО (книга rus_vtb_group_ifrs, листы " +
		"«Ключевые балансовые показатели» и «Ключевые показатели P&L») — международные стандарты, " +
		"периметр группы.",
}

// bankCompanyMetricDocs — что измеряет метрика и как читать её темпы. Ключ — имя метрики, значения
// общие для двух компаний (метрика одна и та же, различаются базы и листы — см. спецификации).
var bankCompanyMetricDocs = map[string]bankCompanyMetricDoc{
	"total_assets": {
		Title:    "Активы (итог баланса)",
		Measures: "Стоимость всех активов эмитента на отчётную дату — итог баланса, млрд ₽.",
		Rates:    bankCompanyStockRates,
	},
	"equity": {
		Title:    "Собственные средства (капитал)",
		Measures: "Собственные средства (капитал) эмитента на отчётную дату — итог раздела собственного капитала баланса, млрд ₽.",
		Rates:    bankCompanyStockRates,
	},
	"customer_loans": {
		Title: "Кредиты и авансы клиентам (кредитный портфель)",
		Measures: "Весь кредитный портфель эмитента на отчётную дату — кредиты и авансы клиентам " +
			"(юридическим и физическим лицам вместе), млрд ₽; база ряда (до или после резервов) — в оговорке ниже.",
		Rates: bankCompanyStockRates,
	},
	"loans_corporations": {
		Title:    "Кредиты корпоративным клиентам (ЮЛ)",
		Measures: "Кредиты и авансы клиентам-юридическим лицам (корпоративный портфель) на отчётную дату, млрд ₽ — один из двух сплитов ряда customer_loans.",
		Rates:    bankCompanyStockRates,
	},
	"loans_individuals": {
		Title:    "Кредиты физическим лицам (ФЛ)",
		Measures: "Кредиты и авансы клиентам-физическим лицам (розничный портфель) на отчётную дату, млрд ₽ — второй сплит ряда customer_loans.",
		Rates:    bankCompanyStockRates,
	},
	"customer_deposits": {
		Title:    "Средства клиентов (депозиты)",
		Measures: "Обязательства перед клиентами на отчётную дату — средства юридических и физических лиц вместе, млрд ₽.",
		Rates:    bankCompanyStockRates,
	},
	"deposits_corporations": {
		Title:    "Средства клиентов-юридических лиц (ЮЛ)",
		Measures: "Средства клиентов-юридических лиц (корпоративные депозиты) на отчётную дату, млрд ₽ — один из двух сплитов ряда customer_deposits.",
		Rates:    bankCompanyStockRates,
	},
	"deposits_individuals": {
		Title:    "Средства клиентов-физических лиц (ФЛ)",
		Measures: "Средства клиентов-физических лиц (вклады населения) на отчётную дату, млрд ₽ — второй сплит ряда customer_deposits.",
		Rates:    bankCompanyStockRates,
	},
	"net_profit": {
		Title:    "Чистая прибыль за месяц",
		Measures: "Чистая прибыль (убыток) за отчётный месяц, млрд ₽ — значение месяца, а не накопленный итог с начала года.",
		Rates:    bankCompanyFlowRates,
	},
	"roe": {
		Title: "Рентабельность капитала (ROE)",
		Measures: "Рентабельность капитала за отчётный месяц, % — отношение прибыли к капиталу, " +
			"как его печатает источник; это не накопленный с начала года ROE, накопительные колонки в ряд не входят.",
		Rates: bankCompanyRatioRates,
	},
	"roa": {
		Title: "Рентабельность активов (ROA)",
		Measures: "Рентабельность активов за отчётный месяц, % — отношение прибыли к активам, " +
			"как его печатает источник; это не накопленный с начала года ROA, накопительные колонки в ряд не входят.",
		Rates: bankCompanyRatioRates,
	},
	"capital_adequacy": {
		Title:    "Достаточность общего капитала (Н1.0)",
		Measures: "Норматив достаточности общего капитала банка (у Сбера — Н1.0) на отчётную дату, %: отношение капитала к активам, взвешенным по уровню риска.",
		Rates:    bankCompanyRatioRates,
	},
	"capital_adequacy_core": {
		Title:    "Достаточность основного капитала (Н1.2)",
		Measures: "Норматив достаточности основного капитала банка (Н1.2) на отчётную дату, %: тот же расчёт, но по основному капиталу.",
		Rates:    bankCompanyRatioRates,
	},
	"capital_adequacy_base": {
		Title:    "Достаточность базового капитала (Н1.1)",
		Measures: "Норматив достаточности базового капитала банка (Н1.1) на отчётную дату, %: самый жёсткий из трёх нормативов достаточности.",
		Rates:    bankCompanyRatioRates,
	},
}

// bankCompanyMetricsMeta — каталог рядов импортёра (source bank_company_metrics): по строке на пару
// «компания + метрика» — у Сбера 14, у ВТБ 11. Собирается из словаря, поэтому разойтись с ним не
// может по построению; покрытие проверяет TestBankCompanyMetricMapWellFormed, а не глаза.
var bankCompanyMetricsMeta = buildBankCompanyMetricsMeta()

// buildBankCompanyMetricsMeta собирает описания рядов словаря в порядке обхода таблиц.
func buildBankCompanyMetricsMeta() []util.SeriesMeta {
	var meta []util.SeriesMeta
	for _, table := range bankCompanyTables {
		for _, spec := range bankCompanyMetricSpecs[table] {
			doc := bankCompanyMetricDocs[spec.Metric]
			meta = append(meta, util.SeriesMeta{
				Source:      bankCompanyMetricsName,
				Series:      bankCompanySourceCompany[table] + "_" + spec.Metric,
				Title:       doc.Title + ", " + bankCompanySourceTitle[table] + ", помесячно",
				Unit:        spec.Unit,
				Frequency:   "M",
				Origin:      table + ", лист «" + spec.Sheet + "», подпись строки «" + spec.Label + "»",
				Description: bankCompanyDescription(table, spec, doc),
			})
		}
	}
	return meta
}

// bankCompanyDescription склеивает описание ряда: что измеряет, как читать темпы, оговорку ряда,
// источник и оговорки единиц и сравнения компаний.
func bankCompanyDescription(table string, spec bankCompanyMetricSpec, doc bankCompanyMetricDoc) string {
	parts := []string{doc.Measures, doc.Rates}
	if spec.Note != "" {
		parts = append(parts, spec.Note)
	}
	parts = append(parts, bankCompanySourceNote[table])
	if spec.Unit == bankCompanyUnitPercent {
		parts = append(parts, bankCompanyPercentNote)
	}
	return strings.Join(append(parts, bankCompanyCrossNote), " ")
}

// bankCompanyPeriodType — тип периода строк company_financials: 'M' — месяц (спека §3.7). Месячную
// гранулярность пишет только этот импортёр, и без 'M' в enum месячный ряд нельзя было бы отличить
// от квартального при одной строке периода.
const bankCompanyPeriodType = "M"

// bankCompanySourcePage — провенанс страницы: у банковских рядов документ — книга целиком, страниц
// в нём нет, поэтому 0 (колонка UInt16, как у рядов Полюса: значение обязательно).
const bankCompanySourcePage = uint16(0)

// bankCompanyPoint — точка сырой таблицы: имя ряда (подпись источника после нормализации разбора),
// дата и значение. Форма общая для обеих таблиц: у них одна legacy-схема (name, date, balance).
type bankCompanyPoint struct {
	Name  string
	Date  time.Time
	Value float64
}

// bankCompanyRow — строка, которую импортёр пишет в company_financials: то, что различается по
// точкам. Свойства прогона (period_type, source_page, loaded_at) в строке не хранятся — их
// подставляет запись (appendBankCompanyRows): так их нельзя случайно сделать разными в одном
// батче, а одна метка загрузки и делает повторный прогон версией, а не ростом таблицы.
type bankCompanyRow struct {
	Company    string
	Metric     string
	Period     string
	SourceKind string
	Value      float64
	Unit       string
	SourceUrl  string
}

// bankCompanyPeriod — период в канонической форме company_financials: «2026M08» (год, литеральная
// M, двузначный месяц). Layout взят у bea (beaTimeLayout): формы периодов в таблице должны
// совпадать. Дата берётся КАК ЕСТЬ, без .UTC(): драйвер отдаёт Date в зоне сессии, и приведение
// сдвинуло бы первое число месяца в предыдущий (прецедент cbr/mp_summary.go:mpDay).
func bankCompanyPeriod(d time.Time) string {
	return d.Format("2006M01")
}

// bankCompanyValue приводит значение источника к единице ряда: коэффициенты источник печатает
// долями (0.146 = 14,6 %), а ряд хранит проценты — единица '%' в колонке unit обязана быть правдой,
// иначе 14,6 % читались бы как 0,146 %. Остатки и потоки идут как есть.
func bankCompanyValue(unit string, value float64) float64 {
	if unit == bankCompanyUnitPercent {
		return value * 100
	}
	return value
}

// bankCompanyMapPoints переводит точки сырой таблицы в строки company_financials: метрика и единица
// — из словаря (bankCompanyMetricMap), период — из даты, значение приводится к единице ряда.
// Второй результат — подписи словаря, которых в таблице не нашлось, в порядке словаря: для прогона
// это не ошибка (сырьё уже импортировано, маппинг — производное, спека §3.7), но каждая — след в
// журнале (Review Focus 4). Точки с подписями вне словаря пропускаются молча: их в сырой таблице
// большинство, и метриками они не являются.
func bankCompanyMapPoints(table string, points []bankCompanyPoint) (rows []bankCompanyRow, missing []string) {
	byLabel := bankCompanyMetricMap[table]
	seen := make(map[string]bool, len(points))
	for _, p := range points {
		seen[p.Name] = true
		metric, ok := byLabel[p.Name]
		if !ok {
			continue
		}
		rows = append(rows, bankCompanyRow{
			Company:    bankCompanySourceCompany[table],
			Metric:     metric.Metric,
			Period:     bankCompanyPeriod(p.Date),
			SourceKind: bankCompanySourceKind[table],
			Value:      bankCompanyValue(metric.Unit, p.Value),
			Unit:       metric.Unit,
			SourceUrl:  bankCompanySourceUrl[table],
		})
	}
	for _, spec := range bankCompanyMetricSpecs[table] {
		if !seen[spec.Label] {
			missing = append(missing, spec.Label)
		}
	}
	return rows, missing
}

// bankCompanyTableExistsQuery — есть ли сырая таблица в текущей БД: импортёр обязан пропускать
// отсутствующие (пустая БД или сырьевой импортёр ещё не отработал) — SELECT из несуществующей
// таблицы упал бы.
const bankCompanyTableExistsQuery = "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?"

// bankCompanyTableExists отвечает, существует ли таблица table в текущей БД.
func bankCompanyTableExists(ctx context.Context, conn driver.Conn, table string) (bool, error) {
	var count uint64
	if err := conn.QueryRow(ctx, bankCompanyTableExistsQuery, table).Scan(&count); err != nil {
		return false, fmt.Errorf("%s: проверка таблицы %s: %w", bankCompanyMetricsName, table, err)
	}
	return count > 0, nil
}

// bankCompanyRawPoints читает сырую таблицу целиком. FINAL обязателен (ReplacingMergeTree): без
// него перезаписанные повторным импортом точки пришли бы по нескольку раз, и в батч уехали бы
// дубли ключа. Колонка значения — balance (legacy-схема пакета): у Сбера и ВТБ она Float32, и
// драйвер отдаёт её в float64.
func bankCompanyRawPoints(ctx context.Context, conn driver.Conn, table string) ([]bankCompanyPoint, error) {
	rows, err := conn.Query(ctx, "SELECT name, date, balance FROM "+table+" FINAL")
	if err != nil {
		return nil, fmt.Errorf("%s: %s: чтение точек: %w", bankCompanyMetricsName, table, err)
	}
	defer func() { _ = rows.Close() }()

	var points []bankCompanyPoint
	for rows.Next() {
		var p bankCompanyPoint
		if err = rows.Scan(&p.Name, &p.Date, &p.Value); err != nil {
			return nil, fmt.Errorf("%s: %s: строка не прочитана: %w", bankCompanyMetricsName, table, err)
		}
		points = append(points, p)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %s: точки не дочитаны: %w", bankCompanyMetricsName, table, err)
	}
	return points, nil
}

// appendBankCompanyRows пишет строки одним батчем по одному Append на строку с проверкой ошибки
// (§5.3, §5.5). Форма строки — схема company_financials: (company, metric, period, period_type,
// source_kind, value, unit, source_url, source_page, loaded_at). Abort батча делает вызывающий:
// у тестового рекордера его нет, а проверка ошибки Append есть и здесь.
func appendBankCompanyRows(batch driver.Batch, rows []bankCompanyRow, loadedAt time.Time) error {
	for _, row := range rows {
		if err := batch.Append(
			row.Company,
			row.Metric,
			row.Period,
			bankCompanyPeriodType,
			row.SourceKind,
			row.Value,
			row.Unit,
			row.SourceUrl,
			bankCompanySourcePage,
			loadedAt,
		); err != nil {
			return fmt.Errorf("%s: запись %s %s за %s: %w", bankCompanyMetricsName, row.Company, row.Metric, row.Period, err)
		}
	}
	return nil
}

// BankCompanyMetrics — импортёр bank_company_metrics (спека §3.7): курированные отчётные метрики
// Сбербанка (РСБУ) и группы ВТБ (МСФО) из уже импортированных сырых таблиц пакета — в общую
// company_financials, рядом с метриками Полюса. Своей таблицы и своих витрин у него нет: сырьё
// лежит в таблицах импортёров sber_finansovie_rezultaty и rus_vtb_group_ifrs, а читатель —
// витрины v_company_financials и v_company_metric_sources, гранты на которые уже выданы. Сеть
// импортёру не нужна, поэтому и Import() свой, без шаблона util.ClickHouseImport (прецедент
// company_views в views/): шаблон объявляет транспорт и точку контроля публикации, а у этого
// импортёра нет ни того, ни другого — внешнего артефакта нет, есть только свои таблицы.
type BankCompanyMetrics struct{}

// Name — имя импортёра в реестре chimport.Stats и шаг dagu/bank.yaml: имя и таблица у него разные
// (таблица общая), поэтому Name() возвращает константу, а не имя таблицы.
func (s *BankCompanyMetrics) Name() string {
	return bankCompanyMetricsName
}

// Import публикует метрики обоих эмитентов: DDL целевой таблицы → по каждой сырой таблице чтение
// FINAL → маппинг по словарю → один батч → каталог рядов. Повторный прогон идемпотентен: ключ
// ReplacingMergeTree(loaded_at) тот же, меняется только метка загрузки.
//
// Пропуски — не ошибки: нет сырой таблицы — пропуск; tbank_group_ifrs — пропуск с логом (источник
// не починен, спека §3.5); подписи словаря нет в таблице — предупреждение и пропуск метрики
// (Review Focus 4): сырьё уже импортировано, а маппинг — производное, и ронять из-за него прогон
// незачем. Сбой чтения, записи или каталога — ошибка прогона.
//
// Порядок шагов принципиален: ВСЁ чтение (`Query`/`QueryRow`) идёт ДО `PrepareBatch`. Открытый батч
// в clickhouse-go v2 держит слот соединения до `Send`/`Abort` — только они возвращают его в пул, —
// поэтому ранний выход по ошибке чтения после `PrepareBatch` оставлял бы слот занятым до конца
// процесса (раннер на ошибке одного импортёра продолжает работу остальных). Ошибка после
// `PrepareBatch` и до `Send` обязана абортить батч (образец — `abortBatch` в
// util/clickhouse_import.go).
func (s *BankCompanyMetrics) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, bankCompanyMetricsDdl); err != nil {
		return count, fmt.Errorf("%s: создание %s: %w", bankCompanyMetricsName, bankCompanyMetricsTable, err)
	}

	// Читаем и мапим все таблицы до открытия батча: строки собираются в память, зато ни один путь
	// ошибки до Send не оставляет соединение занятым. Объём — столько же строк, сколько выбранных
	// точек в сырых таблицах (сотни, не миллионы), так что держать их в срезе дешевле, чем слот
	// соединения.
	var rows []bankCompanyRow
	for _, table := range bankCompanyTables {
		if table == tbIfrsTable {
			log.Warnf("%s: таблица %s пропущена — источник не починен (ссылка ведёт на PDF)", bankCompanyMetricsName, table)
			continue
		}
		exists, existsErr := bankCompanyTableExists(ctx, conn, table)
		if existsErr != nil {
			return count, existsErr
		}
		if !exists {
			log.Warnf("%s: таблицы %s нет в текущей БД — метрики из неё пропущены", bankCompanyMetricsName, table)
			continue
		}
		points, pointsErr := bankCompanyRawPoints(ctx, conn, table)
		if pointsErr != nil {
			return count, pointsErr
		}
		mapped, missing := bankCompanyMapPoints(table, points)
		for _, label := range missing {
			log.Warnf("%s: %s: подписи %q (метрика %s) нет в таблице — метрика пропущена",
				bankCompanyMetricsName, table, label, bankCompanyMetricMap[table][label].Metric)
		}
		rows = append(rows, mapped...)
	}

	// Одна метка загрузки на весь прогон: строки одного прогона — одна версия в
	// ReplacingMergeTree(loaded_at), поэтому повторный прогон не растит таблицу.
	loadedAt := time.Now()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+bankCompanyMetricsTable)
	if err != nil {
		return count, fmt.Errorf("%s: %w", bankCompanyMetricsName, err)
	}
	if err = appendBankCompanyRows(batch, rows, loadedAt); err != nil {
		_ = batch.Abort()
		return count, err
	}
	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %w", bankCompanyMetricsName, err)
	}
	count = int64(batch.Rows())

	if err = util.UpsertSeriesCatalog(ctx, conn, bankCompanyMetricsMeta); err != nil {
		return count, err
	}

	log.Infof("Imported %d rows of %s into %s", count, bankCompanyMetricsName, bankCompanyMetricsTable)

	return count, nil
}

// Регистрацию импортёра держит реестр пакета (bank/importers.go): запись
// {&BankCompanyMetrics{}, "company_financials", ""} — единственная точка append в chimport.Stats.
