package minfin

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	ofzAuctionTable  = "minfin_ofz_auction"
	debtStockTable   = "minfin_debt_stock"
	debtServiceTable = "minfin_debt_service"

	ofzAuctionDdl = `CREATE TABLE IF NOT EXISTS %s (
    date Date,
    format LowCardinality(String),
    issue LowCardinality(String),
    paper_type LowCardinality(String),
    maturity_date Date,
    days_to_maturity UInt32,
    offer_volume Float64,
    cutoff_price Nullable(Float64),
    weighted_price Nullable(Float64),
    cutoff_yield Nullable(Float64),
    weighted_yield Nullable(Float64),
    demand Nullable(Float64),
    placed Nullable(Float64),
    revenue Nullable(Float64),
    coverage Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (date, issue, format);`

	ofzAuctionOrigin   = "minfin.gov.ru/ru/document?TAG_ID_4[]=1349 → годовой файл INTERNET_Auction_Results_rus_<год>_<YYYYMMDD>.xlsx"
	debtInternalOrigin = "minfin.gov.ru/ru/document?id_4=93479, файл Obem_gosdolga_s_garantiyami_god_polnostu_na_<DD_MM_YYYY>.xls, листы <год> г."
	debtExternalOrigin = "minfin.gov.ru/ru/document?id_4=69444, файл GVD_<YYYYMMDD>.xlsx, лист «Объем гос. внешнего долга РФ»"
	ofzStructureOrigin = "minfin.gov.ru, месячные документы «Структура и динамика государственного внутреннего долга…», файл debtDynamicsMonth_<DD.MM.YYYY>.xlsx; 2022 — Struktura_gos_vnutr._dolga_..._ezhemesyachnye_na_rus._na_1_yanvarya_23.xls"
	debtServiceOrigin  = "minfin.gov.ru/ru/statistics/fedbud/execute, файл *_mes.xlsx, лист «месяц», строка «Обслуживание государственного и муниципального долга»"
	borrowingOrigin    = "minfin.gov.ru, документ «Основные показатели исполнения федерального бюджета … в части государственных внутренних заимствований», файл indicatorsBudget_<DD.MM.YYYY>.xlsx"
)

var ofzAuctionSeriesMeta = []util.SeriesMeta{
	{
		Source:      ofzAuctionTable,
		Series:      "demand",
		Title:       "Совокупный спрос на аукционе ОФЗ по номиналу",
		Unit:        "млн руб.",
		Frequency:   "D",
		Origin:      ofzAuctionOrigin + ", колонка «Совокупный объем спроса по номиналу»",
		Description: "Спрос на аукционе по номиналу. Дата — дата аукциона, а не регулярный ряд: одна точка на выпуск и формат (Аукцион/ДРПА). NULL — аукцион не состоялся или сделок не было, а не ноль.",
	},
	{
		Source:      ofzAuctionTable,
		Series:      "placed",
		Title:       "Объём размещения ОФЗ на аукционе по номиналу",
		Unit:        "млн руб.",
		Frequency:   "D",
		Origin:      ofzAuctionOrigin + ", колонка «Объем размещения по номиналу»",
		Description: "Размещено по номиналу. Дата — дата аукциона. NULL — размещения не было, а не ноль.",
	},
	{
		Source:      ofzAuctionTable,
		Series:      "cutoff_yield",
		Title:       "Доходность по цене отсечения на аукционе ОФЗ",
		Unit:        "% годовых",
		Frequency:   "D",
		Origin:      ofzAuctionOrigin + ", колонка «Доходность по цене отсечения»",
		Description: "Доходность по цене отсечения. Для ОФЗ-ПК источник её не рассчитывает; NULL — нет значения.",
	},
	{
		Source:      ofzAuctionTable,
		Series:      "weighted_yield",
		Title:       "Средневзвешенная доходность на аукционе ОФЗ",
		Unit:        "% годовых",
		Frequency:   "D",
		Origin:      ofzAuctionOrigin + ", колонка «Доходность по средневзвешенной цене»",
		Description: "Средневзвешенная доходность аукциона. NULL — нет значения (например, ОФЗ-ПК или несостоявшийся аукцион).",
	},
	{
		Source:      ofzAuctionTable,
		Series:      "coverage_ratio",
		Title:       "Коэффициент удовлетворения спроса на аукционе ОФЗ",
		Unit:        "доля",
		Frequency:   "D",
		Origin:      ofzAuctionOrigin + ", колонка «Коэффициент удовлетворения спроса на аукционе»; при отсутствии — placed/demand",
		Description: "Отношение размещённого объёма к спросу: 0.3 = размещено 30 % спроса. NULL — спрос или размещение отсутствуют, а не ноль.",
	},
}

var debtStockSeriesMeta = []util.SeriesMeta{
	{
		Source:      debtStockTable,
		Series:      "Внутренний долг, всего",
		Title:       "Государственный внутренний долг Российской Федерации, всего",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      debtInternalOrigin + ", строка «Объем государственного внутреннего долга Российской Федерации - всего»",
		Description: "Объём внутреннего долга на 1-е число месяца. Значение включает государственные гарантии; для долга без гарантий вычитать ряд «Внутренний долг, в т.ч. гарантии».",
	},
	{
		Source:      debtStockTable,
		Series:      "Внутренний долг, в т.ч. гарантии",
		Title:       "Государственные гарантии в составе внутреннего долга",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      debtInternalOrigin + ", строка «государственные гарантии Российской Федерации в валюте Российской Федерации»",
		Description: "Государственные гарантии в валюте Российской Федерации на 1-е число месяца.",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ, номинал",
		Title:       "Номинал государственного внутреннего долга в ОФЗ",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", сумма строк ОФЗ-ПК/ПД/АД/ИН (и ОФЗ-н в 2022)",
		Description: "Сумма номиналов ОФЗ-ПК, ОФЗ-ПД, ОФЗ-АД и ОФЗ-ИН на 1-е число месяца; ГСО-ППС/ГСО-ФПС не входят. История: 2022-01 … 2023-01 и 2023-05 … с пропусками 2023-02…04 и 2025-04…08 (файлы источника отсутствуют). NULL — нет публикации, а не ноль.",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ-ПК",
		Title:       "Номинал ОФЗ с переменным купонным доходом",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", строка «ОФЗ-ПК»",
		Description: "Номинал ОФЗ-ПК на 1-е число месяца. Пропуски — как у ряда «ОФЗ, номинал».",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ-ПД",
		Title:       "Номинал ОФЗ с постоянным купонным доходом",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", строка «ОФЗ-ПД»",
		Description: "Номинал ОФЗ-ПД на 1-е число месяца. Пропуски — как у ряда «ОФЗ, номинал».",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ-АД",
		Title:       "Номинал ОФЗ с амортизацией долга",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", строка «ОФЗ-АД»",
		Description: "Номинал ОФЗ-АД на 1-е число месяца. Пропуски — как у ряда «ОФЗ, номинал».",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ-ИН",
		Title:       "Номинал ОФЗ с индексируемым номиналом",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", строка «ОФЗ-ИН»",
		Description: "Номинал ОФЗ-ИН на 1-е число месяца. Пропуски — как у ряда «ОФЗ, номинал».",
	},
	{
		Source:      debtStockTable,
		Series:      "ОФЗ-н",
		Title:       "Номинал ОФЗ для физических лиц",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      ofzStructureOrigin + ", строка «ОФЗ-н»",
		Description: "Номинал ОФЗ-н (розничные) на 1-е число месяца. Ряд есть только в файле 2022 года; с 2023 года ОФЗ-н в структуре не публикуется.",
	},
	{
		Source:      debtStockTable,
		Series:      "Внешний долг, всего",
		Title:       "Государственный внешний долг Российской Федерации, всего",
		Unit:        "млн долл. США",
		Frequency:   "M",
		Origin:      debtExternalOrigin + ", колонка «Всего»",
		Description: "Объём внешнего долга на 1-е число месяца в млн долл. США; включает гарантии в иностранной валюте. Единица отличается от внутреннего долга — не складывать без пересчёта по курсу.",
	},
	{
		Source:      debtStockTable,
		Series:      "Внешний долг, в т.ч. гарантии",
		Title:       "Государственные гарантии в составе внешнего долга",
		Unit:        "млн долл. США",
		Frequency:   "M",
		Origin:      debtExternalOrigin + ", колонка «в т.ч. по государственным гарантиям»",
		Description: "Государственные гарантии в иностранной валюте на 1-е число месяца, млн долл. США.",
	},
}

var debtServiceSeriesMeta = []util.SeriesMeta{
	{
		Source:      debtServiceTable,
		Series:      "Обслуживание государственного и муниципального долга",
		Title:       "Расходы на обслуживание государственного и муниципального долга, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      debtServiceOrigin,
		Description: "Функциональная расходная строка федерального бюджета; в источнике накоплена с начала года, таблица хранит месячную разность (январь равен накоплению за январь). Шире, чем расходы только по федеральным ценным бумагам.",
	},
	{
		Source:      debtServiceTable,
		Series:      "Привлечение",
		Title:       "Фактическое привлечение средств в федеральный бюджет, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      borrowingOrigin + ", колонка «Привлечение»",
		Description: "Фактическое привлечение по государственным ценным бумагам за месяц; в источнике млн руб., переведено в млрд руб. (÷1000). Ряд текущего года: история начинается с первого импорта.",
	},
	{
		Source:      debtServiceTable,
		Series:      "Погашение основного долга",
		Title:       "Погашение основного долга по государственным ценным бумагам, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      borrowingOrigin + ", колонка «Погашение осн. долга»",
		Description: "Фактическое погашение основного долга за месяц; млн руб. → млрд руб. (÷1000). Ряд текущего года.",
	},
	{
		Source:      debtServiceTable,
		Series:      "Чистое привлечение",
		Title:       "Чистое привлечение средств в федеральный бюджет, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      borrowingOrigin + ", колонка «Чистое привлечение»",
		Description: "Привлечение минус погашение основного долга за месяц; млн руб. → млрд руб. (÷1000). Отрицательное значение — чистое погашение. Ряд текущего года.",
	},
	{
		Source:      debtServiceTable,
		Series:      "Расходы на обслуживание",
		Title:       "Расходы на обслуживание государственных ценных бумаг, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      borrowingOrigin + ", колонка «Расходы на обслуживание»",
		Description: "Расходы на обслуживание федеральных ценных бумаг за месяц; млн руб. → млрд руб. (÷1000). Уже, чем ряд «Обслуживание государственного и муниципального долга»: не включает муниципальный долг и другие обязательства.",
	},
}

var ofzStressView = util.View{
	Name:   "v_ofz_stress",
	Tables: []string{ofzAuctionTable, debtStockTable, debtServiceTable, fedbudMesTable, "cbr_zcyc"},
	Select: `WITH auctions AS (
    SELECT * FROM minfin_ofz_auction FINAL
),
nearest AS (
    SELECT
        a.date AS date,
        a.issue AS issue,
        a.format AS format,
        toNullable(z.tenor) AS secondary_tenor,
        toNullable(z.yield) AS secondary_yield,
        row_number() OVER (
            PARTITION BY a.date, a.issue, a.format
            ORDER BY abs(toFloat64(z.tenor) * 365 - a.days_to_maturity)
        ) AS rn
    FROM auctions AS a
    LEFT JOIN cbr_zcyc AS z FINAL ON z.date = a.date
),
debt_service AS (
    SELECT date, name, toNullable(value) AS value FROM minfin_debt_service FINAL
    WHERE name IN ('Обслуживание государственного и муниципального долга', 'Расходы на обслуживание')
),
revenue AS (
    SELECT date, toNullable(value) AS value FROM minfin_fed_bud_mes FINAL
    WHERE name = 'Доходы, всего'
),
ofz_stock AS (
    SELECT date, toNullable(value) AS value FROM minfin_debt_stock FINAL
    WHERE name = 'ОФЗ, номинал'
),
prev_ofz_stock AS (
    SELECT addMonths(date, 1) AS next_date, toNullable(value) AS value
    FROM minfin_debt_stock FINAL
    WHERE name = 'ОФЗ, номинал'
)
SELECT
    a.date AS date,
    a.issue AS issue,
    a.format AS format,
    a.paper_type AS paper_type,
    a.days_to_maturity AS days_to_maturity,
    a.demand AS demand,
    a.placed AS placed,
    coalesce(a.coverage, if(a.demand IS NOT NULL AND a.demand > 0, a.placed / a.demand, NULL)) AS coverage_ratio,
    n.secondary_tenor AS secondary_tenor,
    n.secondary_yield AS secondary_yield,
    a.cutoff_yield AS cutoff_yield,
    if(a.cutoff_yield IS NOT NULL AND n.secondary_yield IS NOT NULL, a.cutoff_yield - n.secondary_yield, NULL) AS cutoff_minus_secondary_pp,
    coalesce(ds_budget.value, ds_securities.value) AS debt_service,
    multiIf(ds_budget.value IS NOT NULL, 'budget', ds_securities.value IS NOT NULL, 'securities', '') AS debt_service_source,
    rev.value AS revenue,
    if(coalesce(ds_budget.value, ds_securities.value) IS NOT NULL AND rev.value > 0, coalesce(ds_budget.value, ds_securities.value) / rev.value, NULL) AS debt_service_to_revenue,
    st.value AS ofz_stock,
    if(st.value IS NOT NULL AND prev.value IS NOT NULL, st.value - prev.value, NULL) AS ofz_stock_change
FROM auctions AS a
LEFT JOIN nearest AS n ON n.date = a.date AND n.issue = a.issue AND n.format = a.format AND n.rn = 1
LEFT JOIN debt_service AS ds_budget ON ds_budget.date = toStartOfMonth(a.date) AND ds_budget.name = 'Обслуживание государственного и муниципального долга'
LEFT JOIN debt_service AS ds_securities ON ds_securities.date = toStartOfMonth(a.date) AND ds_securities.name = 'Расходы на обслуживание'
LEFT JOIN revenue AS rev ON rev.date = toStartOfMonth(a.date)
LEFT JOIN ofz_stock AS st ON st.date = toStartOfMonth(a.date)
LEFT JOIN prev_ofz_stock AS prev ON prev.next_date = toStartOfMonth(a.date)
ORDER BY a.date, a.issue, a.format`,
	Comment: "Долговой стресс ОФЗ: одна строка на аукцион (дата, выпуск, формат). coverage_ratio — размещено/спрос; cutoff_minus_secondary_pp — доходность отсечения минус ближайший тенор КБД ЦБ на дату аукциона (кривая ЦБ, а не сделки вторичного рынка); debt_service_to_revenue — доля обслуживания долга в доходах федерального бюджета за месяц (0.05 = 5 %); debt_service до 2022 — бюджетная строка (федеральный и муниципальный долг), с 2024 — обслуживание федеральных ценных бумаг, 2022–2023 — NULL, источник различает debt_service_source; ofz_stock и ofz_stock_change — номинал ОФЗ на 1-е число месяца и изменение к предыдущему месяцу. Месячные метрики повторяются на каждом аукционе месяца; NULL — нет данных, а не ноль",
	Columns: map[string]string{
		"date":                      "Дата аукциона (Date)",
		"issue":                     "Код выпуска ОФЗ, например 26253RMFS",
		"format":                    "Формат: Аукцион или ДРПА (дополнительное размещение после аукциона)",
		"paper_type":                "Тип бумаги: ОФЗ-ПД, ОФЗ-ПК, ОФЗ-ИН, ОФЗ-АД",
		"days_to_maturity":          "Дней до погашения на дату аукциона",
		"demand":                    "Совокупный спрос по номиналу, млн руб.; NULL — аукцион не состоялся или сделок не было",
		"placed":                    "Размещено по номиналу, млн руб.; NULL — размещения не было",
		"coverage_ratio":            "Отношение размещённого объёма к спросу (доля); NULL — нет спроса или размещения",
		"secondary_tenor":           "Ближайший тенор КБД ЦБ (лет) к days_to_maturity; NULL — нет точки КБД на дату",
		"secondary_yield":           "Доходность КБД ЦБ по ближайшему тенору, % годовых; NULL — нет точки",
		"cutoff_yield":              "Доходность по цене отсечения, % годовых; NULL — нет значения",
		"cutoff_minus_secondary_pp": "cutoff_yield − secondary_yield, п.п.; положительное значение — аукцион дороже кривой; NULL — нет одной из доходностей",
		"debt_service":              "Расходы на обслуживание долга за месяц, млрд руб.: до 2022 — строка бюджета (федеральный и муниципальный долг), с 2024 — расходы на обслуживание федеральных ценных бумаг; 2022–2023 — NULL (публикации нет). Источник различается, см. debt_service_source",
		"debt_service_source":       "Источник debt_service: budget — строка 2.13 федерального бюджета (шире, включает муниципальный долг); securities — расходы на обслуживание федеральных ценных бумаг; пусто — данных за месяц нет",
		"revenue":                   "Доходы федерального бюджета за месяц, млрд руб.; NULL — месяц не опубликован",
		"debt_service_to_revenue":   "debt_service / revenue (доля, 0.05 = 5 %); NULL — нет месяца или доходов",
		"ofz_stock":                 "Номинал ОФЗ на 1-е число месяца, млрд руб.; NULL — нет публикации структуры",
		"ofz_stock_change":          "Изменение ofz_stock к предыдущему месяцу, млрд руб.; NULL — нет одного из месяцев",
	},
}

func publishOfzStress(ctx context.Context, conn driver.Conn, meta []util.SeriesMeta) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, ofzStressView)
	return err
}
