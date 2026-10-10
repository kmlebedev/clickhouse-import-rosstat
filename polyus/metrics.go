package polyus

// MetricDefinition описывает один KPI отчёта: имя в витрине, единицу измерения
// и набор префиксов строки, по которым метрика узнаётся в извлечённом тексте
// страницы. Префиксы английские (исторические релизы) и русские (текущие).
type MetricDefinition struct {
	Name   string
	Unit   string
	Prefix []string
}

// MetricRecord — одно значение метрики за один период.
type MetricRecord struct {
	Company    string `json:"company"`
	Metric     string `json:"metric"`
	Period     string `json:"period"`
	PeriodType string `json:"period_type"`
	// SourceKind — вид документа, из которого пришло значение: "kpi" у
	// пресс-релиза (период берётся из шапки страницы) и "ifrs" у аудированной
	// отчётности (период приходит параметром отчёта). Значения совпадают с
	// Report.Kind. Вид документа входит в ключ витрины: одна и та же метрика за
	// один и тот же период печатается обоими документами, и без этого поля
	// второе значение молча перетирало бы первое (см. batchKey).
	SourceKind string  `json:"source_kind"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	SourceURL  string  `json:"source_url"`
	SourcePage int     `json:"source_page"`
}

// PeriodColumn связывает позицию числового токена в строке с периодом,
// который стоит в шапке этой колонки.
type PeriodColumn struct {
	TokenIndex int
	Period     string
	PeriodType string
}

var metrics = []MetricDefinition{
	{
		Name: "period",
		Unit: "period_type",
		Prefix: []string{
			"$ mln unless specified otherwise",
			"$ million (if not mentioned otherwise)",
			"$ mln (if not mentioned otherwise)",
			"$ million (if not mentioned ",
			"$ mln (if not mentioned",
			"$ млн (если не указано иное)",
		},
	},
	{
		Name:   "gold_output",
		Unit:   "koz",
		Prefix: []string{"Gold output, koz", "Total gold production (koz)", "Gold production (koz)", "Производство золота (тыс. унций)"},
	},
	{
		Name:   "gold_sold",
		Unit:   "koz",
		Prefix: []string{"Gold sold, koz", "Gold sold (koz)", "Реализация золота (тыс. унций)"},
	},
	{
		Name:   "revenue",
		Unit:   "USD million",
		Prefix: []string{"Revenue", "Total revenue", "Выручка"},
	},
	{
		Name:   "operating_profit",
		Unit:   "USD million",
		Prefix: []string{"Operating profit", "Операционная прибыль"},
	},
	{
		Name:   "profit_for_period",
		Unit:   "USD million",
		Prefix: []string{"Profit for the period", "Прибыль за период"},
	},
	{
		Name:   "eps_basic",
		Unit:   "USD/share",
		Prefix: []string{"Earnings per share – basic, $", "Earnings per share – basic (US\nDollar)", "Базовая прибыль на акцию ($)"},
	},
	{
		Name:   "eps_diluted",
		Unit:   "USD/share",
		Prefix: []string{"Earnings per share – diluted, $", "Earnings per share – diluted (US\nDollar)", "Разводненная прибыль на акцию ($)"},
	},
	{
		Name:   "adjusted_net_profit",
		Unit:   "USD million",
		Prefix: []string{"Adjusted net profit", "Скорректированная чистая прибыль"},
	},
	{
		Name:   "adjusted_net_profit_margin",
		Unit:   "percent",
		Prefix: []string{"Adjusted net profit margin, %", "Рентабельность по скорректированной чистой прибыли, %"},
	},
	{
		Name:   "adjusted_ebitda",
		Unit:   "USD million",
		Prefix: []string{"Adjusted EBITDA from continuing operations", "Скорректированный показатель EBITDA"},
	},
	{
		Name:   "capex",
		Unit:   "USD million",
		Prefix: []string{"CAPEX", "Capital expenditure", "Капитальные затраты"},
	},
	{
		Name:   "stripping_capex",
		Unit:   "USD million",
		Prefix: []string{"Stripping CAPEX", "Капитальные затраты по вскрышным работам"},
	},
	{
		Name:   "tcc_per_ounce",
		Unit:   "USD/oz",
		Prefix: []string{"Total cash cost (TCC) per ounce sold, $/oz", "Total cash cost (TCC) per ounce\nsold ($/oz)", "Общие денежные затраты (TCC) на проданную унцию ($)"},
	},
	{
		Name:   "aisc_per_ounce",
		Unit:   "USD/oz",
		Prefix: []string{"All-in sustaining cash cost (AISC) per ounce sold, $/oz", "All-in sustaining cash cost (AISC)\nper ounce sold ($/oz)", "Совокупные денежные затраты (AISC) на проданную унцию ($)"},
	},
	{
		Name:   "cash_and_cash_equivalents",
		Unit:   "USD million",
		Prefix: []string{"Cash and cash equivalents", "Денежные средства и их эквиваленты"},
	},
	{
		Name:   "bank_deposits",
		Unit:   "USD million",
		Prefix: []string{"Bank deposits", "Банковские депозиты"},
	},
	{
		Name:   "net_debt_including_derivatives",
		Unit:   "USD million",
		Prefix: []string{"Net debt (incl. derivatives)", "Чистый долг (с учетом деривативов)"},
	},
	{
		Name:   "net_debt_to_adjusted_ebitda",
		Unit:   "ratio",
		Prefix: []string{"Net debt (incl. derivatives) / adjusted EBITDA, x", "Net debt/adjusted EBITDA (x)"},
	},
}

// MetricNames возвращает имена метрик, которые может выдать разбор PDF, кроме
// псевдометрики "period" (она маркер единиц измерения, а не значение). Каталог
// рядов и его тест на полноту сверяются с этим списком, а не дублируют его.
func MetricNames() []string {
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		if m.Name == "period" {
			continue
		}

		names = append(names, m.Name)
	}

	return names
}

// MetricDefinitions возвращает словарь релизных метрик вместе с их единицами.
// Нужен там, где по имени метрики требуется взять единицу: каталог рядов
// (views/series_meta.go) пишет в series_catalog ту же единицу, что несёт колонка
// unit витрины, и брать её из второго списка значило бы завести копию словаря.
//
// Срез копируется: это словарь, а не состояние — правка вызывающим не должна
// менять разбор.
func MetricDefinitions() []MetricDefinition {
	defs := make([]MetricDefinition, len(metrics))
	copy(defs, metrics)

	return defs
}
