package polyus

// reports — таблица PDF-отчётов Polyus, из которых наполняется витрина
// polyus_financial_metrics. Перенесена из init() legacy-импортёра
// financial/gold_polyus_finance.go (коммит 108f4ca): список URL и номера
// страниц там же, включая закомментированные адреса — они стали записями с
// Enabled: false, чтобы адрес, период и причина отключения не потерялись.
//
// Каталог отчётов: https://polyus.com/en/investors/results-and-reports/
var reports = []Report{
	// -- Проверенные вручную отчёты за 1 п/г 2026 -------------------------
	{
		// Русский пресс-релиз 1 п/г 2026: производственная и финансовая
		// таблицы на одной странице, поэтому Pages — только страница 1.
		// Проверено pdftotext: страница 1 содержит «Производство золота»
		// (тыс. унций) и «Общие денежные затраты (TCC) на проданную унцию».
		URL:     "https://polyus.com/upload/iblock/80e/press_reliz-1h26-_tu_mda_2.pdf",
		Period:  "2026H1",
		Kind:    "kpi",
		Lang:    "ru",
		Pages:   []int{1},
		Enabled: true,
	},
	{
		// Английский МСФО-отчёт 1 п/г 2026: страница 6 — отчёт о прибылях и
		// убытках (Total revenue, Profit for the period), страница 7 — баланс
		// (Total assets). Проверено pdftotext.
		//
		// Разбирается английская версия, а не русская: английская отчитывается
		// в долларах США, как и датапак, русская печатает рубли.
		URL:     "https://polyus.com/upload/iblock/2c4/otchetnost-6m2026_eng.pdf",
		Period:  "2026H1",
		Kind:    "ifrs",
		Lang:    "en",
		Pages:   []int{6, 7},
		Enabled: true,
	},

	// -- Версия за полный 2014 год (единственная включённая в legacy) ------
	{
		URL:     "https://polyus.com/upload/iblock/7fb/report_management-discussion-and-analysis_financial-statements-_fy2014.pdf",
		Period:  "2014FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},

	// -- Отключённые отчёты из legacy-списка -------------------------------
	// У всех причина одна и та же: разбор этих файлов не выверен. В legacy они
	// лежат закомментированными, потому что колонки периодов в их таблицах
	// пришлось бы задавать вручную (там есть PeriodColumnMap по URL), а
	// автоматический разбор шапки для них не проверялся. Включать их без
	// прогона pdftotext нельзя: в пресс-релизах 4Q/FY шапка содержит больше
	// колонок, чем различает parseKPIPage.
	{
		URL:     "https://polyus.com/upload/iblock/9dd/mda_financial_statements_fy2015.pdf",
		Period:  "2015FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/965/fy2016-mda-fs-opinion.pdf",
		Period:  "2016FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		// В legacy-списке страница указана как 5, в PeriodColumnMap — 4;
		// расхождение не выверено.
		URL:     "https://polyus.com/upload/iblock/d51/2017_mda_opinion_fs-_1_.pdf",
		Period:  "2017FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/9bc/polyus-group-ifrs-cons-fs-18_e_signed.pdf",
		Period:  "2018FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf",
		Period:  "2019FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf",
		Period:  "2020FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf",
		Period:  "2021FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf",
		Period:  "2022FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf",
		Period:  "2023FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		URL:     "https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf",
		Period:  "2024FY",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
	{
		// В legacy-списке указана страница 26 — самая дальняя из всех, и
		// единственная, где в правило «одна страница на отчёт» не укладывается
		// ничего: адрес ведёт на англоязычный 2H25-релиз.
		URL:     "https://polyus.com/upload/iblock/eca/2026_03_16_2h25-_-tu-mda-eng.pdf",
		Period:  "2025H2",
		Kind:    "kpi",
		Lang:    "en",
		Enabled: false,
	},
}
