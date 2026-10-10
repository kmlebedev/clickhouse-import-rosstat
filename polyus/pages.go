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

	// -- Включённые релизы 4Q/FY за 2019–2024 ------------------------------
	// Шесть релизов 4Q/FY, у которых на выверенной странице лежит таблица
	// «Comparative financial results» со строкой «Gold production (koz)».
	// Шапка у них шире, чем у FY2014: 2019–2021 печатают подписи периодов
	// разорванными по двум строкам («4Q» на одной, «2019» на другой), а у всех
	// шести между периодами стоят колонки-изменения (Y-o-Y/H-o-H) без года в
	// метке. Оба случая разбирает колоночная модель: полоса шапки собирается по
	// ролям строк, а разорванные подписи склеиваются по X (columns.go).
	// Значения закреплены тестом TestKPIValuesHistoryReports (kpi_test.go).
	{
		// Проверено pdftotext: страница 3 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf",
		Period:  "2019FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{3},
		Enabled: true,
	},
	{
		// Проверено pdftotext: страница 4 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf",
		Period:  "2020FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},
	{
		// Проверено pdftotext: страница 4 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf",
		Period:  "2021FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},
	{
		// Проверено pdftotext: страница 4 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf",
		Period:  "2022FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},
	{
		// Проверено pdftotext: страница 4 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf",
		Period:  "2023FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},
	{
		// Проверено pdftotext: страница 4 содержит "Gold production (koz)".
		URL:     "https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf",
		Period:  "2024FY",
		Kind:    "kpi",
		Lang:    "en",
		Pages:   []int{4},
		Enabled: true,
	},

	// -- Отключённые отчёты из legacy-списка -------------------------------
	// Разбор этих файлов не выверен. В legacy они лежат закомментированными,
	// потому что колонки периодов в их таблицах пришлось бы задавать вручную
	// (там есть PeriodColumnMap по URL), а автоматический разбор шапки для них
	// не проверялся.
	//
	// После включения шести релизов 4Q/FY за 2019–2024 набор отключённых сузился
	// до 2015–2018 и 2025H2, и причина у них одна: это документы другого типа, а
	// не невыверенная страница.
	//
	//  1. Часть набора — не KPI-релизы. 2018FY
	//     (polyus-group-ifrs-cons-fs-18_e_signed.pdf) — консолидированная
	//     МСФО-форма без колоночной периодики, то есть Kind: "ifrs", а не "kpi".
	//  2. 2015–2017 — MD&A с консолидированной отчётностью, тоже не релизы
	//     «Comparative financial results»: у них нет той шапки, из которой KPI-
	//     разбор берёт периоды, и Kind, Lang и Pages для каждого файла
	//     определяются отдельно. У 2017FY к тому же расходятся номера страниц:
	//     в legacy-списке 5, в PeriodColumnMap — 4.
	//  3. 2025H2 (2026_03_16_2h25-_-tu-mda-eng.pdf) — англоязычный 2H25-релиз с
	//     двухшапочной таблицей, которого в legacy-списке не было вовсе.
	//
	// Включать их без прогона pdftotext нельзя: включение вынесено в следующий
	// пункт работ — отчёты 2015–2018
	// (docs/superpowers/specs/2026-10-10-polyus-history-reports-design.md,
	// §4 «Границы»).
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
