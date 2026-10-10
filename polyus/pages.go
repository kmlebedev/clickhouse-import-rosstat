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
	// Разбор этих файлов не выверен. В legacy они лежат закомментированными,
	// потому что колонки периодов в их таблицах пришлось бы задавать вручную
	// (там есть PeriodColumnMap по URL), а автоматический разбор шапки для них
	// не проверялся.
	//
	// Причины отключения три, и выверки одних номеров страниц мало:
	//
	//  1. Номера страниц не выверены. Правило «одна страница на отчёт» для этого
	//     набора не действует: у 2024FY операционная таблица напечатана не на
	//     первых страницах, а на странице 4 внутри «Comparative financial
	//     results» (страницы 9–10 несут другие таблицы), поэтому каждый файл
	//     требует отдельного прогона pdftotext и ручной выверки.
	//  2. Шапка шире, чем различает разбор. Пример — 2024FY, страница 4:
	//     «2024 | 2023 | Y-o-Y | 2H 2024 | 1H 2024 | H-o-H | 2H 2023 | Y-o-Y» —
	//     восемь колонок, из них три колонки-изменения, и метки Y-o-Y/H-o-H не
	//     содержат года вовсе. Колоночная модель различает такие колонки
	//     (columns.go), но именно её готовность к этим шапкам и нужно проверить
	//     на каждом файле перед включением.
	//  3. Часть набора — не KPI-релизы. Например 2018FY
	//     (polyus-group-ifrs-cons-fs-18_e_signed.pdf) — консолидированная
	//     МСФО-форма без колоночной периодики, то есть Kind: "ifrs", а не "kpi";
	//     часть — двухшапочные релизы 4Q/FY. Kind, Lang и Pages для каждого
	//     файла определяются отдельно.
	//
	// Включать их без прогона pdftotext нельзя: включение вынесено в следующий
	// пункт работ (docs/superpowers/specs/2026-10-10-polyus-column-model-design.md,
	// §3.1 «11 отключённых отчётов: почему не в этой итерации»).
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
