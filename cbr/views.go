package cbr

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

var cbrMacroView = util.View{
	Name: "v_cbr_macro",
	Tables: []string{
		"cbr_key_rate", "cbr_currency_usd", "cbr_gold", "cbr_ruania", "cbr_m2", "cbr_credit_m2x",
		"cbr_bank_int_rate", "cbr_int_rate_loans", "cbr_indicators_cpd", "cbr_infl_exp",
		"cbr_loans_to_individuals", "cbr_loans_to_corporations", "cbr_households_b_mes",
	},
	Select: `SELECT 'cbr' AS source, 'cbr_key_rate' AS series, date, toFloat64(rate) AS value FROM cbr_key_rate FINAL
UNION ALL SELECT 'cbr', 'cbr_currency_usd', date, toFloat64(price) FROM cbr_currency_usd FINAL
UNION ALL SELECT 'cbr', 'cbr_gold', date, toFloat64(price) FROM cbr_gold FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_ruo', date, toFloat64(ruo) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_vol', date, toFloat64(vol) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_trans', date, toFloat64(trans) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_min_rate', date, toFloat64(minRate) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_max_rate', date, toFloat64(maxRate) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_p25', date, toFloat64(percentile25) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_p75', date, toFloat64(Percentile75) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_m2 FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_credit_m2x FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_bank_int_rate FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_indicators_cpd FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_infl_exp FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_loans_to_individuals FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_loans_to_corporations FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_households_b_mes FINAL`,
	Comment: "Макроряды ЦБ РФ: ключевая ставка, курс USD, учётная цена золота, RUONIA, М2 SA, кредиты физлицам и юрлицам, ставки по вкладам и кредитам, ИПЦ (темпы), инфляционные ожидания, балансы домашних хозяйств. Платёжного баланса здесь нет: его ряды живут в отдельной витрине v_cbr_bop (source cbr_bop_est и cbr_bop_standard, свои единицы и виды периода) — внешние условия и курс по платёжному балансу искать там. Перед расчётом читать v_series_catalog по (source, series): единицы и привязка даты различаются между рядами",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда cbr",
		"series": "Название ряда; совпадает с series в v_series_catalog. У кредитов физлицам по регионам, кредитов юрлицам по видам деятельности (ОКВЭД), ставок по вкладам, ИПЦ, инфляционных ожиданий и балансов домохозяйств название — текст исходной таблицы ЦБ (строка региона или вида деятельности, заголовок колонки срока); у ставок по кредитам юрлицам имя — комбинация двух ячеек источника, группы заёмщиков («Всего»/«СМП») и срока кредита, которую собирает код (стандарт §6.8)",
		"date":   "Дата наблюдения (Date). Сдвиги даты различаются по рядам: кредиты — на месяц вперёд, инфляционные ожидания — 14-е число, ставки по вкладам и кредитам — предпоследний день месяца; см. description в v_series_catalog",
		"value":  "Значение ряда (Float64). Единицы зависят от ряда: % годовых, руб. за USD, руб./грамм, млрд руб., млн руб., % к предыдущему месяцу — см. unit в v_series_catalog",
	},
}

func publishSeries(ctx context.Context, conn driver.Conn, meta []util.SeriesMeta) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, cbrMacroView)
	return err
}

// afterCbrMacro — производные импортёров пакета: каталог рядов и общая витрина
// v_cbr_macro. Публикуются шаблоном после Send точек и до записи строки версии (§8 п. 13).
func afterCbrMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error { return publishSeries(ctx, conn, meta) }
}

// cbrBopView — витрина платёжного баланса РФ (source = имя таблицы, а не литерал 'cbr'): ряды
// выгрузок ЦБ по платёжному балансу имеют те же имена, что ряды общей витрины v_cbr_macro
// («Счет текущих операций»), и в ней они бы столкнулись, поэтому у платёжного баланса отдельная
// витрина — как у v_cbr_survey и v_cbr_mo_br. Ветки две, по выгрузкам: cbr_bop_est (оценка
// ключевых агрегатов, 51 ряд, млрд долларов США) и cbr_bop_standard (стандартная презентация,
// 34 ряда, млн долларов США) — единицы у них разные, и различает ряды двух выгрузок только
// source.
var cbrBopView = util.View{
	Name:   "v_cbr_bop",
	Tables: []string{"cbr_bop_est", "cbr_bop_standard"},
	Select: `SELECT 'cbr_bop_est' AS source, toString(name) AS series, date, toFloat64(value) AS value FROM cbr_bop_est FINAL
UNION ALL SELECT 'cbr_bop_standard', toString(name), date, toFloat64(value) FROM cbr_bop_standard FINAL`,
	Comment: "Платёжный баланс РФ (ЦБ РФ) двумя выгрузками: оценка ключевых агрегатов " +
		"(cbr_bop_est, 51 ряд: 17 агрегатов × {месяц, квартал, накопленным итогом с начала года}) " +
		"и стандартная презентация (cbr_bop_standard, 34 ряда: те же 17 верхних агрегатов × " +
		"{квартал с 1994 года, год 1994–2025}). Ряды: счёт текущих операций, экспорт и импорт " +
		"товаров, услуг и доходов, товары и услуги, торговый баланс, первичные и вторичные доходы, " +
		"счёт операций с капиталом, сальдо финансового счёта, резервные активы, чистые ошибки и " +
		"пропуски. Единицы различаются по source: у оценки — млрд долларов США, у стандартной " +
		"презентации — млн долларов США (в 1000 раз мельче). Дата точки: последний день отчётного " +
		"месяца, квартала или года; у накопленного итога — последний день последнего покрытого " +
		"месяца (сопоставлять по toStartOfMonth(date), toStartOfQuarter(date) или toYear(date)). " +
		"Лист оценки публикует только текущий год и тот же период прошлого года (накопленный итог, " +
		"два последних квартала и два последних месяца), поэтому её ряды «(месяц)» и «(квартал)» " +
		"набираются выпуск за выпуском: пропуск между точками значит «этот выпуск их не показывает», " +
		"а не «у источника нет данных». Длинную историю (с I квартала 1994 года, в млн долларов США) " +
		"даёт стандартная презентация. " +
		"Ноль — настоящее значение (сальдо нулевое), а не пропуск данных. Значения оценки ЦБ " +
		"ревизует следующим выпуском. Перед использованием читать v_series_catalog по " +
		"(source, series): там названы unit, знак ряда и привязка даты — по имени ряда две " +
		"выгрузки неразличимы",
	Columns: map[string]string{
		"source": "Импортёр-источник: cbr_bop_est — оценка ключевых агрегатов платёжного баланса ЦБ РФ, cbr_bop_standard — его стандартная презентация (квартальная и годовая)",
		"series": "Имя ряда: «<агрегат> (<вид>)»; вид — месяц, квартал или накопленным итогом с начала года у cbr_bop_est (51 ряд) и квартал или год у cbr_bop_standard (34 ряда). Имена рядов двух выгрузок совпадают, различает их source; описания — в v_series_catalog по (source, series)",
		"date":   "Дата точки (Date): последний день отчётного месяца, квартала или года; у накопленного итога — последний день последнего покрытого месяца. Сопоставлять по toStartOfMonth(date), toStartOfQuarter(date) или toYear(date)",
		"value":  "Значение (Float64). Единицы зависят от source: млрд долларов США у cbr_bop_est, млн долларов США у cbr_bop_standard (в 1000 раз мельче) — сверять unit в v_series_catalog. Положительное — превышение поступлений над выплатами или чистый прирост требований, отрицательное — обратное. Ноль — настоящее значение (сальдо нулевое), а не пропуск данных",
	},
}

// afterCbrBop — производные импортёров платёжного баланса: каталог рядов (source = имя таблицы)
// и витрина v_cbr_bop. Публикуются шаблоном после Send точек и до записи строки версии
// (стандарт §10.2).
func afterCbrBop(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrBopView)
		return err
	}
}

// cbrDipView — витрина приложений к бюллетеню ЦБ «О чём говорят тренды» (source = имя таблицы,
// единственный источник): long-форма с винтажными колонками (прецедент расширенной витрины —
// v_keyrate_expectations). Агент читает точку как «значение, каким его опубликовал выпуск
// release на дату publication_date»: один отчётный месяц в разных выпусках — разные строки, а
// не перезапись, поэтому витрина и есть point-in-time слой — по ней видны ревизии (SA-ряды ЦБ
// пересчитывает между выпусками). Набор рядов узкий (8) и вёрстка приложений дрейфует между
// выпусками: отсутствие точки в ряду значит «этот выпуск её не показывает», а не «данных нет».
var cbrDipView = util.View{
	Name:   "v_cbr_dip_bulletin",
	Tables: []string{cbrDipTable},
	Select: "SELECT 'cbr_dip_bulletin' AS source, series, date, value, release, release_id, publication_date FROM cbr_dip_bulletin FINAL",
	Comment: "Приложения к бюллетеню Банка России «О чём говорят тренды» (расчёты ДИП): 8 рядов " +
		"— сезонно сглаженный рост цен (SAAR, % м/м в годовом выражении: все товары и услуги, " +
		"продовольственные, непродовольственные, услуги) и рынок труда (уровень безработицы и " +
		"занятости, каждый парой SA/NSA). Витрина винтажная: у точки стоят номер выпуска (release, " +
		"«26-07») и дата его публикации (publication_date), и один и тот же отчётный месяц разных " +
		"выпусков — разные строки (ключ (series, date, release_id)), а не перезапись: читать как " +
		"«значение, каким его опубликовал выпуск … на дату …». SA-ряды ЦБ пересчитывает между " +
		"выпусками (ревизии видны сравнением строк одного месяца), а NSA-колонка в части выпусков " +
		"идёт без SA-пары. Дата точки — последний день отчётного месяца (в книге источника — " +
		"первое число): сопоставлять по toStartOfMonth(date). Набор рядов и листы дрейфуют между " +
		"выпусками (листа рынка труда нет в 13 выпусках из 49; в 26-07 безработицу заменил уровень " +
		"занятости), поэтому отсутствие точки значит «выпуск её не показывает». Единица — %; перед " +
		"использованием читать v_series_catalog по (source='cbr_dip_bulletin', series)",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда cbr_dip_bulletin",
		"series": "Имя ряда из 8 объявленных: saar_all_goods, saar_food, saar_nonfood, saar_services (SAAR-инфляция, % м/м в годовом выражении), unemployment_sa/unemployment_nsa, employment_sa/employment_nsa (рынок труда, %). Описания и единицы — в v_series_catalog (source='cbr_dip_bulletin')",
		"date":   "Отчётный месяц: последний день месяца (в книге источника — первое число; 2015-01-01 → 2015-01-31). Сопоставлять по toStartOfMonth(date)",
		"value": "Значение ряда (Float64), %: у saar_* — сезонно скорректированный рост цен месяц к месяцу в годовом выражении (SAAR), у рядов рынка труда — уровень безработицы/занятости. " +
			"Значение — то, каким его опубликовал выпуск release на дату publication_date; пустая ячейка источника точкой не становится (месяца в ряду нет), литеральный ноль — точка. Единицы и смысл — в v_series_catalog",
		"release":          "Номер выпуска источника («26-07»: две цифры года группы + номер из подписи «№ 7 / Октябрь»). Выпуск — точка во времени: одна и та же метрика в разных выпусках различается",
		"release_id":       "id выпуска из адреса файла приложения (/Collection/Collection/File/<id>/): ключ винтажа и часть ключа строки. Имя файла ключом не служит — stat_bulletin_23-07.xlsx встречается у двух выпусков; замена файла под тем же id гейтом не отслеживается (лечение — удалить строки выпуска и повторить прогон)",
		"publication_date": "Дата публикации выпуска на сайте ЦБ (атрибут ссылки приложения «Опубликовано …»). Атрибут выпуска, не часть ключа: дату публикации ЦБ может поправить, id — нет",
	},
}

// afterCbrDip — производные импортёра cbr_dip_bulletin: каталог восьми рядов (cbr/dip_meta.go) и
// витрина v_cbr_dip_bulletin с винтажными колонками. Публикуются шаблоном по каждому
// импортированному выпуску (после точек и до строки версии, стандарт §10.2): на первом прогоне
// это 49 повторов, дальше — 0–1 на прогон; идемпотентно и стоит двух запросов к своей таблице
// (отступление 5 спеки §5).
func afterCbrDip(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrDipView)
		return err
	}
}

// cbrMtForecastView — витрина диапазонов среднесрочного прогноза Банка России (source = имя
// таблицы, единственный источник): dedicated-колонки lo/hi, а не long-форма (source, series,
// date, value). Отступление от буквы §10.3 названо в спеке §5.2: диапазон не сводится к паре
// «дата → значение» без распада на две строки, теряющего смысл «это один диапазон», а сужение до
// одного value потеряло бы половину прогноза; прецеденты расширенных витрин —
// v_keyrate_expectations, v_cbr_dip_bulletin. Витрина винтажная: (indicator, year) в разных
// выпусках — разные строки, а не перезапись, и по ней видны ревизии прогноза ЦБ между выпусками.
var cbrMtForecastView = util.View{
	Name:   "v_cbr_mt_forecast",
	Tables: []string{mtForecastTable},
	Select: "SELECT 'cbr_mt_forecast' AS source, meeting_date, release_date, url, indicator, year, lo, hi FROM cbr_mt_forecast FINAL",
	Comment: "Диапазоны среднесрочного прогноза Банка России (базовый сценарий): 16 показателей " +
		"листа «Табл 1» «Комментария к среднесрочному прогнозу» — в том числе среднегодовая " +
		"ключевая ставка и инфляция (декабрь к декабрю и в среднем за год), — какими их " +
		"зафиксировал каждый выпуск. Импортируются оба корпуса: комментарии (страница с xlsx у " +
		"двух новейших выпусков, PDF у остальных) и PDF «Среднесрочный прогноз Банка России» с " +
		"той же «Табл. 1» — по одному на заседание. Прогнозный PDF публикуется в день заседания " +
		"и даёт самую раннюю точку PIT; значения прогноза и комментария одного заседания " +
		"совпадают (различается только типографика), но лежат отдельными строками с разными " +
		"release_date и url — это разные выпуски, а не дубль. Выпуск release_date — точка во " +
		"времени: один и тот же показатель и прогнозный год в разных выпусках — разные строки " +
		"(ключ (indicator, year, release_date)), а не перезапись, поэтому сравнивать диапазоны " +
		"разных выпусков нужно по паре (indicator, year) с указанием release_date, а сдвиг " +
		"прогноза судить разностью границ между выпусками (прецедент — винтажные колонки " +
		"v_cbr_dip_bulletin). lo = hi — точечный прогноз (источник напечатал одно число, а не " +
		"диапазон); у первого прогнозного года диапазон уже, чем у последующих (сноска 1 листа). " +
		"Перед использованием читать v_series_catalog по (source='cbr_mt_forecast', series): там " +
		"описания показателей и код согласования с рядами опроса аналитиков (cbr_mo_br)",
	Columns: map[string]string{
		"source":       "Импортёр-источник: всегда cbr_mt_forecast",
		"meeting_date": "Дата опорного заседания Совета директоров Банка России, к которому относится выпуск (атрибут выпуска, не часть ключа). PDF «Среднесрочный прогноз» публикуется в день заседания, комментарий — через 11–17 дней; выпуски — 4 раза в год (февраль, апрель, июль, октябрь)",
		"release_date": "Дата публикации артефакта выпуска — комментария (страница или PDF) либо «Среднесрочного прогноза» (у прогноза совпадает с meeting_date) — точка во времени, ключ винтажа: значения разных выпусков не затирают друг друга, и прогноз ЦБ на один и тот же год, прочитанный из разных строк, — это разные редакции прогноза. Подмена файла под тем же release_date гейтом прогона не отслеживается (лечение — удалить строки выпуска и повторить прогон)",
		"url":          "Страница выпуска-комментария или PDF-файл (комментарий либо «Среднесрочный прогноз»), с которого взяты данные (провенанс; адрес меняется от выпуска к выпуску). Скачанный артефакт покрывается винтажным архивом версий (source_publications агенту не выдаётся)",
		"indicator":    "Код показателя: cpi_dec (ИПЦ, декабрь к декабрю), cpi_avg (ИПЦ в среднем за год), key_rate_avg (ключевая ставка в среднем за год), gdp_yoy, gdp_q4_yoy, consumption_total, consumption_households, gross_capital_formation, fixed_capital_investment, exports, imports, m2, bank_claims_total, bank_claims_organizations, bank_claims_households, bank_claims_mortgage. Шесть кодов (cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, exports, imports) согласованы с именами рядов макроэкономического опроса ЦБ (source='cbr_mo_br') — прогноз ЦБ сопоставляется с консенсусом аналитиков по общим именам, единицы при этом сверять по unit. Описания — в v_series_catalog",
		"year":         "Прогнозный год (UInt16), к которому относится диапазон. Горизонт меняется от выпуска к выпуску (3–4 года), состав годов берётся из шапки книги",
		"lo":           "Нижняя граница диапазона прогноза (Float64). У точечного прогноза lo = hi (источник напечатал одно число). Единицы — по indicator (см. unit в v_series_catalog: % дек. к дек., % к пред. году, % годовых, % прироста). Значение — то, каким его зафиксировал выпуск release_date: это прогноз ЦБ, а не факт, и следующий выпуск может пересмотреть границы",
		"hi":           "Верхняя граница диапазона прогноза (Float64). Смысл границ: ЦБ публикует прогноз диапазоном, а не точкой; сужение к среднему не предусмотрено источником",
	},
}

// afterCbrMtForecast — производные импортёра cbr_mt_forecast: каталог 16 рядов
// (cbr/mt_forecast_meta.go) и витрина v_cbr_mt_forecast. Публикуются собственным Import()
// импортёра до гейта — на каждом прогоне, включая no-op (самолечение устаревшей витрины и
// каталога между выпусками: выпуск выходит 4 раза в год, а сбой CreateView мог случиться в любом
// из прошлых прогонов). Отступление от буквы §10.2 названо в спеке §5.3; прецедент —
// rosstat_ipc_weeks_bulletins. Идемпотентно и стоит двух запросов к своим таблицам.
func afterCbrMtForecast(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrMtForecastView)
		return err
	}
}

// cbrSurveyView — витрина рядов мониторинга предприятий (source='cbr_survey', импортёр
// cbr_mp_survey); общая витрина cbrMacroView её не покрывает.
var cbrSurveyView = util.View{
	Name:    "v_cbr_survey",
	Tables:  []string{"macro_series"},
	Select:  `SELECT source, series, date, value FROM macro_series FINAL WHERE source = 'cbr_survey'`,
	Comment: "Мониторинг предприятий Банка России (опросы нефинансовых компаний): 13 агрегатных рядов «Экономики всего» и трёх квартальных листов (ценовые ожидания бизнеса, ИБК и условия кредитования; загрузка мощностей, обеспеченность персоналом, ожидания численности) и 368 отраслевых рядов 16 листов (industry_*: ИБК и его факт/ожидания, выпуск, спрос, цены, ценовые ожидания, издержки, условия кредитования, ожидаемый прирост отпускных цен; у всех, кроме условий кредитования, есть сезонно сглаженная пара _sa). Дата — отчётный период: 1-е число месяца, у квартальных — 1-е число месяца, следующего за кварталом; ЦБ публикует волну в первую декаду следующего месяца и нумерует справки по месяцу волны (справка «сентябрь 2026» цитирует точку отчётного периода август 2026). Перед использованием читать v_series_catalog по (source='cbr_survey', series): единицы и знаки различаются, а отраслевые ряды названы industry_<sheet>_<indicator>. ЦБ в комментариях цитирует сезонно сглаженные ряды (…_sa)",
	Columns: map[string]string{
		"source": "Всегда cbr_survey",
		"series": "Код ряда: 13 агрегатных и 368 отраслевых (industry_<sheet>_<indicator>, у 11 из 12 показателей есть _sa) — описания в v_series_catalog (source='cbr_survey')",
		"date":   "Отчётный период: 1-е число месяца; у квартальных рядов — 1-е число месяца, следующего за отчётным кварталом (конвенция rosstat_vvp_kvartal)",
		"value":  "Значение в единицах ряда: пункты баланса ответов, % (загрузка мощностей), % годовых (ожидаемый прирост цен) — см. unit и description в v_series_catalog",
	},
}

// cbrZcycCompareView — сравнение КБД ЦБ (cbr_zcyc, 12 теноров с 04.01.2003) и
// G-curve МосБиржи (ofz_curve, 1y/3y/5y/10y и только с 2026-10-09) по (date, tenor).
// Dedicated-форма, а не long-форма §10.3: это сравнение двух источников одной
// строкой (прецеденты — v_cbr_dip_bulletin, v_cbr_mt_forecast).
var cbrZcycCompareView = util.View{
	Name:   "v_ofz_curve_compare",
	Tables: []string{cbrZcycTable, "ofz_curve"},
	Select: `SELECT date, tenor,
        maxIf(toNullable(yield), source = 'cbr')  AS cbr_yield,
        maxIf(toNullable(yield), source = 'moex') AS moex_yield,
        round(maxIf(toNullable(yield), source = 'moex') - maxIf(toNullable(yield), source = 'cbr'), 2) AS spread_pp
    FROM (
        SELECT 'cbr' AS source, date, tenor, yield FROM cbr_zcyc FINAL
        UNION ALL
        SELECT 'moex', date, replaceRegexpOne(tenor, 'y$', ''), yield
        FROM ofz_curve FINAL WHERE tenor IN ('1y','3y','5y','10y')
    )
    GROUP BY date, tenor`,
	Comment: "Сравнение кривой бескупонной доходности ЦБ РФ (cbr_zcyc: 12 теноров, история " +
		"с 04.01.2003) и G-curve МосБиржи (ofz_curve: теноры 1y/3y/5y/10y — только они и только " +
		"с 2026-10-09). Одна строка на (date, tenor): cbr_yield и moex_yield — уровни двух " +
		"источников, % годовых; spread_pp = moex_yield − cbr_yield, п.п. (положительный — G-curve " +
		"выше КБД ЦБ). spread_pp округлён до 0.01 п.п.; у близкого к нулю отрицательного спреда " +
		"возможна запись -0.0 — это ноль, а не дефект. tenor — каноническая запись КБД ЦБ: 0.25, " +
		"0.5, 0.75, 1, 2, 3, 5, 7, 10, 15, 20, 30 (лет); теноры МосБиржи 1y/3y/5y/10y приведены " +
		"к 1/3/5/10 срезом суффикса y, остальные теноры есть только у ЦБ. NULL в cbr_yield, " +
		"moex_yield или spread_pp означает отсутствие точки у источника (пропуск публикации, " +
		"выходной, ранняя дата у МосБиржи), а не ноль; строки появляются по датам, где есть хотя " +
		"бы один источник. Перед вычислением спредов читать v_series_catalog по (source, series): " +
		"единицы обоих источников — % годовых",
	Columns: map[string]string{
		"date":       "Дата наблюдения (Date): торговый день, за который опубликована КБД ЦБ и/или котировка G-curve МосБиржи",
		"tenor":      "Тенор в канонической записи КБД ЦБ: 0.25, 0.5, 0.75, 1, 2, 3, 5, 7, 10, 15, 20, 30 (лет). Теноры МосБиржи 1y/3y/5y/10y нормализованы в 1/3/5/10; остальные значения есть только у ЦБ",
		"cbr_yield":  "Доходность по КБД ЦБ, % годовых (cbr_zcyc, тенор из колонки tenor). NULL — ЦБ не публиковал точку в этот день, а не ноль",
		"moex_yield": "Доходность по G-curve МосБиржи, % годовых (ofz_curve; только теноры 1/3/5/10 и только с 2026-10-09). NULL — точки нет, а не ноль",
		"spread_pp":  "Спред moex_yield − cbr_yield, процентных пунктов: положительный — G-curve МосБиржи выше КБД ЦБ, отрицательный — ниже. Округлён до 0.01 п.п.; -0.0 у близкого к нулю отрицательного спреда — это ноль, а не ошибка. NULL, если отсутствует хотя бы одна из точек",
	},
}

// cbrZcycSignalView — сигналы долгового рынка по КБД ЦБ: одна строка на дату
// публикации кривой. Dedicated-форма, а не long-форма §10.3: производные колонки
// одной строкой (прецеденты — v_keyrate_expectations, v_cbr_mt_forecast).
//
// Правая сторона обоих ASOF LEFT JOIN обёрнута в toNullable: при настройке
// ClickHouse join_use_nulls=0 (значение по умолчанию) промах join подставляет в
// не-Nullable колонку значение по умолчанию — 0 для ставки и ИПЦ, — и обещание
// NULL в комментарии витрины было бы ложью: до 2013-09-17 key_rate стал бы нулём,
// а спред — самой доходностью ОФЗ. Совпавшая строка остаётся числом, промах —
// NULL. count() = 12 в окне ИПЦ — та же защита от частичного г/г на первых 11
// строках ряда.
var cbrZcycSignalView = util.View{
	Name:   "v_ofz_signal",
	Tables: []string{cbrZcycTable, "cbr_key_rate", "rosstat_ipc_mes"},
	Select: `SELECT c.date AS date,
        c.ofz_1y, c.ofz_2y, c.ofz_10y,
        k.rate AS key_rate,
        round(c.ofz_10y - k.rate, 2) AS ofz_10y_minus_key_rate_pp,
        round(c.ofz_2y  - k.rate, 2) AS ofz_2y_minus_key_rate_pp,
        round(c.ofz_10y - c.ofz_1y, 2) AS slope_10y_1y_pp,
        round(i.cpi_yoy, 2) AS inflation_yoy,
        round(c.ofz_10y - i.cpi_yoy, 2) AS real_yield_10y_pp
    FROM (
        SELECT date,
               maxIf(toNullable(yield), tenor = '1')  AS ofz_1y,
               maxIf(toNullable(yield), tenor = '2')  AS ofz_2y,
               maxIf(toNullable(yield), tenor = '10') AS ofz_10y,
               1 AS _one
        FROM cbr_zcyc FINAL GROUP BY date
    ) AS c
    ASOF LEFT JOIN (
        SELECT date, toNullable(rate) AS rate, 1 AS _one FROM cbr_key_rate FINAL ORDER BY date
    ) AS k
      ON c._one = k._one AND c.date >= k.date
    ASOF LEFT JOIN (
        SELECT date, toNullable(cpi_yoy) AS cpi_yoy, 1 AS _one FROM (
            SELECT date,
                   if(count() OVER (ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) = 12,
                      (exp(sum(log(toFloat64(percent) / 100)) OVER (
                          ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - 1) * 100,
                      NULL) AS cpi_yoy
            FROM rosstat_ipc_mes FINAL
            WHERE name = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
            ORDER BY date
        ) ORDER BY date
    ) AS i ON c._one = i._one AND c.date >= i.date`,
	Comment: "Сигналы долгового рынка по КБД ЦБ: одна строка на дату публикации кривой " +
		"(cbr_zcyc, торговый день). ofz_1y/ofz_2y/ofz_10y — доходность ОФЗ соответствующего " +
		"тенора, % годовых. key_rate — последняя действующая ключевая ставка на дату " +
		"(ступенчатая функция, ASOF JOIN с cbr_key_rate); до 2013-09-17 истории ставки нет — " +
		"NULL, а не ноль. ofz_10y_minus_key_rate_pp и ofz_2y_minus_key_rate_pp — спреды " +
		"ofz − key_rate, п.п.: положительное значение значит «ОФЗ выше ключевой» (рынок требует " +
		"премию к ставке), отрицательное — «ОФЗ ниже ключевой». slope_10y_1y_pp — наклон кривой " +
		"10y − 1y, п.п. (отрицательный — инверсия). inflation_yoy — ИПЦ Росстата г/г, %: " +
		"произведение 12 месячных индексов rosstat_ipc_mes к отчётному месяцу (первые 11 строк " +
		"ряда ИПЦ — NULL, незаконченный год не считается; окно берёт 12 строк, а не 12 календарных " +
		"месяцев, поэтому пропуск месяца в источнике оконная функция не ловит); дата ИПЦ — конец " +
		"отчётного месяца, а лаг публикации ~2 недели не моделируется (для point-in-time " +
		"расчётов дату ИПЦ сдвигать отдельно). real_yield_10y_pp = ofz_10y − inflation_yoy, п.п. " +
		"(ex post, без инфляционных ожиданий). Спреды, наклон и реальная доходность округлены " +
		"до 0.01 п.п.; NULL в любой колонке означает отсутствие точки, ставки или ИПЦ на дату, " +
		"а не ноль. Перед использованием читать v_series_catalog по (source, series)",
	Columns: map[string]string{
		"date":                      "Дата КБД ЦБ (торговый день): одна строка на дату публикации кривой",
		"ofz_1y":                    "Доходность ОФЗ с погашением через 1 год по КБД ЦБ, % годовых (тенор 1). NULL — точки нет, а не ноль",
		"ofz_2y":                    "Доходность ОФЗ с погашением через 2 года по КБД ЦБ, % годовых (тенор 2). NULL — точки нет, а не ноль",
		"ofz_10y":                   "Доходность ОФЗ с погашением через 10 лет по КБД ЦБ, % годовых (тенор 10). NULL — точки нет, а не ноль",
		"key_rate":                  "Ключевая ставка ЦБ, % годовых, действующая на дату: последнее решение не позже date (ASOF). NULL до 2013-09-17 — истории ставки раньше нет, это не ноль",
		"ofz_10y_minus_key_rate_pp": "Спред ofz_10y − key_rate, п.п.: положительный — «ОФЗ выше ключевой», отрицательный — ниже. NULL, если нет ставки или точки",
		"ofz_2y_minus_key_rate_pp":  "Спред ofz_2y − key_rate, п.п.: короткий конец кривой против ключевой ставки. NULL, если нет ставки или точки",
		"slope_10y_1y_pp":           "Наклон кривой 10y − 1y, п.п.: положительный — нормальная форма, отрицательный — инверсия. NULL, если нет одной из точек",
		"inflation_yoy":             "ИПЦ Росстата г/г, %: произведение 12 месячных индексов rosstat_ipc_mes к отчётному месяцу; первые 11 строк ряда ИПЦ — NULL (незаконченный год не считается), но окно берёт 12 строк, а не 12 календарных месяцев — пропуск месяца в источнике оконная функция не ловит. Дата — конец отчётного месяца; лаг публикации ~2 недели не моделируется, для point-in-time расчётов сдвигать отдельно. NULL — нет точки ИПЦ, а не ноль",
		"real_yield_10y_pp":         "Реальная доходность ofz_10y − inflation_yoy, п.п. (ex post, без инфляционных ожиданий). NULL, если нет точки или ИПЦ",
	},
}

// afterCbrZcyc — производные импортёра cbr_zcyc: каталог 12 рядов (cbr/zcyc_meta.go),
// витрина сравнения КБД с G-curve МосБиржи и витрина сигналов. Публикуются шаблоном
// после точек и до строки версии (стандарт §10.2). CreateView пропускает витрину,
// пока нет хотя бы одной исходной таблицы (ofz_curve, cbr_key_rate,
// rosstat_ipc_mes) — это не ошибка: следующий прогон самолечит витрину.
func afterCbrZcyc(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		if _, err := util.CreateView(ctx, conn, cbrZcycCompareView); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrZcycSignalView)
		return err
	}
}
