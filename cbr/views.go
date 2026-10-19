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
	Comment: "Мониторинг предприятий Банка России (опросы нефинансовых компаний): ценовые ожидания бизнеса, ИБК и условия кредитования (месячные); загрузка мощностей, обеспеченность персоналом, ожидания численности (квартальные). Дата — отчётный период: 1-е число месяца, у квартальных — 1-е число месяца, следующего за кварталом; ЦБ публикует волну в первую декаду следующего месяца и нумерует справки по месяцу волны (справка «сентябрь 2026» цитирует точку отчётного периода август 2026). Перед использованием читать v_series_catalog по (source='cbr_survey', series): единицы и знаки различаются. ЦБ в комментариях цитирует сезонно сглаженные ряды (…_sa)",
	Columns: map[string]string{
		"source": "Всегда cbr_survey",
		"series": "Код ряда; 13 рядов — описания в v_series_catalog (source='cbr_survey')",
		"date":   "Отчётный период: 1-е число месяца; у квартальных рядов — 1-е число месяца, следующего за отчётным кварталом (конвенция rosstat_vvp_kvartal)",
		"value":  "Значение в единицах ряда: пункты баланса ответов, % (загрузка мощностей), % годовых (ожидаемый прирост цен) — см. unit и description в v_series_catalog",
	},
}
