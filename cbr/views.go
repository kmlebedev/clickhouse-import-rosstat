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
	Comment: "Макроряды ЦБ РФ: ключевая ставка, курс USD, учётная цена золота, RUONIA, М2 SA, кредиты физлицам и юрлицам, ставки по вкладам и кредитам, ИПЦ (темпы), инфляционные ожидания, балансы домашних хозяйств. Перед расчётом читать v_series_catalog по (source, series): единицы и привязка даты различаются между рядами",
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
