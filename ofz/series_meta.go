package ofz

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

const ofzOrigin = "transaq_securities_info FINAL + transaq_securities FINAL + " +
	"transaq_trades FINAL + cbr_zcyc FINAL; fallback MOEX ISS " +
	"/iss/engines/stock/markets/bonds/boards/{TQCB,TQOB}/securities.json; расчёт ofz_issue_analytics"

func ofzMeta(series, title, unit, description string) util.SeriesMeta {
	return util.SeriesMeta{
		Source:      ofzIssueAnalyticsTable,
		Series:      series,
		Title:       title,
		Unit:        unit,
		Frequency:   "D",
		Origin:      ofzOrigin,
		Description: description,
	}
}

// ofzSeriesMeta describes the numeric columns of v_ofz_issue_analytics. The
// issue (sec_code/board) is a dimension; one catalog row describes the metric
// for all issues.
func ofzSeriesMeta() []util.SeriesMeta {
	return []util.SeriesMeta{
		ofzMeta("ytm", "Доходность к погашению ОФЗ", "% годовых",
			"YTM по последней сделке, ACT/365, для ОФЗ-ПД. NULL — нет сделки, "+
				"неподдерживаемый тип выпуска или YTM не найден; причина в "+
				"calculation_status. Измерения — sec_code и board."),
		ofzMeta("duration", "Дюрация Маколея ОФЗ", "годы",
			"Дюрация Маколея по тем же денежным потокам, что и YTM. NULL — "+
				"YTM не посчитан. Измерения — sec_code и board."),
		ofzMeta("modified_duration", "Модифицированная дюрация ОФЗ", "годы",
			"duration / (1 + ytm/100). NULL — YTM не посчитан. Измерения — "+
				"sec_code и board."),
		ofzMeta("price", "Последняя цена ОФЗ", "% номинала",
			"Последняя сделка на TQCB/TQOB за доступную историю transaq_trades; "+
				"NULL — сделок не было, а не ноль. Измерения — sec_code и board."),
		ofzMeta("accrued_int", "НКД ОФЗ", "руб.",
			"Накопленный купонный доход на одну бумагу из transaq_securities_info. "+
				"NULL — данных нет. Измерения — sec_code и board."),
		ofzMeta("coupon_rate", "Ставка купона ОФЗ", "% годовых",
			"Годовая ставка текущего купона: coupon_value / face_value × 365 / "+
				"coupon_period × 100. Это ставка купона, а не YTM. Измерения — "+
				"sec_code и board."),
		ofzMeta("spread_to_curve", "Спред YTM ОФЗ к КБД ЦБ", "п.п.",
			"YTM минус линейно интерполированная КБД ЦБ на срок до погашения. "+
				"NULL — нет YTM или точки кривой. Измерения — sec_code и board."),
		ofzMeta("turnover", "Оборот сделок ОФЗ за день", "руб.",
			"Сумма price/100 × face_value × quantity × lotsize по сделкам за дату "+
				"расчёта; 0 — сделок не было. Измерения — sec_code и board."),
		ofzMeta("trade_count", "Число сделок ОФЗ за день", "сделок",
			"Количество сделок за дату расчёта; 0 — сделок не было. Измерения — "+
				"sec_code и board."),
		ofzMeta("avg_trade_size", "Средний размер сделки ОФЗ", "руб.",
			"turnover / trade_count за дату расчёта; 0 — сделок не было. "+
				"Измерения — sec_code и board."),
		ofzMeta("days_since_last_trade", "Дней с последней сделки ОФЗ", "дни",
			"Дата расчёта минус дата последней сделки; NULL — сделок не было. "+
				"Измерения — sec_code и board."),
		ofzMeta("days_to_maturity", "Дней до погашения ОФЗ", "дни",
			"Календарных дней от даты расчёта до погашения. Измерения — sec_code "+
				"и board."),
	}
}
