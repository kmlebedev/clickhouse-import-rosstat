package ofz

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

var ofzIssueAnalyticsView = util.View{
	Name:   "v_ofz_issue_analytics",
	Tables: []string{ofzIssueAnalyticsTable},
	Select: "SELECT * FROM ofz_issue_analytics FINAL",
	Comment: "Выпуск-уровневая аналитика ОФЗ на TQCB/TQOB: последняя цена, YTM, " +
		"дюрация, НКД, купон, ликвидность и спред к кривой бескупонной доходности " +
		"ЦБ. Одна строка на выпуск, режим торгов и дату расчёта (МСК). source = " +
		"transaq — данные Transaq (securities_info/trades); source = moex — fallback " +
		"MOEX ISS по последней сессии, когда Transaq не отдаёт securities_info или " +
		"сделки. YTM и дюрация считаются по ACT/365 для ОФЗ-ПД (фиксированный купон, " +
		"погашение в конце); для ОФЗ-АД/ПК/ИН и при отсутствии сделки/данных YTM может " +
		"быть NULL, а причина — в calculation_status. Цена NULL означает, что сделок " +
		"не было, а не ноль; у source = moex статистика сделок (turnover/trade_count) " +
		"не заполняется. Это аналитика, а не рекомендация: стратегии, позиций и " +
		"сигналов здесь нет.",
	Columns: map[string]string{
		"date":                  "Дата расчёта (МСК, Date); одна строка на выпуск, режим торгов и дату",
		"sec_code":              "Код бумаги Transaq (например, SU26238RMFS9)",
		"isin":                  "ISIN выпуска",
		"board":                 "Режим торгов MOEX: TQCB или TQOB",
		"source":                "Источник строки: transaq — transaq_securities_info/trades; moex — MOEX ISS (последняя сессия, без статистики сделок)",
		"short_name":            "Короткое имя выпуска",
		"maturity_date":         "Дата погашения",
		"days_to_maturity":      "Дней от даты расчёта до погашения",
		"coupon_rate":           "Годовая ставка купона, %: coupon_value / face_value × 365 / coupon_period × 100; NULL — нет данных",
		"coupon_period":         "Период купона, дни; NULL — нет данных",
		"face_value":            "Номинал, руб.; NULL — нет данных",
		"accrued_int":           "НКД на одну бумагу, руб.; NULL — нет данных",
		"price":                 "Последняя цена сделки, % номинала; NULL — сделок не было, а не ноль",
		"ytm":                   "Доходность к погашению, % годовых: для source='transaq' — расчёт ACT/365 при calculation_status='ok'; для source='moex' — YIELD MOEX ISS, в т.ч. справочно при unsupported_*; NULL — нет данных",
		"duration":              "Дюрация Маколея, годы; NULL при calculation_status != ok",
		"modified_duration":     "Модифицированная дюрация, годы; NULL при calculation_status != ok",
		"turnover":              "Оборот сделок за дату расчёта, руб.; 0 — сделок не было",
		"trade_count":           "Число сделок за дату расчёта; 0 — сделок не было",
		"avg_trade_size":        "Средний размер сделки за дату расчёта, руб.; 0 — сделок не было",
		"days_since_last_trade": "Дней с последней сделки; NULL — сделок не было",
		"spread_to_curve":       "YTM минус интерполированная КБД ЦБ на срок до погашения, п.п.; NULL — нет YTM или кривой",
		"curve_tenor":           "Теноры КБД ЦБ, между которыми интерполирована доходность (например, 1-3); пусто — спред не считался",
		"calculation_status":    "ok — YTM/дюрация посчитаны; unsupported_amortizing/variable/indexed/unknown — вне v1; no_price — нет сделки; missing_data — нет купона/номинала; no_solution — YTM не найден",
		"updated_at":            "Время записи строки (Europe/Moscow)",
	},
}
