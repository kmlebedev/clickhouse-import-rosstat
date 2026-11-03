package fns

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// fnsBudgetRevenueView — витрина «как опубликовано»: накопленным итогом с начала
// года. Таблица группы — константой пакета, Select — литералом: витрина является
// публичным контрактом агента, и переименование константы не должно молча увести её
// к другой таблице.
var fnsBudgetRevenueView = util.View{
	Name:   "v_fns_budget_revenue",
	Tables: []string{fnsTable},
	Select: `SELECT 'fns_budget_revenue' AS source, series, region, region_kind, date, value
FROM fns_budget_revenue FINAL`,
	Comment: "Поступления налогов и сборов по данным ФНС (форма № 1-НМ): вид налога × уровень " +
		"бюджета × территория. Значение — накопленным итогом с начала года, тыс. руб. (в источнике " +
		"именно так); за месяц читать v_fns_budget_revenue_monthly. Единицы и правила чтения каждого " +
		"ряда — v_series_catalog по (source = 'fns_budget_revenue', series)",
	Columns: map[string]string{
		"source":      "Всегда fns_budget_revenue: форма 1-НМ, единственный источник витрины",
		"series":      "Вид налога и уровень бюджета: admin_revenues_total, profit_tax_federal, ndfl_regional, …; совпадает с series в v_series_catalog",
		"region":      "Территория в написании источника: 'Российская Федерация' (агрегат), один из восьми федеральных округов или один из 85 субъектов",
		"region_kind": "Класс территории: rf — агрегат по стране, federal_district — округ (группировка субъектов, не слагаемое), subject — субъект",
		"date":        "1-е число отчётного месяца; архив помечен датой as-of первого числа следующего месяца",
		"value":       "Накопленным итогом с начала года, тыс. руб.; отрицательное значение — уточнение или возврат за прошедший период",
	},
}

// fnsBudgetRevenueMonthlyView — помесячные поступления: разность накопленных того же
// ряда и территории. Январь равен накопленному за январь; если предыдущего месяца в
// таблице нет (пропуск публикации или разрыв года), значение отдаёт NULL, а не ноль и
// не сумму за несколько месяцев.
var fnsBudgetRevenueMonthlyView = util.View{
	Name:   "v_fns_budget_revenue_monthly",
	Tables: []string{fnsTable},
	Select: `SELECT source, series, region, region_kind, date, value_ytd,
	multiIf(toMonth(date) = 1, value_ytd,
		prev_date = subtractMonths(date, 1), value_ytd - prev_value,
		NULL) AS value
FROM (
	SELECT 'fns_budget_revenue' AS source, series, region, region_kind, date, value AS value_ytd,
		lagInFrame(date) OVER w AS prev_date,
		lagInFrame(value) OVER w AS prev_value
	FROM fns_budget_revenue FINAL
	WINDOW w AS (PARTITION BY series, region ORDER BY date)
)`,
	Comment: "Помесячные поступления налогов и сборов по данным ФНС (форма № 1-НМ): разность " +
		"накопленных с начала года значений соседних месяцев одного ряда и территории. Значение — " +
		"за месяц, тыс. руб.; value_ytd — накопленным итогом, как опубликовано. NULL в value — " +
		"месяц, для которого в источнике нет предыдущего (разрывы 2023-01…2023-07, 2023-09, " +
		"2024-01, 2024-02), а не ноль",
	Columns: map[string]string{
		"source":      "Всегда fns_budget_revenue",
		"series":      "Вид налога и уровень бюджета; совпадает с series в v_series_catalog",
		"region":      "Территория: 'Российская Федерация', федеральный округ или субъект",
		"region_kind": "rf | federal_district | subject",
		"date":        "1-е число отчётного месяца; архив помечен датой as-of первого числа следующего месяца",
		"value":       "Nullable(Float64): поступление за этот месяц, тыс. руб.; NULL — нет предыдущего месяца для разности",
		"value_ytd":   "Накопленным итогом с начала года, тыс. руб., как опубликовано в источнике",
	},
}

// afterFnsBudgetRevenue публикует производные: каталог рядов и обе витрины. Вызывается
// один раз за прогон импортёра — и на пустой дельте тоже, чтобы на чистой базе
// каталог и витрина появились, даже когда импортировать нечего.
func afterFnsBudgetRevenue(ctx context.Context, conn driver.Conn) error {
	meta := fnsBudgetRevenueSeriesMeta()
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	if _, err := util.CreateView(ctx, conn, fnsBudgetRevenueView); err != nil {
		return err
	}
	if _, err := util.CreateView(ctx, conn, fnsBudgetRevenueMonthlyView); err != nil {
		return err
	}
	return nil
}
