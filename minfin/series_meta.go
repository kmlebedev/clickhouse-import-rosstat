package minfin

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	minfinSource        = "minfin"
	minfinMesyatsSource = "minfin_mesyats"
	minfinOilGasSource  = "minfin_oil_gas"
	fedbudMonthlyNote   = "Значение — показатель за месяц, млрд руб.: в исходном файле Минфина данные накопленные с начала года, таблица хранит разность с предыдущим накоплением (январь равен накоплению за январь). Дата — 1-е число месяца; годовая сумма — сумма 12 месяцев."
	fedbudMesOrigin     = "minfin.gov.ru/ru/statistics/fedbud/execute, файл *_mes.xlsx (Приложение 3, таблицы 109–111), лист «месяц»"
	fedbudMesyatsOrigin = "minfin.gov.ru/ru/statistics/conbud/execute, файл *_mesyats.xlsx (Приложение 8, таблицы 115–117), лист «месяц»"
	oilGasOrigin        = "minfin.gov.ru/ru/statistics/fedbud/, файл Svedeniya_<YYYYMMDD>.xlsx, лист «Информационное сообщение»"
)

var minfinBudgetView = util.View{
	Name:   "v_minfin_budget",
	Tables: []string{fedbudMesTable, fedbudMesyatsTable, oilGasTable},
	Select: `SELECT 'minfin' AS source, name AS series, date, toFloat64(value) AS value FROM minfin_fed_bud_mes FINAL
UNION ALL SELECT 'minfin_mesyats', name, date, toFloat64(value) FROM minfin_fed_bud_mesyats FINAL
UNION ALL SELECT 'minfin_oil_gas', name, date, toFloat64(value) FROM minfin_oil_gas_mes FINAL`,
	Comment: "Помесячное исполнение бюджета по данным Минфина: федеральный бюджет (source = minfin), консолидированный бюджет с государственными внебюджетными фондами (source = minfin_mesyats) и нефтегазовые доходы федерального бюджета с НДПИ (source = minfin_oil_gas). Значения — млрд руб. за месяц, дата — 1-е число месяца. Одинаковые названия рядов в разных источниках означают разные показатели: перед расчётом читать v_series_catalog по (source, series)",
	Columns: map[string]string{
		"source": "Источник: minfin — федеральный бюджет; minfin_mesyats — консолидированный бюджет и внебюджетные фонды; minfin_oil_gas — нефтегазовые доходы и НДПИ из Svedeniya_<дата>.xlsx",
		"series": "Название ряда; совпадает с series в v_series_catalog. Без source название не уникально",
		"date":   "Месяц наблюдения (Date): 1-е число месяца",
		"value":  "Значение за месяц в млрд руб. (Float64); отрицательное значение сальдо — у ряда «Дефицит (-)/Профицит (+)» в обоих источниках (minfin и minfin_mesyats), знак ряда «привлечение» — как в источнике",
	},
}

var fedbudMesSeriesMeta = []util.SeriesMeta{
	{
		Source:      minfinSource,
		Series:      "Доходы, всего",
		Title:       "Доходы федерального бюджета, всего, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «Доходы, всего»",
		Description: fedbudMonthlyNote + " Суммарные доходы федерального бюджета.",
	},
	{
		Source:      minfinSource,
		Series:      "Расходы, всего",
		Title:       "Расходы федерального бюджета, всего, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «Расходы, всего»",
		Description: fedbudMonthlyNote + " Суммарные расходы федерального бюджета.",
	},
	{
		Source:      minfinSource,
		Series:      "Акцизы",
		Title:       "Акцизы в федеральном бюджете, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «Акцизы»",
		Description: fedbudMonthlyNote + " Поступления акцизов в федеральный бюджет.",
	},
	{
		Source:      minfinSource,
		Series:      "Национальная оборона",
		Title:       "Расходы федерального бюджета на национальную оборону, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «Национальная оборона»",
		Description: fedbudMonthlyNote + " В импортированных данных ряд заканчивается декабрём 2021 г. (132 месяца): за более поздние месяцы значений нет, это не нулевые расходы.",
	},
	{
		Source:      minfinSource,
		Series:      "привлечение",
		Title:       "Привлечение средств на покрытие дефицита федерального бюджета, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «привлечение»",
		Description: fedbudMonthlyNote + " Привлечение средств (государственные заимствования) для покрытия дефицита федерального бюджета; знак — как в исходном файле. В текущих данных ряд заканчивается июлем 2026 г., на месяц раньше остальных рядов.",
	},
	{
		Source:      minfinSource,
		Series:      "Дефицит (-)/Профицит (+)",
		Title:       "Дефицит (−) / профицит (+) федерального бюджета, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesOrigin + ", строка «Дефицит (-)/Профицит (+)»",
		Description: fedbudMonthlyNote + " Сальдо федерального бюджета: отрицательное значение — дефицит, положительное — профицит.",
	},
}

var fedbudMesyatsSeriesMeta = []util.SeriesMeta{
	{
		Source:      minfinMesyatsSource,
		Series:      "Доходы, всего",
		Title:       "Доходы консолидированного бюджета и внебюджетных фондов, всего, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesyatsOrigin + ", строка «Доходы, всего»",
		Description: fedbudMonthlyNote + " Доходы консолидированного бюджета РФ и государственных внебюджетных фондов. Охват шире федерального бюджета: значения не совпадают с рядом minfin «Доходы, всего».",
	},
	{
		Source:      minfinMesyatsSource,
		Series:      "Расходы, всего",
		Title:       "Расходы консолидированного бюджета и внебюджетных фондов, всего, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesyatsOrigin + ", строка «Расходы, всего»",
		Description: fedbudMonthlyNote + " Расходы консолидированного бюджета РФ и государственных внебюджетных фондов. Охват шире федерального бюджета: значения не совпадают с рядом minfin «Расходы, всего».",
	},
	{
		Source:      minfinMesyatsSource,
		Series:      "Акцизы",
		Title:       "Акцизы в консолидированном бюджете, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesyatsOrigin + ", строка «Акцизы»",
		Description: fedbudMonthlyNote + " Поступления акцизов в консолидированный бюджет РФ и внебюджетные фонды.",
	},
	{
		Source:      minfinMesyatsSource,
		Series:      "Национальная оборона",
		Title:       "Расходы на национальную оборону в консолидированном бюджете, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesyatsOrigin + ", строка «Национальная оборона»",
		Description: fedbudMonthlyNote + " В импортированных данных ряд заканчивается декабрём 2021 г. (132 месяца): за более поздние месяцы значений нет, это не нулевые расходы.",
	},
	{
		Source:      minfinMesyatsSource,
		Series:      "Дефицит (-)/Профицит (+)",
		Title:       "Дефицит (−) / профицит (+) консолидированного бюджета, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      fedbudMesyatsOrigin + ", строка «Дефицит (-)/Профицит (+)»",
		Description: fedbudMonthlyNote + " Сальдо консолидированного бюджета РФ и внебюджетных фондов: отрицательное значение — дефицит, положительное — профицит.",
	},
}

var oilGasSeriesMeta = []util.SeriesMeta{
	{
		Source:      minfinOilGasSource,
		Series:      "Нефтегазовые доходы, всего",
		Title:       "Нефтегазовые доходы федерального бюджета, всего, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      oilGasOrigin + ", строка «Нефтегазовые доходы, всего»",
		Description: "Значение — помесячный поток нефтегазовых доходов федерального бюджета, млрд руб.; в исходном файле это не накопленный итог с начала года. Годовые колонки файла не импортируются: ряд содержит только месячные точки начиная с 2018-01. Дата — 1-е число отчётного месяца; пустая ячейка и «-» — не точка.",
	},
	{
		Source:      minfinOilGasSource,
		Series:      "НДПИ",
		Title:       "Налог на добычу полезных ископаемых (НДПИ) в федеральном бюджете, помесячно",
		Unit:        "млрд руб.",
		Frequency:   "M",
		Origin:      oilGasOrigin + ", строка «Налог на добычу полезных ископаемых»",
		Description: "Значение — помесячный поток НДПИ, млрд руб.; в исходном файле это не накопленный итог с начала года. Агрегат по нефти, газу и газовому конденсату: подстроки источника («Нефть», «Газ», «Газовый конденсат») не импортируются. НДПИ — одна из составляющих нефтегазовых доходов, поэтому сумма двух импортированных рядов не равна ряду «Нефтегазовые доходы, всего». Годовые колонки файла не импортируются: ряд содержит только месячные точки начиная с 2018-01. Дата — 1-е число отчётного месяца; пустая ячейка и «-» — не точка.",
	},
}

func publishFedbud(ctx context.Context, conn driver.Conn, meta []util.SeriesMeta) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, minfinBudgetView)
	return err
}
