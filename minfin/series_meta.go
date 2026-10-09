package minfin

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	minfinSource        = "minfin"
	minfinMesyatsSource = "minfin_mesyats"
	fedbudMonthlyNote   = "Значение — показатель за месяц, млрд руб.: в исходном файле Минфина данные накопленные с начала года, таблица хранит разность с предыдущим накоплением (январь равен накоплению за январь). Дата — 1-е число месяца; годовая сумма — сумма 12 месяцев."
	fedbudMesOrigin     = "minfin.gov.ru/ru/statistics/fedbud/execute, файл *_mes.xlsx (Приложение 3, таблицы 109–111), лист «месяц»"
	fedbudMesyatsOrigin = "minfin.gov.ru/ru/statistics/conbud/execute, файл *_mesyats.xlsx (Приложение 8, таблицы 115–117), лист «месяц»"
)

var minfinBudgetView = util.View{
	Name:   "v_minfin_budget",
	Tables: []string{fedbudMesTable, fedbudMesyatsTable},
	Select: `SELECT 'minfin' AS source, name AS series, date, toFloat64(value) AS value FROM minfin_fed_bud_mes FINAL
UNION ALL SELECT 'minfin_mesyats', name, date, toFloat64(value) FROM minfin_fed_bud_mesyats FINAL`,
	Comment: "Помесячное исполнение бюджета по данным Минфина: федеральный бюджет (source = minfin) и консолидированный бюджет с государственными внебюджетными фондами (source = minfin_mesyats). Значения — млрд руб. за месяц, дата — 1-е число месяца. Одинаковые названия рядов в двух источниках означают разные показатели: перед расчётом читать v_series_catalog по (source, series)",
	Columns: map[string]string{
		"source": "Источник: minfin — федеральный бюджет; minfin_mesyats — консолидированный бюджет и внебюджетные фонды",
		"series": "Название ряда; совпадает с series в v_series_catalog. Без source название не уникально",
		"date":   "Месяц наблюдения (Date): 1-е число месяца",
		"value":  "Значение за месяц в млрд руб. (Float64); отрицательное значение дефицита — только у ряда «Дефицит (-)/Профицит (+)»",
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

func publishFedbud(ctx context.Context, conn driver.Conn, meta []util.SeriesMeta) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, minfinBudgetView)
	return err
}
