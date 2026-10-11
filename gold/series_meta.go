package gold

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

const goldSource = "gold"

var goldSeriesMeta = []util.SeriesMeta{
	{
		Source:    goldSource,
		Series:    goldVenue,
		Title:     "Цена золота: фиксинг MOEX GOLDFIXME, пересчитанный в USD/oz",
		Unit:      "USD за тройскую унцию",
		Frequency: "D",
		Origin:    "iss.moex.com (борд FIXI, инструмент GOLDFIXME, ₽/г) × 31,1034768 г/oz ÷ курс ЦБ cbr_currency_usd",
		Description: "Производная цена золота, не LBMA: фиксинг MOEX GOLDFIXME в рублях за грамм пересчитан в доллары " +
			"за тройскую унцию по курсу ЦБ (последний известный не позже даты; окно 10 дней). Ряд начинается 2024-08-05. " +
			"Темп — отношение к предыдущей строке; строк нет в дни без фиксинга и без курса ЦБ.",
	},
	{
		Source:    goldSource,
		Series:    "comex_front",
		Title:     "Цена золота: COMEX front-month (GC=F), закрытие дня",
		Unit:      "USD за тройскую унцию (фьючерс, не спот)",
		Frequency: "D",
		Origin:    "Yahoo Finance GC=F (фронт-месяц COMEX) через kimi-datasource, сессионный ввод через POST /v1/gold_prices",
		Description: "Цена фронт-месяца фьючерса COMEX из сессионного канала datasource. Не спот и не LBMA: " +
			"базис фьючерса к споту не устраняется. Ряд начинается разовым backfill до 2 лет (лимит источника) " +
			"и пишется сессионно, не импортёром; импортёр этот ряд не трогает. Темп — отношение к предыдущей строке.",
	},
}

var goldPricesView = util.View{
	Name:   "v_gold_prices",
	Tables: []string{goldTable},
	Select: `SELECT 'gold' AS source, toString(venue) AS series, toDate(date) AS date, usd AS value
	FROM gold_prices FINAL`,
	Comment: "Цены золота в USD/oz по площадкам (venue). Две площадки: moex_fix_usd — производная цена " +
		"(фиксинг MOEX GOLDFIXME в ₽/г ÷ курс ЦБ), не LBMA; comex_front — фронт-месяц фьючерса COMEX (GC=F) " +
		"из сессионного канала, не спот. Единицы и метод — в v_series_catalog по (source='gold', series=venue)",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда gold",
		"series": "Площадка/метод цены; совпадает с series в v_series_catalog (moex_fix_usd — производная цена; comex_front — фьючерс, не спот)",
		"date":   "Дата наблюдения (Date)",
		"value":  "Цена золота, USD за тройскую унцию",
	},
}
