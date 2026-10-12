package gold

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

const goldSource = "gold"

// boldVenue — площадка цены золота от bold.report в той же таблице gold_prices. В gold_prices
// series == venue, поэтому константа связывает запись каталога с площадкой, в которую пишет
// импортёр пакета bold.
const boldVenue = "bold"

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

// BoldPriceSeriesMeta — каталог цены золота от bold.report (площадка bold в gold_prices).
// Экспортируется, потому что запись описывает ряд, который пишет импортёр пакета bold: он и
// публикует её — после успешного Send, а в ветке no-op — перепубликацией (описание ряда, которого
// в таблице нет, — обещание данных, которых агент не найдёт). Прецедент такой же связи — comex_front: ряд живёт в gold_prices, а
// пишет его ingest. Текст описания обязан называть главное для читателя: метод источником не
// раскрыт, это не спот, и comex_front этим рядом не подменяется (спека §4.2, §9) — и обязан
// оставаться качественным: снимковых чисел (глубина ряда, расхождения с comex_front на
// контрольных датах) здесь нет намеренно. Число точек устареет при первом же инкременте, а
// каталог читает агент: ложь в описании ряда хуже, чем отсутствие числа. Глубина ряда и текущий
// максимум живут в v_gold_prices (ARCHITECTURE.md §6.3 — там описана сама витрина), а не в каталоге.
func BoldPriceSeriesMeta() util.SeriesMeta {
	return util.SeriesMeta{
		Source:    goldSource,
		Series:    boldVenue,
		Title:     "Цена золота: фьючерсная цена bold.report (ByteTree), не спот и не GC=F",
		Unit:      "USD за тройскую унцию (фьючерс источника, метод детально не раскрыт)",
		Frequency: "D",
		Origin:    "bold.report/api/v1/gold/price.json (инкремент — gold/price-latest.json)",
		Description: "Фьючерсная цена золота, которую источник публикует как «Futures Price»: конкретный метод — контракт " +
			"и время снимка — источником детально не раскрыт, поэтому это не спот, не LBMA и не GC=F. " +
			"С нашим comex_front (GC=F, фронт-месяц COMEX, сессионный ввод) совпадение лишь частичное — " +
			"поэтому ряд идёт отдельной площадкой bold, отдельной серией, а не подменой comex_front. " +
			"Источник — bold.report (ByteTree), ключей нет, режим щадящий (1 запрос в сутки); данные T+1, " +
			"выходные источник опускает. Глубину ряда и текущий максимум смотреть в v_gold_prices, " +
			"а не здесь: число точек меняется с каждым прогоном (описание витрины — ARCHITECTURE.md §6.3). " +
			"Темп — отношение к предыдущей строке.",
	}
}

var goldPricesView = util.View{
	Name:   "v_gold_prices",
	Tables: []string{goldTable},
	Select: `SELECT 'gold' AS source, toString(venue) AS series, toDate(date) AS date, usd AS value
	FROM gold_prices FINAL`,
	Comment: "Цены золота в USD/oz по площадкам (venue). Три площадки: moex_fix_usd — производная цена " +
		"(фиксинг MOEX GOLDFIXME в ₽/г ÷ курс ЦБ), не LBMA; comex_front — фронт-месяц фьючерса COMEX (GC=F) " +
		"из сессионного канала, не спот; bold — фьючерсная цена источника bold.report (ByteTree), метод " +
		"детально не раскрыт: не спот и не GC=F, с comex_front совпадает частично (отдельная серия, а не " +
		"замена). Единицы и метод — в v_series_catalog по (source='gold', series=venue)",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда gold",
		"series": "Площадка/метод цены; совпадает с series в v_series_catalog (moex_fix_usd — производная цена; " +
			"comex_front — фьючерс GC=F, не спот; bold — фьючерсная цена bold.report с нераскрытым методом)",
		"date":  "Дата наблюдения (Date)",
		"value": "Цена золота, USD за тройскую унцию",
	},
}
