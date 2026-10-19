package fao

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

const (
	faoSource = "fao"
	faoNote   = "Дата — 1-е число месяца публикации; значение — индекс за предыдущий месяц. Номинальный индекс, 2014-2016=100; real-индекса в источнике нет. "
)

var faoSeriesMeta = []util.SeriesMeta{
	{
		Source:      faoSource,
		Series:      "Food Price Index",
		Title:       "Индекс продовольственных цен ФАО, сводный",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Food Price Index",
		Description: faoNote + "Сводный индекс международных цен корзины продовольственных товаров; используется в nowcast ИПЦ как внешний ценовой сигнал.",
	},
	{
		Source:      faoSource,
		Series:      "Meat",
		Title:       "Индекс цен ФАО на мясо",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Meat",
		Description: faoNote + "Индекс международных цен на мясо.",
	},
	{
		Source:      faoSource,
		Series:      "Dairy",
		Title:       "Индекс цен ФАО на молочную продукцию",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Dairy",
		Description: faoNote + "Индекс международных цен на молочную продукцию.",
	},
	{
		Source:      faoSource,
		Series:      "Cereals",
		Title:       "Индекс цен ФАО на зерновые",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Cereals",
		Description: faoNote + "Индекс международных цен на зерновые; связан с экспортным паритетом зерна и внутренними ценами.",
	},
	{
		Source:      faoSource,
		Series:      "Oils",
		Title:       "Индекс цен ФАО на растительные масла",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Oils",
		Description: faoNote + "Индекс международных цен на растительные масла; сегмент с повышенной импортной зависимостью.",
	},
	{
		Source:      faoSource,
		Series:      "Sugar",
		Title:       "Индекс цен ФАО на сахар",
		Unit:        "index, 2014-2016=100",
		Frequency:   "M",
		Origin:      "FAO Food Price Index CSV, колонка Sugar",
		Description: faoNote + "Индекс международных цен на сахар. Ряд добавлен при рефакторинге: legacy-разбор брал только первые пять колонок и терял Sugar.",
	},
}
