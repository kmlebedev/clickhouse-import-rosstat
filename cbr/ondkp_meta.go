package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// cbrOndkpSeriesMeta — каталог трёх рядов прогноза ОНДКП. Source — таблица прогноза, а не
// реестр выпусков: series_catalog описывает измеряемые ряды, метаданные выпусков лежат в
// cbr_ondkp.
var cbrOndkpSeriesMeta = []util.SeriesMeta{
	{
		Source:    cbrOndkpForecastTable,
		Series:    ondkpIndicatorKeyRate,
		Title:     "ОНДКП: среднегодовая ключевая ставка в базовом сценарии",
		Unit:      "% годовых",
		Frequency: "A",
		Origin:    "cbr.ru/about_br/publ/ondkp — таблица «Прогноз Банка России в рамках базового сценария», строка «Ключевая ставка, в среднем за год»",
		Description: "Прогноз ЦБ на год из конкретного винтажа ОНДКП (release_year). lo/hi — границы диапазона, lo = hi — точка. " +
			"Читать как винтаж: один и тот же год встречается в нескольких выпусках с разными значениями, as-of — release_year. " +
			"У выпусков 2000–2019 строки нет: прогноз изложен прозой (см. docs/LIMITATIONS.md).",
	},
	{
		Source:    cbrOndkpForecastTable,
		Series:    ondkpIndicatorInflation,
		Title:     "ОНДКП: инфляция в среднем за год в базовом сценарии",
		Unit:      "% к предыдущему году",
		Frequency: "A",
		Origin:    "cbr.ru/about_br/publ/ondkp — строка «Инфляция, в среднем за год»",
		Description: "Прогноз инфляции в среднем за год из винтажа release_year. Не путать с целью по инфляции: " +
			"цель — 4 % с 2015 года, прогноз старших винтажей отклоняется от неё в годы шоков.",
	},
	{
		Source:    cbrOndkpForecastTable,
		Series:    ondkpIndicatorGDP,
		Title:     "ОНДКП: прирост ВВП в базовом сценарии",
		Unit:      "% к предыдущему году",
		Frequency: "A",
		Origin:    "cbr.ru/about_br/publ/ondkp — строка «Валовой внутренний продукт»",
		Description: "Прогноз прироста ВВП из винтажа release_year, диапазон lo/hi. До 2020 года в корпусе нет " +
			"машиночитаемой таблицы базового сценария — ряд начинается с выпуска 2020.",
	},
}
