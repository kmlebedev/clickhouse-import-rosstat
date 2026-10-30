package weeklysa

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// weeklySASeriesMeta — каталог трёх рядов витрины. Описание объясняет агенту
// MCP, что ряд производный и индикативный, а уровни sa_index несопоставимы с
// официальным ИПЦ; ограничения 2009–2018 и 2022+ публикуются, а не замалчиваются.
func weeklySASeriesMeta() []util.SeriesMeta {
	origin := "derived: rosstat_ipc_weeks_bulletins (агрегат «Индекс потребительских цен (недельная оценка)», 2009–2021) + " +
		"rosstat_bi_cpi_weekly (2019+, приоритет на пересечении); X-13ARIMA-SEATS seats_log_cal; " +
		"7-дневная сетка, 4-нед. индекс 28 дней; spec " + weeklySASpecVersion
	description := "Производный ряд контура ключевой ставки: официальный недельный агрегат ИПЦ Росстата, " +
		"4-недельный накопленный индекс на регулярной 7-дневной сетке и сезонная корректировка " +
		"X-13ARIMA-SEATS seats_log_cal (log, авто-выбросы, календарный тест). Дата точки — последний день месяца; " +
		"полные месяцы идут с partial = 0, текущий неполный месяц — nowcast-строка с partial = 1 и as_of последней доступной недели " +
		"(X-13 по 209 полным + текущему). 2009–2018 — бюллетени «Оперативной информации» с 24–51 датой в год, " +
		"интерполированные на 7-дневную сетку; на пересечении 2019–2021 приоритет у BI Росстата. " +
		"2022+ alone в research #348 не значим (DM p = 0,1896), на длинной истории as-of 2012-07+ MAE 2,67 против 3,52 " +
		"(p = 0,0216) — ряд индикативный и не заменяет официальный ИПЦ/SA ЦБ. Ревизии источника возможны; PIT/винтажей нет."
	return []util.SeriesMeta{
		{
			Source:      weeklySATable,
			Series:      "weekly_cpi:sa_index",
			Title:       "Недельный SA-агрегат ИПЦ: 4-нед. индекс",
			Unit:        "индекс (100 = без изменения за 4 недели)",
			Frequency:   "M",
			Origin:      origin,
			Description: description + " Колонка sa_index — сезонно скорректированный 4-нед. накопленный индекс; абсолютный уровень с другими рядами не сравнивать.",
		},
		{
			Source:      weeklySATable,
			Series:      "weekly_cpi:sa_4w",
			Title:       "Недельный SA-агрегат ИПЦ: темп за 4 недели",
			Unit:        "% за 4 недели",
			Frequency:   "M",
			Origin:      origin,
			Description: description + " Колонка sa_4w — темп SA за 4 недели, %: (sa_index/100 − 1) × 100.",
		},
		{
			Source:      weeklySATable,
			Series:      "weekly_cpi:saar",
			Title:       "Недельный SA-агрегат ИПЦ: SAAR",
			Unit:        "% годовых",
			Frequency:   "M",
			Origin:      origin,
			Description: description + " Колонка saar — годовой темп SAAR, %: ((sa_index/100)^13 − 1) × 100 (13 четырёхнедельных окон в году).",
		},
	}
}
