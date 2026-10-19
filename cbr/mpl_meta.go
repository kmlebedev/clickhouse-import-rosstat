package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// cbrMPLSeriesMeta — каталог видов МПЛ. Ряд — сегмент + класс кредита; полосы
// ПДН/срока/взноса/LTV/суммы задаются колонками витрины, а не отдельными рядами.
var cbrMPLSeriesMeta = []util.SeriesMeta{
	{
		Source:    cbrMPLTable,
		Series:    "bank_consumer",
		Title:     "МПЛ банков: потребительские кредиты (с 2025-07)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_banks_01072025.xlsx, лист «Потребительские кредиты»",
		Description: "Макропруденциальный лимит для потребительских кредитов банков с " +
			"универсальной лицензией: доля рискованных кредитов в общем объёме выдач, %. " +
			"С 2025-07 источник объединил кредиты с лимитом кредитования и иные кредиты в " +
			"одном листе; полосы ПДН и срока заданы текстом, limit_pct — уже в процентах. " +
			"Одна строка — правило; pdn_band/term_band — подписи полос, pdn_min/pdn_max/" +
			"term_min/term_max — разобранные границы (nil — границы нет).",
	},
	{
		Source:    cbrMPLTable,
		Series:    "bank_consumer_with_limit",
		Title:     "МПЛ банков: потребительские кредиты с лимитом кредитования (2023–2025)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_banks.xlsx и Macroprudential_limits_banks_0107.xlsx, " +
			"листы «С лимитом кредитования; ПДН/Срок»",
		Description: "Макропруденциальный лимит для потребительских кредитов (займов) с " +
			"лимитом кредитования, банки с универсальной лицензией, формат 2023-01 … 2025-06. " +
			"Значения источника — доли (0.25), в limit_pct нормализованы в проценты (25). " +
			"Полосы ПДН/срока — из Limits_ranges_*; одна строка — одна полоса с одним лимитом.",
	},
	{
		Source:    cbrMPLTable,
		Series:    "bank_consumer_other",
		Title:     "МПЛ банков: иные потребительские кредиты (2023–2025)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_banks.xlsx и Macroprudential_limits_banks_0107.xlsx, " +
			"листы «Иные кредиты (займы); ПДН/Срок»",
		Description: "Макропруденциальный лимит для иных потребительских кредитов (займов) " +
			"без лимита кредитования, банки с универсальной лицензией, формат 2023-01 … " +
			"2025-06. Значения источника — доли, в limit_pct нормализованы в проценты. " +
			"Полосы ПДН/срока — из Limits_ranges_*.",
	},
	{
		Source:    cbrMPLTable,
		Series:    "bank_mortgage",
		Title:     "МПЛ банков: ипотечные кредиты (с 2025-07)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_banks_01072025.xlsx, лист «Кредиты, обеспеченные ипотекой»",
		Description: "Макропруденциальный лимит для ипотечных кредитов банков: доля " +
			"рискованных кредитов, %. Полосы задаются набором ПДН + первоначальный взнос + " +
			"LTV + срок; подстроки источника наследуют лимит и незаполненные условия " +
			"родительской строки. pdn_band/down_payment_band/ltv_band/term_band — подписи, " +
			"*_min/*_max — разобранные границы (nil — границы нет).",
	},
	{
		Source:    cbrMPLTable,
		Series:    "mfo_consumer_with_limit",
		Title:     "МПЛ МФО: потребительские займы с лимитом кредитования (2023–2025)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_mfo.xlsx и Macroprudential_limits_mfo_0107.xlsx, " +
			"лист «С лимитом кредитования; ПДН»",
		Description: "Макропруденциальный лимит для потребительских займов с лимитом " +
			"кредитования, микрофинансовые организации, формат 2023-01 … 2025-06. Значения " +
			"источника — доли, в limit_pct нормализованы в проценты. Полосы ПДН — из " +
			"Limits_ranges_mfo_*.",
	},
	{
		Source:    cbrMPLTable,
		Series:    "mfo_consumer_other",
		Title:     "МПЛ МФО: иные потребительские займы (2023–2025)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_mfo.xlsx и Macroprudential_limits_mfo_0107.xlsx, " +
			"лист «Иные кредиты (займы); ПДН»",
		Description: "Макропруденциальный лимит для иных потребительских займов без лимита " +
			"кредитования, микрофинансовые организации, формат 2023-01 … 2025-06. Значения " +
			"источника — доли, в limit_pct нормализованы в проценты. Полосы ПДН — из " +
			"Limits_ranges_mfo_*.",
	},
	{
		Source:    cbrMPLTable,
		Series:    "mfo_consumer_loan",
		Title:     "МПЛ МФО: потребительские займы (с 2025-07)",
		Unit:      "%",
		Frequency: "Q",
		Origin: "cbr.ru/finstab/instruments/makroprudencialnye-limity, " +
			"Macroprudential_limits_mfo_01072025.xlsx, лист «Потребительские займы»",
		Description: "Макропруденциальный лимит для потребительских займов МФО: доля " +
			"рискованных займов, %. Полосы задаются ПДН + срок (дни) + сумма (тыс. руб.); " +
			"limit_pct — в процентах. pdn_band/term_band/amount_band — подписи, *_min/*_max — " +
			"разобранные границы (nil — границы нет).",
	},
}
