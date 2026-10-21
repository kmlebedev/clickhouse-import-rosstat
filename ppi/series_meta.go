package ppi

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// ppiSASeriesMeta — каталог пяти производных рядов. Описание объясняет агенту
// MCP, что `sa` — нормированный уровень, метрики — проценты, а уровни разных
// рядов несопоставимы по абсолюту; версии входят в origin.
func ppiSASeriesMeta() []util.SeriesMeta {
	consumerOrigin := "derived: rosstat_ppi_product (m/m, mapped KIPC leaves) + X-13ARIMA-SEATS seats_log_cal; taxonomy " + ppiSATaxonomyVersion
	totalOrigin := "derived: rosstat_ppi_ved (m/m, code " + ppiSATotalCode + ") + X-13ARIMA-SEATS seats_log_cal"
	return []util.SeriesMeta{
		{
			Source:    ppiSATable,
			Series:    "total",
			Title:     "ИЦП SA: совокупный индекс цен производителей (Промышленность)",
			Unit:      "индекс (100 = 2013-01); mm_sa/saar/saar3 — %",
			Frequency: "M",
			Origin:    totalOrigin,
			Description: "Сезонно скорректированный совокупный ИЦП Росстата по ВЭД (m/m, код 1323500.029.31), база 100 = 2013-01; " +
				"sa — уровень X-13ARIMA-SEATS (SEATS, log, авто-выбросы, календарный тест), " +
				"mm_sa — темп м/м SA, saar — годовой темп SAAR, saar3 — среднее доступных месячных SAAR (на третьем месяце — двух). " +
				"Ряд охватывает всю промышленность (B+C+D+E), не потребительскую корзину; абсолютный уровень с food/nonfood/all/core не сравнивать. " +
				"spec_version=" + ppiSASpecVersion + "; официальной PPI-спецификации ЦБ нет, паритет с ЦБ не заявлен.",
		},
		{
			Source:    ppiSATable,
			Series:    "food",
			Title:     "ИЦП SA: продовольственные товары (КИПЦ, без алкоголя)",
			Unit:      "индекс (100 = 2021-01); mm_sa/saar/saar3 — %",
			Frequency: "M",
			Origin:    consumerOrigin,
			Description: "Сезонно скорректированный KIPC-агрегат ИЦП продовольственных товаров (группы 01.1.1–01.1.5, 01.1.8, 01.1.9, 01.2), без алкоголя; " +
				"веса 2026, база 100 = 2021-01. sa — уровень, mm_sa — м/м SA, saar — SAAR, saar3 — среднее доступных SAAR (на третьем месяце — двух). " +
				"Уровни food/nonfood/all/core нормированы каждый на свой первый месяц и несопоставимы по абсолюту; сравнивать темпы. " +
				"spec_version=" + ppiSASpecVersion + ", taxonomy=" + ppiSATaxonomyVersion + "; паритет с ЦБ не заявлен.",
		},
		{
			Source:    ppiSATable,
			Series:    "nonfood",
			Title:     "ИЦП SA: непродовольственные товары (КИПЦ, без топлива и табака)",
			Unit:      "индекс (100 = 2021-01); mm_sa/saar/saar3 — %",
			Frequency: "M",
			Origin:    consumerOrigin,
			Description: "Сезонно скорректированный KIPC-агрегат ИЦП непродовольственных товаров (04.3.1.1, 03.1, 03.2, 05.1–05.6, 06.1, 07.1, 07.2.1, 08.1, 09.1, 09.2, 09.3, 09.7, 13.1.2, 13.2), без топлива и табака; " +
				"веса 2026, база 100 = 2021-01. sa — уровень, mm_sa — м/м SA, saar — SAAR, saar3 — среднее доступных SAAR (на третьем месяце — двух). " +
				"Уровни рядов несопоставимы по абсолюту; сравнивать темпы. " +
				"spec_version=" + ppiSASpecVersion + ", taxonomy=" + ppiSATaxonomyVersion + "; паритет с ЦБ не заявлен.",
		},
		{
			Source:    ppiSATable,
			Series:    "all",
			Title:     "ИЦП SA: все потребительские товары (КИПЦ, широкий набор)",
			Unit:      "индекс (100 = 2021-01); mm_sa/saar/saar3 — %",
			Frequency: "M",
			Origin:    consumerOrigin,
			Description: "Сезонно скорректированный широкий KIPC-агрегат: food + alcohol + tobacco + nonfood + fuel; веса 2026, база 100 = 2021-01. " +
				"sa — уровень, mm_sa — м/м SA, saar — SAAR, saar3 — среднее доступных SAAR (на третьем месяце — двух). " +
				"Для сопоставления с чёрной линией ЦБ («все товары без плодоовощей, алкоголя, бензина и табака») primary — ряд core, а не all. " +
				"Уровни рядов несопоставимы по абсолюту; сравнивать темпы. " +
				"spec_version=" + ppiSASpecVersion + ", taxonomy=" + ppiSATaxonomyVersion + "; паритет с ЦБ не заявлен.",
		},
		{
			Source:    ppiSATable,
			Series:    "core",
			Title:     "ИЦП SA: ядро потребительских товаров (КИПЦ, food + non-food)",
			Unit:      "индекс (100 = 2021-01); mm_sa/saar/saar3 — %",
			Frequency: "M",
			Origin:    consumerOrigin,
			Description: "Сезонно скорректированный KIPC-агрегат core = food + nonfood: всё без свежих плодоовощей, алкоголя, топлива и табака; " +
				"веса 2026, база 100 = 2021-01. Это primary-ряд для сопоставления с чёрной линией ЦБ; sa — уровень, mm_sa — м/м SA, saar — SAAR, saar3 — среднее доступных SAAR (на третьем месяце — двух). " +
				"Уровни рядов несопоставимы по абсолюту; сравнивать темпы. " +
				"spec_version=" + ppiSASpecVersion + ", taxonomy=" + ppiSATaxonomyVersion + "; паритет с ЦБ не заявлен.",
		},
	}
}
