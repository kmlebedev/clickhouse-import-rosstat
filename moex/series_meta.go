package moex

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// moexSeriesMeta описывает ряды импортёров пакета moex (stock_prices, ofz_curve)
var moexSeriesMeta = []util.SeriesMeta{
	{
		Source:    moexSource,
		Series:    plzlCode,
		Title:     "Полюс (PLZL): цена закрытия акции на МосБирже, дневная",
		Unit:      "руб. за акцию",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/markets/shares/securities/PLZL/candles.json (interval=24), поле close",
		Description: "Рыночная цена акции Полюса. Темп — отношение close к предыдущему торговому дню; " +
			"строк нет в нерабочие дни. Для NAV-моста: upside/downside = NAV/акция ÷ close − 1. " +
			"Остальные поля свечи (open/max/min/volume) — в таблице stock_prices и витрине v_stock_prices",
	},
	{
		Source:    moexSource,
		Series:    rgbiTenor,
		Title:     "RGBI: индекс рынка облигаций федерального займа, дневной",
		Unit:      "пункты индекса",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/markets/index/securities/RGBI/candles.json (interval=24), поле close",
		Description: "Уровень индекса RGBI (НЕ доходность): рост индекса = рост цен ОФЗ = снижение доходностей. " +
			"Доходности по тенорам — ряды 1y/3y/5y/10y. История — с 2010-01-01",
	},
	{
		Source:    moexSource,
		Series:    "1y",
		Title:     "Доходность ОФЗ 1 года (G-curve МосБиржи), дневная",
		Unit:      "% годовых",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/zcyc.json, блок yearyields, period=1",
		Description: "Годовая доходность параметризованной кривой МосБиржи на срок 1 год. Значение — уровень в % " +
			"годовых, не темп. ⚠️ Эндпоинт отдаёт только снимок текущего дня: история накапливается с даты первого " +
			"запуска импортёра ofz_curve",
	},
	{
		Source:    moexSource,
		Series:    "3y",
		Title:     "Доходность ОФЗ 3 лет (G-curve МосБиржи), дневная",
		Unit:      "% годовых",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/zcyc.json, блок yearyields, period=3",
		Description: "Годовая доходность G-curve на срок 3 года, % годовых. ⚠️ Снимок текущего дня: история " +
			"накапливается с даты первого запуска импортёра ofz_curve",
	},
	{
		Source:    moexSource,
		Series:    "5y",
		Title:     "Доходность ОФЗ 5 лет (G-curve МосБиржи), дневная",
		Unit:      "% годовых",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/zcyc.json, блок yearyields, period=5",
		Description: "Годовая доходность G-curve на срок 5 лет, % годовых. Бенчмарк для локального контура ставки " +
			"DCF и метрики дивидендного спреда (дивдоходность − доходность 5y). ⚠️ Снимок текущего дня: история " +
			"накапливается с даты первого запуска импортёра ofz_curve",
	},
	{
		Source:    moexSource,
		Series:    "10y",
		Title:     "Доходность ОФЗ 10 лет (G-curve МосБиржи), дневная",
		Unit:      "% годовых",
		Frequency: "D",
		Origin:    "MOEX ISS: /iss/engines/stock/zcyc.json, блок yearyields, period=10",
		Description: "Годовая доходность G-curve на срок 10 лет, % годовых. ⚠️ Снимок текущего дня: история " +
			"накапливается с даты первого запуска импортёра ofz_curve",
	},
	{
		Source:    moexSource,
		Series:    brentSeries,
		Title:     "Brent: непрерывный фронт-месяц фьючерса MOEX BR, дневной",
		Unit:      "USD за баррель",
		Frequency: "D",
		Origin: "MOEX ISS: /iss/history/engines/futures/markets/forts/securities/<secid>.json, " +
			"ASSETCODE=BR, поле SETTLEPRICE; контракты обнаруживаются запросом " +
			"/iss/history/engines/futures/markets/forts/securities.json?date=...&assetcode=BR",
		Description: "Расчётная цена дня (SETTLEPRICE) ближайшего по месяцу поставки контракта BR; " +
			"если SETTLEPRICE нет — CLOSE. История с 2009-10-01 (первый фронт-месяц; контракт BR-10.09). Это фьючерсный фронт-месяц, " +
			"а не спот и не back-adjusted continuous ряд: на роллах возможен разрыв, поэтому для " +
			"сопоставления с вендорским рядом CB_COMP нужна оговорка. Ряд добавлен для макро-контролей " +
			"CB_COMP и контура ключевой ставки; RVI и медь — phase-2-moex-market. Витрина — v_moex_market",
	},
}
