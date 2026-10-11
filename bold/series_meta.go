package bold

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// boldETFScope — общий хвост описаний семи рядов ETF-контура. Он повторяется в каждой записи
// намеренно: агент читает каталог построчно (v_series_catalog по (source, series)), а не
// целиком, поэтому оговорки обязаны стоять в той же строке, что и значение. Без них агрегатор
// складывают с мировыми рядами WGC (там все фонды мира и месячная частота), окна читают как
// сумму торговых дней, а «неполный день» принимают за обвал потока.
const boldETFScope = "Источник — bold.report (ByteTree), ключей нет, режим щадящий (4 запроса в сутки, T+1). " +
	"Агрегат 73 фонда (в списке GLD, IAU, IGLN, GLDM, SGLD, 4GLD, PHYS, 518880, GOLD, 1540, ZGLD, SGOL) — " +
	"≈83 % мировой величины WGC, поэтому не складывать и не сравнивать напрямую с etf_holdings_tonnes_world/etf_flow_tonnes_world " +
	"(там все фонды мира и месячная частота). Окна — календарные (7/30/90 дней): сумма 5 торговых дней ≠ culm7day."

// boldETFOrigin — происхождение рядов контура: три ответа «последнего дня» плюс (в полном
// режиме) три файла истории. Пути собираются из констант boldPath*, а не пишутся литералом: у
// источника суффикс `.json` обязателен, и дублированная строка рано или поздно разошлась бы с
// боевыми путями — агент прочитал бы в каталоге путь, на который CDN отвечает 403 (fix round 2,
// Ruling 16). Срез фондов и цена живут своими рядами/таблицами и здесь названы, чтобы читатель
// каталога не искал их в macro_series.
const boldETFOrigin = "bold.report/api/v1/: " + boldPathUnits + ", " + boldPathAum + ", " + boldPathFlows + " " +
	"(ответы *-latest на инкременте, файлы истории — в полном режиме); " +
	"пер-фондовый срез — " + boldPathSummary + " (таблица bold_fund_snapshot), " +
	"цена дня — " + boldPathPrice + " (ряд gold/bold в gold_prices)"

// boldETFSeriesMeta — каталог семи рядов ETF-контура (спека §4.1/§4.4). Единицы — целевые:
// источник публикует унции и USD, в ряды они идут в тоннах, млрд USD и млн USD.
func boldETFSeriesMeta() []util.SeriesMeta {
	return []util.SeriesMeta{
		{
			Source:    boldSource,
			Series:    boldSeriesFlowTonnes,
			Title:     "Дневной поток золота в ETF-контуре bold.report (Δ запасов дня)",
			Unit:      "тонны",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "Разница запасов (units) с предыдущим торговым днём, переведённая в тонны: (units_t − units_{t−1}) / 32150,7465686. " +
				"Знак: рост запасов — приток (плюс), та же конвенция, что у WGC-рядов проекта. База Δ на инкременте — сток предыдущего дня, " +
				"прочитанный из ClickHouse (не из ответа источника). У первой точки ряда потока нет. " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesFlowUsdMln,
			Title:     "Дневной поток золота в ETF-контуре bold.report в USD",
			Unit:      "млн USD",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "Дневной поток в тоннах, умноженный на цену дня источника: (units_t − units_{t−1}) × price_t / 1e6. " +
				"Это не стоимость сделок и не объём торгов: поток — изменение запасов металла, оцененное ценой дня. " +
				"Нет цены дня — нет и точки USD-потока (тоннажный поток при этом пишется). " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesFlow7dTonnes,
			Title:     "Поток золота в ETF-контуре bold.report за 7 календарных дней",
			Unit:      "тонны",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "culm7day / 32150,7465686 — окно источника как опубликовано, без пересчёта из нашего дневного ряда. " +
				"Окна пересекаются (7/30/90 дней), поэтому значения соседних дней нельзя суммировать: это скользящие окна, а не порции. " +
				"Нули и пропуски: строк до 2015-04-01 у источника нет вовсе. " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesFlow30dTonnes,
			Title:     "Поток золота в ETF-контуре bold.report за 30 календарных дней",
			Unit:      "тонны",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "culm30day / 32150,7465686 — окно источника как опубликовано. Сумма пяти торговых дней ему не равна: окно календарное, " +
				"и расхождение с пересчётом из дневного ряда достигает процентов (спека §9, проверено на живых данных). " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesFlow90dTonnes,
			Title:     "Поток золота в ETF-контуре bold.report за 90 календарных дней",
			Unit:      "тонны",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "culm90day / 32150,7465686 — окно источника как опубликовано. Самое длинное окно контура: по нему видно квартальный тренд, " +
				"но сравнивать его с суммой дневного ряда нельзя (окно календарное, ряд — по торговым дням). " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesHoldingsTonnes,
			Title:     "Запасы золота в ETF-контуре bold.report (сток)",
			Unit:      "тонны",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "units / 32150,7465686 — сток металла в 73 фондах на конец дня. Темп — отношение к предыдущей строке; " +
				"ряд публикуется T+1 (данные за прошедший торговый день появляются на следующий). Первые 64 дня истории (2015-01-01…2015-03-31) " +
				"units не несут — точек за них нет, ноль вместо отсутствия данных не пишется. " + boldETFScope,
		},
		{
			Source:    boldSource,
			Series:    boldSeriesAumUsdBn,
			Title:     "Стоимость активов ETF-контура bold.report (AUM, сток)",
			Unit:      "млрд USD",
			Frequency: "D",
			Origin:    boldETFOrigin,
			Description: "aum / 1e9 — совокупная стоимость активов 73 фондов. Публикуется источником в USD; 476,678 млрд $ на 2026-10-09 " +
				"против 574,23 млрд $ мировой величины WGC (30.09.2026) — те самые ≈83 %. AUM считается источником по своей цене фондов, " +
				"поэтому с нашим рядом цены gold/bold может расходиться. " + boldETFScope,
		},
	}
}
