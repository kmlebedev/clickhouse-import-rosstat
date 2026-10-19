package cbr

import (
	"fmt"
	"slices"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Отраслевые листы публичной книги мониторинга предприятий ЦБ (mp_survey_data.xlsx, id 135603).
// В книге 31 лист; 16 из них — отраслевые, по одинаковой геометрии: строка 3 — маркеры блоков
// «Исходные данные» / «Сезонно скорректированные данные», строка 4 — заголовки, данные с 5-й.
// Имена листов — ключ разбора, поэтому они записаны дословно; Slug — стабильная часть имени ряда
// (industry_<slug>_<indicator>), независимая от русской подписи листа.

// industrySheet — отраслевой лист книги и его slug в именах рядов.
type industrySheet struct {
	Sheet string
	Slug  string
	Title string
}

var mpSurveyIndustrySheets = []industrySheet{
	{Sheet: "Промышленность", Slug: "total", Title: "Промышленность (всего)"},
	{Sheet: "Добыча", Slug: "mining", Title: "Добыча полезных ископаемых"},
	{Sheet: "Обработка", Slug: "manufacturing", Title: "Обрабатывающая промышленность"},
	{Sheet: "Обработка_потребительские", Slug: "manufacturing_consumer", Title: "Обработка — потребительские товары"},
	{Sheet: "Обработка_инвестиционные", Slug: "manufacturing_investment", Title: "Обработка — инвестиционные товары"},
	{Sheet: "Обработка_промежуточные", Slug: "manufacturing_intermediate", Title: "Обработка — промежуточные товары"},
	{Sheet: "ЭЭГП", Slug: "electricity", Title: "Электроэнергетика, газ, пар и кондиционирование (ЭЭГП)"},
	{Sheet: "Водоснабжение", Slug: "water", Title: "Водоснабжение и водоотведение"},
	{Sheet: "Сельское хозяйство", Slug: "agriculture", Title: "Сельское хозяйство"},
	{Sheet: "Строительство", Slug: "construction", Title: "Строительство"},
	{Sheet: "Торговля (45+46+47)", Slug: "trade", Title: "Торговля (авто + оптовая + розничная)"},
	{Sheet: "Торговля авто (45)", Slug: "trade_auto", Title: "Торговля автотранспортными средствами"},
	{Sheet: "Торговля оптовая (46)", Slug: "trade_wholesale", Title: "Торговля оптовая"},
	{Sheet: "Торговля розничная (47)", Slug: "trade_retail", Title: "Торговля розничная"},
	{Sheet: "Транспортировка и хранение", Slug: "transport", Title: "Транспортировка и хранение"},
	{Sheet: "Услуги", Slug: "services", Title: "Услуги"},
}

// industryIndicator — показатель, одинаковый на всех 16 отраслевых листах: slug в имени ряда,
// подстрока заголовка колонки (Header), исключения для соседних колонок с тем же заголовком
// (Exclude), наличие SA-пары, единица и текст каталога.
type industryIndicator struct {
	Slug        string
	Header      string
	Exclude     []string
	SA          bool
	Unit        string
	Title       string
	Description string
}

// mpSurveyIndustryIndicators — 12 показателей листа: raw-колонки C…N; SA-пара есть у всех,
// кроме условий кредитования (M), — в блоке P…Z её нет. Порядок задаёт порядок рядов и точек:
// лист → показатель → raw, затем SA.
var mpSurveyIndustryIndicators = []industryIndicator{
	{
		Slug:   "ibk",
		Header: "Индикатор бизнес-климата Банка России",
		// D/E (факт/ожид) содержат тот же заголовок; без исключений findColumn увидел бы
		// три совпадения и упал бы на неоднозначности.
		Exclude:     []string{"(факт)", "(ожид)"},
		SA:          true,
		Unit:        "пункты",
		Title:       "ИБК Банка России",
		Description: "Индикатор бизнес-климата = среднее текущих оценок и ожиданий предприятий (баланс ответов, п.); рост = улучшение делового климата. Прокси разрыва выпуска: числового ряда разрыва ЦБ не публикует.",
	},
	{
		Slug:        "ibk_fact",
		Header:      "Индикатор бизнес-климата Банка России (факт)",
		SA:          true,
		Unit:        "пункты",
		Title:       "ИБК Банка России (факт)",
		Description: "Компонент ИБК — текущие оценки предприятий (баланс ответов, п.).",
	},
	{
		Slug:        "ibk_expected",
		Header:      "Индикатор бизнес-климата Банка России (ожид)",
		SA:          true,
		Unit:        "пункты",
		Title:       "ИБК Банка России (ожидания)",
		Description: "Компонент ИБК — ожидания предприятий (баланс ответов, п.).",
	},
	{
		Slug:        "output_change",
		Header:      "Как изменился объем производства",
		SA:          true,
		Unit:        "пункты",
		Title:       "Изменение объёма производства",
		Description: "Баланс ответов об изменении объёма производства, подрядных работ, товарооборота, услуг (п.); положительный = преобладает рост.",
	},
	{
		Slug:        "output_expected",
		Header:      "Как изменится в ближайшие три месяца объем производства",
		SA:          true,
		Unit:        "пункты",
		Title:       "Ожидаемое изменение объёма производства",
		Description: "Баланс ответов об ожидаемом изменении объёма производства на следующие 3 месяца (п.).",
	},
	{
		Slug:        "demand_change",
		Header:      "Как изменился спрос",
		SA:          true,
		Unit:        "пункты",
		Title:       "Изменение спроса",
		Description: "Баланс ответов об изменении спроса на продукцию, товары, услуги (п.); положительный = преобладает рост.",
	},
	{
		Slug:        "demand_expected",
		Header:      "Как изменится в ближайшие три месяца спрос",
		SA:          true,
		Unit:        "пункты",
		Title:       "Ожидаемое изменение спроса",
		Description: "Баланс ответов об ожидаемом изменении спроса на следующие 3 месяца (п.).",
	},
	{
		Slug:        "price_expectations",
		Header:      "Ценовые ожидания предприятий на следующие 3 месяца",
		SA:          true,
		Unit:        "пункты",
		Title:       "Ценовые ожидания предприятий на 3 месяца",
		Description: "Баланс ответов об ожидаемом изменении отпускных цен на следующие 3 месяца (п.); положительный = преобладает ожидание роста. Главный канал решения ЦБ.",
	},
	{
		Slug:        "price_change",
		Header:      "Как изменились цены на готовую продукцию",
		SA:          true,
		Unit:        "пункты",
		Title:       "Изменение цен на готовую продукцию",
		Description: "Баланс ответов об изменении цен на готовую продукцию, работы/услуги, отпускных цен, тарифов (п.); положительный = преобладает рост цен.",
	},
	{
		Slug:        "cost_change",
		Header:      "Как изменились издержки производства",
		SA:          true,
		Unit:        "пункты",
		Title:       "Изменение издержек производства",
		Description: "Баланс ответов об изменении издержек производства, обращения (п.); положительный = преобладает рост издержек.",
	},
	{
		Slug:        "credit_conditions",
		Header:      "Как изменились условия кредитования",
		SA:          false,
		Unit:        "пункты",
		Title:       "Изменение условий кредитования",
		Description: "Индекс изменения условий кредитования (баланс ответов, п.): отрицательные значения = преобладает ужесточение, рост = смягчение. Сезонной корректировки в источнике нет; SA-варианта нет.",
	},
	{
		Slug:        "price_growth_expected",
		Header:      "Средний ожидаемый",
		SA:          true,
		Unit:        "% годовых",
		Title:       "Ожидаемый прирост отпускных цен (годовое выражение)",
		Description: "Средний ожидаемый предприятиями темп прироста цен на следующие 3 месяца в годовом выражении — аннуализированный средневзвешенный темп, расчёт ЦБ. Вопрос анкеты с 2020 г.; ряд короче остальных (81 точка).",
	},
}

// buildIndustrySeries собирает определения разбора и записи каталога в одном порядке:
// лист → показатель → raw, затем SA (если есть). Двусторонняя сверка имён — в
// TestSeriesMetaCompleteness.
func buildIndustrySeries(sheets []industrySheet, indicators []industryIndicator) ([]seriesDef, []util.SeriesMeta) {
	defs := make([]seriesDef, 0, len(sheets)*23)
	meta := make([]util.SeriesMeta, 0, len(sheets)*23)
	for _, sheet := range sheets {
		for _, ind := range indicators {
			name := "industry_" + sheet.Slug + "_" + ind.Slug
			defs = append(defs, seriesDef{
				Name:    name,
				Sheet:   sheet.Sheet,
				Header:  ind.Header,
				Exclude: ind.Exclude,
			})
			meta = append(meta, industrySeriesMeta(sheet, ind, name, false))
			if !ind.SA {
				continue
			}
			saName := name + "_sa"
			defs = append(defs, seriesDef{
				Name:    saName,
				Sheet:   sheet.Sheet,
				Header:  ind.Header,
				SA:      true,
				Exclude: ind.Exclude,
			})
			meta = append(meta, industrySeriesMeta(sheet, ind, saName, true))
		}
	}
	return defs, meta
}

// industrySeriesMeta — запись каталога для одного отраслевого ряда. Origin называет лист и
// подстроку заголовка (полный заголовок у N различается формулировкой отрасли, поэтому в
// каталоге стоит общая подстрока), Description — смысл показателя, отраслевой разрез и
// конвенцию даты/публикации.
func industrySeriesMeta(sheet industrySheet, ind industryIndicator, name string, sa bool) util.SeriesMeta {
	block := "исходные данные"
	suffix := ""
	if sa {
		block = "сезонно скорректированные данные"
		suffix = ", с.к."
	}
	return util.SeriesMeta{
		Source:      mpSurveySource,
		Series:      name,
		Title:       fmt.Sprintf("%s — %s%s, %s", ind.Title, sheet.Title, suffix, ind.Unit),
		Unit:        ind.Unit,
		Frequency:   "M",
		Origin:      fmt.Sprintf("ЦБ, мониторинг предприятий (mp_survey_data.xlsx, id 135603), лист «%s», колонка «%s…» (%s)", sheet.Sheet, ind.Header, block),
		Description: fmt.Sprintf("%s Отраслевой лист «%s». %s", ind.Description, sheet.Title, surveyDateNote(sa)),
	}
}

// surveyDateNote — общая для всех рядов мониторинга конвенция даты и публикации; у SA-рядов
// добавлена оговорка, что ЦБ цитирует именно сглаженные значения.
func surveyDateNote(sa bool) string {
	note := "Дата — отчётный период: 1-е число месяца. ЦБ публикует волну анкет в первую декаду следующего месяца (~10–15-го) и нумерует справки по месяцу волны: справка «сентябрь 2026» цитирует точку отчётного периода август 2026."
	if sa {
		note += " Ряд сезонно сглаженный — именно такие значения ЦБ цитирует в комментариях."
	}
	return note
}

// mpSurveyIndustrySeries и mpSurveyIndustrySeriesMeta — 368 рядов (16 листов × 23) и их
// описания; генерируются один раз при инициализации пакета.
var mpSurveyIndustrySeries, mpSurveyIndustrySeriesMeta = buildIndustrySeries(mpSurveyIndustrySheets, mpSurveyIndustryIndicators)

// mpSurveyAllSeries / mpSurveyAllMeta — полный набор импортёра: 13 агрегатных рядов из
// series_meta.go плюс 368 отраслевых. slices.Concat возвращает новый срез, поэтому общий
// набор не делит массив с агрегатным (append в вызывающем коде не испортит исходные списки).
var mpSurveyAllSeries = slices.Concat(mpSurveySeries, mpSurveyIndustrySeries)
var mpSurveyAllMeta = slices.Concat(mpSurveySeriesMeta, mpSurveyIndustrySeriesMeta)

// isMonthlySurveySheet — лист с датами в первом столбце (серийные номера Excel), а не с
// квартальными метками «1к02». Месячные — «Экономика всего» и все 16 отраслевых; остальные
// читаемые листы книги квартальные.
func isMonthlySurveySheet(sheet string) bool {
	if sheet == monthlySheet {
		return true
	}
	for _, s := range mpSurveyIndustrySheets {
		if s.Sheet == sheet {
			return true
		}
	}
	return false
}
