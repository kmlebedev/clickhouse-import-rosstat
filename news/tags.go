package news

// Реестр тегов LLM-разметки Telegram-молнии (спека §4.4). Реестр — единственный источник
// правды о допустимых тегах: промпт перечисляет его модели, а validateNewsAnnotations
// отбрасывает всё, чего в нём нет. Поэтому новый тег появляется здесь, а не в промпте.
//
// Direction заполняется только у cb_channel: у канала трансмиссии ДКП направление — свойство
// самого канала (KEY_RATE_CHANNELS.md §3, статический знак), а у person/country/sector/event
// знак задаёт конкретная новость, и в реестре его нет. Слаги могут повторяться между
// измерениями (labor_market и budget_impulse есть и в event, и в cb_channel) — уникальна
// пара (dimension, slug).
type newsTag struct {
	Dimension string
	Slug      string
	Title     string
	Direction string
}

// Измерения реестра. theme в стартовом наборе не заполнен: сюжеты энергоперехода и
// деглобализации пока живут в event, отдельные theme-теги появятся при необходимости.
const (
	newsDimPerson    = "person"
	newsDimCountry   = "country"
	newsDimSector    = "sector"
	newsDimEvent     = "event"
	newsDimTheme     = "theme"
	newsDimCBChannel = "cb_channel"
)

// Статический знак влияния на инфляцию. У новостных тегов допустимы neutral/mixed,
// направленные proinflationary/disinflationary принудительно приводятся к neutral.
const (
	newsDirectionPro     = "proinflationary"
	newsDirectionDez     = "disinflationary"
	newsDirectionNeutral = "neutral"
	newsDirectionMixed   = "mixed"
)

// newsPromptVersion — редакция промпта разметки. Входит в ключ news_annotation
// (prompt_version), поэтому новая редакция добавляет строки рядом со старой, а не затирает
// её. Значение дублируется меткой в system-промпте и закреплено тестом: менять только
// сознательно, вместе с правкой инструкции.
const newsPromptVersion = 1

// newsTags — стартовый набор реестра из спеки §4.4: персоны, страны, секторы, события и
// 11 каналов ключевой ставки из docs/KEY_RATE_CHANNELS.md §3. Порядок — порядок вывода в
// промпте; он фиксирован, чтобы текст system-промпта был воспроизводим.
var newsTags = []newsTag{
	// Персоны: тег ставится на действие или заявление, а не на случайное упоминание.
	{Dimension: newsDimPerson, Slug: "trump", Title: "Дональд Трамп"},
	{Dimension: newsDimPerson, Slug: "putin", Title: "Владимир Путин"},
	{Dimension: newsDimPerson, Slug: "xi_jinping", Title: "Си Цзиньпин"},
	{Dimension: newsDimPerson, Slug: "powell", Title: "Джером Пауэлл"},
	{Dimension: newsDimPerson, Slug: "lagarde", Title: "Кристин Лагард"},
	{Dimension: newsDimPerson, Slug: "nabiullina", Title: "Эльвира Набиуллина"},

	// Страны и экономические блоки.
	{Dimension: newsDimCountry, Slug: "china", Title: "Китай"},
	{Dimension: newsDimCountry, Slug: "usa", Title: "США"},
	{Dimension: newsDimCountry, Slug: "eu", Title: "Евросоюз"},
	{Dimension: newsDimCountry, Slug: "russia", Title: "Россия"},
	{Dimension: newsDimCountry, Slug: "india", Title: "Индия"},

	// Секторы, чувствительные к ставке, сырью и внешним условиям.
	{Dimension: newsDimSector, Slug: "oil_gas", Title: "Нефть и газ"},
	{Dimension: newsDimSector, Slug: "metals", Title: "Металлы"},
	{Dimension: newsDimSector, Slug: "gold", Title: "Золото"},
	{Dimension: newsDimSector, Slug: "banks", Title: "Банки"},
	{Dimension: newsDimSector, Slug: "tech", Title: "Технологии"},
	{Dimension: newsDimSector, Slug: "real_estate", Title: "Недвижимость"},
	{Dimension: newsDimSector, Slug: "agriculture", Title: "Сельское хозяйство"},

	// Событийные сюжеты, которые двигают инфляцию и оценки DCF.
	{Dimension: newsDimEvent, Slug: "energy_transition", Title: "Энергопереход"},
	{Dimension: newsDimEvent, Slug: "deglobalization", Title: "Деглобализация"},
	{Dimension: newsDimEvent, Slug: "sanctions", Title: "Санкции"},
	{Dimension: newsDimEvent, Slug: "tariffs", Title: "Пошлины и тарифы"},
	{Dimension: newsDimEvent, Slug: "energy_prices", Title: "Цены на энергоносители"},
	{Dimension: newsDimEvent, Slug: "budget_impulse", Title: "Бюджетный импульс"},
	{Dimension: newsDimEvent, Slug: "labor_market", Title: "Рынок труда"},
	{Dimension: newsDimEvent, Slug: "inflation_expectations", Title: "Инфляционные ожидания"},
	{Dimension: newsDimEvent, Slug: "credit", Title: "Кредитование"},
	{Dimension: newsDimEvent, Slug: "key_rate", Title: "Ключевая ставка"},

	// Каналы решения ЦБ: определения и направления — KEY_RATE_CHANNELS.md §3.
	{Dimension: newsDimCBChannel, Slug: "current_inflation", Title: "Текущий рост цен", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "core_inflation", Title: "Устойчивая («базовая») инфляция", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "expectations", Title: "Инфляционные и ценовые ожидания", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "labor_market", Title: "Рынок труда", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "credit_transmission", Title: "Кредитование и трансмиссия ДКП", Direction: newsDirectionDez},
	{Dimension: newsDimCBChannel, Slug: "output_gap", Title: "Разрыв выпуска / перегрев спроса", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "budget_impulse", Title: "Бюджетный импульс", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "money_supply", Title: "Денежная масса", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "external_conditions", Title: "Внешние условия и курс", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "savings", Title: "Сберегательное поведение", Direction: newsDirectionDez},
	{Dimension: newsDimCBChannel, Slug: "macroprudential", Title: "Макропруденциальные требования", Direction: newsDirectionDez},
}

// newsTagByKey ищет тег по паре (dimension, slug). Вторая величина — признак наличия:
// validateNewsAnnotations по ней отбрасывает теги вне реестра. Линейный поиск по 39 записям
// дешевле карты и не требует init().
func newsTagByKey(dimension, slug string) (newsTag, bool) {
	for _, tag := range newsTags {
		if tag.Dimension == dimension && tag.Slug == slug {
			return tag, true
		}
	}
	return newsTag{}, false
}
