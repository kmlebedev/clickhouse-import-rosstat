package news

import "strings"

// Реестр тегов LLM-разметки видимого потока новостей (Telegram и RSS, спека §4.3). Реестр — единственный источник
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
// сознательно, вместе с правкой инструкции. Редакция 3 выровняла слаги cb_channel по канону
// cbr/mp_summary_channels.go (KEY_RATE_CHANNELS.md §3.1): реестр и промпт видят только канон,
// а строки редакций ≤2 читаются через newsTagAliases.
const newsPromptVersion = 3

// newsTags — реестр из спеки §4.3: 8 персон, 7 стран, 8 секторов, 12 событий и
// 11 каналов ключевой ставки из docs/KEY_RATE_CHANNELS.md §3 (всего 46). Порядок — порядок
// вывода в промпте; он фиксирован, чтобы текст system-промпта был воспроизводим.
var newsTags = []newsTag{
	// Персоны: тег ставится на действие или заявление, а не на случайное упоминание.
	{Dimension: newsDimPerson, Slug: "trump", Title: "Дональд Трамп"},
	{Dimension: newsDimPerson, Slug: "putin", Title: "Владимир Путин"},
	{Dimension: newsDimPerson, Slug: "xi_jinping", Title: "Си Цзиньпин"},
	{Dimension: newsDimPerson, Slug: "powell", Title: "Джером Пауэлл"},
	{Dimension: newsDimPerson, Slug: "lagarde", Title: "Кристин Лагард"},
	{Dimension: newsDimPerson, Slug: "nabiullina", Title: "Эльвира Набиуллина"},
	{Dimension: newsDimPerson, Slug: "siluanov", Title: "Антон Силуанов"},
	{Dimension: newsDimPerson, Slug: "mishustin", Title: "Михаил Мишустин"},

	// Страны и экономические блоки.
	{Dimension: newsDimCountry, Slug: "china", Title: "Китай"},
	{Dimension: newsDimCountry, Slug: "usa", Title: "США"},
	{Dimension: newsDimCountry, Slug: "eu", Title: "Евросоюз"},
	{Dimension: newsDimCountry, Slug: "russia", Title: "Россия"},
	{Dimension: newsDimCountry, Slug: "india", Title: "Индия"},
	{Dimension: newsDimCountry, Slug: "iran", Title: "Иран"},
	{Dimension: newsDimCountry, Slug: "turkey", Title: "Турция"},

	// Секторы, чувствительные к ставке, сырью и внешним условиям.
	{Dimension: newsDimSector, Slug: "oil_gas", Title: "Нефть и газ"},
	{Dimension: newsDimSector, Slug: "metals", Title: "Металлы"},
	{Dimension: newsDimSector, Slug: "gold", Title: "Золото"},
	{Dimension: newsDimSector, Slug: "banks", Title: "Банки"},
	{Dimension: newsDimSector, Slug: "tech", Title: "Технологии"},
	{Dimension: newsDimSector, Slug: "real_estate", Title: "Недвижимость"},
	{Dimension: newsDimSector, Slug: "agriculture", Title: "Сельское хозяйство"},
	{Dimension: newsDimSector, Slug: "utilities", Title: "Электроэнергетика и тарифы"},

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
	{Dimension: newsDimEvent, Slug: "opec", Title: "ОПЕК+"},
	{Dimension: newsDimEvent, Slug: "ofz_auctions", Title: "Аукционы ОФЗ и госдолг"},

	// Каналы решения ЦБ: определения и направления — KEY_RATE_CHANNELS.md §3.
	{Dimension: newsDimCBChannel, Slug: "current_inflation", Title: "Текущий рост цен", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "core_inflation", Title: "Устойчивая («базовая») инфляция", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "expectations", Title: "Инфляционные и ценовые ожидания", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "labor_market", Title: "Рынок труда", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "lending_transmission", Title: "Кредитование и трансмиссия ДКП", Direction: newsDirectionDez},
	{Dimension: newsDimCBChannel, Slug: "output_gap", Title: "Разрыв выпуска / перегрев спроса", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "budget_impulse", Title: "Бюджетный импульс", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "money_supply", Title: "Денежная масса", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "external_fx", Title: "Внешние условия и курс", Direction: newsDirectionPro},
	{Dimension: newsDimCBChannel, Slug: "savings_behavior", Title: "Сберегательное поведение", Direction: newsDirectionDez},
	{Dimension: newsDimCBChannel, Slug: "autonomous_factors", Title: "Автономные (немонетарные) факторы", Direction: newsDirectionDez},
}

// newsTagByKey ищет тег по паре (dimension, slug). Вторая величина — признак наличия:
// validateNewsAnnotations по ней отбрасывает теги вне реестра. Линейный поиск по 46 записям
// дешевле карты и не требует init().
func newsTagByKey(dimension, slug string) (newsTag, bool) {
	for _, tag := range newsTags {
		if tag.Dimension == dimension && tag.Slug == slug {
			return tag, true
		}
	}
	return newsTag{}, false
}

// newsTagAlias — legacy-слаг канала в уже размеченных строках news_annotation. До унификации
// (#329) реестр тегов хранил credit_transmission, external_conditions, savings и
// macroprudential, тогда как каноном были слаги cbr/mp_summary_channels.go. Алиасы не входят
// в newsTags: промпт и валидация видят только канон, а витрины нормализуют историю на чтении.
type newsTagAlias struct {
	Dimension string
	Legacy    string
	Canonical string
}

// newsTagAliases — единственный источник маппинга legacy-слагов для v_news_events и
// v_news_flash. Строки news_annotation не переписываются: prompt_version входит в ключ, и
// старая разметка остаётся рядом с новой редакцией. Dimension проверяется в SQL, потому что
// слаги повторяются между измерениями (labor_market, budget_impulse).
var newsTagAliases = []newsTagAlias{
	{Dimension: newsDimCBChannel, Legacy: "credit_transmission", Canonical: "lending_transmission"},
	{Dimension: newsDimCBChannel, Legacy: "external_conditions", Canonical: "external_fx"},
	{Dimension: newsDimCBChannel, Legacy: "savings", Canonical: "savings_behavior"},
	{Dimension: newsDimCBChannel, Legacy: "macroprudential", Canonical: "autonomous_factors"},
}

// newsTagCanonicalSQL собирает SQL-выражение, заменяющее legacy-слаг канала на канон в
// витринах. expr — ссылка на колонку тега (в витринах это t.tag); измерение сверяется с
// t.dimension, чтобы алиас не сработал в другом измерении. Без алиасов выражение — сам expr.
func newsTagCanonicalSQL(expr string) string {
	if len(newsTagAliases) == 0 {
		return expr
	}
	var b strings.Builder
	b.WriteString("multiIf(")
	for _, alias := range newsTagAliases {
		b.WriteString("t.dimension = " + newsSQLString(alias.Dimension) + " AND " + expr + " = " + newsSQLString(alias.Legacy) + ", " + newsSQLString(alias.Canonical) + ", ")
	}
	b.WriteString(expr + ")")
	return b.String()
}
