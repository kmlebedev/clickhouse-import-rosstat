package news

import (
	"fmt"
	"strings"
)

// newsEventsDdl — копия канонического DDL news_events из ARCHITECTURE.md §6.2; копии обязаны
// совпадать (то же правило, что у macro_series и gold_prices/gold_forecasts). Ключ
// (date, source, title) взят из канона: повторные прогоны ленты пишут одни и те же позиции,
// и ReplacingMergeTree схлопывает их, а не плодит дубли. DDL выполняется до батчей —
// импортёр обязан отработать и на чистой базе, где таблицы ещё нет.
const newsEventsDdl = `CREATE TABLE IF NOT EXISTS news_events (
			  date DateTime
			, source LowCardinality(String)
			, title String
			, url String
			, description String               -- текст позиции из ленты: у smart-lab — полный текст поста
			                                   -- (HTML после разбора XML-сущностей); у новостных лент — краткое
			                                   -- описание позиции; '' — описания нет
			, content_hash String              -- sha256 текущей версии title+description; у Telegram title в
			                                   -- news_events может быть от v1, а hash — от текущей версии;
			                                   -- ключ LLM-разметки (generic по url+content_hash)
			, sentiment Nullable(Float32)      -- -1..1, заполняет LLM-сессия
		) ENGINE = ReplacingMergeTree ORDER BY (date, source, title);
	`

// newsAlterStatements — миграция живой таблицы до колонок description и content_hash: ADD COLUMN
// IF NOT EXISTS идемпотентен, поэтому выполняется при каждом прогоне, а не «один раз». AFTER url и
// AFTER description — позиции колонок как в DDL: иначе они уехали бы в конец таблицы, и копия
// разошлась бы с каноном. Порядок стейтментов важен: content_hash встаёт после description,
// поэтому сначала должна появиться сама description. Строки, записанные до миграции, получают
// пустые значения: позиции, выпавшие из окна ленты, задним числом не перечитываются (дефолт
// ADD COLUMN).
var newsAlterStatements = []string{
	`ALTER TABLE ` + newsTable + ` ADD COLUMN IF NOT EXISTS description String AFTER url`,
	`ALTER TABLE ` + newsTable + ` ADD COLUMN IF NOT EXISTS content_hash String AFTER description`,
}

// newsMessageVersionDdl — копия канонического DDL news_message_version из ARCHITECTURE.md §6.2
// (спека §4.2, Task 2). Таблица — винтажный слой Telegram: каждая правка или удаление сообщения
// оставляет строку, а ключ ReplacingMergeTree (channel, message_id, version) делает повторную
// запись той же версии no-op. date — момент публикации и не меняется; first_seen — время прогона,
// впервые увидевшего версию; у tombstone description/content_hash пустые, deleted = 1.
const newsMessageVersionDdl = `CREATE TABLE IF NOT EXISTS news_message_version (
			  channel LowCardinality(String)   -- interfaxonline | ifax_go | edisclosure | newssmartlab | markettwits | russianmacro | MarketOverview | mozgovikresearch
			, message_id UInt64
			, version UInt32                   -- 1..N; растёт при правке/удалении
			, date DateTime                    -- момент публикации (не меняется)
			, source LowCardinality(String)    -- interfax | edisclosure | smartlab | markettwits | russianmacro | marketoverview | mozgovikresearch
			, title String                     -- первая строка версии
			, url String                       -- https://t.me/<channel>/<id>
			, description String               -- полный текст версии; '' у tombstone
			, content_hash String              -- sha256; '' у tombstone
			, first_seen DateTime              -- когда версия впервые увидена
			, deleted UInt8                    -- 1 — сообщение исчезло из окна канала
		) ENGINE = ReplacingMergeTree ORDER BY (channel, message_id, version);
	`

// newsAnnotationDdl — копия канонического DDL news_annotation из ARCHITECTURE.md §6.2
// (спека §4.2, Task 3). Таблица generic по паре (url, content_hash): news_watch
// заполняет её для всего видимого потока (Telegram и RSS), ключ не привязан к транспорту. kind разделяет
// doc-строку сообщения (sentiment/relevance/summary) и tag-строки (dimension/tag/direction/
// confidence/evidence); model и prompt_version входят в ключ, поэтому смена модели или
// редакции промпта добавляет разметку рядом со старой, а не затирает её.
const newsAnnotationDdl = `CREATE TABLE IF NOT EXISTS news_annotation (
			  url String
			, content_hash String
			, kind LowCardinality(String)       -- 'doc' | 'tag'
			, method LowCardinality(String)     -- 'llm'
			, model LowCardinality(String)      -- фактическое имя модели
			, prompt_version UInt16
			, dimension LowCardinality(String)  -- '' у doc; person|country|sector|event|theme|cb_channel
			, tag LowCardinality(String)        -- '' у doc; slug реестра
			, direction LowCardinality(String)  -- '' у doc; у tag — proinflationary|disinflationary|neutral|mixed
			                                   -- (у не-cb_channel только neutral|mixed)
			, confidence Float32
			, evidence String                   -- дословная цитата; '' у doc
			, sentiment Float32                 -- -1..1, только doc
			, relevance Float32                 -- 0..1, только doc
			, summary String                    -- 1 предложение, только doc
			, annotated_at DateTime
		) ENGINE = ReplacingMergeTree
		ORDER BY (url, content_hash, kind, method, model, prompt_version, dimension, tag);
	`

// newsSchemaStatements — DDL прогона по порядку: CREATE news_events, ALTER-ы миграции (образец —
// gostSchemaStatements, craw/gost.go), затем CREATE news_message_version и CREATE news_annotation.
// Схема выполняется до чтения лент, поэтому таблицы версий и разметки готовы к записи в любом
// режиме. Возвращается срез, а не переменная: вызывающий не должен получить возможность
// переставить стейтменты местами.
func newsSchemaStatements() []string {
	stmts := make([]string, 0, 3+len(newsAlterStatements))
	stmts = append(stmts, newsEventsDdl)
	stmts = append(stmts, newsAlterStatements...)
	stmts = append(stmts, newsMessageVersionDdl)
	return append(stmts, newsAnnotationDdl)
}

// newsFilter — широкий фильтр тезисов (RE2, регистронезависимо): список из спеки §«Схема и
// витрина», дополненный стемами ставки/денежного рынка (вклады, депозиты, наличные, кредит, ОФЗ,
// RGBI, RUONIA, RUSFAR) — посты smart-lab про ставку и доходности должны доезжать до агента.
// Расширение 1.9 добавляет стемы Telegram-молнии и тезисов спеки §4.5: Китай/китайск,
// деглобализац, энергопереход/энергетическ, тариф — вопросы «что Трамп сказал про пошлины»,
// «что происходит с энергопереходом», «какие новости про Китай и деглобализацию влияют на
// сектор и канал ставки» иначе оставили бы молнию за витриной.
// Осознанно широкий: ложный пропуск события тезиса дороже ложного срабатывания — «доставка»
// попадает на стем «ставк», «полюс» ловится и в географическом смысле, «кредит» задевает
// потребительские сюжеты — расширение оставляет фильтр осознанно широким. Лишнее снимается в
// запросе сессии, а сам список уточняется пересозданием витрины по всей истории: сырая
// news_events хранит ленту целиком и агенту не выдаётся, поэтому сужение фильтра ничего не теряет.
const newsFilter = `(?i)дизел|бензин|керосин|дистиллят|нефтепродукт|топлив|НПЗ|переработк|нефт|Brent|ОПЕК|Ормуз|Иран|санкц|поставк|эмбарго|сделк|Трамп|перемири|пошлин|ставк|ФРС|Федрезерв|инфляц|дефляц|CPI|PPI|ИПЦ|золот|драгметалл|XAU|ETF|резерв|Банк России|ЦБ|Минфин|бюджет|НДПИ|налог|отсечк|Полюс|Сухой Лог|PLZL|вклад|депозит|наличн|кредит|ОФЗ|RGBI|RUONIA|RUSFAR|Китай|китайск|деглобализац|энергопереход|энергетическ|тариф`

// newsViewStatements — витрины читательского контура новостей (правило 11 AGENTS.md): определения
// плюс комментарии таблицы и каждой колонки (ALTER TABLE … MODIFY COMMENT / COMMENT COLUMN —
// образец eia/eiaViews). Стейтменты выполняются по порядку; фильтр тезисов живёт именно здесь,
// потому что читает витрину агент (грант kimi_reader), а сырая news_events ему закрыта.
//
// v_news_events — плоская лента с фильтром тезисов, transport (RSS/Telegram) и LLM-разметкой
// видимого потока (sentiment/relevance/summary/model/prompt_version/tags);
// v_news_flash — слой всех Telegram-каналов (молния interfaxonline/ifax_go,
// корпоративные раскрытия edisclosure, newssmartlab и рыночные каналы) с LLM-разметкой;
// v_news_message_versions —
// винтажный слой правок и удалений; v_news_tags — статический реестр слагов. Число стейтментов —
// контракт (VIEW + комментарий таблицы + комментарий каждой колонки): пропущенный комментарий
// оставляет агента без смысла колонки, лишний — ломает публикацию.
var newsViewStatements = []string{
	// v_news_events: VIEW + комментарий таблицы + 14 комментариев колонок.
	// Литеральный % в LIKE записан как %%: строка — формат fmt.Sprintf, и одиночный %
	// перед кавычкой go vet принял бы за глагол форматирования.
	//
	// Разметка присоединяется к сырой ленте по паре (url, content_hash). Doc-подзапрос
	// сворачивается LIMIT 1 BY: у пары может быть несколько doc-строк (модели/редакции
	// промпта), и витрине нужна последняя — по annotated_at, затем model и prompt_version
	// (детерминированный tie-break). Пустой content_hash в JOIN не участвует: такие строки
	// записаны до миграции 1.9 и не аннотируемы. NULL у sentiment/relevance/summary означает
	// «не размечено», а не «нейтрально»; tag-строки собираются groupArrayIf той же модели и
	// редакции, поэтому теги чужой разметки к doc-строке не прилипают.
	fmt.Sprintf(`CREATE OR REPLACE VIEW v_news_events DEFINER = default SQL SECURITY DEFINER AS
		SELECT e.date AS date,
			toDate(e.date, 'Europe/Moscow') AS date_msk,
			e.source AS source,
			multiIf(e.url LIKE 'https://t.me/%%', 'telegram', 'rss') AS transport,
			e.title AS title,
			e.url AS url,
			e.description AS description,
			e.content_hash AS content_hash,
			if(d.model = '', NULL, d.sentiment) AS sentiment,
			if(d.model = '', NULL, d.relevance) AS relevance,
			if(d.model = '', NULL, d.summary) AS summary,
			d.model AS model,
			d.prompt_version AS prompt_version,
			groupArrayIf(tuple(t.dimension, %s, t.direction), t.tag != '') AS tags
		FROM (SELECT * FROM news_events FINAL) AS e
		LEFT JOIN (
			SELECT url, content_hash, sentiment, relevance, summary, model, prompt_version
			FROM news_annotation FINAL
			WHERE kind = 'doc' AND method = 'llm' AND content_hash != ''
			ORDER BY annotated_at DESC, model DESC, prompt_version DESC
			LIMIT 1 BY url, content_hash
		) AS d ON d.url = e.url AND d.content_hash = e.content_hash
		LEFT JOIN (
			SELECT url, content_hash, model, prompt_version, dimension, tag, direction
			FROM news_annotation FINAL
			WHERE kind = 'tag' AND method = 'llm' AND content_hash != ''
		) AS t ON t.url = d.url AND t.content_hash = d.content_hash
			AND t.model = d.model AND t.prompt_version = d.prompt_version
		WHERE match(e.title, '%s') OR match(e.description, '%s')
		GROUP BY e.date, toDate(e.date, 'Europe/Moscow'), e.source,
			multiIf(e.url LIKE 'https://t.me/%%', 'telegram', 'rss'),
			e.title, e.url, e.description, e.content_hash,
			d.sentiment, d.relevance, d.summary, d.model, d.prompt_version
		ORDER BY date DESC`, newsTagCanonicalSQL("t.tag"), newsFilter, newsFilter),
	`ALTER TABLE v_news_events MODIFY COMMENT 'Событийная лента РФ-канала: RSS «Интерфакс», РБК, ЦБ РФ ×2, TASS, общий smart-lab и два блога, а также Telegram-каналы — молния «Интерфакса» (interfaxonline, ifax_go), корпоративные раскрытия «Интерфакс-ЦРКИ» (edisclosure), новостная лента Смартлаба (newssmartlab) и рыночные каналы markettwits, russianmacro, MarketOverview, mozgovikresearch (импортёр news_watch, каждые 30 минут). transport различает RSS и публичное Telegram-превью; показаны только события по стемам тезисов (широкий фильтр — в определении витрины); полная лента лежит в сырой news_events и агенту не выдаётся. История копится с первого прогона (окна лент 20–100 позиций). sentiment/relevance/summary/tags — LLM-разметка видимых позиций (news_annotation, method=''llm''); NULL/пусто — позиция не размечена, а не «нейтрально»; content_hash — ключ разметки, '''' — строка до миграции и не аннотируема. Тональность Telegram также в v_news_flash.sentiment'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date 'Момент публикации в ленте (pubDate источника со своим офсетом времени; у Telegram — time сообщения)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date_msk 'День публикации по МСК (toDate(date, ''Europe/Moscow'')) — для группировок по дням'`,
	`ALTER TABLE v_news_events COMMENT COLUMN source 'Лента-источник: interfax, rbc, cbr, tass, smartlab, edisclosure, markettwits, russianmacro, marketoverview, mozgovikresearch; оба фида ЦБ — cbr, четыре ленты smart-lab (общий RSS, два блога и newssmartlab_tg) — smartlab, три ленты Интерфакса (RSS, interfaxonline, ifax_go) — interfax, корпоративные раскрытия — edisclosure; различаются transport и url; канал Telegram — в v_news_flash (channel)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN transport 'Транспорт позиции: telegram — публичное превью Telegram-канала (interfaxonline, ifax_go, edisclosure, newssmartlab, markettwits, russianmacro, MarketOverview, mozgovikresearch), rss — RSS-ленты'`,
	`ALTER TABLE v_news_events COMMENT COLUMN title 'Заголовок новости из ленты (нормализован в одну строку); у Telegram — первая строка первой версии сообщения'`,
	`ALTER TABLE v_news_events COMMENT COLUMN url 'Ссылка на публикацию'`,
	// Маркер пустой строки '' записан четырьмя кавычками (''''): одиночная кавычка внутри
	// SQL-литерала удваивается, и агент видит в комментарии именно пустую строку ''.
	`ALTER TABLE v_news_events COMMENT COLUMN description 'Текст позиции из ленты: полный текст поста smart-lab (HTML) или текст Telegram-молнии; '''' — описания нет; при цитировании — атрибуция автора и smart-lab'`,
	`ALTER TABLE v_news_events COMMENT COLUMN content_hash 'sha256 текущей версии title+description; ключ LLM-разметки; '''' — строка записана до миграции 1.9 и не аннотируема'`,
	`ALTER TABLE v_news_events COMMENT COLUMN sentiment 'Тональность −1..1 из LLM-разметки (news_annotation, method=''llm''); NULL — позиция ещё не размечена (не «нейтрально»); тональность Telegram также в v_news_flash.sentiment'`,
	`ALTER TABLE v_news_events COMMENT COLUMN relevance 'Важность для контура ставки и DCF 0..1 из LLM-разметки; NULL — позиция ещё не размечена'`,
	`ALTER TABLE v_news_events COMMENT COLUMN summary 'Краткое резюме (одно предложение) из LLM-разметки; NULL — позиция ещё не размечена'`,
	`ALTER TABLE v_news_events COMMENT COLUMN model 'LLM-модель последней doc-строки разметки; '''' — разметки нет'`,
	`ALTER TABLE v_news_events COMMENT COLUMN prompt_version 'Редакция промпта последней doc-строки разметки (newsPromptVersion); 0 — разметки нет'`,
	`ALTER TABLE v_news_events COMMENT COLUMN tags 'Теги LLM последней doc-строки: Array(Tuple(dimension, tag, direction)); dimension — person|country|sector|event|theme|cb_channel, tag — слаг реестра (расшифровка — v_news_tags; у cb_channel всегда канонический слаг KEY_RATE_CHANNELS.md §3.1, legacy-слаги редакций ≤2 нормализуются на чтении), direction — влияние на инфляцию (proinflationary|disinflationary|neutral|mixed); [] — тегов нет или позиция не размечена'`,

	// v_news_flash: VIEW + комментарий таблицы + 17 комментариев колонок.
	// Последняя версия — argMax(field, version) в подзапросе по (channel, message_id); tombstone
	// отсекается до JOIN. Разметка присоединяется по (url, content_hash), а tag-строки — ещё и по
	// (model, prompt_version): иначе теги одной модели/редакции промпта прилипли бы к doc-строке
	// другой. Нет doc-строки — нет и тегов (groupArrayIf по непустому слагу даёт []).
	`CREATE OR REPLACE VIEW v_news_flash DEFINER = default SQL SECURITY DEFINER AS
		SELECT v.date AS date,
			toDate(v.date, 'Europe/Moscow') AS date_msk,
			v.source AS source,
			v.channel AS channel,
			v.message_id AS message_id,
			v.last_version AS version,
			v.first_seen AS first_seen,
			v.title AS title,
			v.url AS url,
			v.description AS description,
			v.content_hash AS content_hash,
			d.sentiment AS sentiment,
			d.relevance AS relevance,
			d.summary AS summary,
			d.model AS model,
			d.prompt_version AS prompt_version,
			groupArrayIf(tuple(t.dimension, ` + newsTagCanonicalSQL("t.tag") + `, t.direction), t.tag != '') AS tags
		FROM (
			SELECT channel, message_id, max(version) AS last_version,
				argMax(date, version) AS date,
				argMax(source, version) AS source,
				argMax(title, version) AS title,
				argMax(url, version) AS url,
				argMax(description, version) AS description,
				argMax(content_hash, version) AS content_hash,
				argMax(first_seen, version) AS first_seen,
				argMax(deleted, version) AS last_deleted
			FROM news_message_version FINAL
			GROUP BY channel, message_id
		) AS v
		LEFT JOIN (
			SELECT url, content_hash, model, prompt_version, sentiment, relevance, summary
			FROM news_annotation FINAL
			WHERE kind = 'doc' AND method = 'llm'
		) AS d ON d.url = v.url AND d.content_hash = v.content_hash
		LEFT JOIN (
			SELECT url, content_hash, model, prompt_version, dimension, tag, direction
			FROM news_annotation FINAL
			WHERE kind = 'tag' AND method = 'llm'
		) AS t ON t.url = d.url AND t.content_hash = d.content_hash
			AND t.model = d.model AND t.prompt_version = d.prompt_version
		WHERE v.last_deleted = 0
		GROUP BY v.date, toDate(v.date, 'Europe/Moscow'), v.source, v.channel, v.message_id, v.last_version,
			v.first_seen, v.title, v.url, v.description, v.content_hash,
			d.sentiment, d.relevance, d.summary, d.model, d.prompt_version
		ORDER BY date DESC, message_id DESC, model, prompt_version`,
	`ALTER TABLE v_news_flash MODIFY COMMENT 'Telegram-каналы: interfaxonline и ifax_go (молния Интерфакса), edisclosure (корпоративные раскрытия «Интерфакс-ЦРКИ»), newssmartlab (новостная лента Смартлаба), markettwits, russianmacro, MarketOverview, mozgovikresearch: последняя версия каждого сообщения из news_message_version без tombstone (deleted=0) и LLM-разметка из news_annotation (method=''llm''). Одна строка на сообщение × model/prompt_version; без doc-строки значения разметки нулевые, tags пуст. Правки и удаления — в v_news_message_versions, расшифровка тегов — в v_news_tags; полный поток канала — например, WHERE channel = ''edisclosure'''`,
	`ALTER TABLE v_news_flash COMMENT COLUMN date 'Момент публикации сообщения (UTC источника); между версиями не меняется'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN date_msk 'День публикации по МСК (toDate(date, ''Europe/Moscow'')) — для группировок по дням'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN source 'Источник: interfax (Telegram-фиды interfax_tg и ifax_go_tg), edisclosure, smartlab (newssmartlab_tg), markettwits, russianmacro, marketoverview, mozgovikresearch'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN channel 'Канал Telegram: interfaxonline, ifax_go, edisclosure, newssmartlab, markettwits, russianmacro, MarketOverview, mozgovikresearch'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN message_id 'ID сообщения в канале (из data-post превью t.me/s/<channel>)'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN version 'Номер последней версии сообщения; правка текста и возврат удалённого растят версию'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN first_seen 'Когда последняя версия впервые увидена прогоном'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN title 'Первая строка последней версии'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN url 'Ссылка на сообщение: https://t.me/<channel>/<id>'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN description 'Полный нормализованный текст последней версии'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN content_hash 'sha256 нормализованного title+description последней версии; ключ LLM-разметки'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN sentiment 'Тональность −1..1 от LLM; 0 — doc-строки разметки нет'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN relevance 'Релевантность тезисам 0..1 от LLM; 0 — doc-строки разметки нет'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN summary 'Краткое резюме (1 предложение) от LLM; пусто — doc-строки разметки нет'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN model 'LLM-модель, поставившая разметку; пусто — разметки нет'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN prompt_version 'Версия промпта разметки (newsPromptVersion); 0 — разметки нет'`,
	`ALTER TABLE v_news_flash COMMENT COLUMN tags 'Теги LLM: Array(Tuple(dimension, tag, direction)); dimension — person|country|sector|event|theme|cb_channel, tag — слаг реестра (расшифровка — v_news_tags; у cb_channel всегда канонический слаг KEY_RATE_CHANNELS.md §3.1, legacy-слаги редакций ≤2 нормализуются на чтении), direction — влияние на инфляцию (proinflationary|disinflationary|neutral|mixed); [] — тегов нет'`,

	// v_news_message_versions: VIEW + комментарий таблицы + 12 комментариев колонок.
	`CREATE OR REPLACE VIEW v_news_message_versions DEFINER = default SQL SECURITY DEFINER AS
		SELECT channel, message_id, version, date, toDate(date, 'Europe/Moscow') AS date_msk, source,
			title, url, description, content_hash, first_seen, deleted
		FROM news_message_version FINAL
		ORDER BY date DESC, message_id DESC, version DESC`,
	`ALTER TABLE v_news_message_versions MODIFY COMMENT 'Все версии Telegram-каналов (interfaxonline и ifax_go — молния Интерфакса, edisclosure — корпоративные раскрытия «Интерфакс-ЦРКИ», newssmartlab — новостная лента Смартлаба, markettwits, russianmacro, MarketOverview, mozgovikresearch), включая tombstone: история правок и удалений сообщений. deleted=1 — сообщение исчезло из окна канала (description и content_hash пустые, title/url/date — из последней живой версии). Текущее состояние без удалённых — v_news_flash'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN channel 'Канал Telegram: interfaxonline, ifax_go, edisclosure, newssmartlab, markettwits, russianmacro, MarketOverview, mozgovikresearch'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN message_id 'ID сообщения в канале'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN version 'Номер версии: 1 — первая запись, +1 на правку и на удаление (tombstone)'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN date 'Момент публикации (UTC источника); не меняется между версиями'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN date_msk 'День публикации по МСК (toDate(date, ''Europe/Moscow''))'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN source 'Источник: interfax, edisclosure, smartlab, markettwits, russianmacro, marketoverview, mozgovikresearch'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN title 'Первая строка версии (у tombstone — из последней живой версии)'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN url 'Ссылка на сообщение: https://t.me/<channel>/<id>'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN description 'Полный текст версии; пусто у tombstone'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN content_hash 'sha256 нормализованного title+description версии; пусто у tombstone'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN first_seen 'Когда версия впервые увидена прогоном'`,
	`ALTER TABLE v_news_message_versions COMMENT COLUMN deleted '1 — tombstone (сообщение исчезло из окна канала), 0 — живая версия'`,

	// v_news_tags: VIEW (генерируется из реестра newsTags) + комментарий таблицы + 4 комментария
	// колонок. Витрина статическая: агент расшифровывает слаги из v_news_flash.tags.
	newsTagsView(),
	`ALTER TABLE v_news_tags MODIFY COMMENT 'Статический реестр тегов LLM-разметки (news/tags.go): допустимые пары dimension/tag, их человекочитаемые названия и статическое направление каналов ставки. Агент использует витрину для расшифровки слагов из v_news_flash.tags; реестр публикуется импортёром news_watch вместе с остальными витринами'`,
	`ALTER TABLE v_news_tags COMMENT COLUMN dimension 'Измерение тега: person|country|sector|event|theme|cb_channel; в стартовом наборе theme не заполнен'`,
	`ALTER TABLE v_news_tags COMMENT COLUMN tag 'Слаг тега внутри измерения; уникальна пара (dimension, tag)'`,
	`ALTER TABLE v_news_tags COMMENT COLUMN title 'Человекочитаемое название тега'`,
	`ALTER TABLE v_news_tags COMMENT COLUMN direction 'Статическое направление влияния на инфляцию у cb_channel (proinflationary|disinflationary); пусто у остальных измерений — знак задаёт конкретная новость'`,
}

// newsTagsView собирает определение v_news_tags из реестра newsTags: по строке SELECT на тег,
// склеенные UNION ALL. Витрина статическая (реестр — единственный источник правды), поэтому
// генерируется, а не дублируется руками: новый тег в tags.go сразу появляется у агента вместе с
// расшифровкой. Порядок строк — порядок newsTags, витрина воспроизводима от прогона к прогону.
func newsTagsView() string {
	rows := make([]string, 0, len(newsTags))
	for _, tag := range newsTags {
		rows = append(rows, fmt.Sprintf("SELECT %s AS dimension, %s AS tag, %s AS title, %s AS direction",
			newsSQLString(tag.Dimension), newsSQLString(tag.Slug), newsSQLString(tag.Title), newsSQLString(tag.Direction)))
	}
	return "CREATE OR REPLACE VIEW v_news_tags DEFINER = default SQL SECURITY DEFINER AS\n\t" +
		strings.Join(rows, "\n\tUNION ALL ")
}

// newsSQLString — строковый литерал ClickHouse для значений витрины тегов: одиночная кавычка
// внутри значения удваивается (две подряд), как и в текстах комментариев, а обратный слэш —
// тоже служебный символ литерала и удваивается первым. Реестр содержит только статические слаги
// и названия, но экранирование обязательно: название тега — свободный текст.
func newsSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
