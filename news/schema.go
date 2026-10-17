package news

import "fmt"

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
			, content_hash String              -- sha256 нормализованного title+description; ключ версии
			                                   -- для LLM-разметки (generic по url+content_hash)
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
			  channel LowCardinality(String)   -- interfaxonline
			, message_id UInt64
			, version UInt32                   -- 1..N; растёт при правке/удалении
			, date DateTime                    -- момент публикации (не меняется)
			, source LowCardinality(String)    -- interfax
			, title String                     -- первая строка версии
			, url String                       -- https://t.me/<channel>/<id>
			, description String               -- полный текст версии; '' у tombstone
			, content_hash String              -- sha256; '' у tombstone
			, first_seen DateTime              -- когда версия впервые увидена
			, deleted UInt8                    -- 1 — сообщение исчезло из окна канала
		) ENGINE = ReplacingMergeTree ORDER BY (channel, message_id, version);
	`

// newsSchemaStatements — DDL прогона по порядку: CREATE news_events, ALTER-ы миграции (образец —
// gostSchemaStatements, craw/gost.go), затем CREATE news_message_version. Схема выполняется до
// чтения лент, поэтому версии и news_events готовы к записи в любом режиме. Возвращается срез, а
// не переменная: вызывающий не должен получить возможность переставить стейтменты местами.
func newsSchemaStatements() []string {
	stmts := make([]string, 0, 2+len(newsAlterStatements))
	stmts = append(stmts, newsEventsDdl)
	stmts = append(stmts, newsAlterStatements...)
	return append(stmts, newsMessageVersionDdl)
}

// newsFilter — широкий фильтр тезисов (RE2, регистронезависимо): список из спеки §«Схема и
// витрина», дополненный стемами ставки/денежного рынка (вклады, депозиты, наличные, кредит, ОФЗ,
// RGBI, RUONIA, RUSFAR) — посты smart-lab про ставку и доходности должны доезжать до агента.
// Осознанно широкий: ложный пропуск события тезиса дороже ложного срабатывания — «доставка»
// попадает на стем «ставк», «полюс» ловится и в географическом смысле, «кредит» задевает
// потребительские сюжеты — расширение оставляет фильтр осознанно широким. Лишнее снимается в
// запросе сессии, а сам список уточняется пересозданием витрины по всей истории: сырая
// news_events хранит ленту целиком и агенту не выдаётся, поэтому сужение фильтра ничего не теряет.
const newsFilter = `(?i)дизел|бензин|керосин|дистиллят|нефтепродукт|топлив|НПЗ|переработк|нефт|Brent|ОПЕК|Ормуз|Иран|санкц|поставк|эмбарго|сделк|Трамп|перемири|пошлин|ставк|ФРС|Федрезерв|инфляц|дефляц|CPI|PPI|ИПЦ|золот|драгметалл|XAU|ETF|резерв|Банк России|ЦБ|Минфин|бюджет|НДПИ|налог|отсечк|Полюс|Сухой Лог|PLZL|вклад|депозит|наличн|кредит|ОФЗ|RGBI|RUONIA|RUSFAR`

// newsViewStatements — витрина v_news_events: определение плюс комментарии таблицы и каждой
// колонки (ALTER TABLE … MODIFY COMMENT / COMMENT COLUMN — образец eia/eiaViews). Витрина
// публикуется по порядку; фильтр тезисов живёт именно здесь, потому что читает витрину агент
// (грант kimi_reader), а сырая news_events ему закрыта.
var newsViewStatements = []string{
	fmt.Sprintf(`CREATE OR REPLACE VIEW v_news_events DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, toDate(date, 'Europe/Moscow') AS date_msk, source, title, url, description, sentiment
		FROM news_events FINAL WHERE match(title, '%s') ORDER BY date DESC`, newsFilter),
	`ALTER TABLE v_news_events MODIFY COMMENT 'Событийная лента РФ-канала: RSS «Интерфакс», РБК, ЦБ РФ, TASS, smart-lab (импортёр news_watch, каждые 30 минут). Показаны только события по стемам тезисов (широкий фильтр — в определении витрины); полная лента лежит в сырой news_events и агенту не выдаётся. История копится с первого прогона (окна лент 20–100 позиций). sentiment пока NULL — заполняет LLM-сессия (follow-up)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date 'Момент публикации в ленте (pubDate источника со своим офсетом времени)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date_msk 'День публикации по МСК (toDate(date, ''Europe/Moscow'')) — для группировок по дням'`,
	`ALTER TABLE v_news_events COMMENT COLUMN source 'Лента-источник: interfax, rbc, cbr, tass, smartlab; оба фида ЦБ пишутся под cbr, обе ленты smart-lab — под smartlab, различаются url'`,
	`ALTER TABLE v_news_events COMMENT COLUMN title 'Заголовок новости из ленты (нормализован в одну строку)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN url 'Ссылка на публикацию'`,
	// Маркер пустой строки '' записан четырьмя кавычками (''''): одиночная кавычка внутри
	// SQL-литерала удваивается, и агент видит в комментарии именно пустую строку ''.
	`ALTER TABLE v_news_events COMMENT COLUMN description 'Текст позиции из ленты: полный текст поста smart-lab (HTML) или краткое описание новости; '''' — описания нет; при цитировании — атрибуция автора и smart-lab'`,
	`ALTER TABLE v_news_events COMMENT COLUMN sentiment 'Тональность −1..1; заполняет LLM-сессия (канал записи — follow-up; сейчас всегда NULL)'`,
}
