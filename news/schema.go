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
			, sentiment Nullable(Float32)      -- -1..1, заполняет LLM-сессия
		) ENGINE = ReplacingMergeTree ORDER BY (date, source, title);
	`

// newsFilter — широкий фильтр тезисов (RE2, регистронезависимо) из спеки §«Схема и витрина»,
// дословно. Осознанно широкий: ложный пропуск события тезиса дороже ложного срабатывания —
// «доставка» попадает на стем «ставк», «полюс» ловится и в географическом смысле. Лишнее
// снимается в запросе сессии, а сам список уточняется пересозданием витрины по всей истории:
// сырая news_events хранит ленту целиком и агенту не выдаётся, поэтому сужение фильтра
// ничего не теряет.
const newsFilter = `(?i)дизел|бензин|керосин|дистиллят|нефтепродукт|топлив|НПЗ|переработк|нефт|Brent|ОПЕК|Ормуз|Иран|санкц|поставк|эмбарго|сделк|Трамп|перемири|пошлин|ставк|ФРС|Федрезерв|инфляц|дефляц|CPI|PPI|ИПЦ|золот|драгметалл|XAU|ETF|резерв|Банк России|ЦБ|Минфин|бюджет|НДПИ|налог|отсечк|Полюс|Сухой Лог|PLZL`

// newsViewStatements — витрина v_news_events: определение плюс комментарии таблицы и каждой
// колонки (ALTER TABLE … MODIFY COMMENT / COMMENT COLUMN — образец eia/eiaViews). Витрина
// публикуется по порядку; фильтр тезисов живёт именно здесь, потому что читает витрину агент
// (грант kimi_reader), а сырая news_events ему закрыта.
var newsViewStatements = []string{
	fmt.Sprintf(`CREATE OR REPLACE VIEW v_news_events DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, toDate(date, 'Europe/Moscow') AS date_msk, source, title, url, sentiment
		FROM news_events FINAL WHERE match(title, '%s') ORDER BY date DESC`, newsFilter),
	`ALTER TABLE v_news_events MODIFY COMMENT 'Событийная лента РФ-канала: RSS «Интерфакс», РБК, ЦБ РФ, TASS (импортёр news_watch, каждые 30 минут). Показаны только события по стемам тезисов (широкий фильтр — в определении витрины); полная лента лежит в сырой news_events и агенту не выдаётся. История копится с первого прогона (окна лент 25–100 позиций). sentiment пока NULL — заполняет LLM-сессия (follow-up)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date 'Момент публикации в ленте (pubDate источника со своим офсетом времени)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN date_msk 'День публикации по МСК (toDate(date, ''Europe/Moscow'')) — для группировок по дням'`,
	`ALTER TABLE v_news_events COMMENT COLUMN source 'Лента-источник: interfax, rbc, cbr, tass; оба фида ЦБ пишутся под cbr и различаются url'`,
	`ALTER TABLE v_news_events COMMENT COLUMN title 'Заголовок новости из ленты (нормализован в одну строку)'`,
	`ALTER TABLE v_news_events COMMENT COLUMN url 'Ссылка на публикацию'`,
	`ALTER TABLE v_news_events COMMENT COLUMN sentiment 'Тональность −1..1; заполняет LLM-сессия (канал записи — follow-up; сейчас всегда NULL)'`,
}
