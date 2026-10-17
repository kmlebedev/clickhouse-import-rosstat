package news

import (
	"regexp"
	"strings"
	"testing"
)

func TestNewsEventsDdlPinsCanonicalKey(t *testing.T) {
	for _, want := range []string{
		"description String",
		"content_hash String",
		// content_hash стоит между description и sentiment — порядок колонок канона.
		"description String               -- текст позиции из ленты",
		"content_hash String              -- sha256 нормализованного title+description",
		"sentiment Nullable(Float32)      -- -1..1, заполняет LLM-сессия",
		"ENGINE = ReplacingMergeTree ORDER BY (date, source, title)",
	} {
		if !strings.Contains(newsEventsDdl, want) {
			t.Errorf("DDL news_events обязан содержать %q: без него копия расходится с каноном "+
				"ARCHITECTURE.md §6.2, а дедупликация ReplacingMergeTree теряет ключ", want)
		}
	}
	if strings.Index(newsEventsDdl, "content_hash String") > strings.Index(newsEventsDdl, "sentiment Nullable(Float32)") {
		t.Error("content_hash в DDL news_events стоит после sentiment, want между description и sentiment")
	}
}

// TestNewsAlterStatementsPinMigration — пин миграции живой таблицы: ALTER выполняется каждым
// прогоном (ADD COLUMN IF NOT EXISTS идемпотентен) и обязан совпадать с каноном посимвольно.
// AFTER url/AFTER description держат порядок колонок как в DDL: без них description и
// content_hash уехали бы в конец, и копия разошлась бы с каноном. Порядок стейтментов тоже
// контракт: content_hash встаёт после description. Образец пина — craw/gost_test.go.
func TestNewsAlterStatementsPinMigration(t *testing.T) {
	want := []string{
		"ALTER TABLE news_events ADD COLUMN IF NOT EXISTS description String AFTER url",
		"ALTER TABLE news_events ADD COLUMN IF NOT EXISTS content_hash String AFTER description",
	}
	if len(newsAlterStatements) != len(want) {
		t.Fatalf("newsAlterStatements = %d стейтментов, want %d (миграции description и content_hash): %q",
			len(newsAlterStatements), len(want), newsAlterStatements)
	}
	for i := range want {
		if newsAlterStatements[i] != want[i] {
			t.Errorf("newsAlterStatements[%d] = %q, want %q (дословно: порядок колонок — часть канона)",
				i, newsAlterStatements[i], want[i])
		}
	}

	stmts := newsSchemaStatements()
	if len(stmts) != 5 {
		t.Fatalf("newsSchemaStatements() = %d стейтментов, want 5 (CREATE news_events + 2 ALTER + "+
			"CREATE news_message_version + CREATE news_annotation): %q", len(stmts), stmts)
	}
	if stmts[0] != newsEventsDdl {
		t.Errorf("стейтмент 0 не равен newsEventsDdl: %q", stmts[0])
	}
	for i := range want {
		if stmts[i+1] != want[i] {
			t.Errorf("стейтмент %d = %q, want %q (ALTER-ы идут после CREATE, до таблиц версий)",
				i+1, stmts[i+1], want[i])
		}
	}
	if stmts[3] != newsMessageVersionDdl {
		t.Errorf("стейтмент 3 = %q, want DDL news_message_version (таблица версий создаётся после миграции news_events)",
			stmts[3])
	}
	if stmts[4] != newsAnnotationDdl {
		t.Errorf("стейтмент 4 = %q, want DDL news_annotation (таблица разметки создаётся после таблицы версий)",
			stmts[4])
	}
}

// TestNewsMessageVersionDdlPinsCanon — пин винтажной таблицы Telegram: канон спеки §4.2.
// Ключ ReplacingMergeTree (channel, message_id, version) — то, на чём держится no-op
// повторной записи версии; сдвиг типов (UInt64 id, UInt32 version, UInt8 deleted) сломал бы
// совместимость с живой таблицей.
func TestNewsMessageVersionDdlPinsCanon(t *testing.T) {
	for _, want := range []string{
		"channel LowCardinality(String)",
		"message_id UInt64",
		"version UInt32",
		"date DateTime",
		"source LowCardinality(String)",
		"title String",
		"url String",
		"description String",
		"content_hash String",
		"first_seen DateTime",
		"deleted UInt8",
		"ENGINE = ReplacingMergeTree ORDER BY (channel, message_id, version)",
	} {
		if !strings.Contains(newsMessageVersionDdl, want) {
			t.Errorf("DDL news_message_version обязан содержать %q: копия обязана совпадать с каноном "+
				"ARCHITECTURE.md §6.2 (спека §4.2)", want)
		}
	}
}

// TestNewsAnnotationDdlPinsCanon — пин таблицы LLM-разметки: канон спеки §4.2. Ключ
// ReplacingMergeTree (url, content_hash, kind, method, model, prompt_version, dimension, tag)
// держит смену модели/промпта как новые строки, а не затирание; типы (UInt16 prompt_version,
// Float32 confidence/sentiment/relevance) обязаны совпадать с живой таблицей.
func TestNewsAnnotationDdlPinsCanon(t *testing.T) {
	for _, want := range []string{
		"url String",
		"content_hash String",
		"kind LowCardinality(String)",
		"method LowCardinality(String)",
		"model LowCardinality(String)",
		"prompt_version UInt16",
		"dimension LowCardinality(String)",
		"tag LowCardinality(String)",
		"direction LowCardinality(String)",
		"confidence Float32",
		"evidence String",
		"sentiment Float32",
		"relevance Float32",
		"summary String",
		"annotated_at DateTime",
		"ENGINE = ReplacingMergeTree",
		"ORDER BY (url, content_hash, kind, method, model, prompt_version, dimension, tag)",
	} {
		if !strings.Contains(newsAnnotationDdl, want) {
			t.Errorf("DDL news_annotation обязан содержать %q: копия обязана совпадать с каноном "+
				"ARCHITECTURE.md §6.2 (спека §4.2)", want)
		}
	}
}

func TestNewsFilterMatchesThesisStems(t *testing.T) {
	// RE2 — тот же движок, что у match() в ClickHouse: если шаблон компилируется здесь,
	// он скомпилируется и в определении витрины.
	re := regexp.MustCompile(newsFilter)

	for _, title := range []string{
		"Дизельное топливо",
		// Ключевой триггер тезиса из спеки («истечение запрета РФ на экспорт дизеля») и
		// единственный заголовок списка, который ловится только стемом «дизел»: без него
		// выпадение стема из фильтра тест не заметит («Дизельное топливо» закрыто ещё и «топлив»).
		"запрет на экспорт дизеля",
		"Экспорт бензина",
		"нефть Brent",
		"санкции ЕС",
		"ключевая ставка",
		"инфляция",
		"цены на золото",
		"Банк России",
		"Минфин",
		"НДПИ",
		"Полюс",
		"Сухой Лог",
		"Трамп",
		"поставки СПГ",
		// Случай ставки/денежного рынка: «Наличные бьют рекорд…» — реальный пост smart-lab,
		// ради которого фильтр расширен; ловится новыми стемами «вклад|наличн». «Доходность
		// ОФЗ снизилась» и «RUONIA опустилась ниже ключевой» изолируют ОФЗ и RUONIA, а
		// депозитный заголовок покрыт ещё и старым стемом «ставк» — сам по себе стем
		// «депозит» он не доказывает.
		"Наличные бьют рекорд в России, вклады населения не растут",
		"Ставки по депозитам обновили максимум",
		"Доходность ОФЗ снизилась",
		"RUONIA опустилась ниже ключевой",
	} {
		if !re.MatchString(title) {
			t.Errorf("заголовок %q не прошёл фильтр: ложный пропуск события тезиса дороже "+
				"ложного срабатывания (лишнее снимает запрос сессии)", title)
		}
	}

	for _, title := range []string{
		"погода в Москве",
		"футбольный матч",
	} {
		if re.MatchString(title) {
			t.Errorf("заголовок %q прошёл фильтр тезисов, хотя тем не касается", title)
		}
	}
}

func TestNewsViewStatementsShape(t *testing.T) {
	if len(newsViewStatements) != 9 {
		t.Fatalf("newsViewStatements = %d стейтментов, want 9 (VIEW + комментарий таблицы + "+
			"7 комментариев колонок): витрина публикуется по порядку, и лишний/потерянный "+
			"стейтмент ломает публикацию", len(newsViewStatements))
	}

	view := newsViewStatements[0]
	for _, want := range []string{
		"CREATE OR REPLACE VIEW v_news_events",
		"DEFINER = default SQL SECURITY DEFINER",
		"match(title, '" + newsFilter + "')",
		"toDate(date, 'Europe/Moscow')",
		// Порядок колонок — часть контракта витрины: описание стоит между url и sentiment,
		// и сдвиг колонок в SELECT иначе не заметил бы ни один офлайн-тест.
		"url, description, sentiment",
		"ORDER BY date DESC",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("определение витрины обязано содержать %q:\n%s", want, view)
		}
	}

	if !strings.Contains(newsViewStatements[1], "MODIFY COMMENT") {
		t.Errorf("стейтмент 1 — комментарий таблицы (ALTER TABLE … MODIFY COMMENT):\n%s",
			newsViewStatements[1])
	}

	// COMMENT COLUMN на каждую колонку витрины: смысл колонок агент читает только отсюда,
	// а сырая news_events ему закрыта.
	for _, col := range []string{"date", "date_msk", "source", "title", "url", "description", "sentiment"} {
		want := "COMMENT COLUMN " + col + " '"
		found := false
		for _, stmt := range newsViewStatements[2:] {
			if strings.Contains(stmt, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("нет стейтмента %q для колонки %s", want, col)
		}
	}

	// Литералы комментариев содержат одиночные кавычки внутри текста («Europe/Moscow»),
	// а ClickHouse не примет незакрытый литерал: проверяем экранирование всех девяти
	// стейтментов офлайн — живой базы в тестах нет.
	for i, stmt := range newsViewStatements {
		if !newsQuotesBalanced(stmt) {
			t.Errorf("стейтмент %d: литерал не закрыт (одиночную кавычку в тексте надо "+
				"экранировать как ''):\n%s", i, stmt)
		}
	}
}

// newsQuotesBalanced повторяет правило SQL-литерала: кавычка внутри строки удваивается,
// поэтому одиночная кавычка закрывает литерал, а удвоенная — продолжает его.
func newsQuotesBalanced(stmt string) bool {
	inString := false
	for i := 0; i < len(stmt); i++ {
		if stmt[i] != '\'' {
			continue
		}
		if inString {
			if i+1 < len(stmt) && stmt[i+1] == '\'' {
				i++ // экранированная кавычка внутри литерала
				continue
			}
			inString = false
			continue
		}
		inString = true
	}
	return !inString
}
