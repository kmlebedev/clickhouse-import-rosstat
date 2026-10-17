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
		// Стемы 1.9 (спека §4.5): Telegram-молния должна доезжать до витрины по тем же
		// тезисам, что и RSS — «что Трамп сказал про пошлины», «что с энергопереходом»,
		// «Китай и деглобализация», тарифы ЖКУ. Каждый заголовок ловится своим стемом.
		"Китай ограничил экспорт",
		"деглобализация ускоряется",
		"энергопереход",
		"тарифы на ЖКУ",
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
	// VIEW + комментарий таблицы + комментарий каждой колонки:
	// 4 VIEW + (1+8) events + (1+17) flash + (1+12) versions + (1+4) tags = 49.
	const wantStatements = 4 + 9 + 18 + 13 + 5
	if len(newsViewStatements) != wantStatements {
		t.Fatalf("newsViewStatements = %d стейтментов, want %d (4 VIEW + комментарии таблиц и "+
			"колонок): витрины публикуются по порядку, и лишний/потерянный стейтмент ломает "+
			"публикацию", len(newsViewStatements), wantStatements)
	}

	checks := []struct {
		view    string
		columns []string
		wants   []string
	}{
		{
			view:    "v_news_events",
			columns: []string{"date", "date_msk", "source", "transport", "title", "url", "description", "sentiment"},
			wants: []string{
				"CREATE OR REPLACE VIEW v_news_events",
				"DEFINER = default SQL SECURITY DEFINER",
				"multiIf(url LIKE 'https://t.me/%', 'telegram', 'rss') AS transport",
				"match(title, '" + newsFilter + "')",
				"match(description, '" + newsFilter + "')",
				"toDate(date, 'Europe/Moscow')",
				// Порядок колонок — часть контракта витрины: описание стоит между url и sentiment,
				// и сдвиг колонок в SELECT иначе не заметил бы ни один офлайн-тест.
				"title, url, description, sentiment",
				"ORDER BY date DESC",
			},
		},
		{
			view: "v_news_flash",
			// Порядок колонок — контракт витрины (спека §4.5): разметка идёт после content_hash,
			// теги — последней колонкой.
			columns: []string{"date", "date_msk", "source", "channel", "message_id", "version",
				"first_seen", "title", "url", "description", "content_hash", "sentiment",
				"relevance", "summary", "model", "prompt_version", "tags"},
			wants: []string{
				"CREATE OR REPLACE VIEW v_news_flash",
				"DEFINER = default SQL SECURITY DEFINER",
				"FROM news_message_version FINAL",
				// Последняя версия — max(version) и argMax по каждому полю; псевдоним
				// last_version не затеняет колонку version внутри argMax (иначе ClickHouse
				// отверг бы вложенный агрегат).
				"max(version) AS last_version",
				"argMax(date, version)",
				"FROM news_annotation FINAL",
				"LEFT JOIN",
				// Разметка присоединяется по паре (url, content_hash), tag-строки — ещё и по
				// (model, prompt_version): иначе теги одной модели прилипли бы к doc-строке
				// другой редакции промпта.
				"d.url = v.url AND d.content_hash = v.content_hash",
				"t.model = d.model AND t.prompt_version = d.prompt_version",
				"groupArrayIf(tuple(t.dimension, t.tag, t.direction), t.tag != '') AS tags",
				"WHERE v.last_deleted = 0",
				"GROUP BY v.date",
				"ORDER BY date DESC, message_id DESC",
			},
		},
		{
			view:    "v_news_message_versions",
			columns: []string{"channel", "message_id", "version", "date", "date_msk", "source", "title", "url", "description", "content_hash", "first_seen", "deleted"},
			wants: []string{
				"CREATE OR REPLACE VIEW v_news_message_versions",
				"DEFINER = default SQL SECURITY DEFINER",
				"FROM news_message_version FINAL",
				"toDate(date, 'Europe/Moscow') AS date_msk",
				// Tombstone-версии не отфильтрованы — в этом и смысл витрины.
				"deleted",
				"ORDER BY date DESC, message_id DESC, version DESC",
			},
		},
		{
			view:    "v_news_tags",
			columns: []string{"dimension", "tag", "title", "direction"},
			wants: []string{
				"CREATE OR REPLACE VIEW v_news_tags",
				"DEFINER = default SQL SECURITY DEFINER",
				"SELECT 'person' AS dimension, 'trump' AS tag",
				"'cb_channel'",
				"'budget_impulse'",
				"AS direction",
			},
		},
	}
	for _, tc := range checks {
		view := newsViewByName(t, tc.view)
		for _, want := range tc.wants {
			if !strings.Contains(view, want) {
				t.Errorf("определение витрины %s обязано содержать %q:\n%s", tc.view, want, view)
			}
		}

		// COMMENT COLUMN на каждую колонку витрины: смысл колонок агент читает только отсюда,
		// а сырые news_message_version/news_annotation ему закрыты.
		comments := newsViewCommentStatements(tc.view)
		if want := 1 + len(tc.columns); len(comments) != want {
			t.Errorf("витрина %s: %d стейтментов-комментариев, want %d (таблица + %d колонок)",
				tc.view, len(comments), want, len(tc.columns))
		}
		tableComment := false
		for _, stmt := range comments {
			if strings.Contains(stmt, " MODIFY COMMENT '") {
				tableComment = true
				break
			}
		}
		if !tableComment {
			t.Errorf("витрина %s: нет комментария таблицы (ALTER TABLE … MODIFY COMMENT)", tc.view)
		}
		for _, col := range tc.columns {
			want := "ALTER TABLE " + tc.view + " COMMENT COLUMN " + col + " '"
			found := false
			for _, stmt := range comments {
				if strings.Contains(stmt, want) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("нет стейтмента %q для колонки %s", want, col)
			}
		}
	}

	// Реестр тегов — единственный источник витрины: каждый тег даёт строку SELECT, строки
	// склеены UNION ALL. Так новый тег в tags.go сразу виден агенту в расшифровке слагов.
	tagsView := newsViewByName(t, "v_news_tags")
	if got, want := strings.Count(tagsView, "UNION ALL"), len(newsTags)-1; got != want {
		t.Errorf("v_news_tags: %d UNION ALL, want %d (по строке на %d тегов реестра)",
			got, want, len(newsTags))
	}
	if got, want := strings.Count(tagsView, "SELECT "), len(newsTags); got != want {
		t.Errorf("v_news_tags: %d строк SELECT, want %d (по строке на тег реестра)", got, want)
	}

	// Литералы комментариев и витрины тегов содержат одиночные кавычки внутри текста
	// («Europe/Moscow», пустая строка ''), а ClickHouse не примет незакрытый литерал:
	// проверяем экранирование всех стейтментов офлайн — живой базы в тестах нет.
	for i, stmt := range newsViewStatements {
		if !newsQuotesBalanced(stmt) {
			t.Errorf("стейтмент %d: литерал не закрыт (одиночную кавычку в тексте надо "+
				"экранировать как ''):\n%s", i, stmt)
		}
	}
}

// newsViewByName ищет определение витрины по имени, а не по индексу: порядок публикации —
// контракт, но проверка не должна ломаться от сдвига индексов при добавлении комментария.
func newsViewByName(t *testing.T, name string) string {
	t.Helper()
	prefix := "CREATE OR REPLACE VIEW " + name + " "
	for _, stmt := range newsViewStatements {
		if strings.HasPrefix(stmt, prefix) {
			return stmt
		}
	}
	t.Fatalf("в newsViewStatements нет определения витрины %s", name)
	return ""
}

// newsViewCommentStatements собирает комментарии витрины (таблицы и колонок): именно из них
// агент читает смысл колонок, поэтому адресность и число комментариев — часть контракта.
func newsViewCommentStatements(name string) []string {
	prefix := "ALTER TABLE " + name + " "
	var out []string
	for _, stmt := range newsViewStatements {
		if strings.HasPrefix(stmt, prefix) {
			out = append(out, stmt)
		}
	}
	return out
}

// TestNewsSQLStringEscapesLiterals — значения реестра попадают в определение v_news_tags как
// SQL-литералы: кавычка и обратный слэш обязаны быть удвоены, иначе название тега с апострофом
// сломает публикацию витрины, и ошибка вылезет только на живом ClickHouse.
func TestNewsSQLStringEscapesLiterals(t *testing.T) {
	if got, want := newsSQLString(`a'b\c`), `'a''b\\c'`; got != want {
		t.Errorf("newsSQLString = %s, want %s", got, want)
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
