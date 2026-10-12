package news

import (
	"regexp"
	"strings"
	"testing"
)

func TestNewsEventsDdlPinsCanonicalKey(t *testing.T) {
	for _, want := range []string{
		"sentiment Nullable(Float32)",
		"ENGINE = ReplacingMergeTree ORDER BY (date, source, title)",
	} {
		if !strings.Contains(newsEventsDdl, want) {
			t.Errorf("DDL news_events обязан содержать %q: без него копия расходится с каноном "+
				"ARCHITECTURE.md §6.2, а дедупликация ReplacingMergeTree теряет ключ", want)
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
	if len(newsViewStatements) != 8 {
		t.Fatalf("newsViewStatements = %d стейтментов, want 8 (VIEW + комментарий таблицы + "+
			"6 комментариев колонок): витрина публикуется по порядку, и лишний/потерянный "+
			"стейтмент ломает публикацию", len(newsViewStatements))
	}

	view := newsViewStatements[0]
	for _, want := range []string{
		"CREATE OR REPLACE VIEW v_news_events",
		"DEFINER = default SQL SECURITY DEFINER",
		"match(title, '" + newsFilter + "')",
		"toDate(date, 'Europe/Moscow')",
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
	for _, col := range []string{"date", "date_msk", "source", "title", "url", "sentiment"} {
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
	// а ClickHouse не примет незакрытый литерал: проверяем экранирование всех восьми
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
