package craw

import (
	"context"
	"strings"
	"testing"
)

// Тесты витрин v_gost_* офлайн: соединение — фейковое (gostFakeConn из gost_test.go),
// источник — httptest (gostNewTestAPI/gostEveryDayServer). Пинятся три контракта:
// состав и форма стейтментов (число, определения витрин, комментарии), смысл комментариев
// (оговорки, без которых витрина читается неверно) и порядок публикации в Import —
// витрины публикуются в пишущем прогоне и до первого обращения к API.
//
// Число стейтментов — 16, а не 14 из плана: 2 × CREATE OR REPLACE VIEW + комментарий
// таблицы и 9 комментариев колонок строковой витрины + комментарий таблицы и 3 комментария
// колонок дневной. 3.4 млн строк витрины обязаны быть описаны для агента, и «14» в плане
// было арифметической ошибкой (решение контроллера, зафиксировано здесь).

// gostRowViewColumns — колонки строковой витрины по порядку определения: технический `uid`
// в витрину не выводится (провенанс остаётся в сырой таблице).
var gostRowViewColumns = []string{
	"date", "publication_date", "mark", "type", "certificate_number",
	"ecoclass", "enginetrademark", "yearofmanufacturing", "nameofmanufacturer",
}

// gostDailyViewColumns — колонки дневной витрины: день × марка и счётчик сертификатов.
var gostDailyViewColumns = []string{"date", "mark", "certificates"}

// gostCommentCount считает стейтменты-комментарии витрины: MODIFY COMMENT — комментарий
// таблицы, COMMENT COLUMN — комментарии колонок (DDL-стиль проекта: не COMMENT ON).
func gostCommentCount(t *testing.T, stmts []string, prefix string) int {
	t.Helper()
	count := 0
	for _, stmt := range stmts {
		if strings.HasPrefix(stmt, prefix) {
			count++
		}
	}
	return count
}

// gostColumnComment ищет единственный комментарий колонки витрины. Префикс заканчивается
// на `"' "` — иначе `date` совпал бы с колонкой, начинающейся на `date` (в news это
// date/date_msk, здесь — страховка на будущее).
func gostColumnComment(t *testing.T, stmts []string, view, column string) string {
	t.Helper()
	prefix := "ALTER TABLE " + view + " COMMENT COLUMN " + column + " '"
	var found []string
	for _, stmt := range stmts {
		if strings.HasPrefix(stmt, prefix) {
			found = append(found, stmt)
		}
	}
	if len(found) != 1 {
		t.Fatalf("комментариев колонки %s.%s: %d, ожидался один", view, column, len(found))
	}
	return found[0]
}

func TestGostViewStatementsShape(t *testing.T) {
	stmts := gostViewStatements
	if len(stmts) != 16 {
		t.Fatalf("стейтментов %d, ожидалось 16 (2 витрины + комментарий таблицы и 9 колонок строковой + комментарий таблицы и 3 колонки дневной)", len(stmts))
	}

	rows := stmts[0]
	for _, want := range []string{
		"CREATE OR REPLACE VIEW " + gostRowsView,
		"DEFINER = default SQL SECURITY DEFINER",
		"FROM " + gostTable + " FINAL",
		"SELECT date, publication_date, mark, type, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer",
	} {
		if !strings.Contains(rows, want) {
			t.Errorf("строковая витрина без %q: %q", want, rows)
		}
	}
	if strings.Contains(rows, "uid") {
		t.Errorf("строковая витрина выводит техническую колонку uid: %q", rows)
	}
	// 3.4 млн строк: сортировка на каждом запросе не нужна, фильтровать агент обязан по date,
	// а об этом говорит комментарий витрины (TestGostViewsDocumentSemantics).
	if strings.Contains(rows, "ORDER BY") {
		t.Errorf("строковая витрина сортируется: %q", rows)
	}

	daily := stmts[1]
	for _, want := range []string{
		"CREATE OR REPLACE VIEW " + gostDailyView,
		"DEFINER = default SQL SECURITY DEFINER",
		"uniqExact(certificate_number)",
		"GROUP BY date, mark",
		"ORDER BY date DESC, mark",
	} {
		if !strings.Contains(daily, want) {
			t.Errorf("дневная витрина без %q: %q", want, daily)
		}
	}
	// certificates — число уникальных номеров, а не строк таблицы: изменения сертификата
	// (amendments) дают вторую строку с тем же номером, и count() завысил бы день.
	if strings.Contains(daily, "count()") {
		t.Errorf("дневная витрина считает строки вместо уникальных номеров: %q", daily)
	}

	// Комментарии — только после определений витрин: агент читает витрину с описанием, а не
	// комментарии к ещё не созданному объекту.
	comments := stmts[2:]
	if got := gostCommentCount(t, comments, "ALTER TABLE "+gostRowsView+" MODIFY COMMENT"); got != 1 {
		t.Errorf("комментариев таблицы строковой витрины %d, ожидался один", got)
	}
	if got := gostCommentCount(t, comments, "ALTER TABLE "+gostDailyView+" MODIFY COMMENT"); got != 1 {
		t.Errorf("комментариев таблицы дневной витрины %d, ожидался один", got)
	}
	if got := gostCommentCount(t, comments, "ALTER TABLE "+gostRowsView+" COMMENT COLUMN "); got != len(gostRowViewColumns) {
		t.Errorf("комментариев колонок строковой витрины %d, ожидалось %d", got, len(gostRowViewColumns))
	}
	if got := gostCommentCount(t, comments, "ALTER TABLE "+gostDailyView+" COMMENT COLUMN "); got != len(gostDailyViewColumns) {
		t.Errorf("комментариев колонок дневной витрины %d, ожидалось %d", got, len(gostDailyViewColumns))
	}
	// Каждая колонка обеих витрин обязана быть описана: колонка без комментария — это колонка,
	// смысл которой агент угадывает.
	for _, column := range gostRowViewColumns {
		gostColumnComment(t, comments, gostRowsView, column)
	}
	for _, column := range gostDailyViewColumns {
		gostColumnComment(t, comments, gostDailyView, column)
	}
}

func TestGostViewsDocumentSemantics(t *testing.T) {
	// Тексты комментариев — часть контракта витрин: агент читает их как описание данных.
	// Проверяются оговорки, без которых витрина читается неверно (даты, лаг публикации,
	// amendments, год выпуска 0, uniqExact, пустой день), а не дословные формулировки.
	var text strings.Builder
	for _, stmt := range gostViewStatements[2:] {
		text.WriteString(stmt)
		text.WriteString("\n")
	}
	comments := text.String()
	// Регистр — потому что комментарий таблицы начинается с «Реестр СБКТС Росстандарта».
	lowered := strings.ToLower(comments)

	for _, want := range []string{
		"реестр сбктс росстандарта",
		"rest api",
		"gost.ru",
		"дата выдачи сбктс (мск)",
		"дата публикации среза",
		"лаг к выдаче ~2 суток",
		"изменения сертификата дают вторую строку",
		"replacingmergetree final",
		"фильтруйте по `date`",
		"витрина не сортирована",
		"год выпуска",
		"`0` — не указан",
		"до бэкфилла",
		"uniqexact",
		"уникальных номеров",
		"пустой день отсутствует",
	} {
		if !strings.Contains(lowered, want) {
			t.Errorf("в комментариях витрин нет %q:\n%s", want, comments)
		}
	}

	// «Одна строка каждый»: перенос внутри комментария — это либо незакрытая кавычка,
	// либо комментарий, у которого потерялась половина смысла.
	for i, stmt := range gostViewStatements[2:] {
		if strings.Contains(stmt, "\n") {
			t.Errorf("комментарий %d не одной строкой: %q", i, stmt)
		}
	}
}

func TestGostImportWritesPublishViews(t *testing.T) {
	gostPinClock(t, gostTestNow)
	gostEnv(t, map[string]string{gostModeEnv: gostModeSince, gostAppIDEnv: gostTestAppID})
	gostNewTestAPI(t, gostEveryDayServer(t, gostDaySpec{Count: 1}))
	conn := &gostFakeConn{} // пустая таблица: свой прогон дней, но порядок шагов тот же

	if _, err := (&gostImporter{}).Import(context.Background(), conn); err != nil {
		t.Fatalf("since по пустой таблице: %v", err)
	}

	calls := conn.callList()
	want := make(map[string]bool, len(gostViewStatements))
	for _, stmt := range gostViewStatements {
		want[stmt] = true
	}

	var viewIdx []int
	for i, call := range calls {
		stmt := strings.TrimPrefix(call, "exec: ")
		if stmt != call && want[stmt] {
			viewIdx = append(viewIdx, i)
		}
	}
	if len(viewIdx) != len(gostViewStatements) {
		t.Fatalf("Exec стейтментов витрин %d, ожидалось %d; вызовы: %v", len(viewIdx), len(gostViewStatements), calls)
	}
	// Порядок стейтментов — порядок среза: колонка обязана быть описана после создания витрины,
	// а дневная витрина — после строковой (её определение читает таблицу, а не витрину, но
	// отчёт о сбое без порядка не читается).
	for n, i := range viewIdx {
		if got := strings.TrimPrefix(calls[i], "exec: "); got != gostViewStatements[n] {
			t.Errorf("стейтмент витрин %d = %q, ожидался %q", n, got, gostViewStatements[n])
		}
	}
	// Публикация — до первого обращения к API: правка определения витрины обязана уехать вместе
	// с прогоном, а не после него (и не после первой записи дня).
	firstBatch := -1
	for i, call := range calls {
		if strings.HasPrefix(call, "preparebatch:") {
			firstBatch = i
			break
		}
	}
	if firstBatch < 0 {
		t.Fatalf("ни одного батча в прогоне: %v", calls)
	}
	if viewIdx[len(viewIdx)-1] > firstBatch {
		t.Errorf("витрины публикуются после первого батча: последний стейтмент — вызов %d, батч — %d",
			viewIdx[len(viewIdx)-1], firstBatch)
	}
}
