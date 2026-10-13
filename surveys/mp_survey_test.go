package surveys

import (
	"slices"
	"strings"
	"testing"
)

// Name() — имя импортёра в CLICKHOUSE_IMPORT_STAT и в id шага dagu. Оно обязано
// отличаться от имени таблицы macro_series, которое вернул бы встроенный
// util.ClickHouseImport: иначе запуск импортёра попадёт в чужой шаг конвейера.
func TestImporterName(t *testing.T) {
	if got := importer.Name(); got != "cbr_mp_survey" {
		t.Fatalf("Name() = %q, want %q", got, "cbr_mp_survey")
	}
}

// Витрина — контракт для MCP-агента: имя, таблица-источник и отбор по source, без
// которого витрина показала бы ряды других импортёров macro_series. Комментарии
// публикует util.CreateView, поэтому пустой Comment или незакомментированная
// колонка оставят агента без описания единиц измерения.
func TestSurveyViewDefinition(t *testing.T) {
	if got := cbrSurveyView.Name; got != "v_cbr_survey" {
		t.Errorf("Name = %q, want %q", got, "v_cbr_survey")
	}
	if want := []string{"macro_series"}; !slices.Equal(cbrSurveyView.Tables, want) {
		t.Errorf("Tables = %v, want %v", cbrSurveyView.Tables, want)
	}
	for _, fragment := range []string{"macro_series FINAL", "source = 'cbr_survey'"} {
		if !strings.Contains(cbrSurveyView.Select, fragment) {
			t.Errorf("в Select нет %q:\n%s", fragment, cbrSurveyView.Select)
		}
	}
	want := []string{"source", "series", "date", "value"}
	if len(cbrSurveyView.Columns) != len(want) {
		t.Errorf("колонок в комментариях %d, want %d: %v", len(cbrSurveyView.Columns), len(want), cbrSurveyView.Columns)
	}
	for _, col := range want {
		if comment := cbrSurveyView.Columns[col]; comment == "" {
			t.Errorf("колонка %s не описана", col)
		}
	}
	if cbrSurveyView.Comment == "" {
		t.Error("Comment пуст: агент не увидит конвенцию дат и единицы рядов")
	}
}

// DDL публикует перечисление источников macro_series: без 'cbr_survey' в комментарии
// source строка этого импортёра попадает в таблицу с неописанным источником.
func TestSurveyDdlPinsSource(t *testing.T) {
	for _, fragment := range []string{"CREATE TABLE IF NOT EXISTS macro_series", "'cbr_survey'"} {
		if !strings.Contains(mpSurveyDdl, fragment) {
			t.Errorf("в mpSurveyDdl нет %q:\n%s", fragment, mpSurveyDdl)
		}
	}
}
