package cbr

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// Name() — имя импортёра в CLICKHOUSE_IMPORT_STAT и в id шага dagu. Оно обязано
// отличаться от имени таблицы macro_series, которое вернул бы встроенный
// util.ClickHouseImport: иначе запуск импортёра попадёт в чужой шаг конвейера.
func TestImporterName(t *testing.T) {
	if got := mpSurveyImporter.Name(); got != "cbr_mp_survey" {
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

// DDL исполняется как fmt.Sprintf(mpSurveyDdl, mpSurveyTable) (util.ClickHouseImport.Import),
// поэтому пин %s-контракта здесь обязателен: литерал имени таблицы без глагола даёт
// «%!(EXTRA string=macro_series)» и ClickHouse error 62 «Multi-statements are not allowed»
// (живой прогон make import STAT=cbr_mp_survey). Второй ассерт — перечисление источников
// в комментарии source: без 'cbr_survey' строка этого импортёра попадёт в таблицу с
// неописанным источником.
func TestSurveyDdlPinsSource(t *testing.T) {
	for _, fragment := range []string{"CREATE TABLE IF NOT EXISTS %s", "'cbr_survey'"} {
		if !strings.Contains(mpSurveyDdl, fragment) {
			t.Errorf("в mpSurveyDdl нет %q:\n%s", fragment, mpSurveyDdl)
		}
	}
	rendered := fmt.Sprintf(mpSurveyDdl, mpSurveyTable)
	for _, fragment := range []string{"macro_series", "ENGINE = ReplacingMergeTree"} {
		if !strings.Contains(rendered, fragment) {
			t.Errorf("в отрендеренном DDL нет %q:\n%s", fragment, rendered)
		}
	}
	if strings.Contains(rendered, "%!") {
		t.Errorf("Sprintf дописал артефакт форматирования — DDL сломан:\n%s", rendered)
	}
}
