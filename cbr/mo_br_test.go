package cbr

import (
	"fmt"
	"strings"
	"testing"
)

// Name() — это CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml. Имя отличается от
// имени таблицы macro_series, которое вернул бы встроенный util.ClickHouseImport
// (как у cbr_mp_survey): таблица общая с fred/bls/bea, и запуск «по таблице» попал
// бы в чужой шаг конвейера, а TableName при этом обязан остаться macro_series —
// иначе импортёр завёл бы вторую таблицу вместо общей.
func TestMoBrImporterNameAndTable(t *testing.T) {
	if got := moBrImporter.Name(); got != "cbr_mo_br" {
		t.Errorf("Name() = %q, want %q", got, "cbr_mo_br")
	}
	if got := moBrImporter.TableName; got != "macro_series" {
		t.Errorf("TableName = %q, want %q", got, "macro_series")
	}
}

// DDL исполняется через fmt.Sprintf(sql, TableName), поэтому проверяется уже
// подставленный текст. Ключ (source, series, date) и движок ReplacingMergeTree
// обязаны совпасть с общей macro_series: второй ключ под тем же именем таблицы
// молча развёл бы ряды импортёров (CREATE TABLE IF NOT EXISTS существующую таблицу
// не пересоздаёт, и расхождение осталось бы незамеченным ничем, кроме этого теста).
func TestMoBrDdlPinsMacroSeries(t *testing.T) {
	ddl := fmt.Sprintf(moBrDdl, moBrTable)
	for _, fragment := range []string{"macro_series", "ReplacingMergeTree", "ORDER BY (source, series, date)"} {
		if !strings.Contains(ddl, fragment) {
			t.Errorf("в DDL нет %q:\n%s", fragment, ddl)
		}
	}
}
