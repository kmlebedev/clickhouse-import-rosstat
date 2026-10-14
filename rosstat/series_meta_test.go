package rosstat

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// appendRecorder — подмена driver.Batch для офлайн-тестов импортёров (§11.6): собирает
// вызовы Append, чтобы тест видел точки без базы. Ошибка записи не глотается: настроенная
// appendErr возвращается из Append — тест проверяет, что импортёр её вернул вызывающему,
// а не продолжил писать дальше.
type appendRecorder struct {
	rows      []appendedRow
	appendErr error
}

// appendedRow — один вызов Append: ряд, дата и значение точки. Значение — float64 у обоих
// поддерживаемых типов записи, поэтому рядом лежит valueType: без него тест не отличит
// float32-запись legacy-таблицы (rosstat_salaries_mes, колонка Float32) от float64-записи новой
// таблицы, а схема колонки иначе не проверяется ничем. Legacy-тесты, ожидавшие float32 (у
// самой записи шаг представления float32), сравнивают значение явным float64(float32(x)).
type appendedRow struct {
	series    string
	date      time.Time
	value     float64
	valueType string
}

var _ driver.Batch = (*appendRecorder)(nil)

func (r *appendRecorder) Append(v ...any) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	if len(v) != 3 {
		return fmt.Errorf("appendRecorder: Append с %d аргументами, ожидалось 3 (ряд, дата, значение)", len(v))
	}
	series, ok := v[0].(string)
	if !ok {
		return fmt.Errorf("appendRecorder: имя ряда %T, ожидалась строка", v[0])
	}
	date, ok := v[1].(time.Time)
	if !ok {
		return fmt.Errorf("appendRecorder: дата %T, ожидалось time.Time", v[1])
	}
	// Значение принимается в обоих типах: legacy-импортёры пакета пишут float32 (колонка
	// Float32 их таблиц), новые — float64 (колонка Float64). Тип запомнен отдельно — по нему
	// тест проверяет схему колонки.
	var value float64
	switch typed := v[2].(type) {
	case float32:
		value = float64(typed)
	case float64:
		value = typed
	default:
		return fmt.Errorf("appendRecorder: значение %T, ожидалось float32 или float64", v[2])
	}
	r.rows = append(r.rows, appendedRow{series: series, date: date, value: value, valueType: fmt.Sprintf("%T", v[2])})
	return nil
}

func (r *appendRecorder) Abort() error { return nil }

func (r *appendRecorder) AppendStruct(any) error {
	return fmt.Errorf("appendRecorder: AppendStruct не поддерживается")
}

func (r *appendRecorder) Column(int) driver.BatchColumn { return nil }

func (r *appendRecorder) Columns() []column.Interface { return nil }

func (r *appendRecorder) Flush() error { return nil }

func (r *appendRecorder) Send() error { return nil }

func (r *appendRecorder) IsSent() bool { return false }

func (r *appendRecorder) Rows() int { return len(r.rows) }

func newTestWorkbook(t *testing.T, sheets ...string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	for i, sheet := range sheets {
		if i == 0 {
			if err := f.SetSheetName("Sheet1", sheet); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := f.NewSheet(sheet); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func setSheetRows(t *testing.T, f *excelize.File, sheet string, rows [][]any) {
	t.Helper()
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.SetCellValue(sheet, cell, value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func describedSeries(t *testing.T, meta []util.SeriesMeta) map[string]bool {
	t.Helper()
	described := make(map[string]bool, len(meta))
	for _, m := range meta {
		if m.Source != rosstatSource || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q заполнено не полностью", m.Series)
		}
		if described[m.Series] {
			t.Errorf("ряд %q описан дважды", m.Series)
		}
		described[m.Series] = true
	}
	return described
}

func requireParsedDescribed(t *testing.T, parsed []string, described map[string]bool) {
	t.Helper()
	seen := make(map[string]bool, len(parsed))
	for _, name := range parsed {
		if !described[name] {
			t.Errorf("ряд %q не описан в series_meta", name)
		}
		seen[name] = true
	}
	for name := range described {
		if !seen[name] {
			t.Errorf("описанный ряд %q не встретился при разборе", name)
		}
	}
}

// TestIpcWeeksSeriesMetaCoversFixture — односторонний контракт каталога на реальной выгрузке
// (стандарт §11.4): каждый разобранный товар описан в ipcWeeksSeriesMeta. Обратное направление
// («описано ⊆ разобрано») не проверяется: имена товаров даёт источник, а не код, поэтому
// товар, переставший публиковаться в обновлённой или урезанной фикстуре, остаётся описанным.
//
// Проверка дублирует TestIpcWeeksMetaCoversParsedRows (ipc_weeks_test.go): та же фикстура и то
// же направление контракта. Оставлены обе осознанно: эта живёт рядом с каталогом вместе с
// describedSeries, которая проверяет заполненность и уникальность описаний, и до перевода
// импортёра на util.ClickHouseImport читала разбор напрямую (parseIpcWeeks) — теперь снимает
// имена с точек, записанных в appendRecorder (§11.6).
func TestIpcWeeksSeriesMetaCoversFixture(t *testing.T) {
	described := describedSeries(t, ipcWeeksSeriesMeta())
	for _, row := range importIpcWeeksFixture(t) {
		if !described[row.series] {
			t.Errorf("ряд %q из выгрузки не описан в series_meta", row.series)
		}
	}
}
