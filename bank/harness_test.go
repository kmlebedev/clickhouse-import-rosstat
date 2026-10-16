package bank

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// appendedRow — строка, которую разбор импортёра пишет в таблицу. У таблиц пакета с колонкой
// name (domrf_mortgage, rus_vtb_group_ifrs, …) name — имя ряда из разбора; у однорядных таблиц
// колонки name нет, и имя ряда задаёт тест через appendRecorder.series.
type appendedRow struct {
	name  string
	date  time.Time
	value float64
}

// appendRecorder — заглушка driver.Batch для офлайн-тестов (стандарт §11.6): запоминает строки
// вместо записи в ClickHouse. Append намеренно консервативен по вводу: импортёры пишут значение
// то как float64, то как float32 (legacy-колонки пакета), то как строку (значение ячейки xlsx),
// а формы бывают (date, value) и (name, date, value). Незнакомая форма или тип — не паника, а
// ошибка, которую Append возвращает и запоминает в err: импортёр, пробрасывающий ошибку Append,
// донесёт её до теста сам, а если он её проигнорирует, тест увидит err сам. Сырые строки
// остаются в raw: импортёрам с широкой строкой удобнее разбирать raw самим, чем подгонять её
// под appendedRow.
type appendRecorder struct {
	driver.Batch
	series string   // имя ряда однорядной таблицы: у неё в таблице нет колонки name
	cols   []string // ожидаемые колонки в порядке DDL; пусто — форма не объявлена
	raw    [][]any  // строки в том виде, в каком их передал импортёр
	rows   []appendedRow
	err    error // первая ошибка формы/типа: сообщает тест, паники нет
}

// newAppendRecorder — приёмник для тестовых разборов. cols — ожидаемые колонки в порядке DDL
// (можно не объявлять: тогда принимаются обе формы, (date, value) и (name, date, value)).
func newAppendRecorder(cols ...string) *appendRecorder {
	return &appendRecorder{cols: cols}
}

func (r *appendRecorder) Append(v ...any) error {
	r.raw = append(r.raw, v)
	if r.err == nil {
		row, err := r.normalize(v)
		if err != nil {
			r.err = err
		} else {
			r.rows = append(r.rows, row)
		}
	}
	return r.err
}

// normalize приводит одну строку Append к appendedRow. Ошибки называет тип и форму, а не
// паникует: расхождение схемы в тесте обязано читаться как сообщение теста.
func (r *appendRecorder) normalize(v []any) (appendedRow, error) {
	if len(r.cols) > 0 && len(v) != len(r.cols) {
		return appendedRow{}, fmt.Errorf("строка из %d значений, а объявлена схема из %d (%v)", len(v), len(r.cols), r.cols)
	}
	row := appendedRow{name: r.series}
	switch len(v) {
	case 2: // (date, value)
	case 3: // (name, date, value)
		name, ok := v[0].(string)
		if !ok {
			return appendedRow{}, fmt.Errorf("имя ряда пришло как %T, ожидалась строка (значения: %v)", v[0], v)
		}
		row.name, v = name, v[1:]
	default:
		return appendedRow{}, fmt.Errorf("незнакомая форма строки: %d значений (ожидается (date, value) или (name, date, value))", len(v))
	}
	date, ok := v[0].(time.Time)
	if !ok {
		return appendedRow{}, fmt.Errorf("дата пришла как %T, ожидался time.Time", v[0])
	}
	value, err := recorderFloat(v[1])
	if err != nil {
		return appendedRow{}, err
	}
	row.date, row.value = date, value
	return row, nil
}

// recorderFloat принимает значение из Append в тех типах, которыми пишут импортёры пакета:
// float64 (парсеры считают число сами), float32 (колонки legacy-схем), int/int64 (счётчики) и
// string (значение ячейки xlsx, десятичный разделитель — точка или запятая). Остальное —
// понятная ошибка, а не паника.
func recorderFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case string:
		value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(x), ",", "."), 64)
		if err != nil {
			return 0, fmt.Errorf("значение %q не разбирается как число: %w", x, err)
		}
		return value, nil
	default:
		return 0, fmt.Errorf("значение пришло как %T, ожидалось число или строка", v)
	}
}
