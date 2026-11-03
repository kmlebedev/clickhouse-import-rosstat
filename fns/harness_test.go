package fns

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fnsRow — строка таблицы в том виде, в каком её передаёт разбор: колонки DDL
// (date, region, region_kind, series, value).
type fnsRow struct {
	date   time.Time
	region string
	kind   string
	series string
	value  float64
}

// appendRecorder — заглушка driver.Batch для офлайн-тестов разбора: запоминает
// Append-вызовы вместо записи в ClickHouse (стандарт §11.6). Форма строки объявлена
// строго: незнакомое количество или тип значений — ошибка, которую Append отдаёт
// вызывающему, а не паника.
type appendRecorder struct {
	driver.Batch
	rows []fnsRow
	err  error
}

func newFnsRecorder() *appendRecorder { return &appendRecorder{} }

func (r *appendRecorder) Append(values ...any) error {
	if len(values) != 5 {
		r.err = fmt.Errorf("строка из %d значений, а объявлена схема из 5 (date, region, region_kind, series, value)", len(values))
		return r.err
	}
	date, ok := values[0].(time.Time)
	if !ok {
		r.err = fmt.Errorf("колонка date: ожидался time.Time, получен %T", values[0])
		return r.err
	}
	region, ok := values[1].(string)
	if !ok {
		r.err = fmt.Errorf("колонка region: ожидалась строка, получен %T", values[1])
		return r.err
	}
	kind, ok := values[2].(string)
	if !ok {
		r.err = fmt.Errorf("колонка region_kind: ожидалась строка, получен %T", values[2])
		return r.err
	}
	series, ok := values[3].(string)
	if !ok {
		r.err = fmt.Errorf("колонка series: ожидалась строка, получен %T", values[3])
		return r.err
	}
	value, ok := values[4].(float64)
	if !ok {
		r.err = fmt.Errorf("колонка value: ожидался float64, получен %T", values[4])
		return r.err
	}
	if kind != fnsKindRF && kind != fnsKindFederalDistrict && kind != fnsKindSubject {
		r.err = fmt.Errorf("region_kind %q вне rf|federal_district|subject", kind)
		return r.err
	}
	r.rows = append(r.rows, fnsRow{date: date, region: region, kind: kind, series: series, value: value})
	return nil
}

// index — карта «территория/ряд → значение» для точечных сверок в тестах.
func (r *appendRecorder) index() map[string]fnsRow {
	rows := make(map[string]fnsRow, len(r.rows))
	for _, row := range r.rows {
		rows[row.region+"|"+row.series] = row
	}
	return rows
}

// seriesSet — множество разобранных рядов: по нему тест сверяет каталог с разбором.
func (r *appendRecorder) seriesSet() map[string]int {
	series := make(map[string]int, len(fnsSeriesRows))
	for _, row := range r.rows {
		series[row.series]++
	}
	return series
}

// regionSet — множество территорий с классами: проверяет, что канонизация собрала
// 94 территории и что написания разных лет свелись к одному имени.
func (r *appendRecorder) regionSet() map[string]string {
	regions := make(map[string]string, fnsTerritoryCount)
	for _, row := range r.rows {
		regions[row.region] = row.kind
	}
	return regions
}
