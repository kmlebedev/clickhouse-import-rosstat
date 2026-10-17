package bank

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// appendedRow — строка, которую разбор импортёра пишет в таблицу. У таблиц пакета с колонкой
// name (domrf_mortgage, rus_vtb_group_ifrs, …) name — имя ряда из формы Append (name, date,
// value); у однорядных таблиц колонки name нет, и поле остаётся пустым.
type appendedRow struct {
	name  string
	date  time.Time
	value float64
}

// appendRecorder — заглушка driver.Batch для офлайн-тестов (стандарт §11.6): запоминает строки
// вместо записи в ClickHouse. Append намеренно консервативен по вводу: импортёры пишут значение
// то как float64, то как float32 (legacy-колонки пакета), то как строку (значение ячейки xlsx),
// а формы бывают (date, value), (name, date, value) и широкая строка company_financials
// (companyRow), которую объявляет newCompanyRecorder. Незнакомая форма или тип — не паника, а
// ошибка, которую Append возвращает и запоминает в err: импортёр, пробрасывающий ошибку Append,
// донесёт её до теста сам, а если он её проигнорирует, тест увидит err сам. Send, Flush и Abort —
// явные no-op: встроенный driver.Batch — nil-интерфейс, и вызов любого его метода паниковал бы.
type appendRecorder struct {
	driver.Batch
	cols        []string // ожидаемые колонки в порядке DDL; пусто — форма не объявлена
	rows        []appendedRow
	company     []companyRow // строки company_financials: у них своя широкая форма (companyRow)
	companyForm bool         // форма объявлена как company_financials, а не угадана по длине строки
	err         error        // первая ошибка формы/типа: сообщает тест, паники нет
	appendErr   error        // заданная ошибка Append: возвращается до нормализации, строка не пишется
}

// newAppendRecorder — приёмник для тестовых разборов. cols — ожидаемые колонки в порядке DDL
// (можно не объявлять: тогда принимаются формы (date, value) и (name, date, value); широкая
// форма company_financials объявляется отдельно — newCompanyRecorder).
func newAppendRecorder(cols ...string) *appendRecorder {
	return &appendRecorder{cols: cols}
}

// newCompanyRecorder — приёмник широкой формы company_financials (десять колонок DDL). Форма
// объявляется явно, а не угадывается по длине строки: иначе строка любой широкой таблицы пакета
// с тем же числом колонок молча стала бы строкой company_financials (спека §2.4).
func newCompanyRecorder(cols ...string) *appendRecorder {
	rec := newAppendRecorder(cols...)
	rec.companyForm = true
	return rec
}

func (r *appendRecorder) Send() error  { return nil }
func (r *appendRecorder) Flush() error { return nil }
func (r *appendRecorder) Abort() error { return nil }

func (r *appendRecorder) Append(v ...any) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	if r.err == nil {
		r.err = r.normalize(v)
	}
	return r.err
}

// normalize принимает одну строку Append в известной форме и раскладывает её по rows (строки
// сырых таблиц) или company (широкая строка company_financials). Ошибки называют тип и форму, а
// не паникуют: расхождение схемы в тесте обязано читаться как сообщение теста.
func (r *appendRecorder) normalize(v []any) error {
	if len(r.cols) > 0 && len(v) != len(r.cols) {
		return fmt.Errorf("строка из %d значений, а объявлена схема из %d (%v)", len(v), len(r.cols), r.cols)
	}
	if r.companyForm {
		if len(v) != len(companyFinancialsColumnNames) {
			return fmt.Errorf("строка company_financials из %d значений, а форма объявлена на %d колонок (%v)",
				len(v), len(companyFinancialsColumnNames), companyFinancialsColumnNames)
		}
		row, err := companyRowFrom(v)
		if err != nil {
			return err
		}
		r.company = append(r.company, row)
		return nil
	}
	row := appendedRow{}
	switch len(v) {
	case 2: // (date, value)
	case 3: // (name, date, value)
		name, ok := v[0].(string)
		if !ok {
			return fmt.Errorf("имя ряда пришло как %T, ожидалась строка (значения: %v)", v[0], v)
		}
		row.name, v = name, v[1:]
	default:
		return fmt.Errorf("незнакомая форма строки: %d значений (ожидается (date, value), (name, date, value); широкая форма company_financials объявляется newCompanyRecorder)",
			len(v))
	}
	date, ok := v[0].(time.Time)
	if !ok {
		return fmt.Errorf("дата пришла как %T, ожидался time.Time", v[0])
	}
	value, err := recorderFloat(v[1])
	if err != nil {
		return err
	}
	row.date, row.value = date, value
	r.rows = append(r.rows, row)
	return nil
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

// companyFinancialsColumnNames — колонки company_financials в порядке DDL: их число — длина
// широкой формы, которую объявляет newCompanyRecorder, а имена называет ошибка companyRowFrom.
// Число и список держатся в одном месте: разойдясь, они проверяли бы не ту форму и молча.
var companyFinancialsColumnNames = []string{
	"company", "metric", "period", "period_type", "source_kind",
	"value", "unit", "source_url", "source_page", "loaded_at",
}

// companyRow — строка широкой формы company_financials, как её пишет импортёр bank_company_metrics.
// Отдельная от appendedRow: у формы нет ни одного имени ряда, ни одиночной даты, и приведение к
// appendedRow потеряло бы её контракт (тип периода, вид источника, документ, провенанс страницы).
type companyRow struct {
	company    string
	metric     string
	period     string
	periodType string
	sourceKind string
	value      float64
	unit       string
	sourceUrl  string
	sourcePage uint16
	loadedAt   time.Time
}

// companyRowFrom разбирает одну широкую строку Append по колонкам DDL. Типы проверяются строго:
// форма строки — часть контракта импортёра, и расхождение обязано читаться из ошибки, а не
// теряться в рекордере (значение принимается теми же типами, что и у строк сырых таблиц).
func companyRowFrom(v []any) (companyRow, error) {
	var row companyRow
	for _, field := range []struct {
		col int
		dst *string
	}{
		{0, &row.company},
		{1, &row.metric},
		{2, &row.period},
		{3, &row.periodType},
		{4, &row.sourceKind},
		{6, &row.unit},
		{7, &row.sourceUrl},
	} {
		text, ok := v[field.col].(string)
		if !ok {
			return companyRow{}, fmt.Errorf("колонка %q пришла как %T, ожидалась строка (значения: %v)",
				companyFinancialsColumnNames[field.col], v[field.col], v)
		}
		*field.dst = text
	}
	value, err := recorderFloat(v[5])
	if err != nil {
		return companyRow{}, fmt.Errorf("колонка %q: %w", companyFinancialsColumnNames[5], err)
	}
	row.value = value
	page, ok := v[8].(uint16)
	if !ok {
		return companyRow{}, fmt.Errorf("колонка %q пришла как %T, ожидался uint16 (значения: %v)",
			companyFinancialsColumnNames[8], v[8], v)
	}
	row.sourcePage = page
	loadedAt, ok := v[9].(time.Time)
	if !ok {
		return companyRow{}, fmt.Errorf("колонка %q пришла как %T, ожидался time.Time (значения: %v)",
			companyFinancialsColumnNames[9], v[9], v)
	}
	row.loadedAt = loadedAt
	return row, nil
}

// Широкая форма company_financials объявляется явно — newCompanyRecorder: длина строки не признак
// формы. Рекордер с теми же десятью колонками, но без company-формы, обязан отвергнуть
// 10-значную строку как незнакомую: эвристика len(v)==10 молча принимала бы за строку
// company_financials любую широкую таблицу пакета с десятью колонками (спека §2.4).
func TestAppendRecorderCompanyFormExplicit(t *testing.T) {
	row := []any{
		"SBER", "total_assets", "2026M06", "M", "ras",
		float64(65355.1), "млрд ₽", "https://example.test/sber.xlsx", uint16(0),
		time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC),
	}

	plain := newAppendRecorder(companyFinancialsColumnNames...)
	if err := plain.Append(row...); err == nil {
		t.Fatalf("рекордер без company-формы принял 10 значений за строку company_financials: %+v", plain.company)
	}
	if len(plain.company) != 0 {
		t.Fatalf("рекордер без company-формы записал %d company-строк, want 0", len(plain.company))
	}

	rec := newCompanyRecorder(companyFinancialsColumnNames...)
	if err := rec.Append(row...); err != nil {
		t.Fatalf("company-рекордер отверг строку company_financials: %v", err)
	}
	if len(rec.company) != 1 {
		t.Fatalf("company-рекордер записал %d company-строк, want 1", len(rec.company))
	}
	if got := rec.company[0]; got.company != "SBER" || got.metric != "total_assets" || got.period != "2026M06" {
		t.Errorf("company-строка разобрана как %+v, want SBER/total_assets/2026M06", got)
	}
}

func TestAppendRecorderBatchMethodsDoNotPanic(t *testing.T) {
	rec := newAppendRecorder("name", "date", "balance")
	if err := rec.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := rec.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := rec.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}
