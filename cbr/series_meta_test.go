package cbr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// appendedRow — строка, которую разбор импортёра пишет в таблицу. У таблиц с колонкой
// name (m2, credit_m2x, households) name — имя ряда из разбора; у однорядных таблиц
// (cbr_currency_usd, cbr_gold, cbr_key_rate) колонки name нет, и имя ряда задаёт тест
// через appendRecorder.series.
type appendedRow struct {
	name  string
	date  time.Time
	value float64
}

// appendRecorder — заглушка driver.Batch для офлайн-тестов: запоминает строки вместо
// записи в ClickHouse. Append намеренно консервативен по вводу: импортёры пишут значение
// то как float64, то как string (значение ячейки xlsx), а формы бывают (date, value) и
// (name, date, value). Незнакомая форма или тип — не паника, а ошибка, которую Append
// возвращает и запоминает в err: импортёр, пробрасывающий ошибку Append, донесёт её до
// теста сам, а если он её проигнорирует, тест увидит err через checkCbrSeriesMeta.
// Сырые строки остаются в raw: импортёрам с широкой строкой (ruonia: date + 7 метрик)
// удобнее разбирать raw самим, чем подгонять её под appendedRow.
type appendRecorder struct {
	driver.Batch
	series string   // имя ряда однорядной таблицы: у неё в таблице нет колонки name
	cols   []string // ожидаемые колонки в порядке DDL; пусто — форма не объявлена
	raw    [][]any  // строки в том виде, в каком их передал импортёр
	rows   []appendedRow
	err    error // первая ошибка формы/типа: сообщает тест, паники нет
}

// newCbrRecorder — приёмник для тестовых разборов. cols — ожидаемые колонки в порядке DDL
// (можно не объявлять: тогда принимаются обе формы, (date, value) и (name, date, value)).
func newCbrRecorder(cols ...string) *appendRecorder {
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
// паникует: расхождение схемы в тесте T5/T6 должно читаться как сообщение теста.
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
	value, err := cbrRecorderFloat(v[1])
	if err != nil {
		return appendedRow{}, err
	}
	row.date, row.value = date, value
	return row, nil
}

// cbrRecorderFloat принимает значение из Append в тех типах, которыми пишут импортёры:
// float64 (парсеры считают число сами), float32 (ruonia парсит с битностью 32), int/int64
// (счётчики сделок) и string (значение ячейки xlsx, десятичный разделитель — точка или
// запятая). Остальное — понятная ошибка, а не паника.
func cbrRecorderFloat(v any) (float64, error) {
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

// appendCbrPoints пишет точки однорядной таблицы так же, как CrawFunc: (date, value).
// Имя ряда такой таблицы берётся из rec.series.
func appendCbrPoints(rec *appendRecorder, points []cbrPoint) error {
	for _, p := range points {
		if err := rec.Append(p.date, p.value); err != nil {
			return err
		}
	}
	return nil
}

// describeCbrSeriesMeta проверяет полноту описаний и уникальность рядов и возвращает
// множество описанных имён.
func describeCbrSeriesMeta(t *testing.T, source string, meta []util.SeriesMeta) map[string]bool {
	t.Helper()
	described := make(map[string]bool, len(meta))
	for _, m := range meta {
		if m.Source != source || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q (%s) заполнено не полностью", m.Series, source)
		}
		if described[m.Series] {
			t.Errorf("ряд %q (%s) описан дважды", m.Series, source)
		}
		described[m.Series] = true
	}
	return described
}

// checkCbrSeriesMeta проверяет двусторонний контракт каталога series_meta и разбора на
// фикстуре: каждый ряд из разбора описан в каталоге (разбор ⊆ каталог) и каждый
// описанный ряд встречается в разборе (каталог ⊆ разбор). writeRows пишет строки в
// приёмник так же, как это делает импортёр. Пустое имя ряда недопустимо: у однорядных
// таблиц его задаёт тест (appendRecorder.series).
func checkCbrSeriesMeta(t *testing.T, source string, meta []util.SeriesMeta, writeRows func(rec *appendRecorder) error) {
	t.Helper()
	described := describeCbrSeriesMeta(t, source, meta)
	rec := newCbrRecorder()
	if err := writeRows(rec); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	// Импортёр мог проигнорировать ошибку Append — тогда форма строки разошлась со схемой,
	// и «разбор» стоит на неполных данных: сообщаем об этом явно, а не молча.
	if rec.err != nil {
		t.Fatalf("разбор фикстуры: схема строк не совпала с разбором: %v", rec.err)
	}
	if len(rec.rows) == 0 {
		t.Error("разбор не дал ни одной точки: каталог проверить нечем")
	}
	seen := make(map[string]bool, len(rec.rows))
	for _, row := range rec.rows {
		if row.name == "" {
			t.Error("строка разбора без имени ряда: у однорядных таблиц его задаёт тест (appendRecorder.series)")
			continue
		}
		if !described[row.name] {
			t.Errorf("ряд %q из разбора не описан в каталоге (%s)", row.name, source)
		}
		seen[row.name] = true
	}
	for name := range described {
		if !seen[name] {
			t.Errorf("описанный ряд %q (%s) не встретился при разборе", name, source)
		}
	}
}

// cbrFixtureRows разбирает фикстуру xlsx тем же кодом, что и импортёр (ImportFunc), и
// отдаёт строки, которые импортёр записал бы в таблицу. Ошибка разбора, расхождение формы
// строки со схемой приёмника и пустой разбор — падение теста: фикстура T6 и есть контракт.
func cbrFixtureRows(t *testing.T, file string, parse func(xlsx *excelize.File, batch driver.Batch) error) []appendedRow {
	t.Helper()
	f, err := excelize.OpenFile(file)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", file, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = parse(f, rec); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", file, err)
	}
	if rec.err != nil {
		t.Fatalf("разбор фикстуры %s: схема строк не совпала с разбором: %v", file, rec.err)
	}
	if len(rec.rows) == 0 {
		t.Fatalf("разбор фикстуры %s не дал ни одной точки", file)
	}
	return rec.rows
}

// cbrSpotEqual сравнивает значение разбора со значением из baseline-снимка БД. Обе стороны
// приводим к Float32: колонки ЦБ — Float32, поэтому 4668.3 из ячейки файла и
// 4668.2998046875 из витрины — одно и то же число, и допуск 1e-6 честен только после
// приведения к машинной точности столбца.
func cbrSpotEqual(got, want float64) bool {
	return math.Abs(float64(float32(got))-float64(float32(want))) <= 1e-6
}

// cbrCheckSpot сверяет одну точку разбора со спотом из baseline (файл
// .superpowers/sdd/2026-10-13-cbr-refactor/cbr-baseline.md, снимок витрины v_cbr_macro).
func cbrCheckSpot(t *testing.T, rows []appendedRow, name string, date time.Time, want float64) {
	t.Helper()
	const layout = "2006-01-02"
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			if !cbrSpotEqual(row.value, want) {
				t.Errorf("точка (%q, %s) = %v, want %v (baseline)", name, date.Format(layout), row.value, want)
			}
			return
		}
	}
	t.Errorf("в разборе нет точки (%q, %s): спот baseline проверить нечем", name, date.Format(layout))
}

// TestAppendRecorderAcceptsImporterForms: формы и типы, которыми реально пишут импортёры
// T5/T6, принимаются как есть — значение приходит float64, float32, int64 или строкой.
func TestAppendRecorderAcceptsImporterForms(t *testing.T) {
	date := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		series string
		cols   []string
		args   []any
		want   appendedRow
	}{
		"однорядная (date, float64)": {
			series: cbrCurrencyUSDTable,
			cols:   []string{"date", "value"},
			args:   []any{date, 85.4811},
			want:   appendedRow{name: cbrCurrencyUSDTable, date: date, value: 85.4811},
		},
		"однорядная (date, float32)": {
			series: cbrGoldTable,
			cols:   []string{"date", "value"},
			args:   []any{date, float32(11422.95)},
			want:   appendedRow{name: cbrGoldTable, date: date, value: float64(float32(11422.95))},
		},
		"однорядная (date, string с запятой)": {
			series: cbrCurrencyUSDTable,
			args:   []any{date, "85,4811"},
			want:   appendedRow{name: cbrCurrencyUSDTable, date: date, value: 85.4811},
		},
		"табличная (name, date, float64)": {
			cols: []string{"name", "date", "value"},
			args: []any{"наблюдаемая инфляция", date, 14.2},
			want: appendedRow{name: "наблюдаемая инфляция", date: date, value: 14.2},
		},
		"табличная (name, date, string точки)": {
			args: []any{"М2 SA", date, "21.3"},
			want: appendedRow{name: "М2 SA", date: date, value: 21.3},
		},
		"табличная (name, date, int64)": {
			args: []any{"trans", date, int64(7)},
			want: appendedRow{name: "trans", date: date, value: 7},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder(tc.cols...)
			rec.series = tc.series
			if err := rec.Append(tc.args...); err != nil {
				t.Fatalf("Append(%v): %v", tc.args, err)
			}
			if len(rec.rows) != 1 {
				t.Fatalf("записано строк %d, want 1", len(rec.rows))
			}
			got := rec.rows[0]
			if got.name != tc.want.name || !got.date.Equal(tc.want.date) || math.Abs(got.value-tc.want.value) > 1e-6 {
				t.Errorf("строка = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestAppendRecorderRejectsForeignShapesWithoutPanic: форма или тип, которых у импортёров
// T5/T6 нет (широкая строка ruonia, дата строкой, имя не строкой, значение не числом),
// дают ошибку схемы. Паника здесь уронила бы тест — то есть тест и есть проверка «не
// паникует»; сообщение об ошибке при этом должно называть тип и форму.
func TestAppendRecorderRejectsForeignShapesWithoutPanic(t *testing.T) {
	date := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
	for name, args := range map[string][]any{
		"широкая строка (date + 7 метрик)": {date, 1.0, 2.0, 3, 4.0, 5.0, 6.0, 7.0},
		"дата строкой":                     {"08.10.2026", 85.4811},
		"имя не строкой":                   {123, date, 85.4811},
		"значение не числом":               {date, struct{}{}},
		"значение nil":                     {date, nil},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			rec.series = cbrCurrencyUSDTable
			err := rec.Append(args...)
			if err == nil {
				t.Fatalf("Append(%v): ожидалась ошибка формы/типа, а строка принята", args)
			}
			if rec.err == nil {
				t.Fatal("ошибка Append не запомнена в rec.err: тест узнает о расхождении схемы только от импортёра")
			}
			if len(rec.raw) != 1 {
				t.Errorf("сырых строк %d, want 1", len(rec.raw))
			}
		})
	}
}

// TestAppendRecorderRejectsSchemaMismatch: объявленная схема колонок ловит расхождение формы.
func TestAppendRecorderRejectsSchemaMismatch(t *testing.T) {
	rec := newCbrRecorder("date", "value")
	if err := rec.Append(time.Time{}, 1.0, "лишнее"); err == nil {
		t.Error("строка из 3 значений при схеме из 2 должна давать ошибку, а не приниматься")
	}
}

func cbrAllSeriesMeta() [][]util.SeriesMeta {
	return [][]util.SeriesMeta{
		cbrKeyRateSeriesMeta,
		cbrCurrencyUSDSeriesMeta,
		cbrGoldSeriesMeta,
		cbrRuaniaSeriesMeta,
		cbrM2SeriesMeta,
		cbrCreditM2xSeriesMeta,
		cbrBankIntRateSeriesMeta,
		cbrIntRateLoansSeriesMeta,
		cbrIndicatorsCpdSeriesMeta,
		cbrInflExpSeriesMeta,
		cbrLoansToIndSeriesMeta,
		cbrLoansToCorpSeriesMeta,
		householdsBMesSeriesMeta,
	}
}

// TestSeriesMetaWellFormed: у каждого ряда заполнены все поля, ключи (source, series) не дублируются.
func TestSeriesMetaWellFormed(t *testing.T) {
	seen := make(map[string]bool)
	for _, meta := range cbrAllSeriesMeta() {
		for _, m := range meta {
			if m.Source != cbrSource {
				t.Errorf("source %q != %q (series %q)", m.Source, cbrSource, m.Series)
			}
			for field, v := range map[string]string{
				"series": m.Series, "title": m.Title, "unit": m.Unit,
				"frequency": m.Frequency, "origin": m.Origin, "description": m.Description,
			} {
				if v == "" {
					t.Errorf("series %q: пустое поле %s", m.Series, field)
				}
			}
			key := m.Source + "\x00" + m.Series
			if seen[key] {
				t.Errorf("дубль ряда (source=%s, series=%q)", m.Source, m.Series)
			}
			seen[key] = true
		}
	}
	if len(seen) == 0 {
		t.Fatal("каталог cbr пуст")
	}
}

// TestNamedListsCovered: каждое имя из фиксированных списков импортёров описано в каталоге.
func TestNamedListsCovered(t *testing.T) {
	catalog := make(map[string]bool)
	for _, meta := range cbrAllSeriesMeta() {
		for _, m := range meta {
			catalog[m.Series] = true
		}
	}
	lists := map[string][]string{
		"cbr_indicators_cpd":        cbrIndicatorsCpdNames,
		"cbr_infl_exp":              cbrInflExpNames,
		"cbr_credit_m2x":            cbrCreditM2xNames,
		"cbr_bank_int_rate":         cbrBankIntRateNames,
		"cbr_int_rate_loans":        cbrIntRateLoansNames,
		"cbr_loans_to_individuals":  cbrLoansToIndNames,
		"cbr_loans_to_corporations": cbrLoansToCorpNames,
	}
	for table, names := range lists {
		for _, name := range names {
			if !catalog[name] {
				t.Errorf("%s: name %q не описан в каталоге", table, name)
			}
		}
	}
}
