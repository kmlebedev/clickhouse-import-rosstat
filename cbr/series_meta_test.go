package cbr

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
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
// записи в ClickHouse. Колонки идут в порядке DDL импортёра: (date, value) у однорядных
// таблиц и (name, date, value) у таблиц с колонкой name.
type appendRecorder struct {
	driver.Batch
	series string // имя ряда однорядной таблицы: у неё в таблице нет колонки name
	rows   []appendedRow
}

func (r *appendRecorder) Append(v ...any) error {
	row := appendedRow{name: r.series}
	if len(v) == 3 {
		row.name = v[0].(string)
		v = v[1:]
	}
	row.date = v[0].(time.Time)
	row.value = v[1].(float64)
	r.rows = append(r.rows, row)
	return nil
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
	rec := &appendRecorder{}
	if err := writeRows(rec); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
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

func cbrAllSeriesMeta() [][]util.SeriesMeta {
	return [][]util.SeriesMeta{
		cbrKeyRateSeriesMeta,
		cbrCurrencyUSDSeriesMeta,
		cbrGoldSeriesMeta,
		cbrRuaniaSeriesMeta,
		cbrM2SeriesMeta,
		cbrCreditM2xSeriesMeta,
		cbrBankIntRateSeriesMeta,
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

// TestHouseholdsNamesCovered: каждое значение name, которое импортёры cbr пишут в households_b_mes
// (зафиксировано выборкой из локальной БД), описано в каталоге.
func TestHouseholdsNamesCovered(t *testing.T) {
	described := make(map[string]bool, len(householdsBMesSeriesMeta))
	for _, m := range householdsBMesSeriesMeta {
		described[m.Series] = true
	}
	for _, name := range householdsBMesDBNames {
		if !described[name] {
			t.Errorf("name %q есть в households_b_mes, но не описан в каталоге", name)
		}
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

// householdsBMesDBNames — значения name в households_b_mes по состоянию на 2026-10-09.
var householdsBMesDBNames = []string{
	"cтраховщиков",
	"Акции и паи и акции инвестиционных фондов*",
	"Денежные средства на брокерских счетах - всего(5)",
	"Денежные средства на брокерских счетах в иностранной валюте",
	"Денежные средства на брокерских счетах в рублях",
	"Депозитные сертификаты",
	"Депозиты",
	"Депозиты в банках-нерезидентах - всего(2,4)",
	"Депозиты в банках-нерезидентах в иностранной валюте",
	"Депозиты в банках-нерезидентах в рублях",
	"Долговые ценные бумаги",
	"Долговые ценные бумаги нерезидентов - всего(6)",
	"Долговые ценные бумаги нерезидентов в иностранной валюте",
	"Долговые ценные бумаги нерезидентов в рублях",
	"Долгосрочные долговые ценные бумаги резидентов - всего",
	"Долгосрочные долговые ценные бумаги резидентов в иностранной валюте",
	"Долгосрочные долговые ценные бумаги резидентов рублях",
	"Другие депозиты в кредитных организациях - всего",
	"Другие депозиты в кредитных организациях в иностранной валюте",
	"Другие депозиты в кредитных организациях в рублях",
	"Займы, полученные от микрофинансовых компаний",
	"ИЖК, проданные ипотечным агентам, с учетом погашения(9)",
	"Котируемые акции нерезидентов - всего(6)",
	"Котируемые акции нерезидентов в иностранной валюте",
	"Котируемые акции нерезидентов в рублях",
	"Котируемые акции резидентов - всего",
	"Котируемые акции резидентов в иностранной валюте",
	"Котируемые акции резидентов в рублях",
	"Краткосрочные долговые ценные бумаги резидентов - всего",
	"Краткосрочные долговые ценные бумаги резидентов в иностранной валюте",
	"Краткосрочные долговые ценные бумаги резидентов в рублях",
	"Кредиты кредитных организаций - всего",
	"Кредиты кредитных организаций в иностранной валюте",
	"Кредиты кредитных организаций в рублях",
	"Наличная валюта(2)",
	"Наличная иностранная валюта",
	"Наличная национальная валюта",
	"Некотируемые акции резидентов - всего",
	"Паи и акции инвестиционных фондов - нерезидентов - всего(6)",
	"Паи и акции инвестиционных фондов - нерезидентов в иностранной валюте",
	"Паи и акции инвестиционных фондов - нерезидентов в рублях",
	"Паи и акции инвестиционных фондов - резидентов - всего",
	"Паи и акции инвестиционных фондов - резидентов в иностранной валюте",
	"Паи и акции инвестиционных фондов - резидентов в рублях",
	"Пенсионные резервы и пенсионные накопления",
	"Переводные депозиты в кредитных организациях - всего",
	"Переводные депозиты в кредитных организациях в иностранной валюте",
	"Переводные депозиты в кредитных организациях в рублях",
	"Средства на счетах эскроу",
	"Страховые и пенсионные резервы и пенсионные накопления(7)",
	"Страховые резервы",
	"автокредиты - всего",
	"автокредиты в иностранной валюте",
	"автокредиты в рублях",
	"государственного управления",
	"других финансовых организаций",
	"ипотечные жилищные кредиты - всего",
	"ипотечные жилищные кредиты в иностранной валюте",
	"ипотечные жилищные кредиты в рублях",
	"кредитных организаций",
	"начисленные проценты - всего(8)",
	"начисленные проценты в иностранной валюте",
	"начисленные проценты в рублях",
	"нефинансовых организаций",
	"потребительские ссуды - всего",
	"потребительские ссуды в иностранной валюте",
	"потребительские ссуды в рублях",
	"прочие кредиты - всего",
	"прочие кредиты в иностранной валюте",
	"прочие кредиты в рублях",
}
