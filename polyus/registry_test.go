package polyus

import (
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// importerRegisteredAs сообщает, лежит ли в реестре импортёр с таким Name().
func importerRegisteredAs(name string) bool {
	for _, stat := range chimport.Stats {
		if stat.Name() == name {
			return true
		}
	}

	return false
}

// TestImportersAreReachableByStatName держит имя импортёра в реестре chimport.Stats
// отдельно от имени таблицы, в которую он пишет.
//
// Имя регистрации — это то, что набирают операторы (CLICKHOUSE_IMPORT_STAT=...) и
// то, что стоит в dagu/*.yaml. Имя таблицы контрактом не является: у метрик
// компании оно уже сменилось с polyus_financial_metrics на company_financials.
// Пока обе величины были одной константой, смена имени таблицы молча
// переименовала и импортёр: make import STAT=polyus_financial_metrics отвечал
// "no importer named ...", а шаг polyus_financial_metrics в dagu/financial.yaml
// перестал совпадать с чем-либо в реестре.
//
// Проверяется именно реестр, а не Import напрямую: остальные тесты пакета зовут
// парсеры и Import, и потому проходили, пока импортёр был недостижим ни из CLI,
// ни из расписания. Регистрация — единственное, что видит оператор.
//
// Ожидаемые имена здесь литералами, а не константами пакета — и это не
// дублирование. Тест, сверяющий financialMetricsStatName с самим собой, прошёл бы
// и после того, как Name() вернули бы к имени таблицы; а литерал падает ровно на
// этой правке. По той же причине литералом записан и databook_polyus: второй
// импортёр регистрируется из того же пакета и от той же ошибки не защищён ничем —
// его Name() тоже возвращает константу имени таблицы (datapackTable).
func TestImportersAreReachableByStatName(t *testing.T) {
	for _, name := range []string{"polyus_financial_metrics", "databook_polyus"} {
		if !importerRegisteredAs(name) {
			t.Errorf(
				"no importer registered as %q in chimport.Stats: "+
					"make import STAT=%s and the dagu step of the same name would match nothing. "+
					"Registry names are a contract (CLICKHOUSE_IMPORT_STAT and dagu/*.yaml); "+
					"they must not follow the table name",
				name,
				name,
			)
		}
	}
}

// Разбор обязан проставлять SourceKind в КАЖДОЙ записи. Тест на ключ батча этого
// не ловит: он строит записи сам, поэтому пропущенное поле в конструкторе
// (recordsFromLine, parseIFRSPage) не заметит — а запись с пустым SourceKind
// снова столкнётся с любой другой за тот же период, то есть вернёт ровно тот
// дефект, ради которого измерение и заводилось.
func TestParsedRecordsCarrySourceKind(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		kind    string
		page    int
	}{
		{"testdata/press_reliz_1h26_p1.tsv", "kpi", 1},
		{"testdata/press_release_fy2024_p4.tsv", "kpi", 4},
		{"testdata/en_msfo_p6.tsv", "ifrs", 6},
	} {
		lines, err := readTSVLines(tc.fixture)
		if err != nil {
			t.Fatalf("%s: %v", tc.fixture, err)
		}

		if tc.kind == "ifrs" {
			records, err := parseIFRSPage(tc.fixture, tc.fixture, tc.page, "2026H1")
			if err != nil {
				t.Fatalf("%s: %v", tc.fixture, err)
			}
			for _, r := range records {
				if r.SourceKind != tc.kind {
					t.Errorf("%s: %s carries SourceKind %q, want %q", tc.fixture, r.Metric, r.SourceKind, tc.kind)
				}
			}

			continue
		}

		records, _, _ := parseKPILines(lines, tc.fixture, tc.page)
		if len(records) == 0 {
			t.Fatalf("%s: no records to check", tc.fixture)
		}
		for _, r := range records {
			if r.SourceKind != tc.kind {
				t.Errorf("%s: %s carries SourceKind %q, want %q", tc.fixture, r.Metric, r.SourceKind, tc.kind)
			}
		}
	}
}
