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
