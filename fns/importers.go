package fns

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// fnsImporter — запись реестра пакета: сам импортёр, таблица, в которую он пишет, и
// запрос количественного чека версии (префлайта). Поля table и countPoints не видит
// никто, кроме теста-энфорсера: по ним проверяются префикс таблицы и совпадение
// объявленного в реестре префлайта с объявлением самого импортёра (стандарт §8.3).
type fnsImporter struct {
	chimport.ImportStat
	table string
	// countPoints — литерал PublicationCountPoints импортёра. Пустая строка означает
	// «префлайта нет», и у этого импортёра это так: артефактов много (по архиву на
	// каждый отчётный месяц), поэтому отпечаток одного URL не отвечает на вопрос, что
	// уже разобрано (стандарт §9.3). Литерал, а не метод Publication(): тест обязан
	// ловить расхождение этих двух мест.
	countPoints string
}

// fnsImporters — единственная точка регистрации импортёров ФНС (стандарт §8.1).
// Таблица записана литералом намеренно: ссылка на константу пакета сделала бы
// проверку префикса тавтологичной. Порядок повторяет dagu/fns.yaml.
var fnsImporters = []fnsImporter{
	{fnsBudgetRevenueImporter, "fns_budget_revenue", ""},
}

// init — единственная регистрация импортёров ФНС в chimport.Stats; main.go запускает
// всех либо фильтрует по CLICKHOUSE_IMPORT_STAT=Name().
func init() {
	for _, imp := range fnsImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
