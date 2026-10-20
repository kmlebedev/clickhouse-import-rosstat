package minfin

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// minfinImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Поля table и countPoints
// не видит никто, кроме теста TestMinfinImportersPrefixedAndRegistered: по ним
// проверяются префикс таблицы и совпадение объявленного в реестре префлайта с
// объявлением самого импортёра (Publication()).
type minfinImporter struct {
	chimport.ImportStat
	table string
	// countPoints — литерал PublicationCountPoints импортёра ("" — префлайта нет).
	// Литерал, а не метод Publication(): именно расхождение этих двух источников
	// значения и обязан ловить тест реестра.
	countPoints string
}

// minfinImporters — единственная точка регистрации импортёров Минфина: персональных
// init()-append по файлам импортёров больше нет (стандарт §8.1). Порядок повторяет
// dagu/minfin.yaml; в chimport.Stats порядок значения не имеет.
//
// Таблицы и запросы чека записаны литералами намеренно (стандарт §8.2): ссылка на
// константу импортёра сделала бы проверку префикса тавтологичной — тест сравнивал бы
// константу с самой собой и прошёл бы ровно на переименовании таблицы в имя вне
// minfin_-пространства.
var minfinImporters = []minfinImporter{
	{fedbudMes, "minfin_fed_bud_mes", ""},
	{fedbudMesyats, "minfin_fed_bud_mesyats", ""},
	{oilGasMes, "minfin_oil_gas_mes", "SELECT count() FROM minfin_oil_gas_mes FINAL"},
	{ofzAuctionImporter, "minfin_ofz_auction", ""},
	{debtStockImporter, "minfin_debt_stock", ""},
	{debtServiceImporter, "minfin_debt_service", ""},
}

// init — единственная регистрация импортёров Минфина в chimport.Stats. main.go
// запускает всех (или фильтрует по CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range minfinImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
