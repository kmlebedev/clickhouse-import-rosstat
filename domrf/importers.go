package domrf

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// Реестр импортёров пакета: регистрация живёт здесь, а не персональными init() в файлах
// импортёров (стандарт §8.1).
func init() {
	chimport.Stats = append(chimport.Stats,
		newDomrfMortgageMarketImporter(),
		&domrfMortgageWeekly{},
		&domrfPriceIndex{},
	)
}
