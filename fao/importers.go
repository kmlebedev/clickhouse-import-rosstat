package fao

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// faoImporter — запись реестра импортёров пакета: сам импортёр, таблица и
// запрос количественного чека версии (префлайта). У ФАО префлайта нет:
// файл обновляется раз в месяц, шаг DAG — раз в месяц (§9.3).
type faoImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

// faoImporters — единственная точка регистрации импортёров пакета. Таблица и
// запрос чека — литералами (§8.2): ссылка на константу сделала бы проверку
// префикса тавтологичной.
var faoImporters = []faoImporter{
	{faoFoodPriceImporter, "fao_food_price_Indices_monthly", ""},
}

func init() {
	for _, imp := range faoImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
