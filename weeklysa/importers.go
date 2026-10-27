package weeklysa

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// weeklySAImporterEntry — запись реестра пакета: импортёр, таблица и литерал
// количественного чека версии. Литералы, а не ссылки на константы: тест
// сверяет реестр с объявлением импортёра, и сравнение константы с самой собой
// прошло бы ровно на той правке, от которой проверка защищает.
type weeklySAImporterEntry struct {
	chimport.ImportStat
	table       string
	countPoints string
}

// weeklySAImporters — единственная точка регистрации импортёров пакета.
var weeklySAImporters = []weeklySAImporterEntry{
	{&weeklySAImporter{runner: runX13}, "weekly_cpi_sa", "SELECT count() FROM weekly_cpi_sa FINAL"},
}

// init — единственная регистрация пакета в chimport.Stats.
func init() {
	for _, imp := range weeklySAImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
