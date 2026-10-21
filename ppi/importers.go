package ppi

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// ppiImporter — запись реестра пакета: импортёр, таблица и литерал
// количественного чека версии. Литералы, а не ссылки на константы: тест
// сверяет реестр с объявлением импортёра, и сравнение константы с самой собой
// прошло бы ровно на той правке, от которой проверка защищает.
type ppiImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

// ppiImporters — единственная точка регистрации импортёров пакета. Порядок
// совпадает с dagu/ppi.yaml; в chimport.Stats порядок значения не имеет.
var ppiImporters = []ppiImporter{
	{&ppiSAImporter{runner: runX13}, "ppi_sa", "SELECT count() FROM ppi_sa FINAL"},
}

// init — единственная регистрация пакета в chimport.Stats.
func init() {
	for _, imp := range ppiImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
