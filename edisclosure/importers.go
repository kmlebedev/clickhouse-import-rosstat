package edisclosure

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// edisclosureImporters — единственная точка регистрации импортёров пакета.
// Пока запись одна: edisclosure_documents; dagu/edisclosure.yaml содержит
// ровно один шаг с этим именем (тест TestImportersRegistry).
var edisclosureImporters = []chimport.ImportStat{edisclosureImporter}

// init — регистрация импортёров пакета в chimport.Stats. Порядок append
// совпадает с порядком реестра; main.go запускает всех или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name(). Сетевых вызовов и чтения окружения здесь нет.
// append с раскрытием среза, а не цикл: элемент реестра уже chimport.ImportStat,
// и staticcheck (S1011) не пропустил бы поэлементный append.
func init() {
	chimport.Stats = append(chimport.Stats, edisclosureImporters...)
}
