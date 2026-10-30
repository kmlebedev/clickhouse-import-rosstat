package transaq

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// transaqImporters is the single registration point for the package; the importer
// itself has no personal init().
var transaqImporters = []chimport.ImportStat{
	&transaqNetFlows{},
}

func init() {
	chimport.Stats = append(chimport.Stats, transaqImporters...)
}
