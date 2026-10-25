package ofz

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// ofzImporters is the single registration point for the package; the importer
// itself has no personal init().
var ofzImporters = []chimport.ImportStat{
	&ofzIssueAnalytics{},
}

func init() {
	chimport.Stats = append(chimport.Stats, ofzImporters...)
}
