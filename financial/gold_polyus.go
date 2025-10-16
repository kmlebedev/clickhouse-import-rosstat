package financial

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// https://www.polyus.com/ru/investors/results-and-reports/
func init() {
	FinDataBookPolyus := FinDataBook{
		name:         "databook_polyus",
		dataBookPath: "financial/data/polyus_datapack_1h25.xlsx",
		tables:       map[string][]string{},
		insertRow:    "INSERT INTO %s VALUES (?, ?, ?, ?, ?)",
		createTable: `CREATE TABLE IF NOT EXISTS %s (
		   table LowCardinality(String),
 		   name LowCardinality(String),
           segment LowCardinality(String),
		   quarter Date,
           value Float32
		) ENGINE = ReplacingMergeTree()
		ORDER BY (table, name, segment, quarter)`,
	}

	FinDataBookPolyus.tables["Sheet1"] = []string{"OPERATING RESULTS*"}

	chimport.Stats = append(chimport.Stats, &FinDataBookPolyus)
}
