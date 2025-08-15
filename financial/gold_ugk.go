package financial

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

func init() {
	FinDataBookUGK := FinDataBook{
		name:         "databook_ugk",
		dataBookPath: "financial/data/ЮГК_databook_2024.xlsx",
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

	FinDataBookUGK.tables["Выручка по сегментам"] = []string{"Производство", "Выручка по сегментам", "Сегментная EBITDA"}

	chimport.Stats = append(chimport.Stats, &FinDataBookUGK)
}
