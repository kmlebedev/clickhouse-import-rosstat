package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests_std "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/cbr"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fao"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/rosstat"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/sber"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {
	conn, err := clickhouse_tests_std.GetOpenDBConnection("rosstat", clickhouse.Native, nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))
	envStat := os.Getenv("CLICKHOUSE_IMPORT_STAT")
	for _, stat := range chimport.Stats {
		if envStat != "" && stat.Name() != envStat {
			continue
		}
		var rows int64
		if rows, err = stat.Import(ctx, conn); err != nil {
			log.Errorf("%s: %+v", stat.Name(), err)
		}
		log.Infof("Imported %d rows of %s", rows, stat.Name())
	}
}
