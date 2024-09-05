package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests_std "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/rosstat"
	log "github.com/sirupsen/logrus"
)

func main() {
	conn, err := clickhouse_tests_std.GetOpenDBConnection("rosstat", clickhouse.Native, nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))
	for _, stat := range chimport.Stats {
		var rows int64
		if rows, err = stat.Import(ctx, conn); err != nil {
			log.Errorf("%s: %+v", stat.Name(), err)
		}
		log.Infof("Imported %d rows of %s", rows, stat.Name())
	}
}
