package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bank"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/cbr"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fao"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/rosstat"
	log "github.com/sirupsen/logrus"
	"os"
)

var (
	ctx = context.Background()
)

func main() {
	clickhouseOptions, _ := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		log.Fatal(err)
	}
	if err = conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	log.Infof("Connected to clickhouse")
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
