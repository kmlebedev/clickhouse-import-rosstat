package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bank"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/cbr"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/craw"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/customs"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fao"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/financial"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/minfin"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/rosstat"
	log "github.com/sirupsen/logrus"
	"os"
	"slices"
	"strings"
)

var (
	ctx = context.Background()
)

func main() {
	if lvl, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(lvl)
	}
	clickhouseOptions, _ := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		log.Fatal(err)
	}
	if err = conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	log.Infof("Connected to clickhouse")
	envStats := strings.Split(os.Getenv("CLICKHOUSE_IMPORT_STAT"), ",")
	for _, stat := range chimport.Stats {
		if len(envStats) > 0 && !slices.Contains(envStats, stat.Name()) {
			continue
		}
		var rows int64
		if rows, err = stat.Import(ctx, conn); err != nil {
			log.Errorf("%s: %+v", stat.Name(), err)
		}
		log.Infof("Imported %d rows of %s", rows, stat.Name())
	}
}
