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
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fred"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/gold"
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
	clickhouseOptions, err := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		log.Fatal("invalid CLICKHOUSE_URL")
	}
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		log.Fatal(err)
	}
	if err = conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	log.Infof("Connected to clickhouse")
	requested := parseImportFilter(os.Getenv("CLICKHOUSE_IMPORT_STAT"))
	matched := make(map[string]bool)
	failed := false
	for _, stat := range chimport.Stats {
		if len(requested) > 0 && !slices.Contains(requested, stat.Name()) {
			continue
		}
		matched[stat.Name()] = true
		var rows int64
		if rows, err = stat.Import(ctx, conn); err != nil {
			log.Errorf("%s: %+v", stat.Name(), err)
			failed = true
		}
		log.Infof("Imported %d rows of %s", rows, stat.Name())
	}
	for _, name := range requested {
		if !matched[name] {
			log.Warnf("no importer named %q", name)
		}
	}
	if failed {
		os.Exit(1)
	}
}

func parseImportFilter(value string) []string {
	var names []string
	for _, name := range strings.Split(value, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}
