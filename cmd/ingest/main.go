package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/ingest"
	log "github.com/sirupsen/logrus"
)

const defaultAddr = ":8081"

func main() {
	if lvl, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(lvl)
	}
	token := os.Getenv("INGEST_TOKEN")
	if token == "" {
		log.Fatal("INGEST_TOKEN is required")
	}
	clickhouseOptions, err := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		log.Fatal("invalid CLICKHOUSE_URL")
	}
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err = conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	writer := ingest.NewClickHouseWriter(conn)
	if err = writer.EnsureTables(ctx); err != nil {
		log.Fatal(err)
	}
	addr := os.Getenv("INGEST_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	log.Infof("ingest listening on %s", addr)
	server := &http.Server{
		Addr:              addr,
		Handler:           ingest.NewServer(writer, token),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
