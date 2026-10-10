package dcf

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
)

// dcfEngine — импортёр расчётного ядра DCF (Name() — значение CLICKHOUSE_IMPORT_STAT
// и имя шага в dagu). Владеет таблицами mine_plans, price_decks, nav_by_asset;
// model_runs не пишет — это делает ingest-endpoint.
type dcfEngine struct{}

func (s *dcfEngine) Name() string { return "dcf_engine" }

func (s *dcfEngine) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{minePlansCreateTable, priceDecksCreateTable, navByAssetCreateTable} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, err
		}
	}
	log.Infof("dcf tables ensured; core not wired yet")

	return 0, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &dcfEngine{})
}
