package mlregistry

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type mlRegistry struct{}

func (s *mlRegistry) Name() string {
	return "ml_registry"
}

// Import — schema-only: создаёт таблицы ml_runs/ml_predictions и витрины поверх
// них. Внешних источников не читает и данных не пишет, поэтому всегда возвращает
// 0 строк; запускается тем же путём, что и остальные импортёры (make import / dagu).
func (s *mlRegistry) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{mlRunsDdl, mlPredictionsDdl} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, err
		}
	}
	log.Infof("ml_runs/ml_predictions ensured")
	createdViews := 0
	for _, v := range mlViewsList {
		created, err := util.CreateView(ctx, conn, v)
		if err != nil {
			return 0, err
		}
		if created {
			createdViews++
		}
		log.Infof("View %s created: %t", v.Name, created)
	}
	log.Infof("ml_registry: views created %d/%d", createdViews, len(mlViewsList))
	return 0, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &mlRegistry{})
}
