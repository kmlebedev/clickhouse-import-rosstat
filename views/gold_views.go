// Package views создаёт витрины для LLM-агента прогноза золота и NAV PLZL:
// v_model_inputs (входы DCF), v_gold_dashboard (дашборд верификации) и
// v_forecast_accuracy (точность прогнозов из forecast_log).
//
// Импортёр gold_views запускается после ingest-первого-прогона: util.CreateView
// пропускает витрину, пока не существуют все её таблицы (model_runs создаёт ingest),
// поэтому его вызывают отдельным шагом после первого запуска ingest. Гейт проверяет
// только наличие таблиц, а не их заполненность: ipc_mes и ipc_weeks создаются до
// загрузки Росстата, и при его сбое витрина создаётся с NULL или устаревшими ipc_*.
package views

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/polyus"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type goldViews struct {
}

func (s *goldViews) Name() string {
	return "gold_views"
}

func (s *goldViews) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = polyus.EnsureMetricsTable(ctx, conn); err != nil {
		return count, err
	}
	for _, v := range []util.View{modelInputsView, goldDashboardView, forecastAccuracyView} {
		var created bool
		if created, err = util.CreateView(ctx, conn, v); err != nil {
			return count, err
		}
		log.Infof("View %s created: %t", v.Name, created)
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &goldViews{})
}
