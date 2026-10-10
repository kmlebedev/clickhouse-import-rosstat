// Package views создаёт витрины для LLM-агента прогноза золота и NAV PLZL:
// v_model_inputs (входы DCF), v_gold_dashboard (дашборд верификации),
// v_forecast_accuracy (точность прогнозов из forecast_log) и
// v_dcf_assumptions (NPV активов по трём ценовым дека'м и двум контурам ставки).
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
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type goldViews struct {
}

// goldViewsList — витрины импортёра gold_views в том порядке, в каком он их
// создаёт. Отдельная переменная уровня пакета, а не литерал на месте цикла:
// список нужен в двух местах — сам цикл создания и тест регистрации, который
// обязан видеть ИМЕННО то, что создаёт импортёр. Литерал внутри Import тест
// повторить не может, и написанный в тесте свой список проверял бы сам себя:
// витрина, выпавшая из импортёра, оставляла бы тест зелёным.
var goldViewsList = []util.View{modelInputsView, goldDashboardView, forecastAccuracyView, dcfAssumptionsView}

func (s *goldViews) Name() string {
	return "gold_views"
}

func (s *goldViews) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, v := range goldViewsList {
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
