// Package views создаёт витрины для LLM-агента прогноза золота и NAV PLZL:
// v_model_inputs (входы DCF), v_gold_dashboard (дашборд верификации),
// v_forecast_accuracy (точность прогнозов из forecast_log),
// v_dcf_assumptions (NPV активов по трём ценовым дека'м и двум контурам ставки),
// v_imf_macro (золотые резервы ЦБ из IMF IRFCL, source='imf'),
// v_gold_forecasts (консенсус аналитиков и own-сетка из gold_forecasts),
// v_gold_etf_flows (пивот месячных потоков и запасов золотых ETF WGC по регионам),
// v_cb_gold_dynamics (динамика резервов семи ЦБ WGC: MoM/YTD и уровень по стране),
// v_gold_etf_flows_daily (дневной пивот семи рядов ETF-контура bold.report)
// и v_gold_etf_funds_daily (пер-фондовый дневной срез того же контура).
//
// Импортёр gold_views запускается после ingest-первого-прогона: util.CreateView
// пропускает витрину, пока не существуют все её таблицы (model_runs создаёт ingest),
// поэтому его вызывают отдельным шагом после первого запуска ingest. Гейт проверяет
// только наличие таблиц, а не их заполненность: rosstat_ipc_mes и rosstat_ipc_weeks
// создаются до загрузки Росстата, и при его сбое витрина создаётся с NULL или устаревшими rosstat_*.
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
//
// Две последние витрины — контур bold.report: v_gold_etf_flows_daily читает
// macro_series (её создаёт любой импортёр рядов), а v_gold_etf_funds_daily —
// bold_fund_snapshot, которую создаёт импортёр bold_etf_flows. Гейт CreateView
// пропустит витрину фондов, пока среза нет, поэтому gold_views запускают после
// первого прогона bold_etf_flows (расписание — dagu/bold.yaml).
var goldViewsList = []util.View{
	modelInputsView, goldDashboardView, forecastAccuracyView, dcfAssumptionsView,
	imfMacroView, goldForecastsView, goldEtfFlowsView, cbGoldDynamicsView,
	goldEtfFlowsDailyView, boldFundsDailyView,
}

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
