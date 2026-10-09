package calendar

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	eventsTable = "events_calendar"
	eventsDdl   = `CREATE TABLE IF NOT EXISTS ` + eventsTable + ` (
	    event_date Date,
	    event_time Nullable(String),
	    category LowCardinality(String), -- 'fomc','cpi','nfp','eia','wgc','cbr','polyus_ir','moex_rebalance','gov_rf'
	    title String,
	    threshold String,                -- JSON, напр. '{"crack":">50"}'
	    status LowCardinality(String) DEFAULT 'pending'  -- pending|done|verified
	) ENGINE = ReplacingMergeTree ORDER BY (event_date, category, title);
	`
)

// eventsView — витрина календаря событий-триггеров для MCP-агента.
// Сид данных живёт в sql/events_calendar_q4_2026.sql (идемпотентен за счёт ReplacingMergeTree),
// импортёр только создаёт таблицу и витрину.
var eventsView = util.View{
	Name:    "v_events_calendar",
	Tables:  []string{eventsTable},
	Select:  `SELECT * FROM ` + eventsTable + ` FINAL`,
	Comment: "Календарь событий-триггеров прогноза золота/NAV; status: pending|done|verified",
	Columns: map[string]string{
		"event_date": "Дата события (Date)",
		"event_time": "Время события (местное/UTC, строка; NULL — время не фиксировано)",
		"category":   "Категория триггера: fomc/cpi/nfp/eia/wgc/cbr/polyus_ir/moex_rebalance/gov_rf",
		"title":      "Название события и действие модели при его наступлении",
		"threshold":  "Пороги срабатывания в формате JSON, напр. '{\"crack\":\">50\"}'",
		"status":     "Статус отработки: pending — ожидается, done — обработано, verified — сверено с фактом",
	},
}

type eventsCalendar struct {
}

func (s *eventsCalendar) Name() string {
	return eventsTable
}

func (s *eventsCalendar) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, eventsDdl); err != nil {
		return count, err
	}
	created, err := util.CreateView(ctx, conn, eventsView)
	if err != nil {
		return count, err
	}
	log.Infof("View %s created: %t", eventsView.Name, created)
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &eventsCalendar{})
}
