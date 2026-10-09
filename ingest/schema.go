package ingest

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Канонические DDL из ARCHITECTURE.md §6.2 — копировать как есть, не менять.

const modelRunsDdl = `CREATE TABLE IF NOT EXISTS model_runs (
    run_id UUID DEFAULT generateUUIDv4(),
    run_date DateTime,
    trigger_type LowCardinality(String),  -- 'calendar','news','manual'
    trigger_ref String,
    gold_scenario JSON,        -- сценарная сетка + вероятности
    usdrub_path JSON,
    price_deck LowCardinality(String) DEFAULT 'own_scenario',  -- 'spot_flat','consensus_lt','own_scenario'
    discount_rate Float64,     -- 0.05 real USD база + надбавки; локальная ставка — в comment/JSON
    wacc Float64,
    nav_per_share Float64,
    nav_bull Float64, nav_base Float64, nav_bear Float64,
    market_price Float64,
    upside_pct Float64,
    nav_beta_gold Nullable(Float64),   -- рычаг NAV к цене золота (эталон: EBITDA-бета ~14% на +10% Au)
    dividend_status LowCardinality(String) DEFAULT 'suspended',  -- до 2030
    comment String
) ENGINE = ReplacingMergeTree ORDER BY (run_date, run_id);`

const forecastLogDdl = `CREATE TABLE IF NOT EXISTS forecast_log (
    forecast_date Date,
    target_date Date,
    metric LowCardinality(String),   -- 'xau_q_avg','fed_decision','nav','cbr_rate'
    predicted Float64, actual Nullable(Float64),
    error_pct Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (metric, forecast_date);`

const macroSeriesDdl = `CREATE TABLE IF NOT EXISTS macro_series (
    source LowCardinality(String),   -- 'fred','eia','wgc','cme'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);`

// ensureTables создаёт три таблицы контура прогноза (идемпотентно).
func ensureTables(ctx context.Context, conn driver.Conn) error {
	for _, ddl := range []string{modelRunsDdl, forecastLogDdl, macroSeriesDdl} {
		if err := conn.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}
