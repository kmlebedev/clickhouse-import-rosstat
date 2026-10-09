package util

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	seriesCatalogTable = "series_catalog"
	seriesCatalogDdl   = `CREATE TABLE IF NOT EXISTS series_catalog (
				  source LowCardinality(String)
				, series LowCardinality(String)
				, title String
				, unit String
				, frequency LowCardinality(String)
				, origin String
				, description String
			) ENGINE = ReplacingMergeTree ORDER BY (source, series);
		`
	seriesCatalogInsert = "INSERT INTO " + seriesCatalogTable
)

var seriesCatalogViews = []string{
	`CREATE OR REPLACE VIEW v_series_catalog DEFINER = default SQL SECURITY DEFINER AS
		SELECT source, series, title, unit, frequency, origin, description FROM series_catalog FINAL`,
	`ALTER TABLE v_series_catalog MODIFY COMMENT 'Каталог рядов macro_series: для каждого (source, series) — название, единицы, частота, происхождение и как читать значение. Читать перед написанием запроса к ряду'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN source 'Импортёр-источник: fred, bls, bea, cbr, rosstat, minfin, minfin_mesyats, gold, ...'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN series 'Код ряда; совпадает с series в macro_series'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN title 'Название ряда'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN unit 'Единицы измерения значения'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN frequency 'Частота наблюдений: M — месяц, Q — квартал, D — день, W — неделя'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN origin 'Откуда взят ряд: таблица/серия/строка API'`,
	`ALTER TABLE v_series_catalog COMMENT COLUMN description 'Что измеряет ряд, как интерпретировать значение и как считать темпы'`,
}

type SeriesMeta struct {
	Source      string
	Series      string
	Title       string
	Unit        string
	Frequency   string
	Origin      string
	Description string
}

// UpsertSeriesCatalog создаёт series_catalog и витрину v_series_catalog с комментариями
// для MCP-агента, затем батчем записывает описания рядов импортёра.
func UpsertSeriesCatalog(ctx context.Context, conn driver.Conn, rows []SeriesMeta) error {
	if err := conn.Exec(ctx, seriesCatalogDdl); err != nil {
		return fmt.Errorf("%s: %w", seriesCatalogTable, err)
	}
	for _, stmt := range seriesCatalogViews {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("v_series_catalog: %w", err)
		}
	}
	batch, err := conn.PrepareBatch(ctx, seriesCatalogInsert)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err = batch.Append(r.Source, r.Series, r.Title, r.Unit, r.Frequency, r.Origin, r.Description); err != nil {
			return err
		}
	}
	return batch.Send()
}
