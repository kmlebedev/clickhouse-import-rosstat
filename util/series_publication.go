package util

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SeriesPublicationTable — общая таблица публикационных дат рядов: одна строка =
// одна точка ряда и effective-момент, когда она стала публичной. Таблица generic:
// source/series совпадают с series_catalog, поэтому lookup идёт по тем же ключам,
// что и чтение значения. Первый потребитель — cbr_publication_calendar (три релиза
// ЦБ); сырые данные агенту не выдаются, читается витрина v_series_publication.
const SeriesPublicationTable = "series_publication"

// SeriesPublicationDdl — канонический DDL (ARCHITECTURE §6.2). loaded_at — версия
// ReplacingMergeTree: без неё апгрейд fallback → date → exact_time при повторной
// записи того же ключа выбирался бы произвольно между частями. В витрину колонка
// не выходит.
const SeriesPublicationDdl = `CREATE TABLE IF NOT EXISTS ` + SeriesPublicationTable + ` (
    source LowCardinality(String),          -- как в series_catalog: cbr, cbr_survey, …
    series LowCardinality(String),          -- имя ряда; совпадает с series в series_catalog
    period_date Date,                       -- дата точки в таблице источника
    published_at DateTime('Europe/Moscow'), -- effective: 23:59:59 MSK для date-only
    quality LowCardinality(String),         -- exact_time | date | fallback
    source_url String,                      -- страница версии либо индекс архива
    loaded_at DateTime('Europe/Moscow') DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at) ORDER BY (source, series, period_date);`

// SeriesPublication — строка series_publication. PeriodDate — дата точки источника,
// PublishedAt — effective-момент публикации (23:59:59 MSK, если известна только дата).
type SeriesPublication struct {
	Source      string
	Series      string
	PeriodDate  time.Time
	PublishedAt time.Time
	Quality     string
	SourceURL   string
}

// EnsureSeriesPublication создаёт общую таблицу публикационных дат, если её ещё нет.
// Идемпотентна: вызывается каждым прогоном импортёра до записи.
func EnsureSeriesPublication(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, SeriesPublicationDdl); err != nil {
		return fmt.Errorf("%s: %w", SeriesPublicationTable, err)
	}
	return nil
}
