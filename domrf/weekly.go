package domrf

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// Импортёр domrf_mortgage_weekly — недельная ставка предложения по продуктам из блока
// mortgage-dinamika-stavok страницы аналитики: новостройка, семейная, вторичка, рефинансирование,
// льготная (последняя завершилась и короче остальных). Ряд «топ-20 банков», 2021-09 … 2026-10.
//
// Ставка хранится в процентах годовых (18.76), как в источнике. Унаследованная таблица
// domrf_mortgage (пакет bank/) хранит доли (0.187) — витрины пакетов не смешиваются.
type domrfMortgageWeekly struct{}

const (
	domrfWeeklyTable = "domrf_mortgage_weekly"
	// domrfWeeklyDdl — DDL литералом: у импортёра свой Import() (JSON в странице, без XLSX),
	// шаблон util.ClickHouseImport не используется.
	domrfWeeklyDdl = `CREATE TABLE IF NOT EXISTS ` + domrfWeeklyTable + ` (
			  product LowCardinality(String)
			, date Date
			, rate Float64
		) ENGINE = ReplacingMergeTree ORDER BY (product, date);
	`
)

func (s *domrfMortgageWeekly) Name() string {
	return domrfWeeklyTable
}

func (s *domrfMortgageWeekly) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, domrfWeeklyDdl); err != nil {
		return count, err
	}
	page, err := domrfGetPage(domrfAnalyticsPage)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfWeeklyTable, err)
	}
	blocks, err := domrfBlocks(page)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfWeeklyTable, err)
	}
	block, err := domrfBlock[domrfWeeklyBlockJSON](blocks, domrfWeeklyBlock)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfWeeklyTable, err)
	}
	points, err := domrfWeeklyPoints(block)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfWeeklyTable, err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+domrfWeeklyTable)
	if err != nil {
		return count, err
	}
	for i, point := range points {
		if err = batch.Append(point.Product, point.Date, point.Rate); err != nil {
			_ = batch.Abort()
			return count, fmt.Errorf("%s: append точки %d (%s, %s): %w", domrfWeeklyTable, i+1, point.Product, point.Date.Format(domrfWeeklyDateLayout), err)
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек, %d продуктов, %s … %s", domrfWeeklyTable, count, len(block.Titles),
		block.Date[len(block.Date)-1], block.Date[0])
	if err = afterDomrfMacro(ctx, conn); err != nil {
		return count, err
	}
	log.Infof("Imported %d rows of %s", count, domrfWeeklyTable)
	return count, nil
}
