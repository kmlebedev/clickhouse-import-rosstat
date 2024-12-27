package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"strings"
	"time"
)

type cbrKeyRate struct {
}

const (
	cbrKeyRateUrl   = "https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True&UniDbQuery.From=17.09.2013&UniDbQuery.To=%s"
	cbrKeyRateTable = "cbr_key_rate"
	cbrKeyRateDdl   = `CREATE TABLE IF NOT EXISTS ` + cbrKeyRateTable + ` (
			  date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);
	`
	cbrKeyRateInsert     = "INSERT INTO " + cbrKeyRateTable + " VALUES (?, ?)"
	cbrKeyRateTimeLayout = "02.01.2006"
)

func (s *cbrKeyRate) Name() string {
	return cbrKeyRateTable
}

func (s *cbrKeyRate) export() (table *[][]string, err error) {
	c := colly.NewCollector()
	table = new([][]string)
	c.OnHTML(".table-wrapper .table .data tr", func(e *colly.HTMLElement) {
		date := e.DOM.Children().First().Text()
		if date == "Дата" {
			return
		}
		rate := strings.Replace(e.DOM.Children().Next().Text(), ",", ".", -1)
		fmt.Printf("date %s rate %s\n", date, rate)
		*table = append(*table, []string{date, rate})
	})
	if err = c.Visit(fmt.Sprintf(cbrKeyRateUrl, time.Now().Format("01-02-2006"))); err != nil {
		return nil, err
	}
	return table, nil
}

func (s *cbrKeyRate) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, cbrKeyRateDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(cbrKeyRateTimeLayout, row[0])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, cbrKeyRateInsert, date, row[1]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrKeyRate{})
}
