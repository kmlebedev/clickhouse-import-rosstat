package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"strconv"
	"strings"
	"time"
)

type cbrCurrencyUSD struct {
}

const (
	cbrCurrencyUSDUrl   = "https://www.cbr.ru/currency_base/dynamics/?UniDbQuery.Posted=True&UniDbQuery.so=1&UniDbQuery.mode=1&UniDbQuery.VAL_NM_RQ=R01235&UniDbQuery.From=01.01.2022&UniDbQuery.To=%s"
	cbrCurrencyUSDTable = "cbr_currency_usd"
	cbrCurrencyUSDDdl   = `CREATE TABLE IF NOT EXISTS ` + cbrCurrencyUSDTable + ` (
			  date Date
			, price Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);
	`
	cbrCurrencyUSDInsert     = "INSERT INTO " + cbrCurrencyUSDTable + " VALUES (?, ?)"
	cbrCurrencyUSDTimeLayout = "02.01.2006"
)

func (s *cbrCurrencyUSD) Name() string {
	return cbrCurrencyUSDTable
}

func (s *cbrCurrencyUSD) export() (table *[][]string, err error) {
	c := colly.NewCollector()
	table = new([][]string)
	c.OnHTML(".table-wrapper .table .data tr", func(e *colly.HTMLElement) {
		//date := e.DOM.Children().First().Text()
		date := e.DOM.Children().First().Text()
		if date == "Дата" {
			return
		}
		price := strings.Replace(e.DOM.Children().Last().Text(), ",", ".", -1)
		if _, err := strconv.ParseFloat(price, 32); err != nil {
			return
		}
		fmt.Printf("date %s price %s\n", date, price)
		*table = append(*table, []string{date, price})
	})
	if err = c.Visit(fmt.Sprintf(cbrCurrencyUSDUrl, time.Now().AddDate(0, 0, 1).Format("01-02-2006"))); err != nil {
		return nil, err
	}
	return table, nil
}

func (s *cbrCurrencyUSD) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, cbrCurrencyUSDDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(cbrCurrencyUSDTimeLayout, row[0])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, cbrCurrencyUSDInsert, date, row[1]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrCurrencyUSD{})
}
