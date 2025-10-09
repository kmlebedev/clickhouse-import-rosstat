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

// https://www.cbr.ru/statistics/avgprocstav/?UniDbQuery.Posted=True&UniDbQuery.From=1.04.2020&UniDbQuery.To=2.02.2025
// Todo Добавть ставки от https://www.cbr.ru/statistics/bank_sector/int_rat/

type cbrProcStav struct {
}

const (
	// Динамика максимальной процентной ставки https://www.cbr.ru/statistics/avgprocstav/
	cbrProcStavUrl   = "https://www.cbr.ru/statistics/avgprocstav/?UniDbQuery.Posted=True&UniDbQuery.From=1.01.2022&UniDbQuery.To=3.07.2025%s"
	cbrProcStavTable = "cbr_proc_stav"
	cbrProcStavDdl   = `CREATE TABLE IF NOT EXISTS ` + cbrCurrencyUSDTable + ` (
			  date Date
			, price Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);
	`
	cbrProcStavInsert     = "INSERT INTO " + cbrCurrencyUSDTable + " VALUES (?, ?)"
	cbrProcStavTimeLayout = "02.01.2006"
)

func (s *cbrProcStav) Name() string {
	return cbrCurrencyUSDTable
}

func (s *cbrProcStav) export() (table *[][]string, err error) {
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

func (s *cbrProcStav) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
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
	chimport.Stats = append(chimport.Stats, &cbrProcStav{})
}
