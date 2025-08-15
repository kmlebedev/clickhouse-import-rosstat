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

type cbrGold struct {
}

const (
	//https://www.cbr.ru/hd_base/metall/metall_base_new/?UniDbQuery.Posted=True&UniDbQuery.From=01.01.2019&UniDbQuery.To=08.02.2025&UniDbQuery.Gold=true&UniDbQuery.so=1
	cbrGoldUrl   = "https://www.cbr.ru/hd_base/metall/metall_base_new/?UniDbQuery.Posted=True&UniDbQuery.From=01.01.2019&UniDbQuery.To=%s&UniDbQuery.Gold=true&UniDbQuery.so=1"
	cbrGoldTable = "cbr_gold"
	cbrGoldDdl   = `CREATE TABLE IF NOT EXISTS ` + cbrGoldTable + ` (
			  date Date
			, price Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);
	`
	cbrGoldInsert     = "INSERT INTO " + cbrGoldTable + " VALUES (?, ?)"
	cbrGoldTimeLayout = "02.01.2006"
)

func (s *cbrGold) Name() string {
	return cbrGoldTable
}

func (s *cbrGold) export() (table *[][]string, err error) {
	url := fmt.Sprintf(cbrGoldUrl, time.Now().Format("02.01.2006"))
	fmt.Printf("gld export %s\n", url)
	c := colly.NewCollector()
	table = new([][]string)
	c.OnHTML(".table-wrapper .data tr", func(e *colly.HTMLElement) {
		date := e.DOM.Children().First().Text()
		if date == "Дата" {
			return
		}
		price := strings.Replace(strings.Replace(e.DOM.Children().Last().Text(), ",", ".", -1), " ", "", -1)
		// fmt.Printf("date %s price %s\n", date, price)
		if _, err := strconv.ParseFloat(price, 32); err != nil {
			return
		}
		*table = append(*table, []string{date, price})
	})

	if err = c.Visit(url); err != nil {
		return nil, err
	}
	return table, nil
}

func (s *cbrGold) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, cbrGoldDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(cbrGoldTimeLayout, row[0])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, cbrGoldInsert, date, row[1]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrGold{})
}
