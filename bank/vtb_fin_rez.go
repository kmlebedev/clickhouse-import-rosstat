package bank

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (

	// https://www.vtb.ru/ir/statements/results/
	// Имена файлов содержат дату отчётности (rus-vtb-group-ifrs-as-of-31-october-2024.xlsx)
	// и меняются каждый отчётный период, поэтому список берётся со страницы результатов.
	vtbIfrsResultsUrl = "https://www.vtb.ru/ir/statements/results/"
	vtbIfrsTable      = "rus_vtb_group_ifrs"
	vtbIfrsTableDdl   = `CREATE TABLE IF NOT EXISTS ` + vtbIfrsTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	vtbIfrsDataInsert = "INSERT INTO " + vtbIfrsTable + " VALUES (?, ?, ?)"
	vtbIfrsDataField  = "Денежные средства и краткосрочные активы"
	vtbIfrsTimeLayout = "01-02-06"
)

// getVtbIfrsXlsDataUrl собирает ссылки на отчётные XLSX-файлы МСФО со страницы
// результатов ВТБ. Отбираются только основные отчёты (rus-vtb-group-ifrs-as-of-*.xlsx);
// вспомогательные файлы (financial_data_supplement_*) и legacy .xls отбрасываются.
func getVtbIfrsXlsDataUrl() (urls []string) {
	seen := make(map[string]bool)
	c := colly.NewCollector(colly.UserAgent(util.HttpUA))
	c.SetClient(util.HttpClient)
	c.OnHTML(`a[href*="rus-vtb-group-ifrs-as-of"]`, func(e *colly.HTMLElement) {
		href := e.Attr("href")
		if !strings.HasSuffix(strings.ToLower(href), ".xlsx") {
			return
		}
		if seen[href] {
			return
		}
		seen[href] = true
		urls = append(urls, href)
	})
	if err := c.Visit(vtbIfrsResultsUrl); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return urls
}

const vtbIfrsXlsDataPrefix = "https://www.vtb.ru"

type VtbIfrs struct {
}

func (s *VtbIfrs) Name() string {
	return vtbIfrsTable
}

func (s *VtbIfrs) export() (table *[][]string, err error) {
	vtbIfrsXlsData := getVtbIfrsXlsDataUrl()
	if len(vtbIfrsXlsData) == 0 {
		return nil, fmt.Errorf("vtbIfrs: не найдены ссылки на rus-vtb-group-ifrs-as-of-*.xlsx на %s", vtbIfrsResultsUrl)
	}
	var xlsx *excelize.File
	table = new([][]string)
	for _, xlsName := range vtbIfrsXlsData {
		if xlsx, err = util.GetXlsx(vtbIfrsXlsDataPrefix + xlsName); err != nil {
			return nil, fmt.Errorf("get xlsx %s failed: %v", xlsName, err)
		}
		var rows [][]string
		if rows, err = xlsx.GetRows("Ключевые балансовые показатели"); err != nil {
			return nil, err
		}
		_ = xlsx.Close()
		fieldFound := 0
		// Строки с годами
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			// fmt.Printf("file %s row: %s\n", xlsName, row[0])
			if strings.TrimSpace(row[0]) == vtbIfrsDataField {
				fieldFound = i - 1
			}
			if fieldFound == 0 {
				continue
			}
			if len(row) < 2 {
				break
			}
			if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
				continue
			}
			// Колонки с месяцами и пропуском кварталов
			for j, cell := range row[1:] {
				if cell == "" {
					continue
				}
				if j+1 >= len(rows[fieldFound]) {
					break
				}
				if strings.HasPrefix(strings.TrimSpace(rows[fieldFound][j+1]), "изменение") {
					continue
				}
				name := strings.TrimSpace(strings.ReplaceAll(row[0], "-", ""))
				balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				//fmt.Printf("name %s date %v balance %s\n", name, rows[fieldFound][j+1], balance)
				if _, err = strconv.ParseFloat(balance, 32); err != nil {
					return nil, err
				}

				*table = append(*table, []string{name, rows[fieldFound][j+1], balance})
			}
			if row[0] == "Итого собственные средства" {
				break
			}
		}
	}

	return table, nil
}

func (s *VtbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vtbIfrsTableDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(vtbIfrsTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, vtbIfrsDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &VtbIfrs{})
}
