package cbr

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	cbrKeyRateUrl   = "https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True&UniDbQuery.From=17.09.2013&UniDbQuery.To=%s"
	cbrKeyRateTable = "cbr_key_rate"
	cbrKeyRateDdl   = `CREATE TABLE IF NOT EXISTS %s (
			  date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);
	`
	cbrKeyRateInsert = "INSERT INTO " + cbrKeyRateTable
	// cbrKeyRateTimeLayout — формат даты в строках таблицы (13.10.2026).
	cbrKeyRateTimeLayout = "02.01.2006"
	// cbrKeyRateDateLayout — формат UniDbQuery.To (MM-DD-YYYY).
	cbrKeyRateDateLayout = "01-02-2006"
	// cbrKeyRateRowSelector — строки таблицы «Ключевая ставка».
	cbrKeyRateRowSelector = ".table-wrapper .table .data tr"
)

type cbrKeyRate struct {
	util.ClickHouseImport
}

// Import подставляет DataUrl с сегодняшней датой окончания диапазона: UniDbQuery.To должен
// быть актуален на момент запуска, а сетевые вызовы и обращение к времени в init() запрещены
// (ARCHITECTURE.md §8.6). From=17.09.2013 — дата введения ключевой ставки.
func (s *cbrKeyRate) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	s.DataUrl = fmt.Sprintf(cbrKeyRateUrl, time.Now().Format(cbrKeyRateDateLayout))
	return s.ClickHouseImport.Import(ctx, conn)
}

// keyRateRows разбирает HTML-таблицу «Ключевая ставка» в точки. Строка-заголовок «Дата»
// пропускается; пустые (после TrimSpace) дата или значение — пропуск, а непустое
// непарсящееся — ошибка: битая страница не должна выглядеть успешным прогоном.
// Ноль строк — тоже ошибка (fail-fast): без таблицы .data ряд молча стёрся бы в пустой.
func keyRateRows(html []byte) ([]cbrPoint, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("%s: разбор страницы: %w", cbrKeyRateTable, err)
	}
	points := make([]cbrPoint, 0)
	var parseErr error
	doc.Find(cbrKeyRateRowSelector).Each(func(_ int, tr *goquery.Selection) {
		if parseErr != nil {
			return
		}
		cells := tr.Find("td")
		if cells.Length() == 0 {
			return
		}
		date := strings.TrimSpace(cells.Eq(0).Text())
		if date == "" || date == "Дата" {
			return
		}
		if cells.Length() < 2 {
			return
		}
		value := strings.TrimSpace(cells.Eq(1).Text())
		if value == "" {
			return
		}
		d, err := time.Parse(cbrKeyRateTimeLayout, date)
		if err != nil {
			parseErr = fmt.Errorf("%s: %w", cbrKeyRateTable, err)
			return
		}
		v, err := strconv.ParseFloat(strings.ReplaceAll(value, ",", "."), 64)
		if err != nil {
			parseErr = fmt.Errorf("%s: %w", cbrKeyRateTable, err)
			return
		}
		points = append(points, cbrPoint{date: d, value: v})
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: в таблице нет ни одной точки", cbrKeyRateTable)
	}
	return points, nil
}

func cbrKeyRateCraw(crawUrl string, conn driver.Conn) (count int64, err error) {
	page, err := getPage(crawUrl)
	if err != nil {
		return count, fmt.Errorf("%s: %w", cbrKeyRateTable, err)
	}
	points, err := keyRateRows(page)
	if err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, cbrKeyRateInsert)
	if err != nil {
		return count, err
	}
	for _, p := range points {
		if err = batch.Append(p.date, p.value); err != nil {
			return count, err
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек", cbrKeyRateTable, count)
	return count, nil
}

// cbrKeyRateImporter — ключевая ставка; краулер HTML-таблицы. Регистрируется
// реестром cbr/importers.go вместе с остальными импортёрами пакета.
var cbrKeyRateImporter = &cbrKeyRate{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrKeyRateTable,
	CreateTable: []string{cbrKeyRateDdl},
	CrawFunc:    cbrKeyRateCraw,
	AfterImport: afterCbrMacro(cbrKeyRateSeriesMeta),
}}
