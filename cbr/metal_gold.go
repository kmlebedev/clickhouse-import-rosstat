package cbr

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// cbrMetallDoc — ответ https://www.cbr.ru/scripts/xml_metall.asp (учётные цены на
// аффинированные драгоценные металлы). XMLName пинит корень: без пина 200 с HTML-страницей
// заглушки разберётся в пустую структуру без ошибки (см. util.ParseXML).
type cbrMetallDoc struct {
	XMLName xml.Name `xml:"Metall"`
	Records []struct {
		Code string `xml:"Code,attr"`
		Date string `xml:"Date,attr"`
		Buy  string `xml:"Buy"`
	} `xml:"Record"`
}

const (
	cbrGoldUrl   = "https://www.cbr.ru/scripts/xml_metall.asp?date_req1=%s&date_req2=%s"
	cbrGoldTable = "cbr_gold"
	cbrGoldDdl   = `CREATE TABLE IF NOT EXISTS %s (
				  date Date
				, price Float32
			) ENGINE = ReplacingMergeTree ORDER BY (date);
		`
	cbrGoldInsert     = "INSERT INTO " + cbrGoldTable
	cbrGoldTimeLayout = "02.01.2006"
	// cbrGoldDateLayout — формат date_req1/date_req2 у xml_metall.asp.
	cbrGoldDateLayout = "02/01/2006"
	// cbrGoldCode — код золота в ленте xml_metall.asp (учётная цена в рублях за грамм).
	cbrGoldCode = "1"
	// cbrGoldHistoryFrom — начало истории цены (DD/MM/YYYY): 10.01.2013 — первая дата
	// cbr_gold (baseline T1, n=3397); лента глубже не отдаёт.
	cbrGoldHistoryFrom = "10/01/2013"
)

type cbrGold struct {
	util.ClickHouseImport
}

// Import подставляет DataUrl с сегодняшней датой окончания диапазона: date_req2 должен
// быть актуален на момент запуска, а сетевые вызовы и обращение к времени в init()
// запрещены (ARCHITECTURE.md §8.6).
func (s *cbrGold) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	s.DataUrl = fmt.Sprintf(cbrGoldUrl, cbrGoldHistoryFrom, time.Now().Format(cbrGoldDateLayout))
	return s.ClickHouseImport.Import(ctx, conn)
}

// parseMetalGold разбирает xml_metall.asp в точки по золоту (Code == 1). Пустая запись
// (пустые после TrimSpace дата или значение) пропускается: ЦБ отдаёт пустые <Buy/> у дней
// без цены. Непустое непарсящееся значение — ошибка, а не пропуск: битая лента не должна
// выглядеть успешным прогоном. Ноль точек — тоже ошибка (fail-fast): пустой <Metall/> или
// лента без золота молча стёрла бы ряд нулём строк.
func parseMetalGold(doc *cbrMetallDoc) ([]cbrPoint, error) {
	points := make([]cbrPoint, 0, len(doc.Records))
	for _, r := range doc.Records {
		if r.Code != cbrGoldCode {
			continue
		}
		date, value := strings.TrimSpace(r.Date), strings.TrimSpace(r.Buy)
		if date == "" || value == "" {
			continue
		}
		d, err := time.Parse(cbrGoldTimeLayout, date)
		if err != nil {
			return nil, err
		}
		v, err := strconv.ParseFloat(strings.ReplaceAll(value, ",", "."), 64)
		if err != nil {
			return nil, err
		}
		points = append(points, cbrPoint{date: d, value: v})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: в ответе нет ни одной точки (золото Code=%s)", cbrGoldTable, cbrGoldCode)
	}
	return points, nil
}

func cbrGoldCraw(crawUrl string, conn driver.Conn) (count int64, err error) {
	doc := new(cbrMetallDoc)
	if err = util.GetXML(crawUrl, doc); err != nil {
		return count, fmt.Errorf("%s: %w", cbrGoldTable, err)
	}
	points, err := parseMetalGold(doc)
	if err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, cbrGoldInsert)
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
	log.Infof("%s: %d точек", cbrGoldTable, count)
	return count, nil
}

func init() {
	cbrGold := cbrGold{ClickHouseImport: util.ClickHouseImport{
		TableName:   cbrGoldTable,
		CreateTable: []string{cbrGoldDdl},
		CrawFunc:    cbrGoldCraw,
	}}
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &cbrGold, meta: cbrGoldSeriesMeta})
}
