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

// ctx — фон для батчей: импорт не привязан к запросу пользователя, а CrawFunc получает
// только conn (идиом customs/vneshn_torg_countries.go).
var ctx = context.TODO()

// cbrPoint — точка ряда ЦБ: дата наблюдения и значение. Общий тип разборов пакета
// (XML-ленты XML_dynamic.asp/xml_metall.asp, HTML-таблицы key_rate).
type cbrPoint struct {
	date  time.Time
	value float64
}

// cbrValCurs — ответ https://www.cbr.ru/scripts/XML_dynamic.asp (динамика курса валюты).
// XMLName пинит корень: без пина 200 с HTML-страницей заглушки разберётся в пустую
// структуру без ошибки (см. util.ParseXML).
type cbrValCurs struct {
	XMLName xml.Name `xml:"ValCurs"`
	Records []struct {
		Date  string `xml:"Date,attr"`
		Value string `xml:"Value"`
	} `xml:"Record"`
}

const (
	cbrCurrencyUSDUrl   = "https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=%s&date_req2=%s&VAL_NM_RQ=R01235"
	cbrCurrencyUSDTable = "cbr_currency_usd"
	cbrCurrencyUSDDdl   = `CREATE TABLE IF NOT EXISTS %s (
				  date Date
				, price Float32
			) ENGINE = ReplacingMergeTree ORDER BY (date);
		`
	cbrCurrencyUSDInsert     = "INSERT INTO " + cbrCurrencyUSDTable
	cbrCurrencyUSDTimeLayout = "02.01.2006"
	// cbrCurrencyUSDDateLayout — формат date_req1/date_req2 у XML_dynamic.asp.
	cbrCurrencyUSDDateLayout = "02/01/2006"
	// cbrCurrencyUSDHistoryFrom — начало истории курса (DD/MM/YYYY): 10.01.2013 — первая
	// дата cbr_currency_usd (baseline T1, n=3397); лента глубже не отдаёт.
	cbrCurrencyUSDHistoryFrom = "10/01/2013"
)

type cbrCurrencyUSD struct {
	util.ClickHouseImport
}

// Import подставляет DataUrl с сегодняшней датой окончания диапазона: date_req2 должен
// быть актуален на момент запуска, а сетевые вызовы и обращение к времени в init()
// запрещены (ARCHITECTURE.md §8.6).
func (s *cbrCurrencyUSD) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	s.DataUrl = fmt.Sprintf(cbrCurrencyUSDUrl, cbrCurrencyUSDHistoryFrom, time.Now().Format(cbrCurrencyUSDDateLayout))
	return s.ClickHouseImport.Import(ctx, conn)
}

// parseCurrencyUSD разбирает XML_dynamic.asp в точки. Пустая запись (пустые после TrimSpace
// дата или значение) пропускается: ЦБ отдаёт пустые <Value/> у дней без курса. Непустое
// непарсящееся значение — ошибка, а не пропуск: битая лента не должна выглядеть успешным
// прогоном. Ноль точек — тоже ошибка (fail-fast): пустой <ValCurs/> вместо истории молча
// стёр бы ряд нулём строк.
func parseCurrencyUSD(doc *cbrValCurs) ([]cbrPoint, error) {
	points := make([]cbrPoint, 0, len(doc.Records))
	for _, r := range doc.Records {
		date, value := strings.TrimSpace(r.Date), strings.TrimSpace(r.Value)
		if date == "" || value == "" {
			continue
		}
		d, err := time.Parse(cbrCurrencyUSDTimeLayout, date)
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
		return nil, fmt.Errorf("%s: в ответе нет ни одной точки", cbrCurrencyUSDTable)
	}
	return points, nil
}

func cbrCurrencyUSDCraw(crawUrl string, conn driver.Conn) (count int64, err error) {
	doc := new(cbrValCurs)
	if err = util.GetXML(crawUrl, doc); err != nil {
		return count, fmt.Errorf("%s: %w", cbrCurrencyUSDTable, err)
	}
	points, err := parseCurrencyUSD(doc)
	if err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, cbrCurrencyUSDInsert)
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
	log.Infof("%s: %d точек", cbrCurrencyUSDTable, count)
	return count, nil
}

func init() {
	cbrCurrencyUSD := cbrCurrencyUSD{ClickHouseImport: util.ClickHouseImport{
		TableName:   cbrCurrencyUSDTable,
		CreateTable: []string{cbrCurrencyUSDDdl},
		CrawFunc:    cbrCurrencyUSDCraw,
	}}
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &cbrCurrencyUSD, meta: cbrCurrencyUSDSeriesMeta})
}
