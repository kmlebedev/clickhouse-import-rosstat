package craw

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v5"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"time"
)

type TimeSlice []time.Time

// Forward request for length
func (p TimeSlice) Len() int {
	return len(p)
}

// Define compare
func (p TimeSlice) Less(i, j int) bool {
	return p[i].Before(p[j])
}

// Define swap over an array
func (p TimeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

var gostVehicleSafetyCertificate = util.ClickHouseImport{
	TableName: "gost_vehicle_safety_certificate",
	CreateTable: `CREATE TABLE IF NOT EXISTS %s (
			  mark LowCardinality(String)
    		, type LowCardinality(String)
			, date Date
    		, certificate_number String
		) ENGINE = ReplacingMergeTree ORDER BY (mark, type, date, certificate_number);
	`,
	//DataUrl: "https://www.gost.ru/portal/gost/home/activity/compliance/evaluationcompliance/AcknowledgementCorrespondence/safetycertificate018?portal:componentId=ff119059-8bd4-47fc-95f6-a70de17a4b3e&portal:isSecure=false&portal:portletMode=view&navigationalstate=JBPNS_rO0ABXdSAAdvcmRlckJ5AAAAAQAYZGF0ZW9maXNzdWVvZmNlcnRpZmljYXRlAARmcm9tAAAAAQAFMjk4MjAABW9yZGVyAAAAAQAEREVTQwAHX19FT0ZfXw**",
	DataUrl: "https://www.gost.ru/portal/gost/home/activity/compliance/evaluationcompliance/AcknowledgementCorrespondence/safetycertificate018?portal:componentId=ff119059-8bd4-47fc-95f6-a70de17a4b3e&portal:isSecure=false&portal:portletMode=view&navigationalstate=JBPNS_rO0ABXdTAAdvcmRlckJ5AAAAAQAYZGF0ZW9maXNzdWVvZmNlcnRpZmljYXRlAARmcm9tAAAAAQAGMzM0ODIwAAVvcmRlcgAAAAEABERFU0MAB19fRU9GX18*",
	CrawFunc: func(crawUrl string, conn driver.Conn) (err error) {
		c := colly.NewCollector(
			colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"),
			colly.AllowURLRevisit(),
		)
		b := backoff.ExponentialBackOff{
			InitialInterval:     10 * time.Second,
			RandomizationFactor: 0.5,
			Multiplier:          2,
			MaxInterval:         30 * time.Minute,
		}
		ctx := context.TODO()
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO gost_vehicle_safety_certificate"))
		c.SetRequestTimeout(10 * time.Second)
		// #libraryPaging > div:nth-child(2) > a:nth-child(2)
		c.OnHTML("#standartsList > tbody > tr", func(e *colly.HTMLElement) {
			td := e.DOM.Children()
			certMark := td.Eq(0).Text()
			certMtype := td.Eq(1).Text()
			certNum := td.Eq(2).Text()
			certDate := td.Eq(3).Text()
			log.Debugf("mark: %s type: %s, date: %s, num: %s", certMark, certMtype, certDate, certNum)
			certDateTime, err := time.Parse("02.01.2006", certDate)
			if err != nil {
				log.Error(err)
				return
			}
			if err = batch.Append(certMark, certMtype, certDateTime, certNum); err != nil {
				log.Error(err)
				return
			}
		})
		c.OnHTML("#libraryPaging > div:nth-child(2) > a:nth-child(2)", func(e *colly.HTMLElement) {
			link := e.Attr("href")
			log.Infof("Rows %d Link found: %q -> %s\n", batch.Rows(), e.Text, link)
			if err = batch.Send(); err != nil {
				log.Error(err)
				return
			}
			if batch.IsSent() {
				batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO gost_vehicle_safety_certificate"))
				if err != nil {
					log.Error(err)
				}
			}
			operation := func() (string, error) {
				if err = e.Request.Visit(link); err != nil {
					log.Errorf("visit err: %v+", err)
					switch err.Error() {
					case "Gateway Timeout":
						return "", backoff.RetryAfter(240)
					case "context deadline exceeded (Client.Timeout exceeded while awaiting headers)":
						return "", backoff.RetryAfter(60)
					default:
						return "", backoff.RetryAfter(30)
					}
				}
				return "ok", nil
			}
			_, err := backoff.Retry(ctx, operation, backoff.WithBackOff(&b))
			if err != nil {
				log.Errorf("backoff.Retry Error: %+v", err)
				return
			}
		})
		if err := c.Visit(crawUrl); err != nil {
			log.Errorf("First visit err: %v+", err)
			return err
		}
		if err = batch.Send(); err != nil {
			log.Errorf("batch send err: %v+", err)
			return err
		}
		return nil
	},
}

func init() {
	chimport.Stats = append(chimport.Stats, &gostVehicleSafetyCertificate)
}
