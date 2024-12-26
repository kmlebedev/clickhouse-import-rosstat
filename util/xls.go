package util

import (
	"bytes"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var (
	MonthsToNum = map[string]time.Month{
		"январь":   time.January,
		"февраль":  time.February,
		"март":     time.March,
		"апрель":   time.April,
		"май":      time.May,
		"июнь":     time.June,
		"июль":     time.July,
		"август":   time.August,
		"сентябрь": time.September,
		"октябрь":  time.October,
		"ноябрь":   time.November,
		"декабрь":  time.December,
		"янв":      time.January,
		"фев":      time.February,
		"мар":      time.March,
		"апр":      time.April,
		"июн":      time.June,
		"июл":      time.July,
		"авг":      time.August,
		"сен":      time.September,
		"окт":      time.October,
		"ноя":      time.November,
		"дек":      time.December,
	}
	httpTransport = http.Transport{}
	httpClient    = &http.Client{Transport: &httpTransport}
	userAgents    = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
	}
	randomIndex = rand.Intn(len(userAgents))
	randomUA    = userAgents[randomIndex]
)

func GetXlsx(url string) (xlsx *excelize.File, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	// Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
	// fmt.Printf("Header %+v\n", resp.Header)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if len(body) == 0 {
		return nil, fmt.Errorf(("Body size is empty"))
	}
	reader := bytes.NewReader(body)
	if xlsx, err = excelize.OpenReader(reader); err != nil {
		return nil, fmt.Errorf("excelize %v", err)
	}
	log.Infof("Get xlsx file %+v", xlsx.Path)
	return xlsx, nil
}

func GetXls(url string) (xlsx *excelize.File, err error) {
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", randomUA)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	reader := bytes.NewReader(body)
	if xlsx, err = excelize.OpenReader(reader); err != nil {
		return nil, fmt.Errorf("excelize %v", err)
	}
	return xlsx, nil
}

func GetCSV(url string) (records [][]string, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewBufferString(strings.ReplaceAll(string(body), "\r", "\n"))
	return csv.NewReader(reader).ReadAll()
}
