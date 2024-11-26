package util

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/xuri/excelize/v2"
	"io"
	"math/rand"
	"net/http"
)

var (
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
	req.Header.Set("User-Agent", randomUA)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	// Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
	//fmt.Printf("Header %+v\n", resp.Header)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	//fmt.Printf("Body %+v", string(body))
	reader := bytes.NewReader(body)
	if xlsx, err = excelize.OpenReader(reader); err != nil {
		return nil, fmt.Errorf("excelize %v", err)
	}
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
	req.Header.Set("User-Agent", randomUA)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	reader := bytes.NewReader(body)
	csvReader := csv.NewReader(reader)
	return csvReader.ReadAll()
}
