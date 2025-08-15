package util

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"golang.org/x/net/publicsuffix"
	"io"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"
)

var (
	httpClient  *http.Client
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
	userAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
	}
	randomIndex = rand.Intn(len(userAgents))
	randomUA    = userAgents[randomIndex]
)

func init() {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		log.Errorf("system cert pool is null")
		rootCAs = x509.NewCertPool()
	}
	localCertFiles := os.Getenv("CERT_FILES")
	if len(localCertFiles) > 0 {
		// Read in the cert file
		for _, localCertFile := range strings.Split(localCertFiles, ",") {
			certs, err := os.ReadFile(localCertFile)
			if err != nil {
				log.Fatalf("Failed to append %q to RootCAs: %v", localCertFile, err)
			}
			// Append our cert to the system pool
			if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
				log.Fatalf("No certs appended, using system certs only")
			}
			log.Infof("Using local certs from %v", localCertFile)
		}
	}
	// Trust the augmented cert pool in our client
	config := &tls.Config{
		RootCAs: rootCAs,
	}
	tr := &http.Transport{TLSClientConfig: config}
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	httpCookie := &http.Cookie{
		Name:     "redirect_cookie",
		Value:    "1",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
	jar.SetCookies(&url.URL{Scheme: "https", Path: "/", Host: "rosstat.gov.ru"}, []*http.Cookie{httpCookie})
	httpClient = &http.Client{Transport: tr, Jar: jar}
}

func GetFile(url string) (io.ReadCloser, error) {
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
	//fmt.Printf("body %+s\n", string(body))
	return io.NopCloser(bytes.NewReader(body)), nil
}

func GetXlsx(url string) (xlsx *excelize.File, err error) {
	reader, err := GetFile(url)
	if err != nil {
		log.Errorf("GetFile %v", err)
		return nil, err
	}
	if xlsx, err = excelize.OpenReader(reader); err != nil {
		return nil, fmt.Errorf("excelize %v", err)
	}
	log.Infof("Get xlsx file %+v", xlsx.Path)
	return xlsx, nil
}

func GetXls(url string) (xlsx *excelize.File, err error) {
	reader, err := GetFile(url)
	if err != nil {
		return nil, err
	}
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
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'
	return csvReader.ReadAll()
}
