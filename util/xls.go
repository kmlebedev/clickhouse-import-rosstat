package util

import (
	"bytes"
	"context"
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
	HttpClient  *http.Client
	MonthsToNum = map[string]time.Month{
		"январь":   time.January,
		"февраль":  time.February,
		"март":     time.March,
		"апрель":   time.April,
		"май":      time.May,
		"июнь":     time.June,
		"июль":     time.July,
		"август":   time.August,
		"авугст":   time.August,
		"сентябрь": time.September,
		"октябрь":  time.October,
		"ноябрь":   time.November,
		"декабрь":  time.December,
		"января":   time.January,
		"февраля":  time.February,
		"марта":    time.March,
		"апреля":   time.April,
		"мая":      time.May,
		"июня":     time.June,
		"июля":     time.July,
		"августа":  time.August,
		"сентября": time.September,
		"октября":  time.October,
		"ноября":   time.November,
		"декабря":  time.December,
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
	HttpClient = &http.Client{Transport: tr, Jar: jar}
}

// fileTimeout — дедлайн одного запроса GetFile. Переменная (а не константа), чтобы тест
// мог подставить короткий дедлайн вместо двухминутного. Прецеденты — xmlTimeout и
// jsonTimeout (util/xml.go, util/json.go), у обоих минута: они обслуживают фиды и API, а
// GetFile общий для всех пакетов и тянет многомегабайтные XLSX-выгрузки ЦБ, поэтому
// значение взято с запасом. Дедлайн — не украшение: HttpClient создан без Timeout, и
// зависший ответ ЦБ без дедлайна ошибки не даёт вовсе — импортёр ждал бы его вместо
// отчёта о сбое. Регрессия, ради которой дедлайн возвращён: cbr_key_rate ходит через
// GetFile (зовут его util/links.go — GetPage — и импортёры cbr/ и rosstat/ через
// GetXlsx) и при переводе с colly потерял его десятисекундный таймаут;
// key_rate — третий шаг цепочки dagu/cbr.yaml (overlap_policy: skip), и зависание
// остановило бы десять шагов после него.
var fileTimeout = 2 * time.Minute

// GetFile скачивает url общим HttpClient (тот же User-Agent, что у GetXML/GetJSON) и
// отдаёт тело ридером. Статус не проверяется (в отличие от GetJSON/GetXML): HTML-заглушку
// WAF вызывающий увидит ошибкой разбора XLSX — эта ветка у GetXlsx/GetXls уже есть.
// Тело читается целиком до возврата, поэтому дедлайн fileTimeout действует и на чтение;
// запрос ограничен им: зависший ответ иначе не дал бы ошибки.
func GetFile(url string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fileTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	// Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
	// fmt.Printf("Header %+v\n", resp.Header)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("body size is empty")
	}
	//fmt.Printf("body %+s\n", string(body))
	return io.NopCloser(bytes.NewReader(body)), nil
}

// GetFileBytes — тело целиком срезом: шаблону ImportFunc нужны байты дважды — на разбор
// (excelize.OpenReader из памяти) и на винтажную архивацию той же версии файла, а второй
// GET по DataUrl удвоил бы и трафик, и время прогона. Закрытие с проверкой обеих ошибок
// (идиом GetPage, util/links.go): незамеченная ошибка чтения дала бы «успешные» байты
// усечённого файла — такой файл ушёл бы и в разбор, и в архив.
func GetFileBytes(url string) ([]byte, error) {
	reader, err := GetFile(url)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return body, nil
}

// PostFileBytes — POST через общий HttpClient (тот же UA и дедлайн fileTimeout, что у
// GetFile) с произвольными заголовками. Нужен источникам, которые отдают файл не по
// прямому GET, а через RPC: GWT-RPC BI Росстата возвращает путь к temp-CSV, а сам файл
// потом качается GetFileBytes. Ответ не-2xx — ошибка: иначе HTML-заглушка WAF ушла бы
// в разбор как «успешный» CSV. Тело ответа в тексте ошибки обрезается: это диагностика,
// а не данные.
func PostFileBytes(url string, body []byte, headers map[string]string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fileTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA)
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail := string(respBody)
		if len(detail) > 512 {
			detail = detail[:512]
		}
		return nil, fmt.Errorf("POST %s: status %d: %s", url, resp.StatusCode, detail)
	}
	if len(respBody) == 0 {
		return nil, fmt.Errorf("POST %s: body size is empty", url)
	}
	return respBody, nil
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

// GetCSV скачивает CSV-выгрузку и разбирает её в записи: разделитель `;`, CRLF → LF, ведущий
// UTF-8 BOM снимается. BOM убирается именно здесь, до csv.Reader: он часть файла-обёртки, а не
// данные, а encoding/csv считает его началом незакавыченного поля и падает на первой же кавычке
// («bare " in non-quoted-field») — на живом файле SberIndex это значило бы ноль записей.
func GetCSV(url string) (records [][]string, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	body = bytes.TrimPrefix(body, []byte("\ufeff"))
	reader := bytes.NewBufferString(strings.ReplaceAll(string(body), "\r", "\n"))
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'
	return csvReader.ReadAll()
}
