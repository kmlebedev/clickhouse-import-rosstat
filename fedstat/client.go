package fedstat

// Общий транспорт пакета: страница индикатора -> CSRF-токен и cookie -> POST
// downloadData.do. У 37426 формат sdmx (XML), у пяти макро-импортёров — excel
// (BIFF .xls). Сеть — только через util.NewDefaultTLSClient() (он держит cookie
// jar); кастомный TLS-конфиг util.HttpClient источник отвергает пустым/частичным
// ответом (живая сверка 2026-10-20, правило 4 AGENTS.md — клиент всё равно из util).
//
// Порядок selectedFilterIds позиционный: сервер разбирает значения по порядку
// осей (indicator -> unit -> регион -> год -> fixed -> all-dims). Алфавитный
// порядок url.Values.Encode даёт пустой конверт — проверено live 2026-10-21.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	fedstatBaseURLConst = "https://www.fedstat.ru"

	fedstatPageTimeout   = 60 * time.Second
	fedstatExportTimeout = 180 * time.Second
	fedstatMaxBodyBytes  = 64 << 20

	// fedstatBIFFMagic — сигнатура OLE2/BIFF .xls. Content-Type источника —
	// application/vnd.ms-excel, но решает сигнатура: HTML-заглушка «Нет данных»
	// приходит с 200 и без неё.
	fedstatBIFFMagic = "\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
)

var (
	// Переменные (а не константы) — тесты подменяют базовый URL и задержки.
	fedstatBaseURL        = fedstatBaseURLConst
	fedstatRetryDelays    = []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}
	fedstatGetRetryDelays = []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second}

	// fedstatMacroPause — пауза между POST макро-импортёров; серия без паузы
	// получает 503. У 31074 — 30 с (25 годовых запросов, ответ 200–275 КБ).
	fedstatMacroPause = 10 * time.Second
	fedstatIPCPause   = 30 * time.Second
)

// errFedstatNoData — источник ответил HTML «Нет данных для выбранного сочетания
// признаков». Это не пустой ряд и не сбой: комбинация (год/регион/разрез)
// невалидна, импортёр пробует следующий код РФ или пропускает год.
var errFedstatNoData = fmt.Errorf("нет данных для выбранного сочетания признаков")

var (
	// fedstatTokenRe ищет токен в #downloadTokenHolder. На странице несколько
	// name="token"; первый hidden-input принадлежит форме выгрузки (проверено live).
	fedstatTokenRe = regexp.MustCompile(`name="token"\s+value="([^"]+)"`)

	// Разметка JS-конфига grid.init: измерения с 28 пробелами, значения — с 36.
	// Заголовки могут содержать экранированную кавычку — поэтому (?:[^'\\]|\\.)*.
	fedstatDimStartRe = regexp.MustCompile(`(?m)^ {28}(\d{1,7}): \{\n {32}title: '((?:[^'\\]|\\.)*)'`)
	fedstatDimValueRe = regexp.MustCompile(`(?m)^ {36}(\d{1,7}): \{\n {40}title: '((?:[^'\\]|\\.)*)',\n {40}order: (\d+),\n {40}checked: (true|false)`)
)

// fedstatField — поле формы в порядке отправки. Порядок ВАЖЕН: сервер принимает
// и multiset, и selectedFilterIds в порядке осей, но при алфавитном порядке
// ключей (url.Values.Encode) отвечает пустым GenericData без Series.
type fedstatField struct {
	key   string
	value string
}

func fedstatEncodeFields(fields []fedstatField) []byte {
	var body strings.Builder
	for i, field := range fields {
		if i > 0 {
			body.WriteByte('&')
		}
		body.WriteString(url.QueryEscape(field.key))
		body.WriteByte('=')
		body.WriteString(url.QueryEscape(field.value))
	}
	return []byte(body.String())
}

// fedstatMacroClient — клиент макро-импортёров: к request-context добавляется
// общий Timeout клиента. На живой серии 31074 GET иногда зависал на минуты, не
// реагируя на 60-секундный контекст; Timeout — страховка, чтобы прогон не висел
// без лога. Для 37426 клиент оставлен прежним (поведение не меняем).
func fedstatMacroClient() *http.Client {
	client := util.NewDefaultTLSClient()
	client.Timeout = fedstatExportTimeout
	return client
}

// fedstatGetPage — GET страницы индикатора с retry: обрыв GET на длинной серии
// POST — штатное поведение источника (наблюдалось 2026-10-21, восстановление
// ≈5 мин), поэтому последняя пауза 300 с.
func fedstatGetPage(ctx context.Context, client *http.Client, indicatorID string) ([]byte, error) {
	var last error
	for attempt := 0; ; attempt++ {
		page, retry, err := fedstatGetPageOnce(ctx, client, indicatorID)
		if err == nil {
			return page, nil
		}
		last = err
		if !retry || attempt >= len(fedstatGetRetryDelays) {
			return nil, last
		}
		delay := fedstatGetRetryDelays[attempt]
		log.Warnf("%s: GET не удался (%v), повтор через %s", indicatorID, err, delay)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: GET отменён: %w", indicatorID, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func fedstatGetPageOnce(ctx context.Context, client *http.Client, indicatorID string) (body []byte, retry bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, fedstatPageTimeout)
	defer cancel()
	pageURL := fedstatBaseURL + "/indicator/" + indicatorID
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("%s: страница: %w", indicatorID, err)
	}
	req.Header.Set("User-Agent", util.HttpUA)
	resp, err := client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("%s: страница %s: %w", indicatorID, pageURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(io.LimitReader(resp.Body, fedstatMaxBodyBytes))
	if err != nil {
		return nil, true, fmt.Errorf("%s: страница %s: тело не прочитано: %w", indicatorID, pageURL, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("%s: страница %s: http status %d: %s", indicatorID, pageURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("%s: страница %s: http status %d: %s", indicatorID, pageURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if len(body) == 0 {
		return nil, true, fmt.Errorf("%s: страница %s: пустое тело", indicatorID, pageURL)
	}
	return body, false, nil
}

func fedstatToken(page []byte, indicatorID string) (string, error) {
	match := fedstatTokenRe.FindSubmatch(page)
	if len(match) != 2 {
		return "", fmt.Errorf("%s: CSRF-токен (downloadTokenHolder) не найден на странице %s/indicator/%s", indicatorID, fedstatBaseURL, indicatorID)
	}
	return string(match[1]), nil
}

// fedstatPageConfig — коды значений запрошенных измерений в порядке order.
// Нужен для all-dims: у 57806 это все ОКВЭД2, у 31074 — все 1 105 товаров;
// зашивать их в код нельзя — справочник меняется, а POST требует коды, не имена.
func fedstatPageConfig(page []byte, dims []string) (map[string][]string, error) {
	// Страница отдаётся с CRLF; JS-конфиг размечается отступом строк, поэтому
	// переводы строк нормализуются до LF (research-проба делала то же самое).
	page = bytes.ReplaceAll(page, []byte("\r\n"), []byte("\n"))
	starts := fedstatDimStartRe.FindAllSubmatchIndex(page, -1)
	if len(starts) == 0 {
		return nil, fmt.Errorf("%s: JS-конфиг grid.init не найден на странице (%d байт): %s", fedstatBaseURL, len(page), util.ErrorSnippet(page))
	}
	blocks := make([]fedstatDimBlock, 0, len(starts))
	for i, match := range starts {
		end := len(page)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		blocks = append(blocks, fedstatDimBlock{id: string(page[match[2]:match[3]]), start: match[0], end: end})
	}

	result := make(map[string][]string, len(dims))
	for _, dim := range dims {
		block, ok := fedstatFindDim(blocks, dim)
		if !ok {
			return nil, fmt.Errorf("измерение %s не найдено в JS-конфиге страницы %s", dim, fedstatBaseURL)
		}
		values := fedstatDimValueRe.FindAllSubmatch(page[block.start:block.end], -1)
		if len(values) == 0 {
			return nil, fmt.Errorf("у измерения %s нет значений в JS-конфиге страницы %s", dim, fedstatBaseURL)
		}
		type ordered struct {
			code  string
			order int
		}
		items := make([]ordered, 0, len(values))
		for _, match := range values {
			order, err := strconv.Atoi(string(match[3]))
			if err != nil {
				return nil, fmt.Errorf("измерение %s: order %q не число: %w", dim, match[3], err)
			}
			items = append(items, ordered{code: string(match[1]), order: order})
		}
		sort.SliceStable(items, func(i, j int) bool { return items[i].order < items[j].order })
		codes := make([]string, 0, len(items))
		for _, item := range items {
			codes = append(codes, item.code)
		}
		result[dim] = codes
	}
	return result, nil
}

type fedstatDimBlock struct {
	id    string
	start int
	end   int
}

func fedstatFindDim(blocks []fedstatDimBlock, dim string) (fedstatDimBlock, bool) {
	for _, block := range blocks {
		if block.id == dim {
			return block, true
		}
	}
	return fedstatDimBlock{}, false
}

// fedstatPostSDMX — POST с повтором на 429/5xx. Тело кодируется один раз:
// повтор не должен пересобирать запрос.
func fedstatPostSDMX(ctx context.Context, client *http.Client, indicatorID string, encoded []byte) ([]byte, error) {
	return fedstatRetryPost(ctx, indicatorID+" sdmx", func(ctx context.Context) ([]byte, bool, error) {
		return fedstatPostSDMXOnce(ctx, client, indicatorID, encoded)
	})
}

func fedstatPostSDMXOnce(ctx context.Context, client *http.Client, indicatorID string, encoded []byte) (body []byte, retry bool, err error) {
	body, retry, err = fedstatPostOnce(ctx, client, indicatorID, "sdmx", encoded)
	if err != nil {
		return nil, retry, err
	}
	if !bytes.Contains(body, []byte("<GenericData")) {
		return nil, false, fmt.Errorf("%s: POST sdmx: в ответе нет GenericData: %s", indicatorID, util.ErrorSnippet(body))
	}
	return body, false, nil
}

// fedstatPostExcel — POST format=excel. Ожидается BIFF .xls; HTML «Нет данных»
// отдаётся отдельным сентинелом и не повторяется (retry=false).
func fedstatPostExcel(ctx context.Context, client *http.Client, indicatorID string, encoded []byte) ([]byte, error) {
	return fedstatRetryPost(ctx, indicatorID+" excel", func(ctx context.Context) ([]byte, bool, error) {
		return fedstatPostExcelOnce(ctx, client, indicatorID, encoded)
	})
}

func fedstatPostExcelOnce(ctx context.Context, client *http.Client, indicatorID string, encoded []byte) (body []byte, retry bool, err error) {
	body, retry, err = fedstatPostOnce(ctx, client, indicatorID, "excel", encoded)
	if err != nil {
		return nil, retry, err
	}
	if len(body) < len(fedstatBIFFMagic) || string(body[:len(fedstatBIFFMagic)]) != fedstatBIFFMagic {
		text := string(body)
		if strings.Contains(strings.ToLower(text), "нет данных") {
			return nil, false, fmt.Errorf("%s: %w", indicatorID, errFedstatNoData)
		}
		return nil, false, fmt.Errorf("%s: POST excel: ответ не BIFF .xls: %s", indicatorID, util.ErrorSnippet(body))
	}
	return body, false, nil
}

func fedstatRetryPost(ctx context.Context, what string, post func(context.Context) ([]byte, bool, error)) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		body, retry, err := post(ctx)
		if err == nil {
			return body, nil
		}
		if !retry || attempt >= len(fedstatRetryDelays) {
			return nil, err
		}
		delay := fedstatRetryDelays[attempt]
		log.Warnf("%s: POST не удался (%v), повтор через %s", what, err, delay)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: POST отменён: %w", what, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func fedstatPostOnce(ctx context.Context, client *http.Client, indicatorID, format string, encoded []byte) (body []byte, retry bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, fedstatExportTimeout)
	defer cancel()
	exportURL := fedstatBaseURL + "/indicator/downloadData.do?format=" + format
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, exportURL, bytes.NewReader(encoded))
	if err != nil {
		return nil, false, fmt.Errorf("%s: POST: %w", indicatorID, err)
	}
	req.Header.Set("User-Agent", util.HttpUA)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", fedstatBaseURL+"/indicator/"+indicatorID)
	req.Header.Set("Origin", fedstatBaseURL)
	resp, err := client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("%s: POST %s: %w", indicatorID, exportURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(io.LimitReader(resp.Body, fedstatMaxBodyBytes+1))
	if err != nil {
		return nil, true, fmt.Errorf("%s: POST %s: тело не прочитано: %w", indicatorID, exportURL, err)
	}
	if len(body) > fedstatMaxBodyBytes {
		return nil, false, fmt.Errorf("%s: POST %s: ответ больше %d байт", indicatorID, exportURL, fedstatMaxBodyBytes)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("%s: POST %s: http status %d: %s", indicatorID, exportURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("%s: POST %s: http status %d: %s", indicatorID, exportURL, resp.StatusCode, util.ErrorSnippet(body))
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "html") && format == "sdmx" {
		return nil, false, fmt.Errorf("%s: POST %s: источник вернул HTML вместо SDMX XML (вероятно, за выбранный период данных нет): %s", indicatorID, exportURL, util.ErrorSnippet(body))
	}
	return body, false, nil
}
