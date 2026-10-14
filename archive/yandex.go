package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultYandexAPI — адрес REST API Диска. Константа, а не переменная окружения: адрес API — не
// конфигурация конвейера, а его знание; подменяется он только тестами (httptest) через baseURL.
const defaultYandexAPI = "https://cloud-api.yandex.net/v1/disk"

// opTimeout — дедлайн одной операции Store (Put/Get/Exists). Переменная, а не константа, потому что
// тесты подменяют её коротким значением. Дедлайн обязателен именно здесь: util.HttpClient создан
// без Timeout (правило 4 AGENTS.md), а зависший Диск не должен держать прогон — архив объявлен
// опцией, сбой которой не останавливает данные (спека §3.1).
var opTimeout = 2 * time.Minute

// retrySleeps — паузы между попытками шага: две паузы — до трёх попыток. 429 и 503 Диск отдаёт на
// всплеск запросов и на свои работы, а повтор дешевле сбоя архивации: сбой архивации — это сбой
// записи версии (спека §3.3), то есть следующий прогон пойдёт в полный импорт. Переменная: тесты
// подменяют паузы миллисекундой.
var retrySleeps = []time.Duration{time.Second, 2 * time.Second}

// Операции в текстах ошибок: дежурный по логу видит, на каком шаге сломалось, и правит либо токен,
// либо корень архива, а не ищет шаг по коду.
const (
	opMkdir        = "создание папки"
	opUploadHref   = "получение ссылки на загрузку"
	opUpload       = "загрузка"
	opDownloadHref = "получение ссылки на скачивание"
	opDownload     = "скачивание"
	opExists       = "проверка наличия"
)

// YandexDisk — хранилище версий на Яндекс.Диске через REST API (resources/upload/download).
// Своего каталога не знает: путь на Диске собирается из корня архива и относительного ключа,
// поэтому смена корня не меняет ключи, а смена бэкенда — реестр и таблицу версий. Каталоги бэкенд
// заводит сам: иначе первая же архивация в новый корень падала бы «путь не найден», то есть архив
// пришлось бы заводить руками — и пропущенный шаг заметили бы только при попытке восстановления,
// когда файла у источника уже нет.
type YandexDisk struct {
	token   string       // OAuth-токен: живёт в этом поле и в заголовке к API, дальше никуда не идёт
	root    string       // корень архива на Диске вида disk:/rosstat-archive
	baseURL string       // адрес API; в проде пуст → defaultYandexAPI, в тестах — httptest-сервер
	hc      *http.Client // клиент вызывающего: у прода util.HttpClient (TLS-сертификаты РФ)
}

// YandexDisk обязан удовлетворять Store: бэкенды подставляются друг вместо друга (local в dev и
// на приёмке, Диск в проде), и расхождение сигнатур должно ломать сборку, а не прогон.
var _ Store = (*YandexDisk)(nil)

// NewYandex — хранилище на Диске. hc обязателен и не подменяется своим: прод передаёт
// util.HttpClient, единственный в проекте клиент с национальными TLS-сертификатами (правило 4
// AGENTS.md). baseURL пуст в проде и задаётся тестами.
func NewYandex(token, root, baseURL string, hc *http.Client) *YandexDisk {
	if baseURL == "" {
		baseURL = defaultYandexAPI
	}
	return &YandexDisk{token: token, root: root, baseURL: baseURL, hc: hc}
}

func (s *YandexDisk) Name() string {
	return "yandex"
}

func (s *YandexDisk) Put(ctx context.Context, key string, body []byte) error {
	// Дедлайн — на операцию целиком, а не на отдельный запрос: файл выгрузки уходит на Диск одним
	// телом, и таймаут на запрос обрывал бы загрузку на медленном канале, тогда как смысл дедлайна —
	// не дать зависшему хранилищу держать прогон.
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	diskPath := s.diskPath(key)
	for _, dir := range dirsToCreate(diskPath) {
		if err := s.mkdir(ctx, dir); err != nil {
			return err
		}
	}
	href, err := s.signedHref(ctx, opUploadHref, diskPath, "/resources/upload",
		url.Values{"path": {diskPath}, "overwrite": {"true"}})
	if err != nil {
		return err
	}
	return s.upload(ctx, diskPath, href, body)
}

func (s *YandexDisk) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	diskPath := s.diskPath(key)
	href, err := s.signedHref(ctx, opDownloadHref, diskPath, "/resources/download",
		url.Values{"path": {diskPath}})
	if err != nil {
		return nil, err
	}
	body, status, err := s.do(ctx, opDownload, diskPath, http.MethodGet, href, nil, false)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		// Тело не передаётся: ответ пришёл от хоста по подписанной ссылке, а её подпись в тексте
		// ошибки — утечка (statusError).
		return nil, statusError(opDownload, diskPath, status, nil)
	}
	return body, nil
}

// Exists — есть ли версия: GET ресурса отвечает метаданными объекта (200) или «не найдено» (404).
// Отдельной ручки «проверить» у Диска нет, но метаданных и достаточно: префлайт спрашивает только
// «есть или нет», чтобы не качать файл зря (спека §3.2).
func (s *YandexDisk) Exists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	diskPath := s.diskPath(key)
	reqURL := s.apiURL("/resources", url.Values{"path": {diskPath}})
	body, status, err := s.do(ctx, opExists, diskPath, http.MethodGet, reqURL, nil, true)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		// «Нет объекта» — ответ, а не ошибка: тем же (false, nil) заканчивается LocalStore.Exists,
		// иначе префлайт принимал бы штатный ответ «версии нет» за сбой проверки.
		return false, nil
	}
	// body — запрос к API с токеном: тело безопасно для лога и различает причины одного кода (statusError).
	return false, statusError(opExists, diskPath, status, body)
}

// mkdir заводит один каталог. 409 — успех: так Диск отвечает на уже существующую папку, и без
// этого второй Put в тот же каталог источника падал бы всегда, то есть архив работал бы ровно один
// раз на источник.
func (s *YandexDisk) mkdir(ctx context.Context, dir string) error {
	const endpoint = "/resources"
	body, status, err := s.do(ctx, opMkdir, dir, http.MethodPut, s.apiURL(endpoint, url.Values{"path": {dir}}), nil, true)
	if err != nil {
		return err
	}
	if status != http.StatusCreated && status != http.StatusConflict {
		// body — запрос к API с токеном: по фрагменту ответа дежурный различает «нет прав» и «нет места».
		return statusError(opMkdir, dir, status, body)
	}
	return nil
}

// signedHref спрашивает у ручки ресурсов подписанную ссылку на загрузку или скачивание. Сама ссылка
// дальше живёт в локальной переменной: в лог и в тексты ошибок она не попадает — в её query вшито
// право на запись/чтение объекта, то есть временный секрет (спека §3.3).
func (s *YandexDisk) signedHref(ctx context.Context, op, diskPath, endpoint string, params url.Values) (string, error) {
	body, status, err := s.do(ctx, op, diskPath, http.MethodGet, s.apiURL(endpoint, params), nil, true)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		// body — ответ ручки API, а не хоста по ссылке: href в нём не содержится, и фрагмент помогает
		// отличить «нет прав» (токен выдан без scope) от квоты (statusError).
		return "", statusError(op, diskPath, status, body)
	}
	var resp struct {
		Href string `json:"href"`
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("%s %s: разбор ответа: %w", op, diskPath, err)
	}
	// Пустая ссылка — ошибка сразу: запрос на пустой URL дал бы «unsupported protocol scheme», и по
	// такому тексту дежурный не понял бы, что Диск не выдал ссылку.
	if resp.Href == "" {
		return "", fmt.Errorf("%s %s: Диск не вернул ссылку", op, diskPath)
	}
	return resp.Href, nil
}

// upload кладёт тело на подписанную ссылку. Токен туда не добавляется: ссылка уже авторизована, а
// лишний заголовок унёс бы секрет на хост загрузчика. 201 и 202 равноправны — 202 Диск отдаёт,
// когда загрузка встаёт в очередь.
func (s *YandexDisk) upload(ctx context.Context, diskPath, href string, body []byte) error {
	_, status, err := s.do(ctx, opUpload, diskPath, http.MethodPut, href, body, false)
	if err != nil {
		return err
	}
	if status != http.StatusCreated && status != http.StatusAccepted {
		// Тело не передаётся: отвечал хост по подписанной ссылке (см. statusError).
		return statusError(opUpload, diskPath, status, nil)
	}
	return nil
}

// do выполняет один HTTP-шаг с повторами и возвращает тело и статус последней попытки. Повтор — на
// шаг, а не на операцию: повторять ручку ссылки дешевле, чем перекачивать тело файла выгрузки.
// Запрос строится заново на каждую попытку, потому что body — reader, израсходованный первой
// попыткой. withToken включает авторизацию: к API — с токеном, по подписанной ссылке — без него.
func (s *YandexDisk) do(ctx context.Context, op, diskPath, method, rawURL string, body []byte, withToken bool) ([]byte, int, error) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, retrySleeps[attempt-1]); err != nil {
				return nil, 0, fmt.Errorf("%s %s: %w", op, diskPath, err)
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
		if err != nil {
			// Разбор URL тоже отдаёт *url.Error с адресом запроса, а им бывает подписанная ссылка.
			return nil, 0, fmt.Errorf("%s %s: %w", op, diskPath, transportError(err))
		}
		if withToken {
			req.Header.Set("Authorization", "OAuth "+s.token)
		}
		resp, err := s.hc.Do(req)
		if err != nil {
			// Причина сбоя — без URL запроса: у запросов по ссылке URL и есть подписанный href,
			// а текст ошибки уходит в лог (см. transportError).
			return nil, 0, fmt.Errorf("%s %s: %w", op, diskPath, transportError(err))
		}
		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, 0, fmt.Errorf("%s %s: чтение ответа: %w", op, diskPath, readErr)
		}
		// 429 и 503 — «сбавь темп», а не отказ: шаг повторяется, пока в запасе есть пауза.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			if attempt < len(retrySleeps) {
				continue
			}
		}
		return respBody, resp.StatusCode, nil
	}
}

// errSnippetLimit — сколько байт тела ответа Диска остаётся в тексте ошибки: дежурному нужна причина
// («недостаточно прав», «квота»), а не весь ответ ручки.
const errSnippetLimit = 200

// errSnippet сжимает тело ответа до одной строки не длиннее errSnippetLimit байт. Локальная копия
// util.ErrorSnippet, а не импорт: util уже импортирует archive, и обратная зависимость замкнула бы
// цикл импортов. Пустое тело — пустая строка: текст ошибки и без того называет шаг и путь, и
// подставлять в него «(пустое тело)» незачем. Обрезка идёт по байту, а нечитаемая руна на срезе
// вычищается ToValidUTF8: в ответах Диска бывает кириллица, и разрезанная посреди неё строка
// уехала бы в лог мусором.
func errSnippet(body []byte) string {
	snippet := strings.Join(strings.Fields(string(body)), " ")
	if snippet == "" {
		return ""
	}
	if len(snippet) > errSnippetLimit {
		snippet = strings.ToValidUTF8(snippet[:errSnippetLimit], "") + "…"
	}
	return snippet
}

// statusError — ошибка шага по коду ответа. 401 и «квота» (403/413/423/507) оборачивают sentinel-
// ошибки из store.go: по ним дежурный различает «обновить токен» и «освободить место» (спека §3.2),
// и проверяются они через errors.Is, а не по тексту. Остальные коды — обычная ошибка с номером, по
// которому причину ищут в API Диска. В тексте — путь на Диске, а не URL запроса: правится именно он.
//
// Тело ответа добавляется к тексту фрагментом (errSnippet): один и тот же 403 Диск отдаёт и при
// исчерпанной квоте, и при нехватке прав у токена (выдан без scope на disk:/, часто при app:/), а
// дежурному по одному коду не выбрать — освобождать место или перевыпускать токен. Фрагмент
// передаётся только у запросов к API: ответы хостов по подписанной ссылке (загрузчик, файловый хост)
// бывают HTML-страницей, повторяющей URL запроса вместе с подписью, а подпись — временный секрет
// (см. signedHref), поэтому там тело в текст ошибки не берётся.
func statusError(op, diskPath string, status int, body []byte) error {
	tail := ""
	if snippet := errSnippet(body); snippet != "" {
		tail = "; ответ Диска: " + snippet
	}
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("%s %s: %w%s", op, diskPath, ErrAuth, tail)
	case http.StatusForbidden, http.StatusRequestEntityTooLarge, http.StatusLocked, http.StatusInsufficientStorage:
		return fmt.Errorf("%s %s: %w%s", op, diskPath, ErrQuota, tail)
	default:
		return fmt.Errorf("%s %s: статус %d%s", op, diskPath, status, tail)
	}
}

// transportError убирает URL из ошибки транспорта: http.Client оборачивает сбой в *url.Error, чей
// текст содержит адрес запроса, а адрес загрузки и скачивания — подписанная ссылка href. Причина
// сбоя (отказ соединения, дедлайн) сохраняется: она и нужна логу, чтобы дежурный понял, что чинить.
func transportError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}

// sleep — пауза между попытками, прерываемая отменой контекста: остановленный прогон не должен
// ждать её до конца.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// diskPath — путь объекта на Диске: корень архива плюс относительный ключ. Ключ задаёт вызывающий
// (Key), корень навешивает бэкенд — поэтому один и тот же архив читается из разных корней.
func (s *YandexDisk) diskPath(key string) string {
	return s.root + "/" + key
}

// apiURL — адрес ручки API с query-параметрами. path идёт значением параметра, а не частью пути:
// иначе «disk:/» и кириллица в имени файла разъехались бы по URL.
func (s *YandexDisk) apiURL(endpoint string, params url.Values) string {
	return s.baseURL + endpoint + "?" + params.Encode()
}

// dirsToCreate — каталоги, которые надо завести перед записью объекта: все уровни пути на Диске,
// кроме последнего (самого объекта). Ручка ресурсов заводит одну папку за вызов и требует
// существующего родителя, поэтому уровни перебираются от корня архива: корень бывает вложенным
// (в dev и на приёмке ARCHIVE_YANDEX_DIR разный), а всё выше схемы — диск пользователя, он есть
// всегда. Схема («disk:») отделяется, чтобы склейка не дала «disk://rosstat-archive».
func dirsToCreate(diskPath string) []string {
	scheme, rest := "", diskPath
	if i := strings.Index(diskPath, ":"); i >= 0 {
		scheme, rest = diskPath[:i+1], diskPath[i+1:]
	}
	parts := splitDiskPath(rest)
	dirs := make([]string, 0, len(parts))
	for i := 1; i < len(parts); i++ {
		dirs = append(dirs, joinDiskPath(scheme, parts[:i]))
	}
	return dirs
}

// splitDiskPath режет путь на компоненты, выбрасывая пустые: «/a//b» и «/a/b» — один путь, и лишний
// слэш на стыке корня и ключа не должен превращаться в отдельный уровень.
func splitDiskPath(path string) []string {
	var parts []string
	for _, part := range strings.Split(path, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

// joinDiskPath склеивает компоненты обратно со схемой: «disk:» + «/rosstat-archive/cbr_m2».
func joinDiskPath(scheme string, parts []string) string {
	if scheme == "" {
		return strings.Join(parts, "/")
	}
	return scheme + "/" + strings.Join(parts, "/")
}
