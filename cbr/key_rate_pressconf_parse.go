package cbr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Разбор источника стенограмм пресс-конференций ЦБ (спека
// docs/superpowers/specs/2026-10-23-cbr-key-rate-pressconf-design.md §2, §4).
//
// Источник — плейлист YouTube «Пресс-конференции Председателя Банка России по итогам
// заседания Совета директоров»: страница отдаёт ytInitialData со списком видео и
// visitorData для player API. Субтитры — русская дорожка (обычно auto-generated ASR)
// через POST youtubei/v1/player с клиентом VISIONOS и JSON3-выгрузку по baseUrl.
// Разбор чистый: сеть трогают util.GetPage/util.PostFileBytes/util.GetJSON в
// key_rate_pressconf.go, поэтому тесты офлайн на живых фикстурах.

const (
	// pressconfPlaylistURL — плейлист пресс-конференций ЦБ. hl=ru фиксирует локаль
	// страницы: заголовки видео и так русские, но UI-строки влияют на разбор.
	pressconfPlaylistURL = "https://www.youtube.com/playlist?list=PLQocLHHmkYbCsWDJphENzmIqMcrcU-SxD&hl=ru"

	// pressconfPlayerURL — player API YouTube; ключ (INNERTUBE_API_KEY) приходит со
	// страницы плейлиста и в код не хардкодится. Это публичный ключ страницы, не секрет.
	pressconfPlayerURL = "https://www.youtube.com/youtubei/v1/player"

	// Клиент VISIONOS — проверенный путь к субтитрам (2026-10-23): watch-страница
	// отдаёт baseUrl, который возвращает 0 байт, а player API этого клиента — JSON3.
	pressconfClientName    = "VISIONOS"
	pressconfClientVersion = "1.02"
	pressconfClientID      = "101"
	pressconfClientOrigin  = "https://www.youtube.com"
	pressconfUserAgent     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_7_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15"

	// pressconfCaptionLang — язык дорожки: русская речь ЦБ, другой язык не подходит.
	pressconfCaptionLang = "ru"

	// pressconfKindASR / pressconfKindManual — значения caption_kind: автосубтитры
	// (kind='asr' у YouTube) и ручная дорожка (kind пустой).
	pressconfKindASR    = "asr"
	pressconfKindManual = "manual"
)

var (
	// pressconfTitleRe — заголовок пресс-конференции по ключевой ставке. Регистр
	// не важен, месяц — родительный падеж, день без ведущего нуля.
	pressconfTitleRe = regexp.MustCompile(`(?i)^пресс-конференция по ключевой ставке (\d{1,2}) ([а-яё]+) (\d{4}) года$`)

	// pressconfVisitorRe / pressconfAPIKeyRe — служебные значения страницы плейлиста.
	// YouTube кладёт visitorData и в ytcfg, и в ytInitialData; берём первое вхождение.
	pressconfVisitorRe = regexp.MustCompile(`"(?:visitorData|VISITOR_DATA)":"([^"]+)"`)
	pressconfAPIKeyRe  = regexp.MustCompile(`"INNERTUBE_API_KEY":"([^"]+)"`)

	// errPressconfNoCaptions — у видео нет русской дорожки субтитров: импортёр
	// отличает «ещё не готово» от сбоя разбора. Для сегодняшней конференции — warn,
	// для прошлой — ошибка (спека §5).
	errPressconfNoCaptions = errors.New("русская дорожка субтитров не найдена")
)

// pressconfGetPage скачивает страницу плейлиста через общий util.HttpClient с
// consent-cookie SOCS=CAI. Без неё YouTube из EU отдаёт ConsentUi (HTTP 200, ~0,6 МБ)
// без ytInitialData и visitorData; util.GetPage этого не умеет, потому что не
// принимает заголовки. Cookie — состояние согласия, не секрет.
func pressconfGetPage(pageURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", pageURL, err)
	}
	req.Header.Set("User-Agent", pressconfUserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Cookie", "SOCS=CAI")
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", pageURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GET %s: чтение тела: %w", pageURL, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail := string(body)
		if len(detail) > 256 {
			detail = detail[:256]
		}
		return nil, fmt.Errorf("GET %s: status %d: %s", pageURL, resp.StatusCode, detail)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("GET %s: body size is empty", pageURL)
	}
	return body, nil
}

// pressconfItem — позиция плейлиста, прошедшая фильтр заголовка: video_id, заголовок
// и дата заседания из заголовка.
type pressconfItem struct {
	VideoID string
	Title   string
	Date    time.Time
}

// pressconfCue — реплика субтитров: порядок, таймкоды и текст. Seq — порядок
// реплики в документе (с 1); StartMs/EndMs — миллисекунды от начала видео.
type pressconfCue struct {
	Seq     uint16
	StartMs uint32
	EndMs   uint32
	Text    string
}

// pressconfTrack — дорожка субтитров из player response. Kind пустой — ручная
// дорожка, "asr" — автосубтитры.
type pressconfTrack struct {
	BaseURL      string              `json:"baseUrl"`
	LanguageCode string              `json:"languageCode"`
	Kind         string              `json:"kind"`
	Name         pressconfTrackTitle `json:"name"`
}

// pressconfTrackTitle — подпись дорожки (у auto-generated — «rosyjski (wygenerowane
// automatycznie)» в зависимости от локали).
type pressconfTrackTitle struct {
	SimpleText string `json:"simpleText"`
}

// pressconfPlayerDoc — нужная часть ответа youtubei/v1/player: статус, метаданные
// видео и дорожки субтитров. Остальные поля (streamingData, playerConfig) не читаются.
type pressconfPlayerDoc struct {
	PlayabilityStatus struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	} `json:"playabilityStatus"`
	VideoDetails struct {
		VideoID       string `json:"videoId"`
		Title         string `json:"title"`
		LengthSeconds string `json:"lengthSeconds"`
	} `json:"videoDetails"`
	Captions struct {
		PlayerCaptionsTracklistRenderer struct {
			CaptionTracks []pressconfTrack `json:"captionTracks"`
		} `json:"playerCaptionsTracklistRenderer"`
	} `json:"captions"`
}

// pressconfCaptionDoc — JSON3-выгрузка дорожки: события с таймкодами и сегментами
// текста. aAppend — служебное событие-перевод строки, не реплика.
type pressconfCaptionDoc struct {
	Events []pressconfCaptionEvent `json:"events"`
}

type pressconfCaptionEvent struct {
	TStartMs    int64              `json:"tStartMs"`
	DDurationMs int64              `json:"dDurationMs"`
	AAppend     *int               `json:"aAppend"`
	Segs        []pressconfSegment `json:"segs"`
}

type pressconfSegment struct {
	UTF8 string `json:"utf8"`
}

// pressconfPageConfig достаёт visitorData и INNERTUBE_API_KEY со страницы плейлиста.
// Оба значения нужны player API; без них запрос получит LOGIN_REQUIRED/бот-проверку.
func pressconfPageConfig(page []byte) (visitor, apiKey string, err error) {
	visitorMatch := pressconfVisitorRe.FindSubmatch(page)
	if visitorMatch == nil {
		return "", "", fmt.Errorf("%s: на странице плейлиста нет visitorData — вёрстка YouTube изменилась", pressconfName)
	}
	keyMatch := pressconfAPIKeyRe.FindSubmatch(page)
	if keyMatch == nil {
		return "", "", fmt.Errorf("%s: на странице плейлиста нет INNERTUBE_API_KEY — вёрстка YouTube изменилась", pressconfName)
	}
	return string(visitorMatch[1]), string(keyMatch[1]), nil
}

// pressconfPlaylistItems разбирает страницу плейлиста и возвращает только
// пресс-конференции по ключевой ставке, отсортированные по дате. Ноль совпадений —
// ошибка: структура страницы или заголовки изменились, тихий 0 строк скрыл бы это.
func pressconfPlaylistItems(page []byte) ([]pressconfItem, error) {
	data, err := pressconfInitialData(page)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var items []pressconfItem
	pressconfWalk(data, func(node map[string]any) {
		videoID, title, ok := pressconfLockup(node)
		if !ok || videoID == "" || title == "" {
			return
		}
		date, dateErr := pressconfDate(title)
		if dateErr != nil {
			return
		}
		// Дедуп только по прошедшим фильтр: одно и то же видео может встретиться
		// в дереве раньше с обрезанным заголовком (fallback videoId/title), и
		// отметка «уже видели» до фильтра потеряла бы настоящий lockup.
		if seen[videoID] {
			return
		}
		seen[videoID] = true
		items = append(items, pressconfItem{VideoID: videoID, Title: title, Date: mpDay(date)})
	})
	if len(items) == 0 {
		return nil, fmt.Errorf("%s: в плейлисте нет ни одной пресс-конференции по ключевой ставке — вёрстка или заголовки изменились", pressconfName)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Date.Before(items[j].Date) })
	return items, nil
}

// pressconfInitialData вынимает JSON-значение ytInitialData из HTML. json.Decoder
// читает ровно один объект и сам разбирается со скобками внутри строк — счётчик
// скобок на живом JSON ломался бы на фигурных скобках в текстах.
func pressconfInitialData(page []byte) (map[string]any, error) {
	marker := []byte("ytInitialData")
	idx := bytes.Index(page, marker)
	if idx < 0 {
		return nil, fmt.Errorf("%s: на странице плейлиста нет ytInitialData — вёрстка YouTube изменилась", pressconfName)
	}
	rest := page[idx+len(marker):]
	brace := bytes.IndexByte(rest, '{')
	if brace < 0 {
		return nil, fmt.Errorf("%s: ytInitialData без JSON-объекта — вёрстка YouTube изменилась", pressconfName)
	}
	var data map[string]any
	if err := json.NewDecoder(bytes.NewReader(rest[brace:])).Decode(&data); err != nil {
		return nil, fmt.Errorf("%s: ytInitialData не разобран: %w", pressconfName, err)
	}
	return data, nil
}

// pressconfWalk обходит дерево JSON и зовёт fn для каждого объекта. Рекурсия, а не
// фиксированный путь contents/.../lockupViewModel: YouTube перекладывает секции, а
// обход находит элементы независимо от вложенности.
func pressconfWalk(node any, fn func(map[string]any)) {
	switch value := node.(type) {
	case map[string]any:
		fn(value)
		for _, child := range value {
			pressconfWalk(child, fn)
		}
	case []any:
		for _, child := range value {
			pressconfWalk(child, fn)
		}
	}
}

// pressconfLockup достаёт videoId и заголовок из элемента плейлиста. Основная форма —
// lockupViewModel (contentId + metadata.lockupMetadataViewModel.title.content);
// fallback — старый playlistVideoRenderer (videoId + title.runs[].text).
func pressconfLockup(node map[string]any) (videoID, title string, ok bool) {
	if lockup, isLockup := node["lockupViewModel"].(map[string]any); isLockup {
		videoID, _ = lockup["contentId"].(string)
		title = pressconfNestedString(lockup, "metadata", "lockupMetadataViewModel", "title", "content")
		if videoID != "" && title != "" {
			return videoID, title, true
		}
	}
	videoID, _ = node["videoId"].(string)
	if videoID == "" {
		return "", "", false
	}
	title = pressconfRunsText(node["title"])
	if title == "" {
		return "", "", false
	}
	return videoID, title, true
}

// pressconfNestedString — строковое значение по цепочке ключей; любой не-объект или
// не-строка даёт пустую строку (элемент без нужного поля просто не попадёт в выборку).
func pressconfNestedString(node any, keys ...string) string {
	current := node
	for _, key := range keys {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = object[key]
		if !ok {
			return ""
		}
	}
	value, _ := current.(string)
	return value
}

// pressconfRunsText склеивает title.runs[].text старого рендерера.
func pressconfRunsText(node any) string {
	object, ok := node.(map[string]any)
	if !ok {
		return ""
	}
	runs, ok := object["runs"].([]any)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, run := range runs {
		entry, ok := run.(map[string]any)
		if !ok {
			continue
		}
		if text, ok := entry["text"].(string); ok {
			b.WriteString(text)
		}
	}
	return b.String()
}

// pressconfDate разбирает дату из заголовка «Пресс-конференция по ключевой ставке
// 23 октября 2026 года». Заголовок без этой формы — не пресс-конференция контура.
func pressconfDate(title string) (time.Time, error) {
	normalized := strings.TrimSpace(normMPText(title))
	match := pressconfTitleRe.FindStringSubmatch(normalized)
	if match == nil {
		return time.Time{}, fmt.Errorf("заголовок %q: ожидался «Пресс-конференция по ключевой ставке <Д> <месяц> <ГГГГ> года»", title)
	}
	month, ok := mpMonths[strings.ToLower(match[2])]
	if !ok {
		return time.Time{}, fmt.Errorf("заголовок %q: месяц %q не распознан", title, match[2])
	}
	day, err := strconv.Atoi(match[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("заголовок %q: день %q: %w", title, match[1], err)
	}
	year, err := strconv.Atoi(match[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("заголовок %q: год %q: %w", title, match[3], err)
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC), nil
}

// pressconfWanted — пресс-конференции в окне истории: от pressconfHistoryStart до
// сегодня включительно. Будущие конференции не импортируются; дубликаты дат
// схлопываются (первая позиция плейлиста — самая новая).
func pressconfWanted(items []pressconfItem, start, today time.Time) []pressconfItem {
	out := make([]pressconfItem, 0, len(items))
	seen := make(map[time.Time]bool, len(items))
	for _, item := range items {
		date := mpDay(item.Date)
		if date.Before(start) || date.After(today) || seen[date] {
			continue
		}
		seen[date] = true
		out = append(out, pressconfItem{VideoID: item.VideoID, Title: item.Title, Date: date})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Date.Before(out[j].Date) })
	return out
}

// pressconfHeader — записанная шапка: число реплик, ожидаемое в таблице реплик.
type pressconfHeader struct {
	Cues uint16
}

// pressconfMissing — что перекачать: неизвестные даты, шапки без реплик и даты, где
// реплик в таблице меньше, чем обещает шапка (самолечение сбоя второго батча).
// Лишние хвостовые строки (старая версия была длиннее) дырой не считаются.
func pressconfMissing(wanted []pressconfItem, headers map[time.Time]pressconfHeader, cueCounts map[time.Time]uint64) []pressconfItem {
	out := make([]pressconfItem, 0, len(wanted))
	for _, item := range wanted {
		date := mpDay(item.Date)
		header, known := headers[date]
		if !known || header.Cues == 0 || cueCounts[date] < uint64(header.Cues) {
			out = append(out, item)
		}
	}
	return out
}

// pressconfPlayer запрашивает player API с клиентом VISIONOS. Заголовки и тело —
// контракт с YouTube: без visitorData и X-Goog-Visitor-Id приходит LOGIN_REQUIRED.
// Ключ API — публичный ключ страницы, не секрет; в код не хардкодится.
func pressconfPlayer(videoID, visitor, apiKey string) (*pressconfPlayerDoc, error) {
	body, err := json.Marshal(map[string]any{
		"context": map[string]any{
			"client": map[string]any{
				"clientName":    pressconfClientName,
				"clientVersion": pressconfClientVersion,
				"deviceMake":    "Apple",
				"deviceModel":   "RealityDevice17,1",
				"osName":        "visionOS",
				"osVersion":     "26.5.23O471",
				"hl":            "ru",
				"gl":            "RU",
				"visitorData":   visitor,
			},
		},
		"videoId":        videoID,
		"contentCheckOk": true,
		"racyCheckOk":    true,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: тело player-запроса: %w", pressconfName, err)
	}
	headers := map[string]string{
		"Content-Type":             "application/json",
		"User-Agent":               pressconfUserAgent,
		"X-Goog-Visitor-Id":        visitor,
		"X-YouTube-Client-Name":    pressconfClientID,
		"X-YouTube-Client-Version": pressconfClientVersion,
		"Origin":                   pressconfClientOrigin,
	}
	playerURL := pressconfPlayerURL + "?key=" + url.QueryEscape(apiKey)
	raw, err := util.PostFileBytes(playerURL, body, headers)
	if err != nil {
		return nil, fmt.Errorf("%s: player %s: %w", pressconfName, videoID, err)
	}
	var doc pressconfPlayerDoc
	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("%s: player %s: разбор ответа: %w", pressconfName, videoID, err)
	}
	if doc.PlayabilityStatus.Status != "OK" {
		reason := strings.TrimSpace(doc.PlayabilityStatus.Reason)
		if reason == "" {
			reason = "причина не указана"
		}
		return nil, fmt.Errorf("%s: player %s: статус %q: %s", pressconfName, videoID, doc.PlayabilityStatus.Status, reason)
	}
	return &doc, nil
}

// pressconfVerifyPlayer сверяет ответ player API с позицией плейлиста: videoId и дата
// в заголовке видео обязаны совпасть. Иначе плейлист перенаправил на чужое видео, и
// записывать его под датой заседания нельзя.
func pressconfVerifyPlayer(doc *pressconfPlayerDoc, item pressconfItem) error {
	if doc.VideoDetails.VideoID != "" && doc.VideoDetails.VideoID != item.VideoID {
		return fmt.Errorf("%s: player %s вернул videoId %q — плейлист указывает на другое видео",
			pressconfName, item.VideoID, doc.VideoDetails.VideoID)
	}
	date, err := pressconfDate(doc.VideoDetails.Title)
	if err != nil {
		return fmt.Errorf("%s: player %s: %w", pressconfName, item.VideoID, err)
	}
	if !mpDay(date).Equal(mpDay(item.Date)) {
		return fmt.Errorf("%s: player %s: дата видео %s не совпадает с датой плейлиста %s",
			pressconfName, item.VideoID, mpDay(date).Format(pressconfDayLayout), mpDay(item.Date).Format(pressconfDayLayout))
	}
	return nil
}

// pressconfPickTrack выбирает русскую дорожку: ручная (kind пустой) приоритетнее
// автосубтитров. Допускается региональный тег ru-RU. Нет русской — sentinel.
func pressconfPickTrack(tracks []pressconfTrack) (pressconfTrack, error) {
	var asr *pressconfTrack
	for i := range tracks {
		track := tracks[i]
		if !pressconfRussian(track.LanguageCode) {
			continue
		}
		if track.Kind == "" {
			return track, nil
		}
		if track.Kind == pressconfKindASR && asr == nil {
			copy := track
			asr = &copy
		}
	}
	if asr != nil {
		return *asr, nil
	}
	return pressconfTrack{}, errPressconfNoCaptions
}

// pressconfRussian — язык дорожки русский: ru или региональный ru-*.
func pressconfRussian(lang string) bool {
	return lang == pressconfCaptionLang || strings.HasPrefix(lang, pressconfCaptionLang+"-")
}

// pressconfCaptionKind — значение caption_kind: пустой kind у YouTube — ручная
// дорожка, "asr" — автосубтитры.
func pressconfCaptionKind(kind string) string {
	if kind == "" {
		return pressconfKindManual
	}
	return pressconfKindASR
}

// pressconfCaptionURL добавляет fmt=json3 к подписанному baseUrl дорожки. У baseUrl
// из player response параметра fmt нет (проверено 2026-10-23); добавление в конец не
// ломает подпись, потому что fmt не входит в sparams.
func pressconfCaptionURL(baseURL string) string {
	separator := "?"
	if strings.Contains(baseURL, "?") {
		separator = "&"
	}
	return baseURL + separator + "fmt=json3"
}

// pressconfDuration — длительность видео в миллисекундах из lengthSeconds.
func pressconfDuration(doc *pressconfPlayerDoc) (uint32, error) {
	seconds, err := strconv.ParseUint(doc.VideoDetails.LengthSeconds, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s: player %s: lengthSeconds %q: %w", pressconfName, doc.VideoDetails.VideoID, doc.VideoDetails.LengthSeconds, err)
	}
	if seconds > math.MaxUint32/1000 {
		return 0, fmt.Errorf("%s: player %s: lengthSeconds %d не влезает в UInt32 миллисекунд", pressconfName, doc.VideoDetails.VideoID, seconds)
	}
	return uint32(seconds * 1000), nil
}

// pressconfParseCues разбирает JSON3 в реплики: aAppend и пустой текст пропускаются,
// порядок — по tStartMs, seq с 1. Ноль реплик — ошибка: пустая выгрузка не должна
// выглядеть успешным импортом.
func pressconfParseCues(doc *pressconfCaptionDoc) ([]pressconfCue, error) {
	cues := make([]pressconfCue, 0, len(doc.Events))
	for _, event := range doc.Events {
		if event.AAppend != nil {
			continue
		}
		text := pressconfCueText(event.Segs)
		if text == "" {
			continue
		}
		start := event.TStartMs
		if start < 0 {
			start = 0
		}
		end := start + event.DDurationMs
		if event.DDurationMs < 0 || end < start {
			end = start
		}
		if start > math.MaxUint32 || end > math.MaxUint32 {
			return nil, fmt.Errorf("%s: таймкод реплики %d…%d не влезает в UInt32", pressconfName, start, end)
		}
		cues = append(cues, pressconfCue{StartMs: uint32(start), EndMs: uint32(end), Text: text})
	}
	if len(cues) == 0 {
		return nil, fmt.Errorf("%s: субтитры пусты — формат JSON3 изменился или дорожка не содержит текста", pressconfName)
	}
	if len(cues) > math.MaxUint16 {
		return nil, fmt.Errorf("%s: реплик %d — больше UInt16, ключ seq не влезет", pressconfName, len(cues))
	}
	sort.SliceStable(cues, func(i, j int) bool { return cues[i].StartMs < cues[j].StartMs })
	for i := range cues {
		cues[i].Seq = uint16(i + 1)
	}
	return cues, nil
}

// pressconfCueText склеивает сегменты события и нормализует пробелы: NBSP в
// автосубтитрах — обычный пробел, повторные пробелы схлопываются.
func pressconfCueText(segs []pressconfSegment) string {
	var b strings.Builder
	for _, seg := range segs {
		b.WriteString(seg.UTF8)
	}
	return strings.Join(strings.Fields(strings.ReplaceAll(b.String(), "\u00a0", " ")), " ")
}
