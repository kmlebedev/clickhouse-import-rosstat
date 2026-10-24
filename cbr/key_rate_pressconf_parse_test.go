package cbr

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Фикстуры контура стенограмм — урезанные реальные артефакты YouTube:
// playlist.html — ytInitialData с сотней lockupViewModel и ytcfg-плейсхолдерами;
// player.json — player response 11.09.2026 с дорожками en-US/asr и ru/asr;
// captions.json3 — первые 60 и последние 10 событий JSON3 23.10.2026.
var pressconfTestdata = filepath.Join("testdata", "key_rate_pressconf")

// pressconfTestDay — дата для ожиданий теста.
func pressconfTestDay(t *testing.T, value string) time.Time {
	t.Helper()
	date, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("дата %q: %v", value, err)
	}
	return date
}

func pressconfTestRead(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(pressconfTestdata, name))
	if err != nil {
		t.Fatalf("фикстура %s: %v", name, err)
	}
	return data
}

// TestPressconfPageConfig — visitorData и INNERTUBE_API_KEY достаются со страницы
// плейлиста; без них player API получил бы бот-проверку.
func TestPressconfPageConfig(t *testing.T) {
	visitor, apiKey, err := pressconfPageConfig(pressconfTestRead(t, "playlist.html"))
	if err != nil {
		t.Fatalf("pressconfPageConfig: %v", err)
	}
	if visitor != "test-visitor-data" {
		t.Errorf("visitorData = %q, want test-visitor-data", visitor)
	}
	if apiKey != "test-api-key" {
		t.Errorf("apiKey = %q, want test-api-key", apiKey)
	}
	if _, _, err = pressconfPageConfig([]byte("<html>no config</html>")); err == nil {
		t.Error("страница без visitorData/INNERTUBE_API_KEY должна давать ошибку")
	}
}

// TestPressconfPlaylistItems — плейлист даёт 24 пресс-конференции по ключевой
// ставке, включая 15.12.2023; окно истории отсекает её и оставляет 23.
func TestPressconfPlaylistItems(t *testing.T) {
	items, err := pressconfPlaylistItems(pressconfTestRead(t, "playlist.html"))
	if err != nil {
		t.Fatalf("pressconfPlaylistItems: %v", err)
	}
	if len(items) != 24 {
		t.Fatalf("позиций = %d, want 24", len(items))
	}
	if got := items[0].Date.Format("2006-01-02"); got != "2023-12-15" {
		t.Errorf("первая дата = %s, want 2023-12-15", got)
	}
	if items[0].VideoID != "wOVVPkgFOUY" {
		t.Errorf("первый video_id = %q, want wOVVPkgFOUY", items[0].VideoID)
	}
	last := items[len(items)-1]
	if got := last.Date.Format("2006-01-02"); got != "2026-10-23" {
		t.Errorf("последняя дата = %s, want 2026-10-23", got)
	}
	if last.VideoID != "WjLQNQPRc6o" {
		t.Errorf("последний video_id = %q, want WjLQNQPRc6o", last.VideoID)
	}

	wanted := pressconfWanted(items, pressconfHistoryStart, pressconfTestDay(t, "2026-10-23"))
	if len(wanted) != 23 {
		t.Fatalf("окно истории = %d, want 23", len(wanted))
	}
	if got := wanted[0].Date.Format("2006-01-02"); got != "2024-02-16" {
		t.Errorf("первая дата окна = %s, want 2024-02-16", got)
	}
	if wanted[0].VideoID != "-BlT1bt9kC4" {
		t.Errorf("первый video_id окна = %q, want -BlT1bt9kC4", wanted[0].VideoID)
	}
	if got := wanted[len(wanted)-1].Date.Format("2006-01-02"); got != "2026-10-23" {
		t.Errorf("последняя дата окна = %s, want 2026-10-23", got)
	}
}

// TestPressconfPlaylistItemsEmpty — плейлист без пресс-конференций по ключевой
// ставке — ошибка, а не тихий 0 строк.
func TestPressconfPlaylistItemsEmpty(t *testing.T) {
	page := []byte(`<html><script>var ytInitialData = {"contents":{"twoColumnBrowseResultsRenderer":{"tabs":[{"tabRenderer":{"content":{"sectionListRenderer":{"contents":[{"itemSectionRenderer":{"contents":[{"lockupViewModel":{"contentId":"x","metadata":{"lockupMetadataViewModel":{"title":{"content":"Другое видео"}}}}}]}}]}}}}]}}};</script></html>`)
	if _, err := pressconfPlaylistItems(page); err == nil {
		t.Error("плейлист без пресс-конференций должен давать ошибку")
	}
}

// TestPressconfDate — дата из заголовка; битый заголовок — ошибка.
func TestPressconfDate(t *testing.T) {
	for _, tt := range []struct{ title, want string }{
		{"Пресс-конференция по ключевой ставке 23 октября 2026 года", "2026-10-23"},
		{"Пресс-конференция по ключевой ставке 7 июня 2024 года", "2024-06-07"},
		{"Пресс-конференция по ключевой ставке 16 февраля 2024 года", "2024-02-16"},
	} {
		date, err := pressconfDate(tt.title)
		if err != nil {
			t.Errorf("%q: %v", tt.title, err)
			continue
		}
		if got := date.Format("2006-01-02"); got != tt.want {
			t.Errorf("%q = %s, want %s", tt.title, got, tt.want)
		}
	}
	if _, err := pressconfDate("Пресс-конференция по ключевой ставке без даты"); err == nil {
		t.Error("заголовок без даты должен давать ошибку")
	}
}

// TestPressconfPlayerFixture — player response даёт статус, метаданные и дорожки;
// дата заголовка сверяется с датой плейлиста.
func TestPressconfPlayerFixture(t *testing.T) {
	var doc pressconfPlayerDoc
	if err := json.Unmarshal(pressconfTestRead(t, "player.json"), &doc); err != nil {
		t.Fatalf("player.json: %v", err)
	}
	if doc.PlayabilityStatus.Status != "OK" {
		t.Fatalf("статус = %q, want OK", doc.PlayabilityStatus.Status)
	}
	if doc.VideoDetails.VideoID != "Ubes_5oLQEU" {
		t.Errorf("videoId = %q, want Ubes_5oLQEU", doc.VideoDetails.VideoID)
	}
	item := pressconfItem{VideoID: "Ubes_5oLQEU", Date: pressconfTestDay(t, "2026-09-11")}
	if err := pressconfVerifyPlayer(&doc, item); err != nil {
		t.Errorf("pressconfVerifyPlayer: %v", err)
	}
	item.VideoID = "WjLQNQPRc6o"
	if err := pressconfVerifyPlayer(&doc, item); err == nil {
		t.Error("чужой videoId должен давать ошибку")
	}
	item = pressconfItem{VideoID: "Ubes_5oLQEU", Date: pressconfTestDay(t, "2026-10-23")}
	if err := pressconfVerifyPlayer(&doc, item); err == nil {
		t.Error("чужая дата должна давать ошибку")
	}

	track, err := pressconfPickTrack(doc.Captions.PlayerCaptionsTracklistRenderer.CaptionTracks)
	if err != nil {
		t.Fatalf("pressconfPickTrack: %v", err)
	}
	if track.LanguageCode != "ru" || track.Kind != "asr" {
		t.Errorf("дорожка = %q/%q, want ru/asr", track.LanguageCode, track.Kind)
	}
	if got := pressconfCaptionKind(track.Kind); got != pressconfKindASR {
		t.Errorf("caption_kind = %q, want %q", got, pressconfKindASR)
	}
	duration, err := pressconfDuration(&doc)
	if err != nil {
		t.Fatalf("pressconfDuration: %v", err)
	}
	if duration != 4486000 {
		t.Errorf("duration_ms = %d, want 4486000", duration)
	}
}

// TestPressconfPickTrack — ручная русская дорожка приоритетнее автосубтитров;
// только английская — sentinel.
func TestPressconfPickTrack(t *testing.T) {
	tracks := []pressconfTrack{
		{LanguageCode: "en-US", Kind: "asr"},
		{LanguageCode: "ru", Kind: "asr"},
		{LanguageCode: "ru", Kind: ""},
	}
	track, err := pressconfPickTrack(tracks)
	if err != nil {
		t.Fatalf("pressconfPickTrack: %v", err)
	}
	if track.Kind != "" {
		t.Errorf("выбрана дорожка kind=%q, want ручную (kind пустой)", track.Kind)
	}
	if got := pressconfCaptionKind(track.Kind); got != pressconfKindManual {
		t.Errorf("caption_kind = %q, want %q", got, pressconfKindManual)
	}

	track, err = pressconfPickTrack([]pressconfTrack{{LanguageCode: "en-US", Kind: "asr"}, {LanguageCode: "ru", Kind: "asr"}})
	if err != nil || track.LanguageCode != "ru" {
		t.Errorf("ru/asr не выбрана: track=%+v err=%v", track, err)
	}
	if _, err = pressconfPickTrack([]pressconfTrack{{LanguageCode: "en-US", Kind: "asr"}}); !errors.Is(err, errPressconfNoCaptions) {
		t.Errorf("ошибка = %v, want errPressconfNoCaptions", err)
	}
}

// TestPressconfParseCues — JSON3 режется на реплики: aAppend и пустые события
// пропущены, seq с 1, таймкоды не убывают.
func TestPressconfParseCues(t *testing.T) {
	var doc pressconfCaptionDoc
	if err := json.Unmarshal(pressconfTestRead(t, "captions.json3"), &doc); err != nil {
		t.Fatalf("captions.json3: %v", err)
	}
	cues, err := pressconfParseCues(&doc)
	if err != nil {
		t.Fatalf("pressconfParseCues: %v", err)
	}
	if len(cues) != 35 {
		t.Fatalf("реплик = %d, want 35", len(cues))
	}
	if cues[0].Text != "Коллеги, добрый день. Рад вас видеть." {
		t.Errorf("первая реплика = %q", cues[0].Text)
	}
	if cues[len(cues)-1].Text != ">> Спасибо." {
		t.Errorf("последняя реплика = %q", cues[len(cues)-1].Text)
	}
	for i, cue := range cues {
		if cue.Seq != uint16(i+1) {
			t.Errorf("seq[%d] = %d, want %d", i, cue.Seq, i+1)
		}
		if cue.Text == "" {
			t.Errorf("пустая реплика seq=%d", cue.Seq)
		}
		if cue.EndMs < cue.StartMs {
			t.Errorf("end_ms < start_ms у seq=%d: %d < %d", cue.Seq, cue.EndMs, cue.StartMs)
		}
		if i > 0 && cue.StartMs < cues[i-1].StartMs {
			t.Errorf("таймкоды убывают: seq=%d %d < %d", cue.Seq, cue.StartMs, cues[i-1].StartMs)
		}
	}
}

// TestPressconfMissing — перекачиваются неизвестные даты, шапки без реплик и даты,
// где реплик меньше обещанного; лишние хвостовые строки дырой не считаются.
func TestPressconfMissing(t *testing.T) {
	d1 := pressconfTestDay(t, "2024-02-16")
	d2 := pressconfTestDay(t, "2024-03-22")
	d3 := pressconfTestDay(t, "2024-04-26")
	wanted := []pressconfItem{{Date: d1}, {Date: d2}, {Date: d3}}
	headers := map[time.Time]pressconfHeader{
		d1: {Cues: 0},
		d2: {Cues: 10},
		d3: {Cues: 10},
	}
	cueCounts := map[time.Time]uint64{d2: 12, d3: 9}
	got := pressconfMissing(wanted, headers, cueCounts)
	if len(got) != 2 || !got[0].Date.Equal(d1) || !got[1].Date.Equal(d3) {
		t.Errorf("перекачка = %v, want d1 и d3 (шапка без реплик + нехватка)", got)
	}
}

// TestPressconfCaptionURL — fmt=json3 добавляется к подписанному baseUrl.
func TestPressconfCaptionURL(t *testing.T) {
	if got := pressconfCaptionURL("https://example.test/captions?x=1"); got != "https://example.test/captions?x=1&fmt=json3" {
		t.Errorf("URL с query = %q", got)
	}
	if got := pressconfCaptionURL("https://example.test/captions"); got != "https://example.test/captions?fmt=json3" {
		t.Errorf("URL без query = %q", got)
	}
}

// TestCbrKeyRatePressconfDdlPins — DDL-литералы называют свои таблицы (копия
// канонического DDL в ARCHITECTURE §6.2 не должна разъехаться с константами).
func TestCbrKeyRatePressconfDdlPins(t *testing.T) {
	for _, tt := range []struct{ ddl, table string }{
		{pressconfDdl, pressconfTable},
		{pressconfCueDdl, pressconfCueTable},
	} {
		if !containsTable(tt.ddl, tt.table) {
			t.Errorf("DDL не создаёт таблицу %q:\n%s", tt.table, tt.ddl)
		}
	}
	if cbrKeyRatePressconfImporter.Name() != pressconfTable {
		t.Errorf("Name() = %q, want %q", cbrKeyRatePressconfImporter.Name(), pressconfTable)
	}
}
