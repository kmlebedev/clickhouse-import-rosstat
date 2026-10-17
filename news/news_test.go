package news

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// TestMain глушит логи импортёра: Import печатает Infof на каждую прочитанную ленту
// и на каждую строку отчёта check, а проверяются здесь счётчик записанных строк,
// тексты ошибок и состав вызовов соединения. Без этого вывод `go test` состоял бы из
// строк лога, к предмету проверки отношения не имеющих, и падение теста в них тонуло бы.
func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)

	os.Exit(m.Run())
}

func TestImportRejectsUnknownModeBeforeDB(t *testing.T) {
	// Порядок «режим → БД → сеть» закреплён двумя разными способами.
	// До БД: conn=nil — Exec на nil-соединении паникует, значит первый Exec идёт
	// после parseMode. До сети: ленты подменены httptest-сервером со счётчиком, и
	// ни одного запроса к нему быть не должно (fetchFeeds сам conn не трогает, и
	// без подмены тест ушёл бы в настоящую сеть, оставшись зелёным).
	t.Setenv(modeEnv, "bogus")

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "ленты не должны читаться при неверном режиме", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	// Подмена пакетных URL — как в pointFeedsAt (здесь свой сервер со счётчиком
	// хитов): feeds() читает их при каждом вызове, поэтому подменённый адрес подхватится.
	oldInterfax, oldInterfaxTelegram, oldRBC, oldCBRNews, oldCBRPress, oldTASS, oldSmartlabLebedev, oldSmartlabSpydell :=
		feedInterfaxURL, feedInterfaxTelegramURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL,
		feedSmartlabLebedevURL, feedSmartlabSpydellURL
	feedInterfaxURL = srv.URL + "/interfax"
	feedInterfaxTelegramURL = srv.URL + "/interfax_tg"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	feedSmartlabLebedevURL = srv.URL + "/smartlab_lebedev"
	feedSmartlabSpydellURL = srv.URL + "/smartlab_spydell"
	t.Cleanup(func() {
		feedInterfaxURL, feedInterfaxTelegramURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL,
			feedSmartlabLebedevURL, feedSmartlabSpydellURL =
			oldInterfax, oldInterfaxTelegram, oldRBC, oldCBRNews, oldCBRPress, oldTASS, oldSmartlabLebedev, oldSmartlabSpydell
	})

	var s newsImport
	count, err := s.Import(context.Background(), nil)
	if err == nil {
		t.Fatalf("Import при %s=bogus вернул %d строк без ошибки, want ошибку про режим", modeEnv, count)
	}
	if !strings.Contains(err.Error(), modeEnv) {
		t.Errorf("ошибка Import = %q, want упоминание %s (иначе непонятно, какую переменную править)", err, modeEnv)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (до записи дело не дошло)", count)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("Import обратился к лентам %d раз, want 0: неверный режим обязан отвергаться до сети", got)
	}
}

func TestNewsImportNameMatchesSchedule(t *testing.T) {
	// Имя — контракт с dagu: CLICKHOUSE_IMPORT_STAT=news_watch, и промах в нём
	// даёт не ошибку, а предупреждение «no importer named» и зелёный прогон.
	var s newsImport
	if want := "news_watch"; s.Name() != want {
		t.Errorf("newsImport.Name() = %q, want %q", s.Name(), want)
	}
}

func TestParseMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// Пустое значение — дефолт all: ручной запуск без переменной делает полный
		// upsert окна (безопасный дефолт), а не падает. Расписание dagu ставит since.
		{"пусто — all", "", modeAll},
		{"all", modeAll, modeAll},
		{"since", modeSince, modeSince},
		{"check", modeCheck, modeCheck},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMode(tc.in)
			if err != nil {
				t.Fatalf("parseMode(%q): неожиданная ошибка %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseMode(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	// Регистр и мусор — ошибка, а не тихий дефолт all: «ALL» на месте «since»
	// молча перечитал бы окно целиком там, где ждали только новые позиции.
	for _, v := range []string{"ALL", "Since", "full", " ", "all "} {
		got, err := parseMode(v)
		if err == nil {
			t.Fatalf("parseMode(%q) = %q без ошибки, want ошибку", v, got)
		}
		if !strings.Contains(err.Error(), modeEnv) {
			t.Errorf("ошибка parseMode(%q) = %q, want упоминание %s (иначе непонятно, какую переменную править)", v, err, modeEnv)
		}
	}
}

// fixtureName возвращает имя закоммиченной фикстуры ленты и её content-type:
// RSS-лента хранится как .rss, Telegram-превью — как .html. Ключ без ленты
// остаётся .rss путём: сервер отдаст 404, и фикстура не подменится заглушкой.
func fixtureName(key string) (name, contentType string) {
	for _, f := range feeds() {
		if f.Key != key {
			continue
		}
		if f.Kind == feedTelegram {
			return key + ".html", "text/html; charset=utf-8"
		}
		break
	}
	return key + ".rss", "application/rss+xml; charset=utf-8"
}

// serveFixtures поднимает httptest-сервер, отдающий закоммиченные фикстуры лент
// по путям /<Key> — по одной на каждую ленту feeds(). Настоящую сеть тест не трогает:
// прогон воспроизводим офлайн. Фикстуры нет — 404: новая лента без снимка падает
// сбоем ленты, а не подменяется заглушкой. Telegram-превью отдаётся как text/html:
// парсер выбирает транспорт по Kind, но неверный content-type скрыл бы подмену
// живого HTML RSS-заглушкой.
func serveFixtures(t *testing.T, fail map[string]int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		if code, ok := fail[key]; ok {
			http.Error(w, "лента недоступна", code)
			return
		}
		name, contentType := fixtureName(key)
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchFeedsUsesFixturesAndIsolatesFailure(t *testing.T) {
	// Сбойная одна лента: остальные семь обязаны прочитаться. Иначе падение
	// любого источника глушило бы весь прогон, а dagu-ретрай повторял бы семь
	// здоровых лент ради одной больной.
	srv := serveFixtures(t, map[string]int{"cbr_news": http.StatusInternalServerError})
	pointFeedsAt(t, srv)

	results := fetchFeeds(feeds())
	if len(results) != 8 {
		t.Fatalf("fetchFeeds вернул %d результатов, want 8 (по числу лент)", len(results))
	}

	var keys, sources []string
	for i, r := range results {
		keys = append(keys, r.Feed.Key)
		sources = append(sources, r.Feed.Source)
		if r.Feed.Key == "cbr_news" {
			if r.Err == nil {
				t.Errorf("сбойная лента cbr_news: Err == nil, want ошибку (500 не должен выглядеть успехом)")
			}
			if len(r.Items) != 0 {
				t.Errorf("сбойная лента cbr_news: %d позиций, want 0", len(r.Items))
			}
			continue
		}
		if r.Err != nil {
			t.Errorf("лента %s (результат %d): Err = %v, want nil — сбой cbr_news не должен её задевать", r.Feed.Key, i, r.Err)
		}
		if len(r.Items) == 0 {
			t.Errorf("лента %s: 0 позиций при живой фикстуре", r.Feed.Key)
		}
		for j, it := range r.Items {
			if it.Source != r.Feed.Source {
				t.Errorf("лента %s: позиция %d: Source = %q, want %q (метка источника, не имя фикстуры)", r.Feed.Key, j, it.Source, r.Feed.Source)
			}
		}
		if r.Feed.Kind == feedTelegram {
			// SeenIDs — вход T2: без него медиа без текста выглядело бы удалённым.
			if len(r.SeenIDs) == 0 {
				t.Errorf("Telegram-лента %s: SeenIDs пуст, want ID окна", r.Feed.Key)
			}
			for j, it := range r.Items {
				if it.Channel == "" || it.MessageID == 0 {
					t.Errorf("Telegram-лента %s: позиция %d: Channel=%q MessageID=%d, want поля из data-post",
						r.Feed.Key, j, it.Channel, it.MessageID)
				}
				if want := "https://t.me/" + it.Channel + "/"; !strings.HasPrefix(it.URL, want) {
					t.Errorf("Telegram-лента %s: позиция %d: URL = %q, want префикс %q", r.Feed.Key, j, it.URL, want)
				}
			}
		}
	}

	if want := "interfax,interfax_tg,rbc,cbr_news,cbr_press,tass,smartlab_lebedev,smartlab_spydell"; strings.Join(keys, ",") != want {
		t.Errorf("порядок лент = %s, want %s (фиксированный, детерминированный лог)", strings.Join(keys, ","), want)
	}
	// Оба Интерфакса (RSS+Telegram) пишутся под source=interfax, оба фида ЦБ — под cbr,
	// обе ленты smart-lab — под smartlab: витрина различает их url, а не меткой.
	if want := "interfax,interfax,rbc,cbr,cbr,tass,smartlab,smartlab"; strings.Join(sources, ",") != want {
		t.Errorf("метки источников = %s, want %s", strings.Join(sources, ","), want)
	}
}

func TestFeedsHaveFixtures(t *testing.T) {
	configured := feeds()
	if len(configured) != 8 {
		t.Fatalf("feeds() = %d лент, want 8", len(configured))
	}
	keyOf := make(map[string]feedKind, len(configured))
	for _, f := range configured {
		keyOf[f.Key] = f.Kind

		switch f.Kind {
		case feedRSS:
			path := filepath.Join("testdata", f.Key+".rss")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("лента %s: нет фикстуры %s: %v", f.Key, path, err)
				continue
			}
			if len(body) == 0 {
				t.Errorf("фикстура %s пуста", path)
				continue
			}
			// Фикстура обязана быть настоящей лентой: она — источник пинов парсинга
			// (Task 3) и вход офлайн-прогонов, а не заглушка.
			var doc rssDoc
			if err := xml.Unmarshal(body, &doc); err != nil {
				t.Errorf("разбор фикстуры %s: %v", path, err)
				continue
			}
			items, _, err := itemsFromDoc(f.Source, doc)
			if err != nil {
				t.Errorf("itemsFromDoc(%s) на фикстуре %s: %v", f.Source, path, err)
				continue
			}
			if len(items) == 0 {
				t.Errorf("фикстура %s разобралась в 0 позиций", path)
			}
		case feedTelegram:
			path := filepath.Join("testdata", f.Key+".html")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("Telegram-лента %s: нет фикстуры %s: %v", f.Key, path, err)
				continue
			}
			page, err := parseTelegramPage(f.Key, body)
			if err != nil {
				t.Errorf("parseTelegramPage(%s) на фикстуре %s: %v", f.Key, path, err)
				continue
			}
			if len(page.Messages) == 0 {
				t.Errorf("фикстура %s разобралась в 0 сообщений", path)
			}
		default:
			t.Errorf("лента %s: неизвестный вид %d (нет ветки проверки фикстуры)", f.Key, f.Kind)
		}
	}

	// Обратная проверка: фикстура без ленты — мёртвый снимок, который никто
	// не читает; он переживёт смену конфига и будет вводить в заблуждение.
	// Расширение обязано соответствовать транспорту: .rss для RSS, .html для Telegram.
	for _, pattern := range []string{"*.rss", "*.html"} {
		files, err := filepath.Glob(filepath.Join("testdata", pattern))
		if err != nil {
			t.Fatalf("glob testdata/%s: %v", pattern, err)
		}
		for _, path := range files {
			key := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
			kind, ok := keyOf[key]
			if !ok {
				t.Errorf("фикстура %s: ленты с Key=%q нет в feeds()", path, key)
				continue
			}
			wantExt := ".rss"
			if kind == feedTelegram {
				wantExt = ".html"
			}
			if ext := filepath.Ext(path); ext != wantExt {
				t.Errorf("фикстура %s: расширение %s не соответствует транспорту (want %s)", path, ext, wantExt)
			}
		}
	}
}

func TestSelectNewItemsSkipsExistingWritesMissing(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	t0 := time.Date(2026, time.October, 12, 12, 0, 0, 0, msk)

	// Ключ «уже в БД» несёт САМУЮ позднюю дату набора, а опоздавшая позиция — на два
	// часа раньше: фильтр по дате-курсору выбросил бы именно её (дата старее сохранённого
	// максимума), а проверка наличия по ключу — нет. Последняя позиция — дубль ключа
	// внутри прогона: вторая копия не должна уехать отдельной строкой.
	items := []newsItem{
		{Date: t0, Source: "interfax", Title: "уже в БД"},
		{Date: t0.Add(30 * time.Minute), Source: "interfax", Title: "новая"},
		{Date: t0.Add(-2 * time.Hour), Source: "interfax", Title: "опоздавшая"},
		{Date: t0.Add(30 * time.Minute), Source: "interfax", Title: "новая"},
	}

	conn := newNewsFakeConnWithKeys(map[string][]newsKey{
		"interfax": {{date: t0.Unix(), title: "уже в БД"}},
	})
	got, err := selectNewItems(context.Background(), conn, items)
	if err != nil {
		t.Fatalf("selectNewItems: %v", err)
	}

	var titles []string
	for _, it := range got {
		titles = append(titles, it.Title)
	}
	if want := "новая,опоздавшая"; strings.Join(titles, ",") != want {
		t.Errorf("selectNewItems = %q, want %q (только отсутствующие, без дублей, в исходном порядке)", strings.Join(titles, ","), want)
	}
	// Один запрос ключей на источник: группа ровно одна (все позиции — interfax).
	keyCalls := 0
	for _, call := range conn.callList() {
		if strings.Contains(call, storedKeysQuery) {
			keyCalls++
		}
	}
	if keyCalls != 1 {
		t.Errorf("storedKeysQuery выполнен %d раз, want 1 (по одному на источник); вызовы: %v", keyCalls, conn.callList())
	}
	// Нижняя граница запроса — минимум окна источника (опоздавшая, −2ч), а не сохранённый
	// максимум: граница — свойство этого прогона, и она не отсекает опоздавшие позиции.
	if wantCall := "source=interfax since=" + t0.Add(-2*time.Hour).Format(time.RFC3339); !conn.anyCallHas(wantCall) {
		t.Errorf("запрос ключей interfax не содержал %q; вызовы: %v", wantCall, conn.callList())
	}
}

func TestFeedSpansMergesCbrFeeds(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	t1 := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	t2 := time.Date(2026, time.October, 11, 8, 0, 0, 0, msk)
	t3 := time.Date(2026, time.October, 12, 9, 0, 0, 0, msk)

	results := []feedResult{
		// Два фида ЦБ идут под одним source: окно покрытия у источника одно,
		// и оно шире любого из фидов по отдельности.
		{Feed: feed{Key: "cbr_news", Source: "cbr"}, Items: []newsItem{
			{Date: t1, Source: "cbr"}, {Date: t3, Source: "cbr"},
		}},
		{Feed: feed{Key: "cbr_press", Source: "cbr"}, Items: []newsItem{
			{Date: t2, Source: "cbr"},
		}},
		{Feed: feed{Key: "tass", Source: "tass"}, Items: []newsItem{
			{Date: t2, Source: "tass"},
		}},
		// Сбойная лента позиций не даёт, но её ключ обязан остаться в окне
		// источника: без этого сбой выглядел бы сужением окна (см. FailedFeeds).
		{Feed: feed{Key: "rbc", Source: "rbc"}, Err: errors.New("сбой ленты")},
	}

	spans := feedSpans(results)
	if len(spans) != 3 {
		t.Fatalf("feedSpans вернул %d источников, want 3 (cbr, tass и rbc с одним сбойным фидом): %v", len(spans), spans)
	}

	cbr, ok := spans["cbr"]
	if !ok {
		t.Fatalf("feedSpans не вернул источник cbr: %v", spans)
	}
	if want := uint64(3); cbr.Count != want {
		t.Errorf("cbr.Count = %d, want %d (позиции обоих фидов ЦБ суммируются)", cbr.Count, want)
	}
	if !cbr.Min.Equal(t1) {
		t.Errorf("cbr.Min = %v, want %v (минимум по обоим фидам)", cbr.Min, t1)
	}
	if !cbr.Max.Equal(t3) {
		t.Errorf("cbr.Max = %v, want %v (максимум по обоим фидам)", cbr.Max, t3)
	}
	if len(cbr.FailedFeeds) != 0 {
		t.Errorf("cbr.FailedFeeds = %v, want пусто (оба фида ЦБ прочитались)", cbr.FailedFeeds)
	}

	rbc, ok := spans["rbc"]
	if !ok {
		t.Fatalf("feedSpans не вернул источник rbc (сбой ленты обязан быть виден): %v", spans)
	}
	if rbc.Count != 0 || !rbc.Min.IsZero() || !rbc.Max.IsZero() {
		t.Errorf("rbc = %+v, want пустое окно: сбойная лента позиций не даёт", rbc)
	}
	if want := "rbc"; strings.Join(rbc.FailedFeeds, ",") != want {
		t.Errorf("rbc.FailedFeeds = %v, want [%s]", rbc.FailedFeeds, want)
	}
}

func TestFeedSpansMarksFailedFeeds(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	t1 := time.Date(2026, time.October, 12, 16, 0, 0, 0, msk)

	// Частичный сбой: окно строится по выжившей ленте, но помечено неполным —
	// упавшая cbr_news могла покрывать промежуток до её начала.
	partial := feedSpans([]feedResult{
		{Feed: feed{Key: "cbr_news", Source: "cbr"}, Err: errors.New("сбой ленты")},
		{Feed: feed{Key: "cbr_press", Source: "cbr"}, Items: []newsItem{{Date: t1, Source: "cbr"}}},
	})
	span, ok := partial["cbr"]
	if !ok {
		t.Fatalf("спан cbr не создан при сбое cbr_news: %v", partial)
	}
	if want := uint64(1); span.Count != want {
		t.Errorf("cbr.Count = %d, want %d (только позиции выжившей cbr_press)", span.Count, want)
	}
	if !span.Min.Equal(t1) || !span.Max.Equal(t1) {
		t.Errorf("cbr.Min/Max = %v/%v, want %v (окно выжившей ленты)", span.Min, span.Max, t1)
	}
	if want := "cbr_news"; strings.Join(span.FailedFeeds, ",") != want {
		t.Errorf("cbr.FailedFeeds = %v, want [%s]", span.FailedFeeds, want)
	}

	// Полный сбой обоих фидов: спан существует (вызывающий видит ошибки лент),
	// Count == 0, ключи — в порядке feeds().
	full := feedSpans([]feedResult{
		{Feed: feed{Key: "cbr_news", Source: "cbr"}, Err: errors.New("сбой ленты")},
		{Feed: feed{Key: "cbr_press", Source: "cbr"}, Err: errors.New("сбой ленты")},
	})
	span, ok = full["cbr"]
	if !ok {
		t.Fatalf("спан cbr не создан при полном сбое обоих фидов: %v", full)
	}
	if span.Count != 0 {
		t.Errorf("cbr.Count = %d, want 0", span.Count)
	}
	if want := "cbr_news,cbr_press"; strings.Join(span.FailedFeeds, ",") != want {
		t.Errorf("cbr.FailedFeeds = %v, want [%s] (порядок feeds())", span.FailedFeeds, want)
	}
}

func TestDetectGapsFindsMissedWindow(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	storedMax := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMin := time.Date(2026, time.October, 12, 9, 0, 0, 0, msk)

	feed := map[string]sourceSpan{"cbr": {Count: 100, Min: feedMin, Max: feedMin.Add(time.Hour)}}
	stored := map[string]sourceSpan{"cbr": {Count: 100, Min: storedMax.Add(-time.Hour), Max: storedMax}}

	gaps := detectGaps(feed, stored)
	if len(gaps) != 1 {
		t.Fatalf("detectGaps вернул %d строк, want 1: %v", len(gaps), gaps)
	}
	for _, want := range []string{"cbr", storedMax.Format(time.RFC3339), feedMin.Format(time.RFC3339), "пропущенный прогон"} {
		if !strings.Contains(gaps[0], want) {
			t.Errorf("строка дыры %q не содержит %q", gaps[0], want)
		}
	}

	// Порядок строк детерминирован (sort.Strings): обход map случаен, и без
	// сортировки отчёт check менял бы порядок строк от прогона к прогону.
	multi := map[string]sourceSpan{
		"tass":     {Count: 1, Min: feedMin, Max: feedMin},
		"cbr":      {Count: 1, Min: feedMin, Max: feedMin},
		"rbc":      {Count: 1, Min: feedMin, Max: feedMin},
		"interfax": {Count: 1, Min: feedMin, Max: feedMin},
	}
	multiStored := map[string]sourceSpan{
		"tass":     {Count: 1, Min: storedMax, Max: storedMax},
		"cbr":      {Count: 1, Min: storedMax, Max: storedMax},
		"rbc":      {Count: 1, Min: storedMax, Max: storedMax},
		"interfax": {Count: 1, Min: storedMax, Max: storedMax},
	}
	wantOrder := []string{"cbr", "interfax", "rbc", "tass"}
	for i := 0; i < 20; i++ {
		got := detectGaps(multi, multiStored)
		if len(got) != len(wantOrder) {
			t.Fatalf("прогон %d: detectGaps вернул %d строк, want %d: %v", i, len(got), len(wantOrder), got)
		}
		for j, line := range got {
			if !strings.HasPrefix(line, wantOrder[j]+":") {
				t.Fatalf("прогон %d: строка %d = %q, want источник %s (порядок обязан быть отсортирован)", i, j, line, wantOrder[j])
			}
		}
	}
}

func TestDetectGapsSkipsIncompleteSource(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	storedMax := time.Date(2026, time.October, 12, 15, 0, 0, 0, msk)
	feedMin := time.Date(2026, time.October, 12, 16, 0, 0, 0, msk)

	stored := map[string]sourceSpan{"cbr": {Count: 100, Min: storedMax.Add(-2 * time.Hour), Max: storedMax}}
	window := func(failed []string) map[string]sourceSpan {
		return map[string]sourceSpan{"cbr": {Count: 50, Min: feedMin, Max: feedMin.Add(time.Hour), FailedFeeds: failed}}
	}

	// Упавшая cbr_news могла покрывать 15:00…16:00 — реальной пропущенной выборки
	// нет, и объявлять дыру по неполному окну нельзя.
	if gaps := detectGaps(window([]string{"cbr_news"}), stored); len(gaps) != 0 {
		t.Errorf("detectGaps по неполному окну = %v, want пусто (дыру могла закрывать упавшая лента)", gaps)
	}
	// Контроль: те же границы без пометки о сбое дают дыру — проверка не вакуумная.
	gaps := detectGaps(window(nil), stored)
	if len(gaps) != 1 {
		t.Fatalf("detectGaps по полному окну вернул %d строк, want 1: %v", len(gaps), gaps)
	}
	for _, want := range []string{"cbr", storedMax.Format(time.RFC3339), feedMin.Format(time.RFC3339)} {
		if !strings.Contains(gaps[0], want) {
			t.Errorf("строка дыры %q не содержит %q", gaps[0], want)
		}
	}
}

func TestDetectGapsQuietWhenOverlap(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	storedMax := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMin := time.Date(2026, time.October, 12, 9, 0, 0, 0, msk)
	feed := map[string]sourceSpan{"cbr": {Count: 100, Min: feedMin, Max: feedMin.Add(time.Hour)}}

	for _, tc := range []struct {
		name   string
		feed   map[string]sourceSpan
		stored map[string]sourceSpan
	}{
		{
			// Окна перекрываются: сохранённый максимум (12.10 15:00) позже начала ленты
			// (12.10 09:00) — значит, между прогонами окно не проскакивало.
			name:   "перекрытие окон",
			feed:   feed,
			stored: map[string]sourceSpan{"cbr": {Count: 100, Min: storedMax.Add(-24 * time.Hour), Max: feedMin.Add(6 * time.Hour)}},
		},
		{
			// Встык: feed.Min == stored.Max — окно продолжается, а не разрывается.
			name:   "окна стыкуются встык",
			feed:   map[string]sourceSpan{"cbr": {Count: 1, Min: storedMax, Max: storedMax.Add(time.Hour)}},
			stored: map[string]sourceSpan{"cbr": {Count: 1, Min: storedMax.Add(-time.Hour), Max: storedMax}},
		},
		{
			// Первый прогон источника: сохранённых строк нет — считать дыру не из чего.
			name:   "нет окна у источника",
			feed:   feed,
			stored: nil,
		},
		{
			// Тот же первый прогон, но чтение вернуло источник с нулём строк.
			name:   "нулевое окно у источника",
			feed:   feed,
			stored: map[string]sourceSpan{"cbr": {Count: 0}},
		},
		{
			// Все ленты источника упали: окна нет вовсе, только пометка о сбое (дыру
			// видно по ошибке ленты, а не по покрытию).
			name:   "все ленты источника упали",
			feed:   map[string]sourceSpan{"cbr": {FailedFeeds: []string{"cbr_news", "cbr_press"}}},
			stored: map[string]sourceSpan{"cbr": {Count: 100, Min: storedMax.Add(-time.Hour), Max: storedMax}},
		},
		{
			// Источник есть только в базе (ленту убрали из конфига) — по нему не отчитываемся.
			name:   "источник только в базе",
			feed:   map[string]sourceSpan{},
			stored: map[string]sourceSpan{"cbr": {Count: 100, Min: storedMax.Add(-time.Hour), Max: storedMax}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectGaps(tc.feed, tc.stored); len(got) != 0 {
				t.Errorf("detectGaps = %v, want пусто", got)
			}
		})
	}
}

func TestCoverageLinesReportStoredAndFirstRun(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	storedMin := time.Date(2026, time.October, 9, 7, 0, 0, 0, msk)
	storedMax := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMin := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMax := time.Date(2026, time.October, 12, 9, 0, 0, 0, msk)

	feed := map[string]sourceSpan{
		"cbr":  {Count: 100, Min: feedMin, Max: feedMax},
		"tass": {Count: 100, Min: feedMin, Max: feedMax},
	}
	stored := map[string]sourceSpan{"cbr": {Count: 250, Min: storedMin, Max: storedMax}}

	lines := coverageLines(feed, stored)
	if len(lines) != 2 {
		t.Fatalf("coverageLines вернул %d строк, want 2: %v", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "cbr: сохранено 250") {
		t.Errorf("строка 0 = %q, want источник cbr со счётчиком сохранённых строк (порядок отсортирован)", lines[0])
	}
	for _, want := range []string{storedMin.Format(time.RFC3339), storedMax.Format(time.RFC3339), feedMin.Format(time.RFC3339), feedMax.Format(time.RFC3339)} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("строка %q не содержит границу %s", lines[0], want)
		}
	}
	if !strings.HasPrefix(lines[1], "tass:") || !strings.Contains(lines[1], "сохранённых строк нет") {
		t.Errorf("строка 1 = %q, want tass с пометкой «сохранённых строк нет» (первый прогон)", lines[1])
	}
}

func TestCoverageLinesReportsIncomplete(t *testing.T) {
	msk := time.FixedZone("", mskOffset)
	storedMin := time.Date(2026, time.October, 9, 7, 0, 0, 0, msk)
	storedMax := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMin := time.Date(2026, time.October, 10, 12, 0, 0, 0, msk)
	feedMax := time.Date(2026, time.October, 12, 9, 0, 0, 0, msk)

	feed := map[string]sourceSpan{
		// Частичный сбой: одна лента ЦБ из двух не прочиталась — окно выжившей
		// неполно, и check обязан это назвать.
		"cbr": {Count: 50, Min: feedMin, Max: feedMax, FailedFeeds: []string{"cbr_news"}},
		// Все ленты источника упали: позиций нет вовсе, нулевые времена печатать нельзя.
		"rbc": {FailedFeeds: []string{"rbc"}},
		// Источник и не читался ни разу, и лента упала — обе пометки в одной строке.
		"tass": {FailedFeeds: []string{"tass"}},
	}
	stored := map[string]sourceSpan{
		"cbr": {Count: 250, Min: storedMin, Max: storedMax},
		"rbc": {Count: 30, Min: storedMin, Max: storedMax},
	}

	lines := coverageLines(feed, stored)
	if len(lines) != 3 {
		t.Fatalf("coverageLines вернул %d строк, want 3 (cbr, rbc, tass): %v", len(lines), lines)
	}

	for _, want := range []string{"cbr: сохранено 250", "в ленте 50", "неполно: не прочитаны ленты cbr_news"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("строка cbr = %q, want фрагмент %q", lines[0], want)
		}
	}

	// feed.Count == 0: вместо «в ленте 0 (0001-01-01T00:00:00Z…0001-01-01T00:00:00Z)» —
	// словами, иначе нулевое окно читалось бы как настоящее.
	for _, want := range []string{"rbc: сохранено 30", "в ленте 0 (нет прочитанных позиций)", "неполно: не прочитаны ленты rbc"} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("строка rbc = %q, want фрагмент %q", lines[1], want)
		}
	}

	for _, want := range []string{"tass: сохранённых строк нет (первый прогон не выполнялся)", "неполно: не прочитаны ленты tass"} {
		if !strings.Contains(lines[2], want) {
			t.Errorf("строка tass = %q, want фрагмент %q", lines[2], want)
		}
	}

	for _, line := range lines {
		if strings.Contains(line, "0001-01-01") {
			t.Errorf("строка %q печатает нулевое время как настоящее окно", line)
		}
	}
}

// --- офлайн-фейки контура news (паттерн — boldFakeConn, bold/bold_test.go) ---

// pointFeedsAt заменяет URL-ы всех восьми лент на пути httptest-сервера и возвращает
// исходные значения после теста: feeds() читает пакетные переменные при каждом вызове,
// поэтому подмена подхватывается, а t.Cleanup не даёт ей утечь в соседние тесты.
//
// Guard в конце — про новую ленту: неподменённый URL увёл бы офлайн-тест в настоящую
// сеть, и тот падал бы (или, хуже, зеленел) по доступности живого сайта, а не по коду.
func pointFeedsAt(t *testing.T, srv *httptest.Server) {
	t.Helper()
	old := [8]string{feedInterfaxURL, feedInterfaxTelegramURL, feedRBCURL, feedCBRNewsURL,
		feedCBRPressURL, feedTASSURL, feedSmartlabLebedevURL, feedSmartlabSpydellURL}
	feedInterfaxURL = srv.URL + "/interfax"
	feedInterfaxTelegramURL = srv.URL + "/interfax_tg"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	feedSmartlabLebedevURL = srv.URL + "/smartlab_lebedev"
	feedSmartlabSpydellURL = srv.URL + "/smartlab_spydell"
	t.Cleanup(func() {
		feedInterfaxURL, feedInterfaxTelegramURL, feedRBCURL, feedCBRNewsURL,
			feedCBRPressURL, feedTASSURL, feedSmartlabLebedevURL, feedSmartlabSpydellURL =
			old[0], old[1], old[2], old[3], old[4], old[5], old[6], old[7]
	})
	for _, f := range feeds() {
		if !strings.HasPrefix(f.URL, srv.URL) {
			t.Fatalf("лента %s не подменена (%q): офлайн-тест ушёл бы в сеть", f.Key, f.URL)
		}
	}
}

// fixtureItems читает закоммиченную фикстуру ленты тем же путём, что и прогон
// (readFixtureDoc + itemsFromDoc или readTelegramFixture + telegramItems): ожидания
// тестов считаются из фикстур, а не хардкодятся — обновление снимка лент не должно
// требовать правки чисел в тестах.
func fixtureItems(t *testing.T, f feed) []newsItem {
	t.Helper()
	if f.Kind == feedTelegram {
		return telegramItems(f.Source, readTelegramFixture(t, f.Key+".html"))
	}
	items, _, err := itemsFromDoc(f.Source, readFixtureDoc(t, f.Key+".rss"))
	if err != nil {
		t.Fatalf("itemsFromDoc(%s) на фикстуре %s.rss: %v", f.Source, f.Key, err)
	}
	return items
}

// fixtureInventory — позиции фикстур по ключам лент и максимум даты по источникам.
// Максимумы — это то, что лежит в news_events после успешного прогона all: оба фида
// ЦБ складываются в один источник cbr, обе ленты smart-lab — в один smartlab, как
// и в feedSpans.
func fixtureInventory(t *testing.T) (byKey map[string][]newsItem, maxBySource map[string]time.Time) {
	t.Helper()
	byKey = make(map[string][]newsItem, 8)
	maxBySource = make(map[string]time.Time, 5)
	for _, f := range feeds() {
		items := fixtureItems(t, f)
		byKey[f.Key] = items
		for _, it := range items {
			if it.Date.After(maxBySource[f.Source]) {
				maxBySource[f.Source] = it.Date
			}
		}
	}
	return byKey, maxBySource
}

// keyOf/keysOf переводят позиции в ключи наличия news_events: источник задаёт выборку
// (WHERE source = ?), поэтому в newsKey его нет, и ключ — это пара (Unix-секунды, title).
// Ожидания since-тестов считаются из фикстур тем же преобразованием, что и selectNewItems.
func keyOf(it newsItem) newsKey {
	return newsKey{date: it.Date.Unix(), title: it.Title}
}

func keysOf(items []newsItem) []newsKey {
	keys := make([]newsKey, 0, len(items))
	for _, it := range items {
		keys = append(keys, keyOf(it))
	}
	return keys
}

// totalItems — суммарное число позиций инвентаря. Тесты сверяют с ним ожидаемую
// выборку: если она пуста или равна всему набору, проверка фильтра вакуумна.
func totalItems(byKey map[string][]newsItem) int {
	n := 0
	for _, items := range byKey {
		n += len(items)
	}
	return n
}

// newsStoredSpan — сохранённый спан источника в том виде, в каком его отдаёт
// storedSpansQuery: колонки идут в порядке source, count(), min(date), max(date).
type newsStoredSpan struct {
	Source string
	Count  uint64
	Min    time.Time
	Max    time.Time
}

// Виды строк, которые умеет отдавать newsFakeRows: покрытие источников (storedSpansQuery)
// и ключи существующих позиций (storedKeysQuery).
const (
	fakeRowsSpans = "spans"
	fakeRowsKeys  = "keys"
)

// newsFakeRows — фейковый driver.Rows контура news: вид строк выбирается в Query по
// самому запросу, а не по числу вызовов, чтобы since-тесты держали разные наборы ключей
// на источник. Реализованы только те методы, которые зовут readStoredSpans/readStoredKeys
// (Next/Scan/Err/Close); остальные остаются nil от встраивания driver.Rows и паникуют,
// если сценарий пойдёт дальше. Готового фейка driver.Rows в репозитории нет
// (bold-фейк отдаёт driver.Row) — свой.
type newsFakeRows struct {
	driver.Rows
	kind  string // fakeRowsSpans или fakeRowsKeys
	spans []newsStoredSpan
	keys  []newsKey
	err   error // отдать Err(): сценарий «обход строк упал на середине» (приходит из newsFakeConn)
	i     int   // индекс следующей строки: Next двигает его, Scan читает i-1
}

// n — число строк выбранного вида: Next и Scan не должны перепутать наборы.
func (r *newsFakeRows) n() int {
	if r.kind == fakeRowsKeys {
		return len(r.keys)
	}
	return len(r.spans)
}

func (r *newsFakeRows) Next() bool {
	if r.i >= r.n() {
		return false
	}
	r.i++
	return true
}

// Scan принимает приёмники того вида, которым создан фейк: для покрытия — *string,
// *uint64 и два *time.Time (порядок source, count(), min(date), max(date)), для ключей —
// *time.Time и *string (порядок date, title). Порядок колонок — часть контракта запроса,
// и фейк его пинит: сдвиг колонок в SQL не должен остаться незамеченным.
func (r *newsFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > r.n() {
		return fmt.Errorf("фейковая строка (%s): Scan без Next (i=%d, строк %d)", r.kind, r.i, r.n())
	}
	if r.kind == fakeRowsKeys {
		return r.scanKey(dest)
	}
	return r.scanSpan(dest)
}

func (r *newsFakeRows) scanKey(dest []any) error {
	if len(dest) != 2 {
		return fmt.Errorf("фейковый ключ позиции: приёмников %d, want 2 (date, title)", len(dest))
	}
	date, ok := dest[0].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковый ключ позиции: приёмник date %T (want *time.Time)", dest[0])
	}
	title, ok := dest[1].(*string)
	if !ok {
		return fmt.Errorf("фейковый ключ позиции: приёмник title %T (want *string)", dest[1])
	}
	k := r.keys[r.i-1]
	// Время собирается из Unix-секунд ключа: readStoredKeys снова берёт .Unix(), и фейк
	// пинит круговой ход момент ↔ Unix, а не представление в конкретной зоне.
	*date, *title = time.Unix(k.date, 0).UTC(), k.title
	return nil
}

func (r *newsFakeRows) scanSpan(dest []any) error {
	if len(dest) != 4 {
		return fmt.Errorf("фейковая строка покрытия: приёмников %d, want 4 (source, count(), min(date), max(date))", len(dest))
	}
	span := r.spans[r.i-1]
	src, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка покрытия: приёмник source %T (want *string)", dest[0])
	}
	count, ok := dest[1].(*uint64)
	if !ok {
		return fmt.Errorf("фейковая строка покрытия: приёмник count() %T (want *uint64)", dest[1])
	}
	minDate, ok := dest[2].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковая строка покрытия: приёмник min(date) %T (want *time.Time)", dest[2])
	}
	maxDate, ok := dest[3].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковая строка покрытия: приёмник max(date) %T (want *time.Time)", dest[3])
	}
	*src, *count, *minDate, *maxDate = span.Source, span.Count, span.Min, span.Max
	return nil
}

func (r *newsFakeRows) Err() error { return r.err }

func (r *newsFakeRows) Close() error { return nil }

// newsFakeBatch — батч в память: тест проверяет число и состав записанных строк.
type newsFakeBatch struct {
	driver.Batch
	query string
	rows  [][]any
	sent  bool
}

func (b *newsFakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

func (b *newsFakeBatch) Send() error { b.sent = true; return nil }

func (b *newsFakeBatch) Abort() error { return nil }

// newsInsertedRow — строка батча, разобранная по колонкам news_events в порядке
// insertItems (date, source, title, url, description, sentiment).
type newsInsertedRow struct {
	Date        time.Time
	Source      string
	Title       string
	URL         string
	Description string
	Sentiment   *float32
}

// inserted разбирает записанные строки по именам колонок: порядок аргументов Append — контракт
// с явным списком колонок newsInsert (порядок — как в каноническом DDL), и перепутанные поля
// обязаны валить тест, а не уезжать в базу. Типизированный nil в sentiment — ожидаемое
// состояние (канал LLM — follow-up): непустой sentiment означал бы запись мимо канала.
func (b *newsFakeBatch) inserted(t *testing.T) []newsInsertedRow {
	t.Helper()
	rows := make([]newsInsertedRow, 0, len(b.rows))
	for i, v := range b.rows {
		if len(v) != 6 {
			t.Fatalf("строка батча %d: значений %d, want 6 (date, source, title, url, description, sentiment)", i, len(v))
		}
		date, ok := v[0].(time.Time)
		if !ok {
			t.Fatalf("строка батча %d: date = %T (want time.Time)", i, v[0])
		}
		source, ok := v[1].(string)
		if !ok {
			t.Fatalf("строка батча %d: source = %T (want string)", i, v[1])
		}
		title, ok := v[2].(string)
		if !ok {
			t.Fatalf("строка батча %d: title = %T (want string)", i, v[2])
		}
		url, ok := v[3].(string)
		if !ok {
			t.Fatalf("строка батча %d: url = %T (want string)", i, v[3])
		}
		description, ok := v[4].(string)
		if !ok {
			t.Fatalf("строка батча %d: description = %T (want string)", i, v[4])
		}
		sentiment, ok := v[5].(*float32)
		if !ok {
			t.Fatalf("строка батча %d: sentiment = %T (want *float32)", i, v[5])
		}
		if sentiment != nil {
			t.Errorf("строка батча %d: sentiment = %v, want NULL (канал заполнения — follow-up)", i, *sentiment)
		}
		rows = append(rows, newsInsertedRow{
			Date: date, Source: source, Title: title, URL: url, Description: description, Sentiment: sentiment,
		})
	}
	return rows
}

// newsFakeConn — фейковое соединение контура news: встраивание driver.Conn оставляет
// неиспользуемые методы nil (паника, если тест зайдёт дальше сценария), а переопределены
// ровно три метода, которыми пользуется Import: Exec (DDL и витрина), Query
// (storedSpansQuery — readStoredSpans в check; storedKeysQuery — readStoredKeys в since)
// и PrepareBatch (запись позиций). spans — очередь ответов на запрос покрытия; keys —
// ключи существующих позиций по источникам. Пустые наборы означают «в news_events нет строк».
// queryErr и rowsErr включают ошибочные пути чтения: первый — сам Query, второй — обход строк
// (rows.Err уже после Next); оба задаются полями постфактум, чтобы не плодить конструкторы.
type newsFakeConn struct {
	driver.Conn
	mu       sync.Mutex
	calls    []string
	spans    []newsStoredSpan
	keys     map[string][]newsKey
	queryErr error // Query вернёт её вместо строк
	rowsErr  error // её отдаст rows.Err()
	batches  []*newsFakeBatch
}

// newNewsFakeConn — соединение режима check: сохранённые окна есть, ключей нет.
func newNewsFakeConn(stored ...newsStoredSpan) *newsFakeConn {
	return &newsFakeConn{spans: stored}
}

// newNewsFakeConnWithKeys — соединение режима since: ключи существующих позиций по
// источникам. Источник без записи в map отдаёт пустой набор — как источник без строк.
func newNewsFakeConnWithKeys(keys map[string][]newsKey) *newsFakeConn {
	return &newsFakeConn{keys: keys}
}

func (c *newsFakeConn) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *newsFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.record("exec: " + query)
	return nil
}

// Query отвечает по виду запроса (а не по номеру вызова): покрытие — очередью spans,
// ключи — набором запрошенного источника. Аргументы запроса ключей (source, since)
// попадают в записанный вызов: тесты пинят и источник, и нижнюю границу окна.
func (c *newsFakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	label := "query: " + query
	var src string
	if len(args) == 2 {
		if s, ok := args[0].(string); ok {
			src = s
			if since, ok := args[1].(time.Time); ok {
				label += fmt.Sprintf(" [source=%s since=%s]", src, since.Format(time.RFC3339))
			}
		}
	}
	c.record(label)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	switch query {
	case storedSpansQuery:
		rows := c.spans
		c.spans = nil // очередь отдаётся один раз: повторный Query вернул бы уже пустое окно
		return &newsFakeRows{kind: fakeRowsSpans, spans: rows, err: c.rowsErr}, nil
	case storedKeysQuery:
		return &newsFakeRows{kind: fakeRowsKeys, keys: c.keys[src], err: c.rowsErr}, nil
	}
	return nil, fmt.Errorf("newsFakeConn: неожиданный запрос %q", query)
}

func (c *newsFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.record("preparebatch: " + query)
	batch := &newsFakeBatch{query: query}
	c.mu.Lock()
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return batch, nil
}

func (c *newsFakeConn) callList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *newsFakeConn) batchList() []*newsFakeBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*newsFakeBatch(nil), c.batches...)
}

// anyCallHas — был ли хоть один вызов, содержащий подстроку: по этому признаку тесты
// различают шаги прогона (DDL, публикация витрины, чтение покрытия, батч).
func (c *newsFakeConn) anyCallHas(fragment string) bool {
	for _, call := range c.callList() {
		if strings.Contains(call, fragment) {
			return true
		}
	}
	return false
}

// onlyBatch возвращает единственный открытый батч: прогон пишет все позиции одним
// батчем (правило 2 AGENTS.md), поэтому второй PrepareBatch — уже дефект.
func (c *newsFakeConn) onlyBatch(t *testing.T, query string) *newsFakeBatch {
	t.Helper()
	batches := c.batchList()
	if len(batches) != 1 {
		t.Fatalf("PrepareBatch вызван %d раз, want 1 (%s); вызовы: %v", len(batches), query, c.callList())
	}
	if batches[0].query != query {
		t.Fatalf("батч открыт запросом %q, want %q", batches[0].query, query)
	}
	return batches[0]
}

// --- композиция трёх режимов Import() ---

func TestImportCheckModeWritesNothing(t *testing.T) {
	// check — режим чтения: он ходит только в сырую news_events и витрину не публикует.
	// Тест держит оба свойства разом: строк не пишется (PrepareBatch не вызывался) и
	// витрина не пересоздаётся (в вызовах нет ни CREATE OR REPLACE VIEW, ни имени
	// v_news_events). Без него правка, при которой check начнёт писать или трогать
	// витрину, прошла бы make all зелёной: живой приёмки трёх режимов в CI нет.
	t.Setenv(modeEnv, modeCheck)
	pointFeedsAt(t, serveFixtures(t, nil))

	var s newsImport

	// Первый прогон: сохранённых окон нет (Query отдаёт пустую очередь) — дыр нет и
	// ошибки нет. Отсутствие ошибки здесь же доказывает, что все восемь лент прочитались:
	// сбой любой из них вернулся бы через feedErrs, а не остался незамеченным.
	firstRun := newNewsFakeConn()
	count, err := s.Import(context.Background(), firstRun)
	if err != nil {
		t.Fatalf("Import(%s) на пустой news_events: %v", modeCheck, err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (%s не пишет)", count, modeCheck)
	}
	if got := firstRun.batchList(); len(got) != 0 {
		t.Errorf("check открыл %d батчей, want 0: режим чтения строк не пишет", len(got))
	}
	if !firstRun.anyCallHas(newsEventsDdl) {
		t.Errorf("check не выполнил DDL news_events (импортёр обязан отработать на чистой базе); вызовы: %v", firstRun.callList())
	}
	if !firstRun.anyCallHas(storedSpansQuery) {
		t.Errorf("check не прочитал покрытие запросом %q; вызовы: %v", storedSpansQuery, firstRun.callList())
	}
	if firstRun.anyCallHas("CREATE OR REPLACE VIEW") {
		t.Errorf("check публикует витрину (CREATE OR REPLACE VIEW в вызовах), want чтение без публикации: %v", firstRun.callList())
	}
	if firstRun.anyCallHas("v_news_events") {
		t.Errorf("check трогает витрину v_news_events; вызовы: %v", firstRun.callList())
	}

	// Окно ленты уже прошло мимо сохранённого максимума — check обязан назвать дыру
	// ошибкой (режим ручной: зелёный отчёт с дырой никто не перечитает) и по-прежнему
	// ничего не писать.
	stale := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.FixedZone("", mskOffset))
	gap := newNewsFakeConn(newsStoredSpan{Source: "tass", Count: 1, Min: stale, Max: stale})
	count, err = (&newsImport{}).Import(context.Background(), gap)
	if err == nil {
		t.Fatalf("Import(%s) при сохранённом максимуме tass %s (позже него лента) вернул %d строк без ошибки, want ошибку про дыру покрытия",
			modeCheck, stale.Format(time.RFC3339), count)
	}
	if !strings.Contains(err.Error(), "дыры покрытия") {
		t.Errorf("ошибка Import = %q, want упоминание «дыры покрытия»", err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк при найденной дыре, want 0", count)
	}
	if got := gap.batchList(); len(got) != 0 {
		t.Errorf("check открыл %d батчей при найденной дыре, want 0: дыра — диагноз, а не повод писать", len(got))
	}
	if gap.anyCallHas("v_news_events") {
		t.Errorf("check трогает витрину v_news_events при найденной дыре; вызовы: %v", gap.callList())
	}
}

func TestImportAllModeWritesAllFeeds(t *testing.T) {
	// all — режим расписания: пишутся все позиции текущего окна всех восьми лент одним
	// батчем. Число строк считается из фикстур (readFixtureDoc + itemsFromDoc), а не
	// хардкодится: обновление снимка лент не должно требовать правки числа в тесте.
	t.Setenv(modeEnv, modeAll)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	// Ключ включает url: url и description — обе строки, и перепутанные колонки ловит только
	// значение (типы в inserted() их не различают).
	var want []string // source|title|url всех позиций окна — так сверяются и состав, и число
	for _, f := range feeds() {
		for _, it := range byKey[f.Key] {
			want = append(want, it.Source+"|"+it.Title+"|"+it.URL)
		}
	}
	if len(want) == 0 {
		t.Fatal("фикстуры разобрались в 0 позиций — ожидание теста вакуумное")
	}

	conn := newNewsFakeConn()
	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s): %v", modeAll, err)
	}
	if int(count) != len(want) {
		t.Errorf("Import вернул %d строк, want %d (Σ позиций фикстур)", count, len(want))
	}

	batch := conn.onlyBatch(t, newsInsert)
	if !conn.anyCallHas("CREATE OR REPLACE VIEW") {
		t.Errorf("all не опубликовал витрину, want публикацию: в отличие от check, режим записи обязан её пересоздать; вызовы: %v", conn.callList())
	}
	// Миграция живой пятиколоночной таблицы выполняется и этим режимом, а текст её пиньится
	// дословно: расхождение с каноном (порядок колонок) иначе прошло бы незамеченным.
	if wantAlter := "ALTER TABLE news_events ADD COLUMN IF NOT EXISTS description String AFTER url"; !conn.anyCallHas(wantAlter) {
		t.Errorf("all не выполнил миграцию %q; вызовы: %v", wantAlter, conn.callList())
	}
	rows := batch.inserted(t)
	if len(rows) != len(want) {
		t.Errorf("в батч аппенднуто %d строк, want %d (Σ позиций фикстур)", len(rows), len(want))
	}
	if !batch.sent {
		t.Error("Send не вызван: батч в таблицу не уехал")
	}

	got := make([]string, 0, len(rows))
	sources := make(map[string]bool, 5)
	for _, row := range rows {
		got = append(got, row.Source+"|"+row.Title+"|"+row.URL)
		sources[row.Source] = true
		// У smart-lab описание — не анонс, а полный текст поста, и пустым он в батч попасть
		// не может: потеря колонки на пути в Append иначе всплыла бы только на живом прогоне.
		if row.Source == "smartlab" && row.Description == "" {
			t.Errorf("позиция smartlab %q записана с пустым description: текст поста потерян по пути в батч", row.Title)
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("состав батча = %d строк, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("состав батча расходится с фикстурами: строка %d = %q, want %q", i, got[i], want[i])
		}
	}

	// Семь лент — пять источников: оба фида ЦБ пишутся под source cbr, обе ленты
	// smart-lab — под smartlab.
	srcs := make([]string, 0, len(sources))
	for src := range sources {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	if wantSrcs := "cbr,interfax,rbc,smartlab,tass"; strings.Join(srcs, ",") != wantSrcs {
		t.Errorf("источники батча = %s, want %s", strings.Join(srcs, ","), wantSrcs)
	}
}

func TestImportSinceModeWritesOnlyMissing(t *testing.T) {
	// since — режим расписания: пишутся только отсутствующие в news_events позиции (ключ
	// date/source/title), а не всё окно. Тест держит обе ветки — «часть ключей уже есть»
	// (в батч уходит ровно остальное) и «все ключи на месте» (0 строк, PrepareBatch не
	// вызывался). Вторая ловит правку, теряющую фильтр наличия: без него since
	// переписывал бы окно целиком.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)

	// Существующие ключи — по первой позиции tass и rbc. Остальные позиции этих лент и
	// все позиции прочих обязаны уехать в батч.
	present := map[string]map[newsKey]bool{
		"tass": {keyOf(byKey["tass"][0]): true},
		"rbc":  {keyOf(byKey["rbc"][0]): true},
	}
	var want []string
	for _, f := range feeds() {
		for _, it := range byKey[f.Key] {
			if present[it.Source][keyOf(it)] {
				continue
			}
			want = append(want, it.Source+"|"+it.Title)
		}
	}
	if len(want) == 0 || len(want) == totalItems(byKey) {
		t.Fatalf("ожидание теста вакуумное: want %d из %d позиций", len(want), totalItems(byKey))
	}

	conn := newNewsFakeConnWithKeys(map[string][]newsKey{
		"tass": keysOf(byKey["tass"][:1]),
		"rbc":  keysOf(byKey["rbc"][:1]),
	})
	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s): %v", modeSince, err)
	}
	if int(count) != len(want) {
		t.Errorf("Import вернул %d строк, want %d (позиции, которых нет в news_events)", count, len(want))
	}
	batch := conn.onlyBatch(t, newsInsert)
	if !batch.sent {
		t.Error("Send не вызван: батч в таблицу не уехал")
	}
	rows := batch.inserted(t)
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.Source+"|"+row.Title)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("состав батча расходится с ожиданием:\n got %v\nwant %v", got, want)
	}
	if !conn.anyCallHas(storedKeysQuery) {
		t.Errorf("since не прочитал ключи запросом %q; вызовы: %v", storedKeysQuery, conn.callList())
	}

	// (б) все ключи фикстур на месте: писать нечего — 0 строк и ни одного батча.
	allKeys := make(map[string][]newsKey, 4)
	for _, f := range feeds() {
		allKeys[f.Source] = append(allKeys[f.Source], keysOf(byKey[f.Key])...)
	}
	conn = newNewsFakeConnWithKeys(allKeys)
	count, err = s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s) при полном покрытии: %v", modeSince, err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (все ключи фикстур уже в news_events)", count)
	}
	if got := conn.batchList(); len(got) != 0 {
		t.Errorf("since открыл %d батчей, want 0: писать нечего", len(got))
	}
}

func TestImportSinceModeCbrSecondFeedNoWatermark(t *testing.T) {
	// У ЦБ два фида под одним source='cbr', и их фронты независимы. Позиция «отставшего»
	// фида (дата между фронтами) отсутствует в БД и обязана записаться, хотя её дата
	// старее сохранённого максимума cbr: дата-курсор её теряет, проверка наличия по ключу
	// — нет. Позиция с ключом, который уже есть в БД (та же, что несёт и другой фид ЦБ), —
	// пропускается.
	t.Setenv(modeEnv, modeSince)

	msk := time.FixedZone("", mskOffset)
	storedAt := time.Date(2026, time.October, 12, 14, 0, 0, 0, msk)
	pressAt := time.Date(2026, time.October, 12, 12, 30, 0, 0, msk)
	freshAt := time.Date(2026, time.October, 12, 15, 0, 0, 0, msk)

	newsXML := rssXML(
		rssEntry{title: "ЦБ: уже сохранённая позиция", date: storedAt},
		rssEntry{title: "ЦБ: свежая позиция фронта", date: freshAt},
	)
	pressXML := rssXML(
		// Опоздавшая: дата раньше сохранённого максимума cbr (14:00) и раньше фронта
		// новостного фида (15:00) — при фильтре по дате она не записалась бы.
		rssEntry{title: "ЦБ: опоздавший пресс-релиз", date: pressAt},
		// Тот же ключ, что уже в БД (и что несёт другой фид ЦБ), — пропускается.
		rssEntry{title: "ЦБ: уже сохранённая позиция", date: storedAt},
	)
	pointFeedsAt(t, serveSyntheticCbr(t, newsXML, pressXML))

	byKey, _ := fixtureInventory(t)
	// Ключи interfax/rbc/tass/smartlab — все позиции фикстур: эти источники не пишутся,
	// и внимание теста остаётся на cbr. Обе ленты smart-lab несут один source, поэтому
	// ключи складываются, как в feedSpans; то же у Интерфакса: RSS и Telegram пишутся
	// под source=interfax, и покрыты обязаны быть позиции обоих транспортов.
	keys := map[string][]newsKey{
		"interfax": append(keysOf(byKey["interfax"]), keysOf(byKey["interfax_tg"])...),
		"rbc":      keysOf(byKey["rbc"]),
		"tass":     keysOf(byKey["tass"]),
		"smartlab": append(keysOf(byKey["smartlab_lebedev"]), keysOf(byKey["smartlab_spydell"])...),
		"cbr":      {{date: storedAt.Unix(), title: "ЦБ: уже сохранённая позиция"}},
	}

	conn := newNewsFakeConnWithKeys(keys)
	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s): %v", modeSince, err)
	}
	if count != 2 {
		t.Errorf("Import вернул %d строк, want 2 (опоздавший пресс-релиз + свежая позиция фронта)", count)
	}
	rows := conn.onlyBatch(t, newsInsert).inserted(t)
	var got []string
	for _, row := range rows {
		if row.Source != "cbr" {
			t.Errorf("записан источник %q, want cbr (позиции остальных лент уже покрыты)", row.Source)
		}
		got = append(got, row.Title)
		if row.Title == "ЦБ: опоздавший пресс-релиз" && !row.Date.Before(storedAt) {
			t.Errorf("опоздавшая позиция записана с датой %s, want раньше сохранённого максимума %s",
				row.Date.Format(time.RFC3339), storedAt.Format(time.RFC3339))
		}
	}
	sort.Strings(got)
	want := []string{"ЦБ: опоздавший пресс-релиз", "ЦБ: свежая позиция фронта"}
	sort.Strings(want)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("состав батча = %v, want %v", got, want)
	}
	// Нижняя граница запроса ключей cbr — минимум окна источника (опоздавшая, 12:30), а не
	// сохранённый максимум: при границе по максимуму опоздавшую не нашли бы вовсе.
	if wantCall := "source=cbr since=" + pressAt.Format(time.RFC3339); !conn.anyCallHas(wantCall) {
		t.Errorf("запрос ключей cbr не содержал %q; вызовы: %v", wantCall, conn.callList())
	}
}

func TestImportSinceModeKeysReadErrorJoinsFeedErrors(t *testing.T) {
	// Ошибка чтения ключей в since не должна вытеснять отказы лент: dagu печатает одну строку
	// на прогон, и в ней обязаны быть и сбой базы, и ключ упавшей ленты. Режим пишущий, но при
	// ошибке выборки до батча дело не доходит — строк не пишется.
	t.Setenv(modeEnv, modeSince)
	// cbr_news падает (500), остальные семь лент — закоммиченные фикстуры.
	pointFeedsAt(t, serveFixtures(t, map[string]int{"cbr_news": http.StatusInternalServerError}))

	conn := newNewsFakeConnWithKeys(nil)
	conn.queryErr = errors.New("чтение ключей упало")

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при ошибке чтения ключей вернул %d строк без ошибки, want ошибку", modeSince, count)
	}
	for _, want := range []string{"чтение ключей упало", "cbr_news"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка Import = %q, want фрагмент %q (сбой БД и ключ упавшей ленты вместе)", err, want)
		}
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк при ошибке чтения ключей, want 0", count)
	}
	if got := conn.batchList(); len(got) != 0 {
		t.Errorf("since открыл %d батчей при ошибке чтения ключей, want 0", len(got))
	}
	if !conn.anyCallHas(storedKeysQuery) {
		t.Errorf("since не обратился к ключам запросом %q; вызовы: %v", storedKeysQuery, conn.callList())
	}
}

func TestReadStoredKeysRowsError(t *testing.T) {
	// Ошибка обхода строк (обрыв соединения на середине) обязана вернуться из readStoredKeys:
	// молча вернувшаяся половина ключей записала бы уже сохранённые позиции повторно.
	conn := newNewsFakeConnWithKeys(nil)
	conn.rowsErr = errors.New("обрыв соединения на середине")

	_, err := readStoredKeys(context.Background(), conn, "interfax", time.Now())
	if err == nil {
		t.Fatal("readStoredKeys при rows.Err() != nil вернул nil-ошибку, want ошибку")
	}
	if !strings.Contains(err.Error(), "обрыв соединения на середине") {
		t.Errorf("ошибка readStoredKeys = %q, want упоминание причины (rows.Err)", err)
	}
}

func TestImportSinceModeAllFeedsFailed(t *testing.T) {
	// Все восемь лент упали: items пуст, и в since до чтения ключей дело не доходит — ошибка
	// собирается из отказов лент (ключ каждой), строк не пишется.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, map[string]int{
		"interfax":         http.StatusInternalServerError,
		"interfax_tg":      http.StatusInternalServerError,
		"rbc":              http.StatusInternalServerError,
		"cbr_news":         http.StatusInternalServerError,
		"cbr_press":        http.StatusInternalServerError,
		"tass":             http.StatusInternalServerError,
		"smartlab_lebedev": http.StatusInternalServerError,
		"smartlab_spydell": http.StatusInternalServerError,
	}))

	conn := newNewsFakeConn()
	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при сбое всех лент вернул %d строк без ошибки, want ошибку", modeSince, count)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк при сбое всех лент, want 0", count)
	}
	for _, key := range []string{"interfax", "interfax_tg", "rbc", "cbr_news", "cbr_press", "tass", "smartlab_lebedev", "smartlab_spydell"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("ошибка Import = %q, want ключ упавшей ленты %q", err, key)
		}
	}
	if got := conn.batchList(); len(got) != 0 {
		t.Errorf("since открыл %d батчей при сбое всех лент, want 0", len(got))
	}
	keyCalls := 0
	for _, call := range conn.callList() {
		if strings.Contains(call, storedKeysQuery) {
			keyCalls++
		}
	}
	if keyCalls != 0 {
		t.Errorf("since обратился к ключам %d раз при пустом окне, want 0 (до selectNewItems дело не дошло); вызовы: %v",
			keyCalls, conn.callList())
	}
}

// rssEntry — позиция синтетической ленты: заголовок и дата публикации. Заголовки —
// простой текст (XML-экранирование не нужно), ссылки синтетические.
type rssEntry struct {
	title string
	date  time.Time
}

// rssXML собирает минимальный RSS 2.0: формат pubDate — time.RFC1123Z, тот же, что у всех
// живых RSS-лент (с числовым офсетом), поэтому синтетическая лента проходит тот же разбор
// (readFixtureDoc + itemsFromDoc), что и фикстуры.
func rssXML(entries ...rssEntry) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>`)
	for i, e := range entries {
		fmt.Fprintf(&b, `<item><title>%s</title><link>https://example.test/%d</link><pubDate>%s</pubDate></item>`,
			e.title, i, e.date.Format(time.RFC1123Z))
	}
	b.WriteString(`</channel></rss>`)
	return b.String()
}

// serveSyntheticCbr поднимает httptest-сервер, где два фида ЦБ отдают синтетические ленты,
// а остальные шесть — закоммиченные фикстуры: тест держит под контролем окно именно ЦБ, не
// переписывая testdata.
func serveSyntheticCbr(t *testing.T, newsXML, pressXML string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		var (
			body        []byte
			contentType = "application/rss+xml; charset=utf-8"
		)
		switch key {
		case "cbr_news":
			body = []byte(newsXML)
		case "cbr_press":
			body = []byte(pressXML)
		default:
			var name string
			name, contentType = fixtureName(key)
			b, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				http.NotFound(w, r)
				return
			}
			body = b
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}
