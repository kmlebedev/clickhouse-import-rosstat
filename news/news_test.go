package news

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// TestMain глушит логи импортёра: Import печатает Infof на каждую прочитанную ленту
// и на каждую строку отчёта check, а проверяются здесь счётчик записанных строк,
// тексты ошибок и состав вызовов соединения. Без этого вывод `go test` состоял бы из
// строк лога, к предмету проверки отношения не имеющих, и падение теста в них тонуло бы.
func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)

	// Офлайн-тесты не должны зависеть от env разработчика: с живыми LLM_* тестовый
	// прогон ушёл бы к настоящему провайдеру. Тесты, которым нужна конфигурация,
	// выставляют её через t.Setenv и получают исходное (пустое) значение назад.
	for _, key := range []string{newsLLMBaseURLEnv, newsLLMAPIKeyEnv, newsLLMModelEnv} {
		_ = os.Unsetenv(key)
	}

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
	oldInterfax, oldInterfaxTelegram, oldEdisclosure, oldRBC, oldCBRNews, oldCBRPress, oldTASS, oldSmartlabLebedev, oldSmartlabSpydell :=
		feedInterfaxURL, feedInterfaxTelegramURL, feedEdisclosureURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL,
		feedSmartlabLebedevURL, feedSmartlabSpydellURL
	feedInterfaxURL = srv.URL + "/interfax"
	feedInterfaxTelegramURL = srv.URL + "/interfax_tg"
	feedEdisclosureURL = srv.URL + "/edisclosure_tg"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	feedSmartlabLebedevURL = srv.URL + "/smartlab_lebedev"
	feedSmartlabSpydellURL = srv.URL + "/smartlab_spydell"
	t.Cleanup(func() {
		feedInterfaxURL, feedInterfaxTelegramURL, feedEdisclosureURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL,
			feedSmartlabLebedevURL, feedSmartlabSpydellURL =
			oldInterfax, oldInterfaxTelegram, oldEdisclosure, oldRBC, oldCBRNews, oldCBRPress, oldTASS, oldSmartlabLebedev, oldSmartlabSpydell
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
	// Сбойная одна лента: остальные восемь обязаны прочитаться. Иначе падение
	// любого источника глушило бы весь прогон, а dagu-ретрай повторял бы восемь
	// здоровых лент ради одной больной.
	srv := serveFixtures(t, map[string]int{"cbr_news": http.StatusInternalServerError})
	pointFeedsAt(t, srv)

	results := fetchFeeds(feeds())
	if len(results) != 9 {
		t.Fatalf("fetchFeeds вернул %d результатов, want 9 (по числу лент)", len(results))
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

	if want := "interfax,interfax_tg,edisclosure_tg,rbc,cbr_news,cbr_press,tass,smartlab_lebedev,smartlab_spydell"; strings.Join(keys, ",") != want {
		t.Errorf("порядок лент = %s, want %s (фиксированный, детерминированный лог)", strings.Join(keys, ","), want)
	}
	// Оба Интерфакса (RSS+Telegram) пишутся под source=interfax, edisclosure_tg — под
	// отдельным source=edisclosure (другой канал), оба фида ЦБ — под cbr, обе ленты
	// smart-lab — под smartlab: витрина различает их url, а не меткой.
	if want := "interfax,interfax,edisclosure,rbc,cbr,cbr,tass,smartlab,smartlab"; strings.Join(sources, ",") != want {
		t.Errorf("метки источников = %s, want %s", strings.Join(sources, ","), want)
	}
}

func TestFeedsHaveFixtures(t *testing.T) {
	configured := feeds()
	if len(configured) != 9 {
		t.Fatalf("feeds() = %d лент, want 9", len(configured))
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

// pointFeedsAt заменяет URL-ы всех девяти лент на пути httptest-сервера и возвращает
// исходные значения после теста: feeds() читает пакетные переменные при каждом вызове,
// поэтому подмена подхватывается, а t.Cleanup не даёт ей утечь в соседние тесты.
//
// Guard в конце — про новую ленту: неподменённый URL увёл бы офлайн-тест в настоящую
// сеть, и тот падал бы (или, хуже, зеленел) по доступности живого сайта, а не по коду.
func pointFeedsAt(t *testing.T, srv *httptest.Server) {
	t.Helper()
	old := [9]string{feedInterfaxURL, feedInterfaxTelegramURL, feedEdisclosureURL, feedRBCURL,
		feedCBRNewsURL, feedCBRPressURL, feedTASSURL, feedSmartlabLebedevURL, feedSmartlabSpydellURL}
	feedInterfaxURL = srv.URL + "/interfax"
	feedInterfaxTelegramURL = srv.URL + "/interfax_tg"
	feedEdisclosureURL = srv.URL + "/edisclosure_tg"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	feedSmartlabLebedevURL = srv.URL + "/smartlab_lebedev"
	feedSmartlabSpydellURL = srv.URL + "/smartlab_spydell"
	t.Cleanup(func() {
		feedInterfaxURL, feedInterfaxTelegramURL, feedEdisclosureURL, feedRBCURL,
			feedCBRNewsURL, feedCBRPressURL, feedTASSURL, feedSmartlabLebedevURL, feedSmartlabSpydellURL =
			old[0], old[1], old[2], old[3], old[4], old[5], old[6], old[7], old[8]
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
	byKey = make(map[string][]newsItem, 9)
	maxBySource = make(map[string]time.Time, 6)
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

// telegramFixtureWindow — границы окна Telegram-фикстуры по датам публикации: тот же
// расчёт, что делает Import перед чтением версий (минимум и максимум по окну ленты).
func telegramFixtureWindow(items []newsItem) (min, max time.Time) {
	min, max = items[0].Date, items[0].Date
	for _, it := range items[1:] {
		if it.Date.Before(min) {
			min = it.Date
		}
		if it.Date.After(max) {
			max = it.Date
		}
	}
	return min, max
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

// Виды строк, которые умеет отдавать newsFakeRows: покрытие источников (storedSpansQuery),
// ключи существующих позиций (storedKeysQuery), версии Telegram (telegramVersionsQuery) и
// кандидаты LLM-разметки (newsAnnotationCandidatesQuery).
const (
	fakeRowsSpans      = "spans"
	fakeRowsKeys       = "keys"
	fakeRowsVersions   = "versions"
	fakeRowsCandidates = "candidates"
)

// newsFakeRows — фейковый driver.Rows контура news: вид строк выбирается в Query по
// самому запросу, а не по числу вызовов, чтобы since-тесты держали разные наборы ключей
// на источник. Реализованы только те методы, которые зовут readStoredSpans/readStoredKeys/
// readTelegramVersions (Next/Scan/Err/Close); остальные остаются nil от встраивания
// driver.Rows и паникуют, если сценарий пойдёт дальше. Готового фейка driver.Rows в
// репозитории нет (bold-фейк отдаёт driver.Row) — свой.
type newsFakeRows struct {
	driver.Rows
	kind       string // fakeRowsSpans, fakeRowsKeys, fakeRowsVersions или fakeRowsCandidates
	spans      []newsStoredSpan
	keys       []newsKey
	versions   []telegramVersion
	candidates []newsItem
	err        error // отдать Err(): сценарий «обход строк упал на середине» (приходит из newsFakeConn)
	i          int   // индекс следующей строки: Next двигает его, Scan читает i-1
}

// n — число строк выбранного вида: Next и Scan не должны перепутать наборы.
func (r *newsFakeRows) n() int {
	switch r.kind {
	case fakeRowsKeys:
		return len(r.keys)
	case fakeRowsVersions:
		return len(r.versions)
	case fakeRowsCandidates:
		return len(r.candidates)
	default:
		return len(r.spans)
	}
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
// *time.Time и *string (порядок date, title), для версий — колонки telegramVersionsQuery
// в её порядке. Порядок колонок — часть контракта запроса, и фейк его пинит: сдвиг колонок
// в SQL не должен остаться незамеченным.
func (r *newsFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > r.n() {
		return fmt.Errorf("фейковая строка (%s): Scan без Next (i=%d, строк %d)", r.kind, r.i, r.n())
	}
	switch r.kind {
	case fakeRowsKeys:
		return r.scanKey(dest)
	case fakeRowsVersions:
		return r.scanVersion(dest)
	case fakeRowsCandidates:
		return r.scanCandidate(dest)
	default:
		return r.scanSpan(dest)
	}
}

// scanCandidate пинит порядок Scan newsAnnotationCandidatesQuery: date, source, title, url,
// description, content_hash. Порядок колонок — часть контракта запроса: сдвиг в SQL или
// в разборе кандидатов не должен остаться незамеченным.
func (r *newsFakeRows) scanCandidate(dest []any) error {
	if len(dest) != 6 {
		return fmt.Errorf("фейковый кандидат разметки: приёмников %d, want 6 (date, source, title, url, description, content_hash)", len(dest))
	}
	it := r.candidates[r.i-1]
	date, ok := dest[0].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник date %T (want *time.Time)", dest[0])
	}
	source, ok := dest[1].(*string)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник source %T (want *string)", dest[1])
	}
	title, ok := dest[2].(*string)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник title %T (want *string)", dest[2])
	}
	url, ok := dest[3].(*string)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник url %T (want *string)", dest[3])
	}
	description, ok := dest[4].(*string)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник description %T (want *string)", dest[4])
	}
	hash, ok := dest[5].(*string)
	if !ok {
		return fmt.Errorf("фейковый кандидат: приёмник content_hash %T (want *string)", dest[5])
	}
	*date, *source, *title, *url, *description, *hash =
		it.Date, it.Source, it.Title, it.URL, it.Description, it.ContentHash
	return nil
}

// scanVersion пинит порядок Scan telegramVersionsQuery: channel, message_id, version, date,
// source, title, url, description, content_hash, first_seen, deleted. Типы — как у колонок
// канона (UInt64/UInt32/UInt8), иначе живой Scan разошёлся бы с фейком.
func (r *newsFakeRows) scanVersion(dest []any) error {
	if len(dest) != 11 {
		return fmt.Errorf("фейковая строка версии: приёмников %d, want 11 (channel, message_id, version, date, source, title, url, description, content_hash, first_seen, deleted)", len(dest))
	}
	v := r.versions[r.i-1]
	channel, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник channel %T (want *string)", dest[0])
	}
	messageID, ok := dest[1].(*uint64)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник message_id %T (want *uint64)", dest[1])
	}
	version, ok := dest[2].(*uint32)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник version %T (want *uint32)", dest[2])
	}
	date, ok := dest[3].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник date %T (want *time.Time)", dest[3])
	}
	source, ok := dest[4].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник source %T (want *string)", dest[4])
	}
	title, ok := dest[5].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник title %T (want *string)", dest[5])
	}
	url, ok := dest[6].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник url %T (want *string)", dest[6])
	}
	description, ok := dest[7].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник description %T (want *string)", dest[7])
	}
	hash, ok := dest[8].(*string)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник content_hash %T (want *string)", dest[8])
	}
	firstSeen, ok := dest[9].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник first_seen %T (want *time.Time)", dest[9])
	}
	deleted, ok := dest[10].(*uint8)
	if !ok {
		return fmt.Errorf("фейковая строка версии: приёмник deleted %T (want *uint8)", dest[10])
	}
	*channel, *messageID, *version, *date, *source, *title, *url, *description, *hash, *firstSeen, *deleted =
		v.Channel, v.MessageID, v.Version, v.Date, v.Source, v.Title, v.URL, v.Description, v.ContentHash, v.FirstSeen, v.Deleted
	return nil
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
// sendErr задаёт сбой Send для сценария «вторая таблица упала, первая уже записана».
type newsFakeBatch struct {
	driver.Batch
	query   string
	rows    [][]any
	sent    bool
	sendErr error
}

func (b *newsFakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

func (b *newsFakeBatch) Send() error {
	if b.sendErr != nil {
		return b.sendErr
	}
	b.sent = true
	return nil
}

func (b *newsFakeBatch) Abort() error { return nil }

// newsInsertedRow — строка батча, разобранная по колонкам news_events в порядке
// insertItems (date, source, title, url, description, content_hash, sentiment).
type newsInsertedRow struct {
	Date        time.Time
	Source      string
	Title       string
	URL         string
	Description string
	ContentHash string
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
		if len(v) != 7 {
			t.Fatalf("строка батча %d: значений %d, want 7 (date, source, title, url, description, content_hash, sentiment)", i, len(v))
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
		hash, ok := v[5].(string)
		if !ok {
			t.Fatalf("строка батча %d: content_hash = %T (want string)", i, v[5])
		}
		sentiment, ok := v[6].(*float32)
		if !ok {
			t.Fatalf("строка батча %d: sentiment = %T (want *float32)", i, v[6])
		}
		if sentiment != nil {
			t.Errorf("строка батча %d: sentiment = %v, want NULL (канал заполнения — follow-up)", i, *sentiment)
		}
		rows = append(rows, newsInsertedRow{
			Date: date, Source: source, Title: title, URL: url, Description: description,
			ContentHash: hash, Sentiment: sentiment,
		})
	}
	return rows
}

// insertedVersions разбирает строки батча news_message_version по именам колонок в порядке
// telegramVersionInsert (channel, message_id, version, date, source, title, url, description,
// content_hash, first_seen, deleted). Перепутанные поля обязаны валить тест, а не уезжать в базу.
func (b *newsFakeBatch) insertedVersions(t *testing.T) []telegramVersion {
	t.Helper()
	out := make([]telegramVersion, 0, len(b.rows))
	for i, v := range b.rows {
		if len(v) != 11 {
			t.Fatalf("строка версий %d: значений %d, want 11 (channel, message_id, version, date, source, title, url, description, content_hash, first_seen, deleted)",
				i, len(v))
		}
		channel, ok := v[0].(string)
		if !ok {
			t.Fatalf("строка версий %d: channel = %T (want string)", i, v[0])
		}
		messageID, ok := v[1].(uint64)
		if !ok {
			t.Fatalf("строка версий %d: message_id = %T (want uint64)", i, v[1])
		}
		version, ok := v[2].(uint32)
		if !ok {
			t.Fatalf("строка версий %d: version = %T (want uint32)", i, v[2])
		}
		date, ok := v[3].(time.Time)
		if !ok {
			t.Fatalf("строка версий %d: date = %T (want time.Time)", i, v[3])
		}
		source, ok := v[4].(string)
		if !ok {
			t.Fatalf("строка версий %d: source = %T (want string)", i, v[4])
		}
		title, ok := v[5].(string)
		if !ok {
			t.Fatalf("строка версий %d: title = %T (want string)", i, v[5])
		}
		url, ok := v[6].(string)
		if !ok {
			t.Fatalf("строка версий %d: url = %T (want string)", i, v[6])
		}
		description, ok := v[7].(string)
		if !ok {
			t.Fatalf("строка версий %d: description = %T (want string)", i, v[7])
		}
		hash, ok := v[8].(string)
		if !ok {
			t.Fatalf("строка версий %d: content_hash = %T (want string)", i, v[8])
		}
		firstSeen, ok := v[9].(time.Time)
		if !ok {
			t.Fatalf("строка версий %d: first_seen = %T (want time.Time)", i, v[9])
		}
		deleted, ok := v[10].(uint8)
		if !ok {
			t.Fatalf("строка версий %d: deleted = %T (want uint8)", i, v[10])
		}
		out = append(out, telegramVersion{
			Channel: channel, MessageID: messageID, Version: version, Date: date, Source: source,
			Title: title, URL: url, Description: description, ContentHash: hash,
			FirstSeen: firstSeen, Deleted: deleted,
		})
	}
	return out
}

// insertedAnnotations разбирает строки батча news_annotation по именам колонок в порядке
// newsAnnotationInsert (url, content_hash, kind, method, model, prompt_version, dimension,
// tag, direction, confidence, evidence, sentiment, relevance, summary, annotated_at).
// Перепутанные поля обязаны валить тест, а не уезжать в базу.
func (b *newsFakeBatch) insertedAnnotations(t *testing.T) []newsAnnotationRow {
	t.Helper()
	out := make([]newsAnnotationRow, 0, len(b.rows))
	for i, v := range b.rows {
		if len(v) != 15 {
			t.Fatalf("строка разметки %d: значений %d, want 15 (url, content_hash, kind, method, model, "+
				"prompt_version, dimension, tag, direction, confidence, evidence, sentiment, relevance, "+
				"summary, annotated_at)", i, len(v))
		}
		row := newsAnnotationRow{}
		strDsts := []*string{
			&row.URL, &row.ContentHash, &row.Kind, &row.Method, &row.Model,
			&row.Dimension, &row.Tag, &row.Direction, &row.Evidence, &row.Summary,
		}
		// Позиции строковых колонок в порядке Append; числовые и время разбираются отдельно.
		strIdx := []int{0, 1, 2, 3, 4, 6, 7, 8, 10, 13}
		for j, dst := range strDsts {
			got, ok := v[strIdx[j]].(string)
			if !ok {
				t.Fatalf("строка разметки %d: колонка %d = %T (want string)", i, strIdx[j], v[strIdx[j]])
			}
			*dst = got
		}
		promptVersion, ok := v[5].(uint16)
		if !ok {
			t.Fatalf("строка разметки %d: prompt_version = %T (want uint16)", i, v[5])
		}
		confidence, ok := v[9].(float32)
		if !ok {
			t.Fatalf("строка разметки %d: confidence = %T (want float32)", i, v[9])
		}
		sentiment, ok := v[11].(float32)
		if !ok {
			t.Fatalf("строка разметки %d: sentiment = %T (want float32)", i, v[11])
		}
		relevance, ok := v[12].(float32)
		if !ok {
			t.Fatalf("строка разметки %d: relevance = %T (want float32)", i, v[12])
		}
		annotatedAt, ok := v[14].(time.Time)
		if !ok {
			t.Fatalf("строка разметки %d: annotated_at = %T (want time.Time)", i, v[14])
		}
		row.PromptVersion, row.Confidence, row.Sentiment, row.Relevance, row.AnnotatedAt =
			promptVersion, confidence, sentiment, relevance, annotatedAt
		out = append(out, row)
	}
	return out
}

// newsFakeConn — фейковое соединение контура news: встраивание driver.Conn оставляет
// неиспользуемые методы nil (паника, если тест зайдёт дальше сценария), а переопределены
// ровно три метода, которыми пользуется Import: Exec (DDL и витрина), Query
// (storedSpansQuery — readStoredSpans в check; storedKeysQuery — readStoredKeys в since;
// telegramVersionsQuery — readTelegramVersions в пишущих режимах;
// newsAnnotationCandidatesQuery — readNewsAnnotationCandidates в LLM-шаге) и PrepareBatch
// (запись позиций, версий и разметки). spans — очередь ответов на запрос покрытия; keys —
// ключи существующих позиций по источникам; versions — версии Telegram по каналам;
// candidates — бэклог LLM-разметки. Пустые наборы означают «строк нет».
// versionsErr/keysErr/candidatesErr включают ошибочные пути Query точечно (иначе сбой одного
// чтения не дал бы проверить остальные), rowsErr — обход строк (rows.Err уже после Next);
// sendErr — сбой Send выбранного батча по запросу. Всё задаётся полями постфактум, чтобы
// не плодить конструкторы.
type newsFakeConn struct {
	driver.Conn
	mu         sync.Mutex
	calls      []string
	spans      []newsStoredSpan
	keys       map[string][]newsKey
	versions   map[string][]telegramVersion
	candidates []newsItem
	// candidateArgs — аргументы последнего запроса кандидатов в порядке `?`: filter, filter,
	// model, prompt_version. Фейк SQL не разбирает, поэтому порядок аргументов проверяется здесь.
	candidateArgs []any
	versionsErr   error            // Query версий вернёт её вместо строк
	keysErr       error            // Query ключей вернёт её вместо строк
	candidatesErr error            // Query кандидатов разметки вернёт её вместо строк
	rowsErr       error            // её отдаст rows.Err()
	sendErr       map[string]error // сбой Send батча по тексту запроса
	batches       []*newsFakeBatch
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
// ключи — набором запрошенного источника, версии — набором канала. Аргументы запроса
// (source/channel, since) попадают в записанный вызов: тесты пинят и ключ, и нижнюю
// границу окна.
//
// У кандидатов разметки четыре аргумента в порядке `?`: filter, filter, model, prompt_version.
// Фейк не разбирает SQL, поэтому дополнительно запоминает их в candidateArgs: тест пинит
// именно порядок (дефект «have no arg for param ?» вылез только на живой базе).
func (c *newsFakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	label := "query: " + query
	var key string
	if len(args) == 2 {
		if s, ok := args[0].(string); ok {
			key = s
			if since, ok := args[1].(time.Time); ok {
				label += fmt.Sprintf(" [source=%s since=%s]", key, since.Format(time.RFC3339))
			}
			if promptVersion, ok := args[1].(uint16); ok {
				label += fmt.Sprintf(" [model=%s prompt_version=%d]", key, promptVersion)
			}
		}
	}
	if len(args) == 4 {
		if _, ok := args[0].(string); ok {
			if _, ok := args[1].(string); ok {
				if model, ok := args[2].(string); ok {
					if promptVersion, ok := args[3].(uint16); ok {
						label += fmt.Sprintf(" [model=%s prompt_version=%d]", model, promptVersion)
					}
				}
			}
		}
	}
	c.record(label)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Кандидаты разметки — запрос функции (LIMIT из NEWS_LLM_BACKLOG_LIMIT вшит в SQL),
	// поэтому сравнение идёт по префиксу отдельно: в expression-switch Go требует
	// константные case.
	if strings.HasPrefix(query, newsAnnotationCandidatesQueryPrefix) {
		c.candidateArgs = append([]any(nil), args...)
		if c.candidatesErr != nil {
			return nil, c.candidatesErr
		}
		return &newsFakeRows{kind: fakeRowsCandidates, candidates: c.candidates, err: c.rowsErr}, nil
	}
	switch query {
	case storedSpansQuery:
		rows := c.spans
		c.spans = nil // очередь отдаётся один раз: повторный Query вернул бы уже пустое окно
		return &newsFakeRows{kind: fakeRowsSpans, spans: rows, err: c.rowsErr}, nil
	case storedKeysQuery:
		if c.keysErr != nil {
			return nil, c.keysErr
		}
		return &newsFakeRows{kind: fakeRowsKeys, keys: c.keys[key], err: c.rowsErr}, nil
	case telegramVersionsQuery:
		if c.versionsErr != nil {
			return nil, c.versionsErr
		}
		return &newsFakeRows{kind: fakeRowsVersions, versions: c.versions[key], err: c.rowsErr}, nil
	}
	return nil, fmt.Errorf("newsFakeConn: неожиданный запрос %q", query)
}

func (c *newsFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.record("preparebatch: " + query)
	c.mu.Lock()
	batch := &newsFakeBatch{query: query, sendErr: c.sendErr[query]}
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return batch, nil
}

func (c *newsFakeConn) callList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// candidateArgsList — аргументы последнего запроса кандидатов в порядке `?` (filter, filter,
// model, prompt_version): фейк SQL не разбирает, поэтому порядок пинится отдельным тестом.
func (c *newsFakeConn) candidateArgsList() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]any(nil), c.candidateArgs...)
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

// anyCallHasPrefix — был ли вызов Query, запрос которого начинается с префикса. Кандидаты
// разметки строятся функцией (LIMIT вшит в SQL), поэтому их вызов ищется по префиксу
// запроса, а не по точному равенству. Записанный вызов начинается с "query: ".
func (c *newsFakeConn) anyCallHasPrefix(prefix string) bool {
	for _, call := range c.callList() {
		if strings.HasPrefix(call, "query: "+prefix) {
			return true
		}
	}
	return false
}

// batchByQuery возвращает единственный батч, открытый указанным запросом: прогон пишет
// каждую таблицу одним батчем (правило 2 AGENTS.md), поэтому второй PrepareBatch того же
// запроса — уже дефект.
func (c *newsFakeConn) batchByQuery(t *testing.T, query string) *newsFakeBatch {
	t.Helper()
	var found []*newsFakeBatch
	for _, batch := range c.batchList() {
		if batch.query == query {
			found = append(found, batch)
		}
	}
	if len(found) != 1 {
		t.Fatalf("PrepareBatch с запросом %q вызван %d раз, want 1; вызовы: %v", query, len(found), c.callList())
	}
	return found[0]
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
	// ошибки нет. Отсутствие ошибки здесь же доказывает, что все девять лент прочитались:
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
	// check — режим чтения покрытия: версии Telegram он не читает (их чтение — часть
	// пишущего прогона), иначе сбой news_message_version валил бы диагностику покрытия.
	if firstRun.anyCallHas(telegramVersionsQuery) {
		t.Errorf("check прочитал версии Telegram запросом %q, want чтение без версий: %v",
			telegramVersionsQuery, firstRun.callList())
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
	// all — режим расписания: пишутся все позиции текущего окна всех девяти лент, затем
	// версии Telegram отдельным батчем. Число строк считается из фикстур (readFixtureDoc +
	// itemsFromDoc), а не хардкодится: обновление снимка лент не должно требовать правки
	// числа в тесте.
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

	// Порядок батчей — часть контракта: сначала news_events, затем версии; сбой версий
	// не должен отменять уже записанный текущий текст.
	batches := conn.batchList()
	if len(batches) != 2 || batches[0].query != newsInsert || batches[1].query != telegramVersionInsert {
		t.Fatalf("батчи = %v, want [newsInsert, telegramVersionInsert]", conn.callList())
	}
	batch := conn.batchByQuery(t, newsInsert)
	if !conn.anyCallHas("CREATE OR REPLACE VIEW") {
		t.Errorf("all не опубликовал витрину, want публикацию: в отличие от check, режим записи обязан её пересоздать; вызовы: %v", conn.callList())
	}
	// Миграции живой таблицы выполняются и этим режимом, а тексты пиньются дословно:
	// расхождение с каноном (порядок колонок) иначе прошло бы незамеченным.
	for _, wantAlter := range []string{
		"ALTER TABLE news_events ADD COLUMN IF NOT EXISTS description String AFTER url",
		"ALTER TABLE news_events ADD COLUMN IF NOT EXISTS content_hash String AFTER description",
	} {
		if !conn.anyCallHas(wantAlter) {
			t.Errorf("all не выполнил миграцию %q; вызовы: %v", wantAlter, conn.callList())
		}
	}
	rows := batch.inserted(t)
	if len(rows) != len(want) {
		t.Errorf("в батч аппенднуто %d строк, want %d (Σ позиций фикстур)", len(rows), len(want))
	}
	if !batch.sent {
		t.Error("Send не вызван: батч в таблицу не уехал")
	}

	// Версии Telegram — второй батч: на пустой истории по v1 на сообщение, и это
	// относится к обоим каналам транспорта (interfaxonline и edisclosure).
	versionBatch := conn.batchByQuery(t, telegramVersionInsert)
	versionRows := versionBatch.insertedVersions(t)
	var tgItems []newsItem
	for _, key := range []string{"interfax_tg", "edisclosure_tg"} {
		tgItems = append(tgItems, byKey[key]...)
	}
	if len(versionRows) != len(tgItems) {
		t.Errorf("в батч версий аппенднуто %d строк, want %d (v1 на сообщение Telegram)", len(versionRows), len(tgItems))
	}
	if !versionBatch.sent {
		t.Error("Send батча версий не вызван: версии в таблицу не уехали")
	}
	// Ключ — канал и ID: у двух каналов свои нумерации, и совпадение ID не должно
	// склеить версии разных каналов.
	versionByChannelID := make(map[string]telegramVersion, len(versionRows))
	for _, v := range versionRows {
		versionByChannelID[v.Channel+"/"+fmt.Sprint(v.MessageID)] = v
	}
	for _, it := range tgItems {
		v, ok := versionByChannelID[it.Channel+"/"+fmt.Sprint(it.MessageID)]
		if !ok {
			t.Errorf("нет версии для сообщения %s/%d", it.Channel, it.MessageID)
			continue
		}
		if v.Channel != it.Channel || v.Source != it.Source || v.URL != it.URL {
			t.Errorf("версия %s/%d: channel/source/url = %q/%q/%q, want %q/%q/%q",
				it.Channel, it.MessageID, v.Channel, v.Source, v.URL, it.Channel, it.Source, it.URL)
		}
		if v.Version != 1 || v.Deleted != 0 {
			t.Errorf("версия %s/%d = v%d deleted=%d, want v1 deleted=0", it.Channel, it.MessageID, v.Version, v.Deleted)
		}
		if v.Title != it.Title || v.Description != it.Description || v.ContentHash != it.ContentHash {
			t.Errorf("версия %s/%d = %+v, want текущий текст %+v", it.Channel, it.MessageID, v, it)
		}
		if !v.Date.Equal(it.Date) {
			t.Errorf("версия %s/%d: date = %v, want дату публикации %v", it.Channel, it.MessageID, v.Date, it.Date)
		}
		if v.FirstSeen.IsZero() {
			t.Errorf("версия %s/%d: first_seen нулевой, want время прогона", it.Channel, it.MessageID)
		}
	}

	got := make([]string, 0, len(rows))
	sources := make(map[string]bool, 6)
	for _, row := range rows {
		got = append(got, row.Source+"|"+row.Title+"|"+row.URL)
		sources[row.Source] = true
		// У smart-lab описание — не анонс, а полный текст поста, и пустым он в батч попасть
		// не может: потеря колонки на пути в Append иначе всплыла бы только на живом прогоне.
		if row.Source == "smartlab" && row.Description == "" {
			t.Errorf("позиция smartlab %q записана с пустым description: текст поста потерян по пути в батч", row.Title)
		}
		// content_hash заполняют обе ленты — и RSS, и Telegram; пустое значение означало бы,
		// что колонка потерялась по пути в Append (у LLM-разметки ключ generic).
		if row.ContentHash == "" {
			t.Errorf("позиция %q записана с пустым content_hash", row.Title)
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

	// Девять лент — шесть источников: оба фида ЦБ пишутся под source cbr, обе ленты
	// smart-lab — под smartlab, edisclosure_tg — под отдельным edisclosure.
	srcs := make([]string, 0, len(sources))
	for src := range sources {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	if wantSrcs := "cbr,edisclosure,interfax,rbc,smartlab,tass"; strings.Join(srcs, ",") != wantSrcs {
		t.Errorf("источники батча = %s, want %s", strings.Join(srcs, ","), wantSrcs)
	}
}

func TestImportSinceModeWritesOnlyMissing(t *testing.T) {
	// since — режим расписания: для RSS пишутся только отсутствующие в news_events позиции
	// (ключ date/source/title), а Telegram-текущие — upsert'ом всегда. Тест держит обе
	// ветки — «часть ключей уже есть» (в батч уходит ровно остальное) и «все ключи на
	// месте» (RSS писать нечего, но Telegram по-прежнему пишется). Вторая ловит правку,
	// теряющую фильтр наличия RSS: без него since переписывал бы окно целиком.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	// Оба Telegram-канала (interfaxonline и edisclosure) пишутся upsert'ом в любом случае.
	tgCount := len(byKey["interfax_tg"]) + len(byKey["edisclosure_tg"])

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
	batch := conn.batchByQuery(t, newsInsert)
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
	if got := conn.batchByQuery(t, telegramVersionInsert).insertedVersions(t); len(got) != tgCount {
		t.Errorf("батч версий = %d строк, want %d (на пустой истории — v1 на сообщение)", len(got), tgCount)
	}

	// (б) все ключи фикстур на месте: RSS писать нечего, но Telegram-текущие пишутся
	// всегда (upsert), поэтому батч news_events содержит ровно сообщения Telegram,
	// а второй батч — их v1. Число строк Import — по-прежнему строки news_events.
	allKeys := make(map[string][]newsKey, 6)
	for _, f := range feeds() {
		allKeys[f.Source] = append(allKeys[f.Source], keysOf(byKey[f.Key])...)
	}
	conn = newNewsFakeConnWithKeys(allKeys)
	count, err = s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s) при полном покрытии RSS: %v", modeSince, err)
	}
	if int(count) != tgCount {
		t.Errorf("Import вернул %d строк, want %d (Telegram-текущие пишутся upsert'ом всегда)", count, tgCount)
	}
	newsRows := conn.batchByQuery(t, newsInsert).inserted(t)
	if len(newsRows) != tgCount {
		t.Errorf("батч news_events = %d строк, want %d (только Telegram)", len(newsRows), tgCount)
	}
	for _, row := range newsRows {
		if !strings.HasPrefix(row.URL, "https://t.me/") {
			t.Errorf("в since при полном покрытии записана RSS-позиция %q (%s), want только Telegram",
				row.Title, row.URL)
		}
	}
	if got := conn.batchByQuery(t, telegramVersionInsert).insertedVersions(t); len(got) != tgCount {
		t.Errorf("батч версий = %d строк, want %d", len(got), tgCount)
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
	// Ключи RSS-лент interfax/rbc/tass/smartlab — все позиции фикстур: эти источники не
	// пишутся, и внимание теста остаётся на cbr. Обе ленты smart-lab несут один source,
	// поэтому ключи складываются, как в feedSpans; у Интерфакса в map лежат и ключи
	// Telegram — в since-фильтр они не попадают (Telegram пишется upsert'ом), но лишними
	// не мешают.
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
	// К двум новым позициям ЦБ добавляются Telegram-текущие обоих каналов: они пишутся
	// upsert'ом всегда.
	tgCount := len(byKey["interfax_tg"]) + len(byKey["edisclosure_tg"])
	if int(count) != 2+tgCount {
		t.Errorf("Import вернул %d строк, want %d (опоздавший пресс-релиз + свежая позиция фронта + Telegram upsert)",
			count, 2+tgCount)
	}
	if got := conn.batchByQuery(t, telegramVersionInsert).insertedVersions(t); len(got) != tgCount {
		t.Errorf("батч версий = %d строк, want %d (v1 сообщений Telegram)", len(got), tgCount)
	}
	rows := conn.batchByQuery(t, newsInsert).inserted(t)
	var got, tgGot []string
	for _, row := range rows {
		switch {
		case row.Source == "cbr":
			got = append(got, row.Title)
			if row.Title == "ЦБ: опоздавший пресс-релиз" && !row.Date.Before(storedAt) {
				t.Errorf("опоздавшая позиция записана с датой %s, want раньше сохранённого максимума %s",
					row.Date.Format(time.RFC3339), storedAt.Format(time.RFC3339))
			}
		case (row.Source == "interfax" || row.Source == "edisclosure") && strings.HasPrefix(row.URL, "https://t.me/"):
			tgGot = append(tgGot, row.Title)
		default:
			t.Errorf("записан источник %q (%s), want cbr или Telegram (позиции остальных RSS уже покрыты)",
				row.Source, row.URL)
		}
	}
	if len(tgGot) != tgCount {
		t.Errorf("Telegram-позиций в батче = %d, want %d (upsert текущих)", len(tgGot), tgCount)
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
	// cbr_news падает (500), остальные восемь лент — закоммиченные фикстуры.
	pointFeedsAt(t, serveFixtures(t, map[string]int{"cbr_news": http.StatusInternalServerError}))

	conn := newNewsFakeConnWithKeys(nil)
	// Сбой точечно в ключах: чтение версий Telegram до него обязано пройти, иначе тест
	// проверял бы не тот путь.
	conn.keysErr = errors.New("чтение ключей упало")

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

func TestReadTelegramVersionsReadsChannelWindow(t *testing.T) {
	since := time.Date(2026, time.October, 17, 9, 0, 0, 0, time.UTC)
	date := since.Add(30 * time.Minute)
	firstSeen := since.Add(time.Hour)
	conn := newNewsFakeConn()
	conn.versions = map[string][]telegramVersion{
		"interfaxonline": {{
			Channel: "interfaxonline", MessageID: 80244, Version: 2, Date: date,
			Source: "interfax", Title: "Заголовок", URL: "https://t.me/interfaxonline/80244",
			Description: "Текст", ContentHash: contentHash("Заголовок", "Текст"),
			FirstSeen: firstSeen, Deleted: 0,
		}},
	}

	versions, err := readTelegramVersions(context.Background(), conn, "interfaxonline", since)
	if err != nil {
		t.Fatalf("readTelegramVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("versions = %d, want 1", len(versions))
	}
	got := versions[0]
	want := conn.versions["interfaxonline"][0]
	if got != want {
		t.Errorf("версия = %+v, want %+v (порядок Scan — контракт telegramVersionsQuery)", got, want)
	}
	if wantCall := "source=interfaxonline since=" + since.Format(time.RFC3339); !conn.anyCallHas(wantCall) {
		t.Errorf("запрос версий не содержал %q; вызовы: %v", wantCall, conn.callList())
	}
	if !conn.anyCallHas(telegramVersionsQuery) {
		t.Errorf("readTelegramVersions не выполнил запрос %q; вызовы: %v", telegramVersionsQuery, conn.callList())
	}
}

func TestReadTelegramVersionsRowsError(t *testing.T) {
	// Ошибка обхода строк (обрыв соединения на середине) обязана вернуться: молча
	// вернувшаяся половина истории дала бы повторные v1 и лишние tombstone.
	conn := newNewsFakeConn()
	conn.versions = map[string][]telegramVersion{
		"interfaxonline": {{Channel: "interfaxonline", MessageID: 1, Version: 1}},
	}
	conn.rowsErr = errors.New("обрыв соединения на середине")

	_, err := readTelegramVersions(context.Background(), conn, "interfaxonline", time.Now())
	if err == nil {
		t.Fatal("readTelegramVersions при rows.Err() != nil вернул nil-ошибку, want ошибку")
	}
	if !strings.Contains(err.Error(), "обрыв соединения на середине") {
		t.Errorf("ошибка readTelegramVersions = %q, want упоминание причины (rows.Err)", err)
	}
}

func TestImportSinceModeTelegramVersionsAndTombstone(t *testing.T) {
	// Композиция since: RSS уже покрыты, Telegram-текущие пишутся upsert'ом; правка даёт v2,
	// а известное сообщение окна, которого нет в SeenIDs, — tombstone v2. title в news_events
	// остаётся от v1 (ключ date/source/title не разъезжается).
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	tgItems := byKey["interfax_tg"]
	if len(tgItems) < 2 {
		t.Fatalf("Telegram-фикстура = %d сообщений, want ≥ 2 (нужны правка и живое сообщение)", len(tgItems))
	}
	// edisclosure_tg пишется тем же путём, но на пустой истории: он даёт по v1 на
	// сообщение, и его позиции обязаны попасть в те же батчи.
	edisclosureItems := byKey["edisclosure_tg"]
	telegramTotal := len(tgItems) + len(edisclosureItems)
	windowMin, windowMax := telegramFixtureWindow(tgItems)

	edited := tgItems[0]
	const oldTitle, oldText = "Старая первая строка", "Старый текст сообщения"
	tombID := uint64(999001)
	tombDate := windowMin.Add(windowMax.Sub(windowMin) / 2)
	oldFirstSeen := windowMin.Add(-24 * time.Hour)
	// История соответствует прошлому прогону: у каждого текущего сообщения есть v1,
	// кроме отредактированного — у него v1 со старым текстом.
	existing := make([]telegramVersion, 0, len(tgItems)+1)
	for i, it := range tgItems {
		title, text := it.Title, it.Description
		if i == 0 {
			title, text = oldTitle, oldText
		}
		existing = append(existing, telegramVersion{
			Channel: it.Channel, MessageID: it.MessageID, Version: 1, Date: it.Date,
			Source: it.Source, Title: title, URL: it.URL, Description: text,
			ContentHash: contentHash(title, text), FirstSeen: oldFirstSeen, Deleted: 0,
		})
	}
	existing = append(existing, telegramVersion{
		Channel: "interfaxonline", MessageID: tombID, Version: 1, Date: tombDate,
		Source: "interfax", Title: "Удалённая молния", URL: fmt.Sprintf("https://t.me/interfaxonline/%d", tombID),
		Description: "Текст удалённой", ContentHash: contentHash("Удалённая молния", "Текст удалённой"),
		FirstSeen: oldFirstSeen, Deleted: 0,
	})

	// Все RSS-ключи фикстур на месте: в news_events едут только Telegram-текущие.
	rssKeys := make(map[string][]newsKey, 6)
	for _, f := range feeds() {
		if f.Kind == feedRSS {
			rssKeys[f.Source] = append(rssKeys[f.Source], keysOf(byKey[f.Key])...)
		}
	}
	conn := newNewsFakeConnWithKeys(rssKeys)
	conn.versions = map[string][]telegramVersion{"interfaxonline": existing}

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s): %v", modeSince, err)
	}
	if int(count) != telegramTotal {
		t.Errorf("Import вернул %d строк, want %d (Telegram upsert обоих каналов + ничего из покрытого RSS)", count, telegramTotal)
	}

	if wantCall := "source=interfaxonline since=" + windowMin.Format(time.RFC3339); !conn.anyCallHas(wantCall) {
		t.Errorf("чтение версий не содержало %q; вызовы: %v", wantCall, conn.callList())
	}

	newsRows := conn.batchByQuery(t, newsInsert).inserted(t)
	if len(newsRows) != telegramTotal {
		t.Fatalf("батч news_events = %d строк, want %d (Telegram-текущие обоих каналов)", len(newsRows), telegramTotal)
	}
	var editedRow *newsInsertedRow
	for i := range newsRows {
		if newsRows[i].URL == edited.URL {
			editedRow = &newsRows[i]
		}
	}
	if editedRow == nil {
		t.Fatalf("в батче нет строки отредактированного сообщения %s", edited.URL)
	}
	if editedRow.Title != oldTitle {
		t.Errorf("title отредактированной позиции = %q, want %q (title v1 — стабильный ключ news_events)",
			editedRow.Title, oldTitle)
	}
	if editedRow.Description != edited.Description || editedRow.ContentHash != edited.ContentHash {
		t.Errorf("отредактированная позиция записана не текущим текстом: %+v, want description/hash из %+v",
			editedRow, edited)
	}

	verRows := conn.batchByQuery(t, telegramVersionInsert).insertedVersions(t)
	wantVersions := 2 + len(edisclosureItems) // правка v2 + tombstone v2 + v1 каждого edisclosure
	if len(verRows) != wantVersions {
		t.Fatalf("батч версий = %d строк, want %d (правка v2, tombstone v2 и v1 edisclosure): %+v",
			len(verRows), wantVersions, verRows)
	}
	editVer := findVersion(t, verRows, edited.MessageID, 2)
	if editVer.Deleted != 0 || editVer.ContentHash != edited.ContentHash || editVer.Title != edited.Title {
		t.Errorf("версия правки = %+v, want v2 текущего текста %+v", editVer, edited)
	}
	if !editVer.Date.Equal(edited.Date) {
		t.Errorf("версия правки: date = %v, want дату публикации %v", editVer.Date, edited.Date)
	}
	tomb := findVersion(t, verRows, tombID, 2)
	if tomb.Deleted != 1 || tomb.Description != "" || tomb.ContentHash != "" {
		t.Errorf("tombstone = %+v, want deleted=1 с пустыми description/content_hash", tomb)
	}
	if tomb.Title != "Удалённая молния" || !tomb.Date.Equal(tombDate) || tomb.URL == "" {
		t.Errorf("tombstone = %+v, want title/date/url из последней живой версии", tomb)
	}
	if tomb.FirstSeen.IsZero() {
		t.Error("tombstone: first_seen нулевой, want время прогона")
	}
}

func TestImportTelegramVersionReadErrorWritesRSSOnly(t *testing.T) {
	// Ошибка чтения версий изолируется Telegram-лентой: без истории diffTelegram принял бы
	// её сообщения за v1 и затёр нумерацию, поэтому Telegram-позиции этого прогона не
	// пишутся, а семь RSS-лент пишутся как обычно. Прогон отчитывается ошибкой (dagu
	// краснеет), но уже собранные данные не теряет.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	wantRSS := 0
	for _, f := range feeds() {
		if f.Kind == feedRSS {
			wantRSS += len(byKey[f.Key])
		}
	}

	conn := newNewsFakeConn()
	conn.versionsErr = errors.New("чтение версий упало")

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при ошибке чтения версий вернул %d строк без ошибки, want ошибку", modeSince, count)
	}
	if !strings.Contains(err.Error(), "чтение версий упало") {
		t.Errorf("ошибка Import = %q, want упоминание причины", err)
	}
	if int(count) != wantRSS {
		t.Errorf("Import вернул %d строк, want %d (RSS пишутся, Telegram-лента пропущена)", count, wantRSS)
	}
	newsBatch := conn.batchByQuery(t, newsInsert)
	if !newsBatch.sent {
		t.Fatal("батч news_events не отправлен: сбой версий Telegram не должен отменять RSS")
	}
	rows := newsBatch.inserted(t)
	if len(rows) != wantRSS {
		t.Errorf("в батче news_events %d строк, want %d (только RSS)", len(rows), wantRSS)
	}
	for _, row := range rows {
		if strings.HasPrefix(row.URL, "https://t.me/") {
			t.Errorf("в батч попала Telegram-позиция %q при сбое чтения версий", row.URL)
		}
	}
	for _, batch := range conn.batchList() {
		if batch.query == telegramVersionInsert {
			t.Error("батч версий открыт при сбое чтения истории: версии нельзя писать вслепую")
		}
	}
	if !conn.anyCallHas(telegramVersionsQuery) {
		t.Errorf("Import не обратился к версиям запросом %q; вызовы: %v", telegramVersionsQuery, conn.callList())
	}
}

func TestImportTelegramVersionWriteErrorKeepsCurrentRows(t *testing.T) {
	// Сбой второго батча (версий) не отменяет уже записанный news_events: текущий текст
	// остаётся в таблице, count — строки news_events, а ошибка видна в dagu; повтор
	// идемпотентен по ключу ReplacingMergeTree.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	wantRows := totalItems(byKey) // since без ключей пишет всё окно: RSS + Telegram

	conn := newNewsFakeConn()
	conn.sendErr = map[string]error{telegramVersionInsert: errors.New("запись версий упала")}

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при сбое записи версий вернул %d строк без ошибки, want ошибку", modeSince, count)
	}
	if !strings.Contains(err.Error(), "запись версий упала") {
		t.Errorf("ошибка Import = %q, want упоминание причины", err)
	}
	if int(count) != wantRows {
		t.Errorf("Import вернул %d строк, want %d (news_events уже записаны до сбоя версий)", count, wantRows)
	}
	newsBatch := conn.batchByQuery(t, newsInsert)
	if !newsBatch.sent {
		t.Error("батч news_events не отправлен: сбой версий не должен отменять текущие строки")
	}
	if len(newsBatch.inserted(t)) != wantRows {
		t.Errorf("в батче news_events %d строк, want %d", len(newsBatch.inserted(t)), wantRows)
	}
	if versionBatch := conn.batchByQuery(t, telegramVersionInsert); versionBatch.sent {
		t.Error("батч версий помечен отправленным, хотя Send вернул ошибку")
	}
}

func TestImportTelegramFeedFailureNoTombstone(t *testing.T) {
	// Сбойные Telegram-ленты не доходят до diffTelegram: у них SeenIDs == nil, и трактовать
	// это как пустое окно (tombstone всего канала) нельзя. Версии обоих каналов в этом
	// прогоне не читаются и не пишутся, хотя история interfaxonline в фейке есть.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, map[string]int{
		"interfax_tg":    http.StatusInternalServerError,
		"edisclosure_tg": http.StatusInternalServerError,
	}))

	byKey, _ := fixtureInventory(t)
	rssKeys := make(map[string][]newsKey, 6)
	for _, f := range feeds() {
		if f.Kind == feedRSS {
			rssKeys[f.Source] = append(rssKeys[f.Source], keysOf(byKey[f.Key])...)
		}
	}
	tgItems := byKey["interfax_tg"]
	windowMin, windowMax := telegramFixtureWindow(tgItems)
	conn := newNewsFakeConnWithKeys(rssKeys)
	conn.versions = map[string][]telegramVersion{
		"interfaxonline": {{
			Channel: "interfaxonline", MessageID: tgItems[0].MessageID, Version: 1,
			Date: windowMin.Add(windowMax.Sub(windowMin) / 2), Source: "interfax",
			Title: tgItems[0].Title, URL: tgItems[0].URL,
			Description: tgItems[0].Description, ContentHash: tgItems[0].ContentHash,
			FirstSeen: windowMin, Deleted: 0,
		}},
	}

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при сбое Telegram-лент вернул %d строк без ошибки, want ошибку ленты", modeSince, count)
	}
	for _, key := range []string{"interfax_tg", "edisclosure_tg"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("ошибка Import = %q, want ключ упавшей ленты %s", err, key)
		}
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (RSS покрыт, Telegram упал, tombstone нет)", count)
	}
	if conn.anyCallHas(telegramVersionsQuery) {
		t.Errorf("сбойные ленты дошли до чтения версий: SeenIDs == nil нельзя трактовать как пустое окно; вызовы: %v",
			conn.callList())
	}
	if got := conn.batchList(); len(got) != 0 {
		t.Errorf("Import открыл %d батчей при сбое Telegram и покрытом RSS, want 0", len(got))
	}
}

func TestImportOneTelegramFeedFailureIsolated(t *testing.T) {
	// Сбой одной Telegram-ленты не должен задевать вторую: здоровая лента пишет текущие
	// сообщения и свои версии, упавшая — не читает историю и не даёт tombstone. Оба
	// направления проверяются отдельно: падение interfax_tg при живом edisclosure_tg
	// и зеркально.
	for _, tc := range []struct {
		name    string
		failed  string
		healthy string
		channel string
		history bool
	}{
		{name: "interfax_tg down", failed: "interfax_tg", healthy: "edisclosure_tg", channel: "edisclosure"},
		{name: "edisclosure_tg down", failed: "edisclosure_tg", healthy: "interfax_tg", channel: "interfaxonline", history: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(modeEnv, modeSince)
			pointFeedsAt(t, serveFixtures(t, map[string]int{tc.failed: http.StatusInternalServerError}))

			byKey, _ := fixtureInventory(t)
			rssKeys := make(map[string][]newsKey, 6)
			for _, f := range feeds() {
				if f.Kind == feedRSS {
					rssKeys[f.Source] = append(rssKeys[f.Source], keysOf(byKey[f.Key])...)
				}
			}
			conn := newNewsFakeConnWithKeys(rssKeys)
			if tc.history {
				tgItems := byKey["interfax_tg"]
				windowMin, windowMax := telegramFixtureWindow(tgItems)
				conn.versions = map[string][]telegramVersion{
					"interfaxonline": {{
						Channel: "interfaxonline", MessageID: tgItems[0].MessageID, Version: 1,
						Date: windowMin.Add(windowMax.Sub(windowMin) / 2), Source: "interfax",
						Title: tgItems[0].Title, URL: tgItems[0].URL,
						Description: tgItems[0].Description, ContentHash: tgItems[0].ContentHash,
						FirstSeen: windowMin, Deleted: 0,
					}},
				}
			}

			var s newsImport
			count, err := s.Import(context.Background(), conn)
			if err == nil || !strings.Contains(err.Error(), tc.failed) {
				t.Fatalf("Import(%s) при сбое %s: count=%d err=%v, want ошибку с ключом упавшей ленты",
					modeSince, tc.failed, count, err)
			}
			if strings.Contains(err.Error(), tc.healthy) {
				t.Errorf("ошибка Import = %q содержит ключ здоровой ленты %s: сбой одной ленты задел другую",
					err, tc.healthy)
			}
			if want := len(byKey[tc.healthy]); int(count) != want {
				t.Errorf("Import вернул %d строк, want %d (текущие позиции здоровой ленты %s)", count, want, tc.healthy)
			}
			if !conn.anyCallHas("source=" + tc.channel) {
				t.Errorf("версии здоровой ленты %s не читались; вызовы: %v", tc.channel, conn.callList())
			}
			versionBatch := conn.batchByQuery(t, telegramVersionInsert)
			versions := versionBatch.insertedVersions(t)
			if len(versions) == 0 {
				t.Fatalf("батч версий здоровой ленты %s пуст", tc.channel)
			}
			for _, v := range versions {
				if v.Channel != tc.channel {
					t.Errorf("в батче версий строка чужого канала %q, want только %q", v.Channel, tc.channel)
				}
				if v.Deleted != 0 {
					t.Errorf("здоровая лента %s дала tombstone %+v при сбое другой ленты", tc.channel, v)
				}
			}
		})
	}
}

func TestImportSinceModeAllFeedsFailed(t *testing.T) {
	// Все девять лент упали: items пуст, и в since до чтения ключей дело не доходит — ошибка
	// собирается из отказов лент (ключ каждой), строк не пишется.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, map[string]int{
		"interfax":         http.StatusInternalServerError,
		"interfax_tg":      http.StatusInternalServerError,
		"edisclosure_tg":   http.StatusInternalServerError,
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
	for _, key := range []string{"interfax", "interfax_tg", "edisclosure_tg", "rbc", "cbr_news", "cbr_press", "tass", "smartlab_lebedev", "smartlab_spydell"} {
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
// а остальные семь — закоммиченные фикстуры: тест держит под контролем окно именно ЦБ, не
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

// --- композиция LLM-разметки в Import() ---

// setNewsLLMEnv выставляет конфигурацию разметки на время теста: значения фейковые,
// ключ — test-key; t.Setenv возвращает исходное (пустое) состояние после теста.
func setNewsLLMEnv(t *testing.T, baseURL, model string) {
	t.Helper()
	t.Setenv(newsLLMBaseURLEnv, baseURL)
	t.Setenv(newsLLMAPIKeyEnv, "test-key")
	t.Setenv(newsLLMModelEnv, model)
}

// clearNewsLLMEnv выключает LLM-шаг: пустая переменная — основание для деградации.
func clearNewsLLMEnv(t *testing.T) {
	t.Helper()
	t.Setenv(newsLLMProfileEnv, "")
	t.Setenv(newsLLMBaseURLEnv, "")
	t.Setenv(newsLLMAPIKeyEnv, "")
	t.Setenv(newsLLMModelEnv, "")
	t.Setenv(newsLLMGLMBaseURLEnv, "")
	t.Setenv(newsLLMGLMAPIKeyEnv, "")
	t.Setenv(newsLLMGLMModelEnv, "")
}

// newsTestCandidate — кандидат разметки в том виде, в каком его отдаёт
// newsAnnotationCandidatesQuery: Telegram-сообщение с непустым content_hash.
func newsTestCandidate(n int) newsItem {
	const title = "Трамп объявил о пошлинах"
	const text = "Трамп объявил о новых пошлинах на импорт из Китая."
	return newsItem{
		Date:        time.Date(2026, time.October, 17, 9, n, 0, 0, time.UTC),
		Source:      "interfax",
		Title:       title,
		URL:         fmt.Sprintf("https://t.me/interfaxonline/%d", 80000+n),
		Description: text,
		ContentHash: contentHash(title, text),
	}
}

// newsFakeBatchByQuery ищет единственный батч по запросу, но, в отличие от batchByQuery,
// не валит тест при отсутствии: сценарии «батча быть не должно» проверяют именно это.
func newsFakeBatchByQuery(conn *newsFakeConn, query string) *newsFakeBatch {
	for _, batch := range conn.batchList() {
		if batch.query == query {
			return batch
		}
	}
	return nil
}

// TestImportWithoutLLMEnvSkipsAnnotation — без LLM_BASE_URL/LLM_API_KEY/NEWS_LLM_MODEL шаг
// разметки пропускается: кандидаты не читаются, батч news_annotation не открывается,
// а импорт остаётся зелёным (деградация, догон следующим прогоном).
func TestImportWithoutLLMEnvSkipsAnnotation(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	clearNewsLLMEnv(t)
	pointFeedsAt(t, serveFixtures(t, nil))

	conn := newNewsFakeConn()
	conn.candidates = []newsItem{newsTestCandidate(1)}

	var s newsImport
	if _, err := s.Import(context.Background(), conn); err != nil {
		t.Fatalf("Import(%s) без LLM env: %v (деградация обязана быть зелёной)", modeSince, err)
	}
	if conn.anyCallHasPrefix(newsAnnotationCandidatesQueryPrefix) {
		t.Errorf("без LLM env прочитаны кандидаты разметки; вызовы: %v", conn.callList())
	}
	if batch := newsFakeBatchByQuery(conn, newsAnnotationInsert); batch != nil {
		t.Errorf("без LLM env открыт батч %s: %+v", newsAnnotationTable, batch.rows)
	}
}

// TestImportWithoutLLMEnvWarns — отсутствие env не молчит: в лог уходит один warn с именами
// переменных, иначе отсутствие разметки выглядело бы тихим дефектом. Уровень логирования
// поднимается на время теста и возвращается к Panic из TestMain.
func TestImportWithoutLLMEnvWarns(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	clearNewsLLMEnv(t)
	pointFeedsAt(t, serveFixtures(t, nil))

	prevLevel := log.GetLevel()
	hook := logtest.NewGlobal()
	log.SetLevel(log.WarnLevel)
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		hook.Reset()
		log.SetOutput(os.Stderr)
		log.SetLevel(prevLevel)
	})

	var s newsImport
	if _, err := s.Import(context.Background(), newNewsFakeConn()); err != nil {
		t.Fatalf("Import(%s) без LLM env: %v (деградация обязана быть зелёной)", modeSince, err)
	}
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "LLM-разметка пропущена") &&
			strings.Contains(entry.Message, "NEWS_LLM_MODEL") {
			return
		}
	}
	t.Errorf("в логе нет warn об отсутствии LLM env; записи: %+v", hook.AllEntries())
}

// TestImportAnnotatesBacklogWithFakeLLM — с фейковым провайдером LLM-шаг читает кандидатов,
// размечает их батчем и пишет doc+tag строки с model/prompt_version прогона.
func TestImportAnnotatesBacklogWithFakeLLM(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	candidates := []newsItem{newsTestCandidate(1), newsTestCandidate(2)}
	payload := newsTestPayloadBody(t,
		newsTestValidMessage(candidates[0].URL),
		newsTestValidMessage(candidates[1].URL))
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestContentBody(t, payload)})
	setNewsLLMEnv(t, mock.url, "test-model")

	conn := newNewsFakeConn()
	conn.candidates = candidates

	var s newsImport
	if _, err := s.Import(context.Background(), conn); err != nil {
		t.Fatalf("Import(%s) с фейковым LLM: %v", modeSince, err)
	}
	if !conn.anyCallHasPrefix(newsAnnotationCandidatesQueryPrefix) {
		t.Errorf("LLM-шаг не прочитал кандидатов запросом с префиксом %q; вызовы: %v",
			newsAnnotationCandidatesQueryPrefix, conn.callList())
	}
	if want := fmt.Sprintf("model=test-model prompt_version=%d", newsPromptVersion); !conn.anyCallHas(want) {
		t.Errorf("запрос кандидатов не содержал %q (model и prompt_version — часть ключа); вызовы: %v",
			want, conn.callList())
	}

	batch := newsFakeBatchByQuery(conn, newsAnnotationInsert)
	if batch == nil {
		t.Fatalf("батч %s не открыт; вызовы: %v", newsAnnotationTable, conn.callList())
	}
	if !batch.sent {
		t.Error("Send батча разметки не вызван: строки не уехали в таблицу")
	}
	rows := batch.insertedAnnotations(t)
	docs := newsAnnotationKindRows(rows, newsKindDoc)
	tags := newsAnnotationKindRows(rows, newsKindTag)
	if len(docs) != len(candidates) {
		t.Errorf("doc-строк %d, want %d (по одной на кандидата)", len(docs), len(candidates))
	}
	if len(tags) != len(candidates) {
		t.Errorf("tag-строк %d, want %d (по тегу на кандидата)", len(tags), len(candidates))
	}
	byURL := make(map[string]newsAnnotationRow, len(docs))
	for _, row := range rows {
		if row.Model != "test-model" || row.PromptVersion != newsPromptVersion || row.Method != newsMethodLLM {
			t.Errorf("строка разметки %+v: model/prompt_version/method не из прогона", row)
		}
		if row.AnnotatedAt.IsZero() {
			t.Errorf("строка разметки %+v: annotated_at нулевой", row)
		}
		if row.Kind == newsKindDoc {
			byURL[row.URL] = row
		}
	}
	for _, candidate := range candidates {
		if _, ok := byURL[candidate.URL]; !ok {
			t.Errorf("нет doc-строки для кандидата %s", candidate.URL)
		}
	}
}

// TestImportLLMProviderErrorKeepsMessages — ошибка провайдера не отменяет запись
// news_events и версий: LLM-шаг лишь предупреждает, импорт остаётся зелёным, а батч
// разметки не открывается; догон — следующим прогоном.
func TestImportLLMProviderErrorKeepsMessages(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	mock := newNewsTestLLMMock(t, newsTestLLMResponse{
		Status: http.StatusInternalServerError,
		Body:   `{"error":{"type":"api_error","message":"провайдер упал"}}`,
	})
	setNewsLLMEnv(t, mock.url, "test-model")

	conn := newNewsFakeConn()
	conn.candidates = []newsItem{newsTestCandidate(1)}

	byKey, _ := fixtureInventory(t)
	wantRows := totalItems(byKey)

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s) при отказе LLM: %v (ошибка провайдера — warn, не ошибка импорта)", modeSince, err)
	}
	if int(count) != wantRows {
		t.Errorf("Import вернул %d строк, want %d (сообщения обязаны записаться)", count, wantRows)
	}
	if batch := conn.batchByQuery(t, newsInsert); !batch.sent {
		t.Error("батч news_events не отправлен: отказ LLM не должен отменять запись сообщений")
	}
	if batch := conn.batchByQuery(t, telegramVersionInsert); !batch.sent {
		t.Error("батч версий не отправлен: отказ LLM не должен отменять запись версий")
	}
	if batch := newsFakeBatchByQuery(conn, newsAnnotationInsert); batch != nil {
		t.Errorf("при отказе провайдера открыт батч разметки: %+v", batch.rows)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов к LLM %d, want 1 (отказ не повторяется)", mock.calls())
	}
}

// TestImportAnnotationWriteErrorIsWarning — сбой записи news_annotation не делает Import
// красным и не отменяет уже записанные сообщения: dagu не должен ретраить весь прогон
// из-за разметки, догон — следующим прогоном.
func TestImportAnnotationWriteErrorIsWarning(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	candidate := newsTestCandidate(1)
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{
		Body: newsTestContentBody(t, newsTestPayloadBody(t, newsTestValidMessage(candidate.URL))),
	})
	setNewsLLMEnv(t, mock.url, "test-model")

	conn := newNewsFakeConn()
	conn.candidates = []newsItem{candidate}
	conn.sendErr = map[string]error{newsAnnotationInsert: errors.New("запись разметки упала")}

	var s newsImport
	if _, err := s.Import(context.Background(), conn); err != nil {
		t.Fatalf("Import(%s) при сбое записи разметки: %v (сбой разметки — warn, не ошибка импорта)", modeSince, err)
	}
	if batch := conn.batchByQuery(t, newsInsert); !batch.sent {
		t.Error("батч news_events не отправлен: сбой разметки не должен отменять сообщения")
	}
	batch := newsFakeBatchByQuery(conn, newsAnnotationInsert)
	if batch == nil {
		t.Fatal("батч разметки не открыт")
	}
	if batch.sent {
		t.Error("батч разметки помечен отправленным, хотя Send вернул ошибку")
	}
}

// TestImportNoopRunAnnotatesBacklog — бэклог разметки догоняется и тогда, когда писать в
// news_events нечего: в since покрытое RSS и упавшие Telegram-ленты дают len(items)==0,
// но LLM-шаг стоит до выхода из прогона и обязан разметить накопленное.
func TestImportNoopRunAnnotatesBacklog(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, map[string]int{
		"interfax_tg":    http.StatusInternalServerError,
		"edisclosure_tg": http.StatusInternalServerError,
	}))

	candidate := newsTestCandidate(1)
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{
		Body: newsTestContentBody(t, newsTestPayloadBody(t, newsTestValidMessage(candidate.URL))),
	})
	setNewsLLMEnv(t, mock.url, "test-model")

	byKey, _ := fixtureInventory(t)
	rssKeys := make(map[string][]newsKey, 6)
	for _, f := range feeds() {
		if f.Kind == feedRSS {
			rssKeys[f.Source] = append(rssKeys[f.Source], keysOf(byKey[f.Key])...)
		}
	}
	conn := newNewsFakeConnWithKeys(rssKeys)
	conn.candidates = []newsItem{candidate}

	var s newsImport
	count, err := s.Import(context.Background(), conn)
	if err == nil {
		t.Fatalf("Import(%s) при сбое Telegram-лент вернул %d строк без ошибки, want ошибку ленты", modeSince, count)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (RSS покрыт, Telegram-ленты упали)", count)
	}
	if conn.anyCallHas(newsInsert) {
		t.Errorf("no-op-прогон открыл батч news_events; вызовы: %v", conn.callList())
	}
	batch := newsFakeBatchByQuery(conn, newsAnnotationInsert)
	if batch == nil || !batch.sent {
		t.Fatalf("бэклог не размечен при len(items)==0: батч %v; вызовы: %v", batch, conn.callList())
	}
	if docs := newsAnnotationKindRows(batch.insertedAnnotations(t), newsKindDoc); len(docs) != 1 {
		t.Errorf("doc-строк %d, want 1 (бэклог догоняется на no-op-прогоне)", len(docs))
	}
}

// TestImportCheckModeDoesNotCallLLM — check — режим чтения покрытия: LLM не вызывается
// даже при заполненном env, иначе диагностика ходила бы к провайдеру и тратила деньги.
func TestImportCheckModeDoesNotCallLLM(t *testing.T) {
	t.Setenv(modeEnv, modeCheck)
	pointFeedsAt(t, serveFixtures(t, nil))

	mock := newNewsTestLLMMock(t)
	setNewsLLMEnv(t, mock.url, "test-model")

	conn := newNewsFakeConn()
	var s newsImport
	if _, err := s.Import(context.Background(), conn); err != nil {
		t.Fatalf("Import(%s): %v", modeCheck, err)
	}
	if conn.anyCallHasPrefix(newsAnnotationCandidatesQueryPrefix) {
		t.Errorf("check прочитал кандидатов разметки; вызовы: %v", conn.callList())
	}
	if mock.calls() != 0 {
		t.Errorf("check обратился к LLM %d раз, want 0", mock.calls())
	}
	if batch := newsFakeBatchByQuery(conn, newsAnnotationInsert); batch != nil {
		t.Errorf("check открыл батч разметки: %+v", batch.rows)
	}
}

// TestNewsAnnotationCandidatesQueryCoversRSS — кандидаты разметки — весь видимый поток:
// Telegram не сужен (url LIKE), RSS проходит тот же newsFilter, что и v_news_events, а окно
// 35 дней снято осознанно — стоимость ограничивает LIMIT бэклога, а не возраст позиции.
func TestNewsAnnotationCandidatesQueryCoversRSS(t *testing.T) {
	query := newsAnnotationCandidatesQuery(50)
	for _, want := range []string{
		"match(title, ?)",
		"match(description, ?)",
		"url LIKE 'https://t.me/%'",
		"content_hash != ''",
		"LIMIT 50",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("кандидатный запрос не содержит %q:\n%s", want, query)
		}
	}
	// Ровно четыре ? — filter, filter, model, prompt_version. newsFilter литералом дал бы
	// ещё два ? внутри (?i), и драйвер ClickHouse ответил бы «have no arg for param ?».
	// Счётчик ловит именно этот класс дефекта: живой прогон его вскрыл, фейк — нет.
	if got := strings.Count(query, "?"); got != 4 {
		t.Errorf("в кандидатном запросе %d плейсхолдеров ?, want 4 (filter, filter, model, prompt_version):\n%s",
			got, query)
	}
	if strings.Contains(query, newsFilter) {
		t.Errorf("кандидатный запрос содержит newsFilter литералом: (?i) даст драйверу лишние ?:\n%s", query)
	}
	if strings.Contains(query, "INTERVAL 35 DAY") {
		t.Errorf("кандидатный запрос сужен окном 35 дней, want весь видимый поток:\n%s", query)
	}
	// LIMIT — аргумент функции, а не константа: бэкфилл через NEWS_LLM_BACKLOG_LIMIT
	// обязан менять запрос, иначе переменная была бы декоративной.
	if other := newsAnnotationCandidatesQuery(123); !strings.Contains(other, "LIMIT 123") {
		t.Errorf("newsAnnotationCandidatesQuery(123) не содержит LIMIT 123:\n%s", other)
	}
}

// TestReadNewsAnnotationCandidatesBindsFilter — readNewsAnnotationCandidates передаёт в SQL
// четыре аргумента в порядке `?`: filter, filter, model, prompt_version. Фейковое соединение
// SQL не разбирает, поэтому порядок проверяется по записанным candidateArgs: именно
// расхождение числа ? и аргументов дало «have no arg for param ?» на живой базе.
func TestReadNewsAnnotationCandidatesBindsFilter(t *testing.T) {
	conn := newNewsFakeConn()
	conn.candidates = []newsItem{newsTestCandidate(1)}

	candidates, err := readNewsAnnotationCandidates(context.Background(), conn, "test-model", newsPromptVersion, 50)
	if err != nil {
		t.Fatalf("readNewsAnnotationCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("кандидатов %d, want 1", len(candidates))
	}
	// prompt_version — uint16: без явного приведения untyped-константа стала бы int,
	// и сравнение с аргументом драйвера падало бы по типу, а не по значению.
	want := []any{newsFilter, newsFilter, "test-model", uint16(newsPromptVersion)}
	args := conn.candidateArgsList()
	if len(args) != len(want) {
		t.Fatalf("аргументов запроса %d, want %d (filter, filter, model, prompt_version): %v",
			len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("аргумент %d = %#v, want %#v (порядок — порядок ? в SQL)", i, args[i], want[i])
		}
	}
}

// TestImportUsesBacklogLimit — NEWS_LLM_BACKLOG_LIMIT доезжает до SQL кандидатов: переменная
// читается до запроса, а LIMIT вшивается в текст. Без этой проверки опечатка в имени
// переменной оставила бы бэкфилл на штатных 50 позициях, и заметить это было бы нечем.
func TestImportUsesBacklogLimit(t *testing.T) {
	t.Setenv(modeEnv, modeSince)
	t.Setenv(newsLLMBacklogLimitEnv, "123")
	pointFeedsAt(t, serveFixtures(t, nil))

	candidate := newsTestCandidate(1)
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{
		Body: newsTestContentBody(t, newsTestPayloadBody(t, newsTestValidMessage(candidate.URL))),
	})
	setNewsLLMEnv(t, mock.url, "test-model")

	conn := newNewsFakeConn()
	conn.candidates = []newsItem{candidate}

	var s newsImport
	if _, err := s.Import(context.Background(), conn); err != nil {
		t.Fatalf("Import(%s) с %s=123: %v", modeSince, newsLLMBacklogLimitEnv, err)
	}
	found := false
	for _, call := range conn.callList() {
		if !strings.HasPrefix(call, "query: "+newsAnnotationCandidatesQueryPrefix) {
			continue
		}
		found = true
		if !strings.Contains(call, "LIMIT 123") {
			t.Errorf("кандидатный запрос не содержит LIMIT 123: %s", call)
		}
	}
	if !found {
		t.Errorf("LLM-шаг не прочитал кандидатов запросом с префиксом %q; вызовы: %v",
			newsAnnotationCandidatesQueryPrefix, conn.callList())
	}
}
