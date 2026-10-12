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

	// Подмена пакетных URL — как в TestFetchFeedsUsesFixturesAndIsolatesFailure:
	// feeds() читает их при каждом вызове, поэтому подменённый адрес подхватится.
	oldInterfax, oldRBC, oldCBRNews, oldCBRPress, oldTASS :=
		feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL
	feedInterfaxURL = srv.URL + "/interfax"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	t.Cleanup(func() {
		feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL =
			oldInterfax, oldRBC, oldCBRNews, oldCBRPress, oldTASS
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
		// Пустое значение — дефолт: расписание dagu ставит NEWS_WATCH_MODE=all,
		// но ручной запуск без переменной обязан работать так же, а не падать.
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

// serveFixtures поднимает httptest-сервер, отдающий закоммиченные фикстуры лент
// по путям /<Key>. Настоящую сеть тест не трогает: прогон воспроизводим офлайн.
func serveFixtures(t *testing.T, fail map[string]int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		if code, ok := fail[key]; ok {
			http.Error(w, "лента недоступна", code)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", key+".rss"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchFeedsUsesFixturesAndIsolatesFailure(t *testing.T) {
	// Сбойная одна лента: остальные четыре обязаны прочитаться. Иначе падение
	// любого источника глушило бы весь прогон, а dagu-ретрай повторял бы четыре
	// здоровые ленты ради одной больной.
	srv := serveFixtures(t, map[string]int{"cbr_news": http.StatusInternalServerError})

	// feeds() собирает конфиг из пакетных переменных — подменяем их на адреса
	// httptest и возвращаем исходные значения после теста, иначе подмена утечёт
	// в другие тесты пакета.
	oldInterfax, oldRBC, oldCBRNews, oldCBRPress, oldTASS :=
		feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL
	feedInterfaxURL = srv.URL + "/interfax"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	t.Cleanup(func() {
		feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL =
			oldInterfax, oldRBC, oldCBRNews, oldCBRPress, oldTASS
	})

	results := fetchFeeds(feeds())
	if len(results) != 5 {
		t.Fatalf("fetchFeeds вернул %d результатов, want 5 (по числу лент)", len(results))
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
	}

	if want := "interfax,rbc,cbr_news,cbr_press,tass"; strings.Join(keys, ",") != want {
		t.Errorf("порядок лент = %s, want %s (фиксированный, детерминированный лог)", strings.Join(keys, ","), want)
	}
	// Оба фида ЦБ пишутся под одним source=cbr: витрина различает их url, а не меткой.
	if want := "interfax,rbc,cbr,cbr,tass"; strings.Join(sources, ",") != want {
		t.Errorf("метки источников = %s, want %s", strings.Join(sources, ","), want)
	}
}

func TestFeedsHaveFixtures(t *testing.T) {
	configured := feeds()
	if len(configured) != 5 {
		t.Fatalf("feeds() = %d лент, want 5", len(configured))
	}
	keyOf := make(map[string]bool, len(configured))
	for _, f := range configured {
		keyOf[f.Key] = true

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
	}

	// Обратная проверка: фикстура без ленты — мёртвый снимок, который никто
	// не читает; он переживёт смену конфига и будет вводить в заблуждение.
	files, err := filepath.Glob(filepath.Join("testdata", "*.rss"))
	if err != nil {
		t.Fatalf("glob testdata/*.rss: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("в testdata нет ни одной фикстуры *.rss")
	}
	for _, path := range files {
		key := strings.TrimSuffix(filepath.Base(path), ".rss")
		if !keyOf[key] {
			t.Errorf("фикстура %s: ленты с Key=%q нет в feeds()", path, key)
		}
	}
}

func TestSelectSinceFiltersStrictlyAfterCursor(t *testing.T) {
	msk := time.FixedZone("", mskOffset) // фиксированная зона: результат не зависит от TZ хоста
	cursor := time.Date(2026, time.October, 12, 12, 0, 0, 0, msk)

	items := []newsItem{
		{Date: cursor.Add(-time.Minute), Source: "interfax", Title: "11:59"},
		{Date: cursor, Source: "interfax", Title: "12:00 — ровно на курсоре"},
		// Тот же момент, что и курсор, но записанный в UTC: сравнение идёт по моменту,
		// а не по настенным часам зоны записи.
		{Date: cursor.UTC(), Source: "interfax", Title: "12:00 по МСК, записанное в UTC"},
		{Date: cursor.Add(time.Minute), Source: "interfax", Title: "12:01"},
		{Date: cursor.Add(-time.Hour), Source: "tass", Title: "чужой источник без курсора"},
	}

	got := selectSince(items, map[string]time.Time{"interfax": cursor})
	var titles []string
	for _, it := range got {
		titles = append(titles, it.Title)
	}
	// Курсор строгий: позиция ровно на нём уже сохранена (курсор — max(date) из
	// news_events), и повторный проход по границе писал бы то же событие заново.
	if want := "12:01,чужой источник без курсора"; strings.Join(titles, ",") != want {
		t.Errorf("selectSince по курсору 12:00 = %q, want %q", strings.Join(titles, ","), want)
	}

	// Нет курсора (первый прогон источника, нулевое время) — проходят все позиции.
	if all := selectSince(items, nil); len(all) != len(items) {
		t.Errorf("selectSince без курсоров = %d позиций, want %d (первый прогон берёт всё окно)", len(all), len(items))
	}
	zero := selectSince(items, map[string]time.Time{"interfax": {}, "tass": {}})
	if len(zero) != len(items) {
		t.Errorf("selectSince с нулевыми курсорами = %d позиций, want %d", len(zero), len(items))
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

// pointFeedsAt заменяет URL-ы всех пяти лент на пути httptest-сервера и возвращает
// исходные значения после теста: feeds() читает пакетные переменные при каждом вызове,
// поэтому подмена подхватывается, а t.Cleanup не даёт ей утечь в соседние тесты.
func pointFeedsAt(t *testing.T, srv *httptest.Server) {
	t.Helper()
	old := [5]string{feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL}
	feedInterfaxURL = srv.URL + "/interfax"
	feedRBCURL = srv.URL + "/rbc"
	feedCBRNewsURL = srv.URL + "/cbr_news"
	feedCBRPressURL = srv.URL + "/cbr_press"
	feedTASSURL = srv.URL + "/tass"
	t.Cleanup(func() {
		feedInterfaxURL, feedRBCURL, feedCBRNewsURL, feedCBRPressURL, feedTASSURL =
			old[0], old[1], old[2], old[3], old[4]
	})
}

// fixtureItems читает закоммиченную фикстуру ленты тем же путём, что и прогон
// (readFixtureDoc + itemsFromDoc): ожидания тестов считаются из фикстур, а не
// хардкодятся — обновление снимка лент не должно требовать правки чисел в тестах.
func fixtureItems(t *testing.T, key, source string) []newsItem {
	t.Helper()
	items, _, err := itemsFromDoc(source, readFixtureDoc(t, key+".rss"))
	if err != nil {
		t.Fatalf("itemsFromDoc(%s) на фикстуре %s.rss: %v", source, key, err)
	}
	return items
}

// fixtureInventory — позиции фикстур по ключам лент и максимум даты по источникам.
// Максимумы — это то, что лежит в news_events после успешного прогона all: оба фида
// ЦБ складываются в один источник cbr, как и в feedSpans.
func fixtureInventory(t *testing.T) (byKey map[string][]newsItem, maxBySource map[string]time.Time) {
	t.Helper()
	byKey = make(map[string][]newsItem, 5)
	maxBySource = make(map[string]time.Time, 4)
	for _, f := range feeds() {
		items := fixtureItems(t, f.Key, f.Source)
		byKey[f.Key] = items
		for _, it := range items {
			if it.Date.After(maxBySource[f.Source]) {
				maxBySource[f.Source] = it.Date
			}
		}
	}
	return byKey, maxBySource
}

// spansFromMaxima превращает максимумы источников в строки storedSpansQuery — тот вид,
// в каком news_events FINAL отвечает после успешного прогона. Порядок источников
// отсортирован: обход map случаен, а детерминированный вход упрощает разбор падения.
// Min отставлен от Max на час: курсор since берётся из Max, и такая пара делает
// перепутанные min/max в readStoredSpans видимыми (с Min вместо Max окно курсора
// расширилось бы, и выборка «новых позиций нет» перестала бы быть пустой).
func spansFromMaxima(maxima map[string]time.Time) []newsStoredSpan {
	srcs := make([]string, 0, len(maxima))
	for src := range maxima {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	spans := make([]newsStoredSpan, 0, len(srcs))
	for _, src := range srcs {
		spans = append(spans, newsStoredSpan{
			Source: src,
			Count:  1,
			Min:    maxima[src].Add(-time.Hour),
			Max:    maxima[src],
		})
	}
	return spans
}

// newsStoredSpan — сохранённый спан источника в том виде, в каком его отдаёт
// storedSpansQuery: колонки идут в порядке source, count(), min(date), max(date).
type newsStoredSpan struct {
	Source string
	Count  uint64
	Min    time.Time
	Max    time.Time
}

// newsFakeRows — фейковый driver.Rows для storedSpansQuery. Реализованы только те
// методы, которые зовёт readStoredSpans (Next/Scan/Err/Close); остальные остаются nil
// от встраивания driver.Rows и паникуют, если сценарий пойдёт дальше него.
// Готового фейка driver.Rows в репозитории нет (bold-фейк отдаёт driver.Row) — свой.
type newsFakeRows struct {
	driver.Rows
	spans []newsStoredSpan
	i     int // индекс следующей строки: Next двигает его, Scan читает i-1
}

func (r *newsFakeRows) Next() bool {
	if r.i >= len(r.spans) {
		return false
	}
	r.i++
	return true
}

// Scan принимает ровно те приёмники, что и readStoredSpans: *string, *uint64 и два
// *time.Time. Порядок колонок — часть контракта запроса, и фейк его пинит: сдвиг
// min/max в SQL не должен остаться незамеченным.
func (r *newsFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.spans) {
		return fmt.Errorf("фейковая строка покрытия: Scan без Next (i=%d, строк %d)", r.i, len(r.spans))
	}
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

func (r *newsFakeRows) Err() error { return nil }

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
// insertItems (date, source, title, url, sentiment).
type newsInsertedRow struct {
	Date      time.Time
	Source    string
	Title     string
	URL       string
	Sentiment *float32
}

// inserted разбирает записанные строки по именам колонок: порядок аргументов Append —
// контракт (он же порядок колонок DDL), и перепутанные поля обязаны валить тест, а не
// уезжать в базу. Типизированный nil в sentiment — ожидаемое состояние (канал LLM —
// follow-up): непустой sentiment означал бы запись мимо канала.
func (b *newsFakeBatch) inserted(t *testing.T) []newsInsertedRow {
	t.Helper()
	rows := make([]newsInsertedRow, 0, len(b.rows))
	for i, v := range b.rows {
		if len(v) != 5 {
			t.Fatalf("строка батча %d: значений %d, want 5 (date, source, title, url, sentiment)", i, len(v))
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
		sentiment, ok := v[4].(*float32)
		if !ok {
			t.Fatalf("строка батча %d: sentiment = %T (want *float32)", i, v[4])
		}
		if sentiment != nil {
			t.Errorf("строка батча %d: sentiment = %v, want NULL (канал заполнения — follow-up)", i, *sentiment)
		}
		rows = append(rows, newsInsertedRow{Date: date, Source: source, Title: title, URL: url, Sentiment: sentiment})
	}
	return rows
}

// newsFakeConn — фейковое соединение контура news: встраивание driver.Conn оставляет
// неиспользуемые методы nil (паника, если тест зайдёт дальше сценария), а переопределены
// ровно три метода, которыми пользуется Import: Exec (DDL и витрина), Query
// (storedSpansQuery — его зовёт readStoredSpans) и PrepareBatch (запись позиций).
// stored — очередь ответов Query; пустая очередь означает «в news_events нет строк».
type newsFakeConn struct {
	driver.Conn
	mu      sync.Mutex
	calls   []string
	stored  []newsStoredSpan
	batches []*newsFakeBatch
}

func newNewsFakeConn(stored ...newsStoredSpan) *newsFakeConn {
	return &newsFakeConn{stored: stored}
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

func (c *newsFakeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.record("query: " + query)
	c.mu.Lock()
	defer c.mu.Unlock()
	rows := c.stored
	c.stored = nil // очередь отдаётся один раз: повторный Query вернул бы уже пустое окно
	return &newsFakeRows{spans: rows}, nil
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
	// ошибки нет. Отсутствие ошибки здесь же доказывает, что все пять лент прочитались:
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
	// all — режим расписания: пишутся все позиции текущего окна всех пяти лент одним
	// батчем. Число строк считается из фикстур (readFixtureDoc + itemsFromDoc), а не
	// хардкодится: обновление снимка лент не должно требовать правки числа в тесте.
	t.Setenv(modeEnv, modeAll)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, _ := fixtureInventory(t)
	var want []string // source|title всех позиций окна — так сверяются и состав, и число
	for _, f := range feeds() {
		for _, it := range byKey[f.Key] {
			want = append(want, it.Source+"|"+it.Title)
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
	rows := batch.inserted(t)
	if len(rows) != len(want) {
		t.Errorf("в батч аппенднуто %d строк, want %d (Σ позиций фикстур)", len(rows), len(want))
	}
	if !batch.sent {
		t.Error("Send не вызван: батч в таблицу не уехал")
	}

	got := make([]string, 0, len(rows))
	sources := make(map[string]bool, 4)
	for _, row := range rows {
		got = append(got, row.Source+"|"+row.Title)
		sources[row.Source] = true
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

	// Пять лент — четыре источника: оба фида ЦБ пишутся под source cbr.
	srcs := make([]string, 0, len(sources))
	for src := range sources {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	if wantSrcs := "cbr,interfax,rbc,tass"; strings.Join(srcs, ",") != wantSrcs {
		t.Errorf("источники батча = %s, want %s", strings.Join(srcs, ","), wantSrcs)
	}
}

func TestImportSinceModeUsesCursor(t *testing.T) {
	// since — догон по курсорам из news_events (max(date) по источнику): пишутся только
	// позиции строго новее курсора. Тест держит обе ветки — «всё уже собрано» (0 строк,
	// батч не открывался) и «курсор сдвинут назад» (в батч уходит ровно то, что новее).
	// Обе ловят правку, теряющую фильтр: без него since переписывал бы окно целиком.
	t.Setenv(modeEnv, modeSince)
	pointFeedsAt(t, serveFixtures(t, nil))

	byKey, maxima := fixtureInventory(t)

	// (а) курсоры на максимумах фикстур: после успешного all новика нет (курсор строгий —
	// позиция ровно на нём уже сохранена), поэтому выборка пуста и батч не открывается.
	var s newsImport
	conn := newNewsFakeConn(spansFromMaxima(maxima)...)
	count, err := s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s) при курсорах на максимумах фикстур: %v", modeSince, err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, want 0 (максимумы фикстур уже собраны, выбирать нечего)", count)
	}
	if got := conn.batchList(); len(got) != 0 {
		t.Errorf("since открыл %d батчей при пустой выборке, want 0", len(got))
	}

	// (б) курсор interfax отодвинут на вторую позицию фикстуры (в лентах позиции идут
	// от свежих к старым, так что она старее первой): в батч обязаны уйти ровно позиции
	// interfax новее курсора, остальные источники не дают ничего.
	interfax := byKey["interfax"]
	if len(interfax) < 2 {
		t.Fatalf("фикстура interfax: %d позиций, want ≥2 (курсор по второй)", len(interfax))
	}
	cursor := interfax[1].Date
	if !cursor.Before(interfax[0].Date) {
		t.Fatalf("фикстура interfax не упорядочена по убыванию даты: %s … %s",
			interfax[0].Date.Format(time.RFC3339), cursor.Format(time.RFC3339))
	}
	var want []newsItem
	for _, it := range interfax {
		if it.Date.After(cursor) {
			want = append(want, it)
		}
	}
	// Курсор обязан резать окно, а не совпадать с его границей: иначе «0 строк против всего
	// окна» перестало бы различать работу фильтра и его потерю.
	if len(want) == 0 || len(want) == len(interfax) {
		t.Fatalf("курсор %s не режет окно interfax (%d позиций): ожидание теста вакуумное",
			cursor.Format(time.RFC3339), len(interfax))
	}

	moved := make(map[string]time.Time, len(maxima))
	for src, max := range maxima {
		moved[src] = max
	}
	moved["interfax"] = cursor

	conn = newNewsFakeConn(spansFromMaxima(moved)...)
	count, err = s.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import(%s) с курсором interfax %s: %v", modeSince, cursor.Format(time.RFC3339), err)
	}
	if int(count) != len(want) {
		t.Errorf("Import вернул %d строк, want %d (позиции interfax новее курсора)", count, len(want))
	}
	batch := conn.onlyBatch(t, newsInsert)
	if !batch.sent {
		t.Error("Send не вызван: батч в таблицу не уехал")
	}
	rows := batch.inserted(t)
	var wantKeys []string
	for _, it := range want {
		wantKeys = append(wantKeys, it.Source+"|"+it.Title)
	}
	got := make([]string, 0, len(rows))
	sources := make(map[string]bool, 1)
	for _, row := range rows {
		if !row.Date.After(cursor) {
			t.Errorf("since записал позицию %s (%s) не новее курсора %s — фильтр потерян",
				row.Title, row.Date.Format(time.RFC3339), cursor.Format(time.RFC3339))
		}
		got = append(got, row.Source+"|"+row.Title)
		sources[row.Source] = true
	}
	sort.Strings(got)
	sort.Strings(wantKeys)
	if len(got) != len(wantKeys) {
		t.Fatalf("в батч аппенднуто %d строк, want %d (позиции interfax новее курсора; вызовы: %v)",
			len(got), len(wantKeys), conn.callList())
	}
	for i := range got {
		if got[i] != wantKeys[i] {
			t.Fatalf("состав батча расходится с ожиданием: строка %d = %q, want %q", i, got[i], wantKeys[i])
		}
	}
	srcs := make([]string, 0, len(sources))
	for src := range sources {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	if len(srcs) != 1 || srcs[0] != "interfax" {
		t.Errorf("источники батча = %v, want только interfax (курсоры остальных источников — на максимумах)", srcs)
	}
}
