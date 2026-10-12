package news

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
		// Сбойная лента окна не даёт: пустой span выглядел бы дырой покрытия.
		{Feed: feed{Key: "rbc", Source: "rbc"}, Err: errors.New("сбой ленты")},
	}

	spans := feedSpans(results)
	if len(spans) != 2 {
		t.Fatalf("feedSpans вернул %d источников, want 2 (cbr и tass; сбойный rbc окна не даёт): %v", len(spans), spans)
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
			// Лента отдала ноль позиций (сбой виден в feedResult.Err): пустое окно
			// не повод объявлять дыру.
			name:   "пустое окно ленты",
			feed:   map[string]sourceSpan{"cbr": {Count: 0}},
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
