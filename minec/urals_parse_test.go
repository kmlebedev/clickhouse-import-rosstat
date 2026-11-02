package minec

import (
	"os"
	"strings"
	"testing"
	"time"
)

// readFixture читает реальную страницу источника из testdata: тесты разбора
// офлайн и на артефактах Минэка, а не на синтетике (стандарт §11.1).
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("фикстура %s: %v", name, err)
	}
	return body
}

func mustMonth(t *testing.T, year int, month time.Month) time.Time {
	t.Helper()
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
}

// TestMinecParseListingFirstPage — первая страница листинга: полная страница с
// <title> раздела, 10 материалов, из них 5 Urals за 2026-06 … 2026-10.
func TestMinecParseListingFirstPage(t *testing.T) {
	page, err := minecParseListing(readFixture(t, "materials_p1.html"), true)
	if err != nil {
		t.Fatalf("minecParseListing: %v", err)
	}
	if page.items != 10 {
		t.Errorf("материалов на странице %d, ожидалось 10", page.items)
	}
	wantMonths := []time.Time{
		mustMonth(t, 2026, time.October),
		mustMonth(t, 2026, time.September),
		mustMonth(t, 2026, time.August),
		mustMonth(t, 2026, time.July),
		mustMonth(t, 2026, time.June),
	}
	if len(page.urals) != len(wantMonths) {
		t.Fatalf("ссылок на Urals %d, ожидалось %d", len(page.urals), len(wantMonths))
	}
	for i, link := range page.urals {
		if !link.month.Equal(wantMonths[i]) {
			t.Errorf("ссылка %d: месяц %s, ожидался %s", i, link.month.Format("2006-01"), wantMonths[i].Format("2006-01"))
		}
		if !strings.Contains(link.title, "Юралс") {
			t.Errorf("ссылка %d: заголовок %q без «Юралс»", i, link.title)
		}
		if !strings.HasSuffix(link.href, ".html") {
			t.Errorf("ссылка %d: href %q не страница", i, link.href)
		}
	}
	wantHref := "/material/departments/d12/konyunktura_mirovyh_tovarnyh_rynkov/o_srednem_urovne_cen_nefti_sorta_yurals_za_oktyabr_2026_goda.html"
	if page.urals[0].href != wantHref {
		t.Errorf("первая ссылка %q, ожидалась %q", page.urals[0].href, wantHref)
	}
	// Второй материал страницы — «новое морское месторождение»: он считается в
	// items (конец списка определяется по любому материалу), но в urals не попадает.
	for _, link := range page.urals {
		if strings.Contains(link.title, "морском месторождении") {
			t.Errorf("в ряды попал чужой материал: %q", link.title)
		}
	}
}

// TestMinecParseListingFragment — страницы 2+ сайт отдаёт AJAX-фрагментом без
// <title>: маркер не требуется, месяцы 2026-01 … 2026-05.
func TestMinecParseListingFragment(t *testing.T) {
	page, err := minecParseListing(readFixture(t, "materials_p2.html"), false)
	if err != nil {
		t.Fatalf("minecParseListing: %v", err)
	}
	if page.items != 10 {
		t.Errorf("материалов на странице %d, ожидалось 10", page.items)
	}
	wantMonths := []time.Time{
		mustMonth(t, 2026, time.May),
		mustMonth(t, 2026, time.April),
		mustMonth(t, 2026, time.March),
		mustMonth(t, 2026, time.February),
		mustMonth(t, 2026, time.January),
	}
	if len(page.urals) != len(wantMonths) {
		t.Fatalf("ссылок на Urals %d, ожидалось %d", len(page.urals), len(wantMonths))
	}
	for i, link := range page.urals {
		if !link.month.Equal(wantMonths[i]) {
			t.Errorf("ссылка %d: месяц %s, ожидался %s", i, link.month.Format("2006-01"), wantMonths[i].Format("2006-01"))
		}
	}
}

// TestMinecParseListingEndPage — страницы за последней источник отдаёт телом
// «\n\n» со статусом 200: это конец списка, а не ошибка разбора.
func TestMinecParseListingEndPage(t *testing.T) {
	page, err := minecParseListing(readFixture(t, "materials_end.html"), false)
	if err != nil {
		t.Fatalf("конец списка прочитан как ошибка: %v", err)
	}
	if page.items != 0 || len(page.urals) != 0 {
		t.Errorf("на странице конца списка материалов %d, ссылок %d", page.items, len(page.urals))
	}
}

// TestMinecParseListingRejectsArticle — статья вместо листинга: у полной страницы
// нет <title> раздела, у фрагмента нет ни одной карточки. Оба случая — ошибка,
// иначе подменённая страница обнулила бы обход молча.
func TestMinecParseListingRejectsArticle(t *testing.T) {
	body := readFixture(t, "yurals_2026_10.html")
	if _, err := minecParseListing(body, true); err == nil {
		t.Error("статья принята за первую страницу листинга: маркер раздела не проверен")
	}
	if _, err := minecParseListing(body, false); err == nil {
		t.Error("статья принята за фрагмент листинга: страница без карточек не дала ошибки")
	}
}

// TestMinecParseArticle — три эпохи разметки цены: 2018 («Средняя цена на нефть
// сорта … на мировых рынках»), 2024 («Уровень цены»), 2025+ («Средняя цена»).
// Значения — литералы из живых страниц источника.
func TestMinecParseArticle(t *testing.T) {
	checks := []struct {
		fixture string
		price   float64
		month   time.Time
	}{
		{"yurals_2018_01.html", 68.60, mustMonth(t, 2018, time.January)},
		{"yurals_2024_01.html", 64.14, mustMonth(t, 2024, time.January)},
		{"yurals_2026_10.html", 101.46, mustMonth(t, 2026, time.October)},
		{"yurals_2026_sentyabr.html", 92.08, mustMonth(t, 2026, time.September)},
		{"yurals_2026_avgust.html", 67.11, mustMonth(t, 2026, time.August)},
		{"yurals_2026_iyul.html", 59.02, mustMonth(t, 2026, time.July)},
		{"yurals_2026_iyun.html", 63.52, mustMonth(t, 2026, time.June)},
	}
	for _, check := range checks {
		price, month, err := minecParseArticle(readFixture(t, check.fixture))
		if err != nil {
			t.Errorf("%s: %v", check.fixture, err)
			continue
		}
		if price != check.price {
			t.Errorf("%s: цена %v, ожидалась %v", check.fixture, price, check.price)
		}
		if !month.Equal(check.month) {
			t.Errorf("%s: месяц %s, ожидался %s", check.fixture, month.Format("2006-01"), check.month.Format("2006-01"))
		}
	}
}

// TestMinecParseArticleRejectsOtherMaterial — реальная страница того же раздела
// про новое морское месторождение: она не про Urals (цена в ней «долл. США за
// тонну»), и импортёр обязан упасть, а не записать чужое число. Вторая ветка —
// страница листинга вместо статьи: заголовка статьи у неё нет.
func TestMinecParseArticleRejectsOtherMaterial(t *testing.T) {
	_, _, err := minecParseArticle(readFixture(t, "onm_2026_06.html"))
	if err == nil {
		t.Fatal("страница про другое месторождение разобрана без ошибки")
	}
	if !strings.Contains(err.Error(), "не про Urals") {
		t.Errorf("ошибка не называет причину отказа: %v", err)
	}
	if _, _, err := minecParseArticle(readFixture(t, "materials_p1.html")); err == nil {
		t.Error("страница листинга принята за статью: заголовок статьи не проверен")
	}
}

// TestMinecParseTitleMonth — формы заголовка из листинга и статьи, включая «за
// май» (не «мая»), и отказ на заголовке без месяца.
func TestMinecParseTitleMonth(t *testing.T) {
	checks := []struct {
		title string
		want  time.Time
	}{
		{"О среднем уровне цен нефти сорта «Юралс» за октябрь 2026 года", mustMonth(t, 2026, time.October)},
		{"О средней цене на нефть сорта «Юралс» за май 2018 года", mustMonth(t, 2018, time.May)},
		{"О среднем уровне цены нефти сорта «Юралс» за январь 2024 года.*", mustMonth(t, 2024, time.January)},
		{"За ДЕКАБРЬ 2019 года", mustMonth(t, 2019, time.December)},
	}
	for _, check := range checks {
		got, err := minecParseTitleMonth(check.title)
		if err != nil {
			t.Errorf("%q: %v", check.title, err)
			continue
		}
		if !got.Equal(check.want) {
			t.Errorf("%q: месяц %s, ожидался %s", check.title, got.Format("2006-01"), check.want.Format("2006-01"))
		}
	}
	if _, err := minecParseTitleMonth("О ценах на нефть"); err == nil {
		t.Error("заголовок без месяца разобран без ошибки")
	}
}

// TestMinecParsePrice — десятичная запятая; нулевое, отрицательное и выше
// minecMaxPrice значение — ошибка.
func TestMinecParsePrice(t *testing.T) {
	checks := map[string]float64{
		"101,46": 101.46,
		"40,95":  40.95,
	}
	for raw, want := range checks {
		got, err := minecParsePrice(raw)
		if err != nil {
			t.Errorf("%q: %v", raw, err)
			continue
		}
		if got != want {
			t.Errorf("%q: %v, ожидалось %v", raw, got, want)
		}
	}
	for _, raw := range []string{"0,00", "-1,00", "abc", "1234,56"} {
		if _, err := minecParsePrice(raw); err == nil {
			t.Errorf("%q: ошибки нет", raw)
		}
	}
}

// TestMinecPriceRegexBoundary — левая граница цены: число слева не «съедается»
// в неверную цену, а четырёхзначное значение не матчится вовсе.
func TestMinecPriceRegexBoundary(t *testing.T) {
	cases := map[string]string{
		"в 2026 101,46 долл. США за баррель":   "101,46",
		"составила 64,14 долл. США за баррель": "64,14",
		"1101,46 долл. США за баррель":         "",
	}
	for text, want := range cases {
		m := minecPriceRe.FindStringSubmatch(text)
		got := ""
		if m != nil {
			got = m[1]
		}
		if got != want {
			t.Errorf("%q: цена %q, ожидалась %q", text, got, want)
		}
	}
	if m := minecTitleMonthRe.FindStringSubmatch("база данных 2024"); m != nil {
		t.Errorf("«за» внутри слова принято за предлог: %v", m)
	}
}

// TestMinecSeriesMeta — каталог описывает ровно тот ряд, который пишет импортёр,
// и не оставляет поля пустыми: агент читает единицы и привязку даты только отсюда.
func TestMinecSeriesMeta(t *testing.T) {
	if len(minecSeriesMeta) != 1 {
		t.Fatalf("рядов в каталоге %d, ожидался 1", len(minecSeriesMeta))
	}
	meta := minecSeriesMeta[0]
	if meta.Source != minecSource || meta.Series != minecUralsSeries {
		t.Errorf("каталог описывает %s/%s, а импортёр пишет %s/%s", meta.Source, meta.Series, minecSource, minecUralsSeries)
	}
	if meta.Title == "" || meta.Unit == "" || meta.Frequency != "M" || meta.Origin == "" || meta.Description == "" {
		t.Errorf("ряд описан не полностью: %+v", meta)
	}
	if !strings.Contains(meta.Description, "2018-06") {
		t.Error("в описании ряда нет оговорки о пропуске 2018-06/07: агент прочитает разрыв как отсутствие данных")
	}
}
