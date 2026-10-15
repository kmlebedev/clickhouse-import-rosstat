package cbr

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// dipFixture читает живую HTML-фикстуру страницы выпусков бюллетеня
// (cbr/testdata/dip_page.html — страница https://www.cbr.ru/ec_research/mb/, снятая
// 2026-10-15). Ожидаемые значения ниже — литералами, а не вычислением: тест, сверяющий
// разбор с самим собой, прошёл бы и после порчи селектора или регекса.
func dipFixture(t *testing.T) *goquery.Document {
	t.Helper()
	page, err := os.ReadFile("testdata/dip_page.html")
	if err != nil {
		t.Fatalf("чтение testdata/dip_page.html: %v", err)
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(page)))
	if err != nil {
		t.Fatalf("разбор testdata/dip_page.html: %v", err)
	}
	return doc
}

// dipDocFromHTML — документ из HTML-строки: мутации фикстуры для fail-fast-веток
// (строкой, а не файлом — правки точечные и не заслуживают отдельных фикстур).
func dipDocFromHTML(t *testing.T, html string) *goquery.Document {
	t.Helper()
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("разбор html-строки: %v", err)
	}
	return doc
}

// TestDipParseReleasesFixture — разбор живой страницы выпусков: 49 ссылок на приложения
// (класс a.versions_item носят и 284 ссылки на PDF-выпуски, и индексы новостей — без
// отбора по href на .xlsx разбор взял бы чужие файлы), 49 уникальных id (ключ винтажа),
// порядок страницы (свежие первыми) и сборка абсолютного адреса из относительного href.
func TestDipParseReleasesFixture(t *testing.T) {
	releases, err := dipParseReleases(dipFixture(t))
	if err != nil {
		t.Fatalf("разбор живой страницы: %v", err)
	}
	if len(releases) != 49 {
		t.Fatalf("выпусков %d, want 49 (на странице 49 ссылок на приложения .xlsx)", len(releases))
	}
	seen := make(map[string]bool, len(releases))
	for _, r := range releases {
		if seen[r.ID] {
			t.Errorf("дубль id %q в разборе: ключ винтажа не уникален", r.ID)
		}
		seen[r.ID] = true
	}
	if len(seen) != 49 {
		t.Errorf("уникальных id %d, want 49", len(seen))
	}

	// Порядок страницы (свежие первыми) и сборка абсолютного адреса из относительного href.
	first := releases[0]
	if first.ID != "62444" {
		t.Errorf("первая запись: id = %q, want %q (свежие первыми — как на странице)", first.ID, "62444")
	}
	if want := dipUrlPrefix + "/Collection/Collection/File/62444/bulletin_26-07.xlsx"; first.URL != want {
		t.Errorf("первая запись: url = %q, want %q", first.URL, want)
	}
	if first.Release != "26-07" {
		t.Errorf("первая запись: release = %q, want %q (№ 7 / Октябрь в группе 2026)", first.Release, "26-07")
	}
	if !first.PublicationDate.Equal(time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("первая запись: публикация = %s, want 2026-10-14", first.PublicationDate)
	}
}

// TestDipParseReleasesSpots — spot-значения из разных эпох страницы: современная вёрстка
// (2026, 2025), год с полной нумерацией в тексте (2020: «№ 8 / Декабрь 2020») и дубль имени
// файла stat_bulletin_23-07.xlsx (id 46493 и 46646): release собирается из подписи и года
// группы, а не из имени файла, поэтому у дубля имени release разные (23-07 и 23-08).
func TestDipParseReleasesSpots(t *testing.T) {
	releases, err := dipParseReleases(dipFixture(t))
	if err != nil {
		t.Fatalf("разбор живой страницы: %v", err)
	}
	byID := make(map[string]dipRelease, len(releases))
	for _, r := range releases {
		byID[r.ID] = r
	}
	utc := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	for _, c := range []struct {
		id      string
		release string
		pub     time.Time
	}{
		{"62444", "26-07", utc(2026, time.October, 14)},
		{"62343", "26-06", utc(2026, time.September, 1)},
		{"46646", "23-08", utc(2023, time.December, 5)},
		{"46493", "23-07", utc(2023, time.October, 17)},
		{"29360", "20-06", utc(2020, time.October, 14)},
		{"31723", "20-08", utc(2020, time.December, 9)},
	} {
		r, ok := byID[c.id]
		if !ok {
			t.Errorf("id %s не найден в разборе", c.id)
			continue
		}
		if r.Release != c.release {
			t.Errorf("id %s: release = %q, want %q", c.id, r.Release, c.release)
		}
		if !r.PublicationDate.Equal(c.pub) {
			t.Errorf("id %s: публикация = %s, want %s", c.id, r.PublicationDate.Format("2006-01-02"), c.pub.Format("2006-01-02"))
		}
	}
}

// TestDipParseReleasesOldest — старейшая запись страницы: последняя ссылка последней
// группы. Порядок страницы (свежие первыми) без сортировки: сортировку делает импортёр.
func TestDipParseReleasesOldest(t *testing.T) {
	releases, err := dipParseReleases(dipFixture(t))
	if err != nil {
		t.Fatalf("разбор живой страницы: %v", err)
	}
	last := releases[len(releases)-1]
	if last.ID != "29360" {
		t.Errorf("последняя запись: id = %q, want %q", last.ID, "29360")
	}
	if want := dipUrlPrefix + "/Collection/Collection/File/29360/stat_bulletin_20-06.xlsx"; last.URL != want {
		t.Errorf("последняя запись: url = %q, want %q", last.URL, want)
	}
	if last.Release != "20-06" {
		t.Errorf("последняя запись: release = %q, want %q", last.Release, "20-06")
	}
	if !last.PublicationDate.Equal(time.Date(2020, time.October, 14, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("последняя запись: публикация = %s, want 2020-10-14", last.PublicationDate)
	}
}

// TestDipParseReleasesFailFast — ветки стандарта §6.4 на мутированном HTML: любое
// отклонение структуры страницы — ошибка, а не «0 выпусков» или частичный результат.
func TestDipParseReleasesFailFast(t *testing.T) {
	// Живой фрагмент страницы (2023, дубль имени файла) — основа мутаций.
	fragment := `<html><body>
<div class="versions_items " data-versions-items="2023 г." data-item-select="2023 г.">
    <a class="versions_item" href="/Collection/Collection/File/46646/stat_bulletin_23-07.xlsx"
       data-tooltip data-tooltip-content="Опубликовано 05 декабря 2023"
       data-versions-item="">№&#160;8&#160;/ Декабрь</a>
    <a class="versions_item" href="/Collection/Collection/File/46493/stat_bulletin_23-07.xlsx"
       data-tooltip data-tooltip-content="Опубликовано 17 октября 2023"
       data-versions-item="">№&#160;7&#160;/ Октябрь</a>
</div>
</body></html>`

	t.Run("NoLinks", func(t *testing.T) {
		// Страница без блока выпусков: структура источника изменилась — ошибка, а не пустой список.
		if releases, err := dipParseReleases(dipDocFromHTML(t, "<html><body><p>пусто</p></body></html>")); err == nil {
			t.Errorf("разбор страницы без блока выпусков = %v, want ошибка", releases)
		}
	})

	t.Run("NoTooltip", func(t *testing.T) {
		// Ссылка без data-tooltip-content: дата публикации — часть винтажа, и выпуск без
		// неё в разбор не годится (пропуск сделал бы выпуск без даты публикации).
		mutated := strings.Replace(fragment, ` data-tooltip-content="Опубликовано 05 декабря 2023"`, "", 1)
		if releases, err := dipParseReleases(dipDocFromHTML(t, mutated)); err == nil {
			t.Errorf("разбор ссылки без %s = %v, want ошибка", dipTooltipAttr, releases)
		}
	})

	t.Run("BadMonth", func(t *testing.T) {
		// Дата с неразбираемым месяцем: ЦБ сменил формулировку подсказки — ошибка с
		// исходным текстом подсказки, а не «0 выпусков».
		mutated := strings.Replace(fragment, "05 декабря 2023", "05 цветенья 2023", 1)
		_, err := dipParseReleases(dipDocFromHTML(t, mutated))
		if err == nil {
			t.Fatal("разбор подсказки с неразбираемым месяцем прошёл без ошибки")
		}
		if !strings.Contains(err.Error(), "Опубликовано 05") {
			t.Errorf("ошибка %q не называет исходный текст подсказки", err)
		}
	})

	t.Run("DuplicateID", func(t *testing.T) {
		// Дубль id: id — ключ винтажа (release_id), и два выпуска с одним id дали бы в
		// таблице две версии одного ключа. Здесь он же ловит и дубль имени файла:
		// stat_bulletin_23-07.xlsx с одним id дважды — не тот случай, что на живой
		// странице (там имя повторяется, а id — нет).
		mutated := strings.Replace(fragment, "File/46646/", "File/46493/", 1)
		if releases, err := dipParseReleases(dipDocFromHTML(t, mutated)); err == nil {
			t.Errorf("разбор страницы с дублем id = %d выпусков, want ошибка", len(releases))
		}
	})

	t.Run("NonNumericID", func(t *testing.T) {
		// Нецифровой id: id — ключ винтажа (release_id), и нецифровой путь — не тот источник.
		mutated := strings.Replace(fragment, "File/46646/", "File/abc/", 1)
		_, err := dipParseReleases(dipDocFromHTML(t, mutated))
		if err == nil || !strings.Contains(err.Error(), "abc") {
			t.Errorf("разбор нецифрового id: err = %v, want ошибка, называющая «abc»", err)
		}
	})

	t.Run("NoReleaseNumber", func(t *testing.T) {
		// Текст ссылки без «№ N»: номер выпуска — часть release, без него винтаж не собрать.
		mutated := strings.Replace(fragment, "№&#160;8&#160;/ Декабрь", "Октябрь", 1)
		if releases, err := dipParseReleases(dipDocFromHTML(t, mutated)); err == nil {
			t.Errorf("разбор ссылки без номера выпуска = %d выпусков, want ошибка", len(releases))
		}
	})

	t.Run("NoGroupYear", func(t *testing.T) {
		// Группа без «г.»: год берётся только из атрибута группы — ошибка, а не «0 выпусков».
		mutated := strings.Replace(fragment, `data-versions-items="2023 г."`, `data-versions-items="2023"`, 1)
		if releases, err := dipParseReleases(dipDocFromHTML(t, mutated)); err == nil {
			t.Errorf("разбор группы без года = %d выпусков, want ошибка", len(releases))
		}
	})
}

// TestDipParsePublished — дата публикации без магии локали: месяц — русское название
// в родительном падеже через util.MonthsToNum, результат — полночь UTC (дата-атрибут
// выпуска, не точка ряда).
func TestDipParsePublished(t *testing.T) {
	got, err := dipParsePublished("Опубликовано 14 октября 2026")
	if err != nil {
		t.Fatalf("разбор подсказки: %v", err)
	}
	if want := time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("dipParsePublished = %s, want %s", got, want)
	}
	// Формулировка, не являющаяся датой публикации, — ошибка с исходным текстом.
	if _, err := dipParsePublished("Обновлено 14 октября 2026"); err == nil {
		t.Error("разбор подсказки без слова «Опубликовано» прошёл без ошибки")
	}
}
