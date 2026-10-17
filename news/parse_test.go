package news

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mskOffset — офсет московского времени, который несут pubDate всех наших лент
// («+0300»). Фиксированная зона (не time.Local): офсет приходит из ленты, и тест
// не должен зависеть от таймзоны машины, где он запущен.
const mskOffset = 3 * 3600

func TestParsePubDateVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want time.Time
		ok   bool
	}{
		{
			// Современная форма лент ЦБ/ТАСС/Интерфакса: секунды и числовой офсет.
			name: "секунды и числовой офсет",
			in:   "Mon, 12 Oct 2026 13:43:00 +0300",
			want: time.Date(2026, time.October, 12, 13, 43, 0, 0, time.FixedZone("", mskOffset)),
			ok:   true,
		},
		{
			// Старая форма без секунд: у части лент минута точности, и позицию нельзя
			// терять только из-за отсутствия секунд.
			name: "без секунд",
			in:   "Mon, 12 Oct 2026 13:43 +0300",
			want: time.Date(2026, time.October, 12, 13, 43, 0, 0, time.FixedZone("", mskOffset)),
			ok:   true,
		},
		{
			// Именованная зона: раскладка RFC1123, офсет нулевой. GMT и UTC — единственные
			// аббревиатуры, которые можно принять: обе однозначны и не зависят от TZ хоста.
			name: "именованная зона GMT",
			in:   "Mon, 12 Oct 2026 13:43:00 GMT",
			want: time.Date(2026, time.October, 12, 13, 43, 0, 0, time.UTC),
			ok:   true,
		},
		{
			name: "именованная зона UTC",
			in:   "Mon, 12 Oct 2026 13:43:00 UTC",
			want: time.Date(2026, time.October, 12, 13, 43, 0, 0, time.UTC),
			ok:   true,
		},
		{
			// Прочие аббревиатуры time.Parse принимает как зону с нулевым смещением —
			// момент молча сдвигается (для МСК на 3 часа), причём сдвиг зависит от TZ
			// машины. Такая дата обязана считаться нераспознанной: позиция уйдёт в skipped.
			name: "аббревиатура MSK отвергается",
			in:   "Mon, 12 Oct 2026 13:43:00 MSK",
			want: time.Time{},
			ok:   false,
		},
		{
			name: "аббревиатура CET отвергается",
			in:   "Mon, 12 Oct 2026 13:43:00 CET",
			want: time.Time{},
			ok:   false,
		},
		{
			// 12.10.2026 — формат элемента rbc_news:date, а не pubDate. Если однажды
			// в pubDate приедет такая запись, позиция обязана попасть в skipped,
			// а не разобраться «почти правильно».
			name: "формат rbc_news:date",
			in:   "12.10.2026",
			want: time.Time{},
			ok:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePubDate(tc.in)
			if ok != tc.ok {
				t.Fatalf("parsePubDate(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			}
			if !ok {
				if !got.IsZero() {
					t.Errorf("parsePubDate(%q) при неудаче вернул %v, want нулевое время", tc.in, got)
				}
				return
			}
			if !got.Equal(tc.want) {
				t.Errorf("parsePubDate(%q) = %v, want %v (тот же момент)", tc.in, got, tc.want)
			}
			// Equal игнорирует зону, а зона — часть данных: момент публикации хранится
			// с офсетом источника (DateTime в news_events), поэтому сверяем и смещение.
			if got.Format(time.RFC3339) != tc.want.Format(time.RFC3339) {
				t.Errorf("parsePubDate(%q) = %s, want %s (офсет источника обязан сохраниться)",
					tc.in, got.Format(time.RFC3339), tc.want.Format(time.RFC3339))
			}
		})
	}
}

func TestItemsFromDocMapsFixtureItem(t *testing.T) {
	doc := readFixtureDoc(t, "interfax.rss")

	items, _, err := itemsFromDoc("interfax", doc)
	if err != nil {
		t.Fatalf("itemsFromDoc(interfax) на живой фикстуре: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("itemsFromDoc(interfax) вернул 0 позиций при %d item-ах в ленте",
			len(doc.Channel.Items))
	}
	for i, it := range items {
		if it.Date.IsZero() {
			t.Errorf("позиция %d (%q): нулевая дата — в news_events она уехала бы как 1970 год", i, it.Title)
		}
		if it.Source != "interfax" {
			t.Errorf("позиция %d: Source = %q, want %q", i, it.Source, "interfax")
		}
	}

	// Пин первой позиции фикстуры: значения прочитаны из закоммиченного
	// news/testdata/interfax.rss, а не выдуманы. Пин ловит перепутанные теги
	// (title/link/pubDate) — на живых данных это молчаливая подмена полей.
	first := items[0]
	if wantTitle := "В Белгородской области поврежден объект инфраструктуры"; first.Title != wantTitle {
		t.Errorf("Title первой позиции = %q, want %q", first.Title, wantTitle)
	}
	if wantURL := "https://www.interfax.ru/russia/1121865"; first.URL != wantURL {
		t.Errorf("URL первой позиции = %q, want %q", first.URL, wantURL)
	}
	if want := time.Date(2026, time.October, 12, 14, 22, 0, 0, time.FixedZone("", mskOffset)); !first.Date.Equal(want) {
		t.Errorf("Date первой позиции = %v, want %v", first.Date, want)
	}
}

func TestItemsFromDocParsesDescription(t *testing.T) {
	// smart-lab отдаёт в description полный текст поста (HTML после разбора XML-сущностей) —
	// ради этого колонка и заведена. Пин на живой фикстуре: пост /blog/1363355.php (разбор
	// сентябрьской инфляции у spydell) обязан доехать до позиции целиком, а не анонсом.
	doc := readFixtureDoc(t, "smartlab_spydell.rss")

	items, _, err := itemsFromDoc("smartlab", doc)
	if err != nil {
		t.Fatalf("itemsFromDoc(smartlab_spydell) на живой фикстуре: %v", err)
	}

	var found bool
	for _, it := range items {
		if !strings.Contains(it.URL, "/blog/1363355.php") {
			continue
		}
		found = true
		if it.Description == "" {
			t.Fatalf("позиция %s: Description пуст — текст поста не разобрался", it.URL)
		}
		// Краевые пробелы срезаются (pretty-print ленты вокруг текстового узла), а сам
		// текст сохраняется как есть: витрина отдаёт агенту именно текст поста.
		if it.Description != strings.TrimSpace(it.Description) {
			t.Errorf("позиция %s: Description с краевыми пробелами (trim не сработал)", it.URL)
		}
		for _, want := range []string{"устойчивая инфляция", "Замедление обеспечили овощи и фрукты"} {
			if !strings.Contains(it.Description, want) {
				t.Errorf("позиция %s: Description не содержит %q (текст поста обрезан?)", it.URL, want)
			}
		}
		if want := "По оценке Банка России"; !strings.HasPrefix(it.Description, want) {
			head := []rune(it.Description)
			if len(head) > 60 {
				head = head[:60]
			}
			t.Errorf("позиция %s: Description = %q…, want префикс %q", it.URL, string(head), want)
		}
	}
	if !found {
		t.Fatalf("в фикстуре smartlab_spydell.rss нет позиции с url /blog/1363355.php — пин вакуумный")
	}
}

func TestItemsFromDocSkipsInvalidItems(t *testing.T) {
	// Синтетическая лента: три битые позиции (без pubDate, пробельный заголовок,
	// нераспознанная дата) и одна валидная с «грязными» пробелами — она же проверяет
	// нормализацию заголовка, обрезку краёв ссылки и описания.
	doc := decodeDoc(t, `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item><title>Без даты</title><link>https://example.test/1</link></item>
  <item><title>   </title><link>https://example.test/2</link><pubDate>Mon, 12 Oct 2026 13:43:00 +0300</pubDate></item>
  <item><title>Битая дата</title><link>https://example.test/3</link><pubDate>12.10.2026</pubDate></item>
  <item><title>  Валидная   позиция  </title><link> https://example.test/4 </link><description>  Текст описания  </description><pubDate>Mon, 12 Oct 2026 13:43:00 +0300</pubDate></item>
</channel></rss>`)

	items, skipped, err := itemsFromDoc("interfax", doc)
	if err != nil {
		t.Fatalf("itemsFromDoc: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1 (валидна ровно одна позиция из %d)", len(items), len(doc.Channel.Items))
	}
	if skipped != 3 {
		t.Errorf("skipped = %d, want 3 (без даты, пробельный заголовок, битая дата)", skipped)
	}
	if want := "Валидная позиция"; items[0].Title != want {
		t.Errorf("Title = %q, want %q (серии пробелов схлопываются, края срезаются)", items[0].Title, want)
	}
	if want := "https://example.test/4"; items[0].URL != want {
		t.Errorf("URL = %q, want %q (ссылка обрезается по краям)", items[0].URL, want)
	}
	if want := "Текст описания"; items[0].Description != want {
		t.Errorf("Description = %q, want %q (края срезаются, текст описания сохраняется)", items[0].Description, want)
	}

	// Позиция без ссылки — не пропуск: пустой link сохраняется как есть, иначе
	// лента с одним лишь заголовком теряла бы событие тезиса. Описания у неё нет —
	// это тоже не пропуск и не ошибка: в колонку уходит пустая строка.
	linkless := decodeDoc(t, `<rss><channel><item><title>Без ссылки</title><pubDate>Mon, 12 Oct 2026 13:43:00 +0300</pubDate></item></channel></rss>`)
	items, skipped, err = itemsFromDoc("cbr", linkless)
	if err != nil {
		t.Fatalf("itemsFromDoc(без ссылки): %v", err)
	}
	if len(items) != 1 || skipped != 0 {
		t.Fatalf("позиция без link: items = %d, skipped = %d, want 1 и 0", len(items), skipped)
	}
	if items[0].URL != "" {
		t.Errorf("URL позиции без link = %q, want пустую строку", items[0].URL)
	}
	if items[0].Description != "" {
		t.Errorf("Description позиции без описания = %q, want пустую строку (позиция не пропущена)", items[0].Description)
	}
}

func TestItemsFromDocEmptyFeedFails(t *testing.T) {
	// Ноль валидных позиций — ошибка, а не «успех с нулём»: иначе сменившая форму
	// лента (или 200-заглушка WAF) выглядела бы прогоном без новостей.
	for _, tc := range []struct {
		name string
		xml  string
		want string
	}{
		{
			name: "лента без item-ов",
			xml:  `<rss><channel><title>Интерфакс</title></channel></rss>`,
			want: "пустая лента: нет распознанных позиций (item-ов 0)",
		},
		{
			name: "все позиции битые",
			xml: `<rss><channel>
  <item><title>Без даты</title><link>https://example.test/1</link></item>
  <item><title></title><link>https://example.test/2</link><pubDate>12.10.2026</pubDate></item>
</channel></rss>`,
			want: "пустая лента: нет распознанных позиций (item-ов 2)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items, _, err := itemsFromDoc("tass", decodeDoc(t, tc.xml))
			if err == nil {
				t.Fatalf("itemsFromDoc вернул %d позиций без ошибки, want ошибку %q", len(items), tc.want)
			}
			if err.Error() != tc.want {
				t.Errorf("ошибка = %q, want %q", err.Error(), tc.want)
			}
			if len(items) != 0 {
				t.Errorf("items = %d, want 0 (при ошибке позиции не отдаются)", len(items))
			}
		})
	}
}

func TestFixturesParse(t *testing.T) {
	// Семь живых лент: парные ленты пишутся под общим source (оба фида ЦБ — «cbr»,
	// обе ленты smart-lab — «smartlab»; различает их url), иначе витрина получила бы
	// лишние источники там, где источник один — Банк России или smart-lab.
	for _, tc := range []struct {
		file   string
		source string
		// skipped — фактическое значение на закоммиченной фикстуре. Ноль у всех семи:
		// у каждой позиции есть pubDate в одной из трёх раскладок и непустой заголовок.
		// Если фикстура обновится и skipped станет ненулевым, тест укажет, где именно
		// лента сменила форму, и причину надо будет разобрать (а не поднять значение).
		skipped int
	}{
		{"interfax.rss", "interfax", 0},
		{"rbc.rss", "rbc", 0},
		{"cbr_news.rss", "cbr", 0},
		{"cbr_press.rss", "cbr", 0},
		{"tass.rss", "tass", 0},
		{"smartlab_lebedev.rss", "smartlab", 0},
		{"smartlab_spydell.rss", "smartlab", 0},
	} {
		t.Run(tc.file, func(t *testing.T) {
			doc := readFixtureDoc(t, tc.file)

			items, skipped, err := itemsFromDoc(tc.source, doc)
			if err != nil {
				t.Fatalf("itemsFromDoc(%s): %v", tc.file, err)
			}
			if len(items) == 0 {
				t.Fatalf("%s: 0 позиций при %d item-ах", tc.file, len(doc.Channel.Items))
			}
			if skipped != tc.skipped {
				t.Errorf("%s: skipped = %d, want %d", tc.file, skipped, tc.skipped)
			}
			// Позиции не теряются молча: каждая либо в items, либо посчитана в skipped.
			if len(items)+skipped != len(doc.Channel.Items) {
				t.Errorf("%s: items + skipped = %d, а item-ов в ленте %d",
					tc.file, len(items)+skipped, len(doc.Channel.Items))
			}
			for i, it := range items {
				if it.Source != tc.source {
					t.Errorf("%s: позиция %d: Source = %q, want %q", tc.file, i, it.Source, tc.source)
				}
				if it.Date.IsZero() {
					t.Errorf("%s: позиция %d (%q): нулевая дата", tc.file, i, it.Title)
				}
				if it.Title == "" {
					t.Errorf("%s: позиция %d: пустой заголовок в items (должен был уйти в skipped)", tc.file, i)
				}
			}
		})
	}
}

func TestRssDocRejectsForeignRoot(t *testing.T) {
	// Carry-решение Task 1: rssDoc пинит корень «rss». Без пина 200-страница HTML
	// (заглушка WAF) разобралась бы в нулевую структуру без ошибки — тот самый тихий
	// ноль, от которого GetXML защищается проверкой статуса. С пином чужой корень
	// обязан дать ошибку декодирования, а не пустую ленту.
	var doc rssDoc
	if err := xml.Unmarshal([]byte("<html><body>Доступ ограничен</body></html>"), &doc); err == nil {
		t.Fatalf("HTML-страница разобралась в rssDoc без ошибки: корень не пиннится, " +
			"и заглушка WAF неотличима от пустой ленты")
	}
}

// readFixtureDoc читает закоммиченную фикстуру ленты и разбирает её в rssDoc.
// Тесты ходят в файлы, а не в сеть: прогон обязан быть воспроизводимым офлайн,
// а форма ленты зафиксирована снимком на конкретную дату.
func readFixtureDoc(t *testing.T, name string) rssDoc {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("нет фикстуры %s: %v", name, err)
	}
	var doc rssDoc
	if err := xml.Unmarshal(body, &doc); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", name, err)
	}
	return doc
}

// decodeDoc разбирает XML-литерал, собранный прямо в тесте (синтетические ленты:
// пустая, битые позиции), — тот же разбор, что у readFixtureDoc, минус файл.
func decodeDoc(t *testing.T, body string) rssDoc {
	t.Helper()
	var doc rssDoc
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("разбор тестового XML: %v\n%s", err, body)
	}
	return doc
}
