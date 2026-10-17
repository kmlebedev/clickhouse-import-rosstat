package news

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readTelegramFixture читает закоммиченный HTML-снимок превью t.me и разбирает его
// тем же parseTelegramPage, что и живой прогон: тест и код не расходятся в разборе
// разметки, а сеть в тестах не участвует.
func readTelegramFixture(t *testing.T, name string) telegramPage {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("нет фикстуры %s: %v", name, err)
	}
	page, err := parseTelegramPage(strings.TrimSuffix(name, ".html"), body)
	if err != nil {
		t.Fatalf("parseTelegramPage(%s): %v", name, err)
	}
	return page
}

func TestParseTelegramFixture(t *testing.T) {
	page := readTelegramFixture(t, "interfax_tg.html")

	// Фикстура — четыре настоящих сообщения окна (80244…80247), вырезанных из живой
	// страницы без правки разметки. Ни одно не пропущено: если число изменится,
	// сначала надо понять, что случилось с разметкой, а не поднимать ожидание.
	if len(page.Messages) != 4 {
		t.Fatalf("сообщений = %d, want 4 (фикстура — четыре настоящих поста)", len(page.Messages))
	}
	if page.Skipped != 0 {
		t.Errorf("Skipped = %d, want 0 (все сообщения фикстуры валидны)", page.Skipped)
	}
	if len(page.SeenIDs) != len(page.Messages) {
		t.Errorf("SeenIDs = %d, want %d (на каждое разобранное сообщение)", len(page.SeenIDs), len(page.Messages))
	}

	// Пин первого сообщения: значения прочитаны из закоммиченного
	// news/testdata/interfax_tg.html, а не выдуманы. Пин ловит перепутанные
	// селекторы (data-post/time/текст) — на живых данных это молчаливая подмена полей.
	first := page.Messages[0]
	if want := "interfaxonline"; first.Channel != want {
		t.Errorf("Channel = %q, want %q", first.Channel, want)
	}
	if want := uint64(80244); first.MessageID != want {
		t.Errorf("MessageID = %d, want %d", first.MessageID, want)
	}
	if want := time.Date(2026, time.October, 16, 15, 53, 27, 0, time.UTC); !first.Date.Equal(want) {
		t.Errorf("Date = %v, want %v (UTC из time[datetime])", first.Date, want)
	}
	if want := "https://t.me/interfaxonline/80244"; first.URL != want {
		t.Errorf("URL = %q, want %q (собирается из data-post)", first.URL, want)
	}
	const wantTitle = "Луганская Народная Республика находится полностью под российским контролем, сообщил президент РФ Владимир Путин"
	if first.Title != wantTitle {
		t.Errorf("Title = %q, want %q (первая непустая строка)", first.Title, wantTitle)
	}
	const wantText = "Луганская Народная Республика находится полностью под российским контролем, сообщил президент РФ Владимир Путин\n\n" +
		`"Под контролем киевского режима остается чуть больше 10% территории Донецкой Народной Республики и 16,5% Запорожской области", - уточнил он.`
	if first.Text != wantText {
		t.Errorf("Text = %q, want %q (полный текст без подписи, &quot; раскрыт)", first.Text, wantText)
	}
	if strings.Contains(first.Text, "@interfaxonline") {
		t.Errorf("Text = %q: подпись @interfaxonline не снята", first.Text)
	}

	// Репост с подписью @ifax_go: подпись снимается, а HTML-сущности (&quot;)
	// раскрываются — в тексте должны остаться настоящие кавычки.
	second := page.Messages[1]
	if want := uint64(80245); second.MessageID != want {
		t.Errorf("MessageID второго сообщения = %d, want %d", second.MessageID, want)
	}
	if strings.Contains(second.Text, "@ifax_go") {
		t.Errorf("Text сообщения %d = %q: подпись @ifax_go не снята", second.MessageID, second.Text)
	}
	if !strings.Contains(second.Text, `"Полюса"`) {
		t.Errorf("Text сообщения %d не содержит раскрытого &quot;Полюса&quot;: %q", second.MessageID, second.Text)
	}
	if strings.Contains(second.Text, "&quot;") {
		t.Errorf("Text сообщения %d содержит нераскрытую сущность &quot;: %q", second.MessageID, second.Text)
	}

	// Ни в одном сообщении подпись канала не должна остаться последней строкой.
	for _, m := range page.Messages {
		lines := strings.Split(m.Text, "\n")
		last := strings.TrimSpace(lines[len(lines)-1])
		if telegramSignatureRe.MatchString(last) {
			t.Errorf("сообщение %d: последняя строка %q — не снятая подпись", m.MessageID, last)
		}
	}
}

func TestTelegramItemsMapFields(t *testing.T) {
	page := readTelegramFixture(t, "interfax_tg.html")

	items := telegramItems("interfax", page)
	if len(items) != len(page.Messages) {
		t.Fatalf("telegramItems вернул %d позиций, want %d", len(items), len(page.Messages))
	}
	first := items[0]
	if first.Source != "interfax" {
		t.Errorf("Source = %q, want interfax (источник, не ключ ленты)", first.Source)
	}
	if first.Channel != "interfaxonline" || first.MessageID != 80244 {
		t.Errorf("Channel/MessageID = %q/%d, want interfaxonline/80244", first.Channel, first.MessageID)
	}
	if want := contentHash(first.Title, first.Description); first.ContentHash != want || first.ContentHash == "" {
		t.Errorf("ContentHash = %q, want %q (sha256 title+description)", first.ContentHash, want)
	}
	if first.Date.IsZero() || first.Title == "" || first.URL == "" {
		t.Errorf("позиция Telegram потеряла обязательное поле: %+v", first)
	}
}

func TestParseTelegramMediaWithoutText(t *testing.T) {
	// Медиа без подписи: позиции нет (Skipped), но ID обязан остаться в SeenIDs —
	// иначе T2 решит, что сообщение исчезло из окна, и сделает ложный tombstone.
	const page = `<div class="tgme_widget_message" data-post="interfaxonline/90001">
  <div class="tgme_widget_message_photo_wrap"></div>
  <time datetime="2026-10-17T10:00:00+00:00"></time>
</div>
<div class="tgme_widget_message" data-post="interfaxonline/90002">
  <time datetime="2026-10-17T10:01:00+00:00"></time>
  <div class="tgme_widget_message_text">Текст <b>молнии</b></div>
</div>`

	parsed, err := parseTelegramPage("interfax_tg", []byte(page))
	if err != nil {
		t.Fatalf("parseTelegramPage: %v", err)
	}
	if len(parsed.Messages) != 1 {
		t.Fatalf("Messages = %d, want 1 (медиа без текста — не позиция)", len(parsed.Messages))
	}
	if parsed.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1 (медиа без текста)", parsed.Skipped)
	}
	for _, id := range []uint64{90001, 90002} {
		if _, ok := parsed.SeenIDs[id]; !ok {
			t.Errorf("SeenIDs не содержит %d; медиа без текста нельзя считать удалённым", id)
		}
	}
	if got := parsed.Messages[0].MessageID; got != 90002 {
		t.Errorf("MessageID = %d, want 90002", got)
	}
}

func TestParseTelegramSkipsBrokenPostAndDate(t *testing.T) {
	// Битый data-post/дата — не ошибка всего окна: сообщение уходит в Skipped, а окно
	// продолжает разбираться. ID, который удалось прочитать, остаётся в SeenIDs.
	const page = `<div class="tgme_widget_message" data-post="interfaxonline/not-a-number">
  <time datetime="2026-10-17T10:00:00+00:00"></time>
  <div class="tgme_widget_message_text">Битый data-post</div>
</div>
<div class="tgme_widget_message" data-post="interfaxonline/90003">
  <time datetime="вчера"></time>
  <div class="tgme_widget_message_text">Битая дата</div>
</div>
<div class="tgme_widget_message" data-post="interfaxonline/90004">
  <div class="tgme_widget_message_text">Нет времени</div>
</div>
<div class="tgme_widget_message" data-post="/90005">
  <time datetime="2026-10-17T10:00:00+00:00"></time>
  <div class="tgme_widget_message_text">Пустой канал</div>
</div>
<div class="tgme_widget_message" data-post="interfaxonline/90006">
  <time datetime="2026-10-17T10:00:00+00:00"></time>
  <div class="tgme_widget_message_text">Валидная молния</div>
</div>`

	parsed, err := parseTelegramPage("interfax_tg", []byte(page))
	if err != nil {
		t.Fatalf("parseTelegramPage: %v", err)
	}
	if len(parsed.Messages) != 1 {
		t.Fatalf("Messages = %d, want 1 (валидна ровно одна позиция)", len(parsed.Messages))
	}
	if got := parsed.Messages[0].MessageID; got != 90006 {
		t.Errorf("MessageID валидной позиции = %d, want 90006", got)
	}
	if parsed.Skipped != 4 {
		t.Errorf("Skipped = %d, want 4 (битый data-post, дата, нет времени, пустой канал)", parsed.Skipped)
	}
	for _, id := range []uint64{90003, 90004, 90005, 90006} {
		if _, ok := parsed.SeenIDs[id]; !ok {
			t.Errorf("SeenIDs не содержит %d (ID читается даже у пропущенного сообщения)", id)
		}
	}
}

func TestParseTelegramEmptyPageFails(t *testing.T) {
	// Ноль узлов сообщений — чужая страница или заглушка WAF; ноль распознанных
	// сообщений при живых узлах — смена разметки/данных. Оба случая — ошибка,
	// а не «успех с нулём»: пустое окно не должно выглядеть нормальным прогоном.
	for _, tc := range []struct {
		name string
		page string
		want string
	}{
		{
			name: "пустое тело",
			page: "",
			want: "нет сообщений",
		},
		{
			name: "чужая страница",
			page: `<html><body>Доступ ограничен</body></html>`,
			want: "нет сообщений",
		},
		{
			name: "все сообщения битые",
			page: `<div class="tgme_widget_message" data-post="interfaxonline/90001">
  <time datetime="вчера"></time>
  <div class="tgme_widget_message_text">Битая дата</div>
</div>`,
			want: "нет распознанных сообщений",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseTelegramPage("interfax_tg", []byte(tc.page))
			if err == nil {
				t.Fatalf("parseTelegramPage вернул %d сообщений без ошибки, want ошибку %q", len(parsed.Messages), tc.want)
			}
			if !strings.Contains(err.Error(), "interfax_tg") {
				t.Errorf("ошибка = %q, want упоминание source (иначе сбой не отнести к ленте)", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка = %q, want фрагмент %q", err, tc.want)
			}
			if len(parsed.Messages) != 0 {
				t.Errorf("при ошибке Messages = %d, want 0", len(parsed.Messages))
			}
		})
	}
}

func TestParseTelegramTextNormalization(t *testing.T) {
	// Один синтетический пост держит сразу все правила текста: <br/>, закрывающие
	// блочные теги, серии пустых строк, &quot; и снятие подписи. Разметка — та же,
	// что у настоящих постов (инлайн-теги внутри div.tgme_widget_message_text).
	const page = `<div class="tgme_widget_message" data-post="interfaxonline/91000">
  <time datetime="2026-10-17T11:00:00+00:00"></time>
  <div class="tgme_widget_message_text" dir="auto"><b>Первая</b> строка<br/><br/><br/><p>Вторая &quot;цитата&quot;</p><div>Третья</div><br/><a href="https://t.me/interfaxonline">@interfaxonline</a></div>
</div>`

	parsed, err := parseTelegramPage("interfax_tg", []byte(page))
	if err != nil {
		t.Fatalf("parseTelegramPage: %v", err)
	}
	const want = "Первая строка\n\nВторая \"цитата\"\nТретья"
	if got := parsed.Messages[0].Text; got != want {
		t.Errorf("Text = %q, want %q (br/блоки → переводы строк, 3+ → 2, подпись снята)", got, want)
	}
	if got := parsed.Messages[0].Title; got != "Первая строка" {
		t.Errorf("Title = %q, want %q", got, "Первая строка")
	}
}

func TestTelegramTitleTruncatedByWord(t *testing.T) {
	// Заголовок длиннее лимита режется по последнему пробелу: обрывок слова
	// в title хуже, чем чуть более короткий читаемый заголовок.
	line := strings.Repeat("слово ", 80)
	got := telegramTitle(line+"\nвторая строка", telegramTitleRunes)
	if n := len([]rune(got)); n > telegramTitleRunes {
		t.Errorf("длина title = %d рун, want ≤ %d", n, telegramTitleRunes)
	}
	if strings.HasSuffix(got, "слов") || strings.HasSuffix(got, "сло") {
		t.Errorf("title %q оборван посреди слова", got)
	}
	for _, word := range strings.Fields(got) {
		if word != "слово" {
			t.Errorf("title содержит обрывок слова %q: %q", word, got)
		}
	}

	if got, want := telegramTitle("один два три", 11), "один два"; got != want {
		t.Errorf("telegramTitle(limit=11) = %q, want %q (рез по границе слова)", got, want)
	}
}

func TestContentHashStableAndChanges(t *testing.T) {
	const title, description = "Заголовок", "Полный текст молнии"

	first := contentHash(title, description)
	if second := contentHash(title, description); first != second {
		t.Fatalf("contentHash не стабилен: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Errorf("contentHash = %q (длина %d), want 64 hex-символа sha256", first, len(first))
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Errorf("contentHash = %q не hex: %v", first, err)
	}
	// Разделитель "\n" не даёт склеиться разным парам: ("ab","c") и ("a","bc")
	// обязаны дать разные ключи версий.
	if got := contentHash("ab", "c"); got == contentHash("a", "bc") {
		t.Errorf("contentHash склеил разные пары title/description в один ключ")
	}
	if got := contentHash(title, description+"!"); got == first {
		t.Errorf("contentHash не изменился при изменении description")
	}
	if got := contentHash(title+"!", description); got == first {
		t.Errorf("contentHash не изменился при изменении title")
	}
}

// TestTelegramChannelFromURL — имя канала из адреса публичного превью: проверка канала
// держится на этой форме, и смена конфига на другой URL не должна молча отключать сверку.
func TestTelegramChannelFromURL(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"https://t.me/s/interfaxonline", "interfaxonline"},
		{"https://t.me/s/interfaxonline/", "interfaxonline"},
		{"https://t.me/interfaxonline", ""},
		{"https://example.com/other", ""},
	} {
		if got := telegramChannelFromURL(tc.url); got != tc.want {
			t.Errorf("telegramChannelFromURL(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}
}

// TestCheckTelegramChannel — data-post обязан совпадать с каналом конфига: иначе редирект
// превью или смена разметки писали бы в версии и витрину чужой канал.
func TestCheckTelegramChannel(t *testing.T) {
	msgs := []telegramMessage{{Channel: "interfaxonline", MessageID: 1}}
	if err := checkTelegramChannel("interfaxonline", msgs); err != nil {
		t.Errorf("checkTelegramChannel(совпадает) = %v, want nil", err)
	}
	if err := checkTelegramChannel("", msgs); err != nil {
		t.Errorf("checkTelegramChannel(пустой want) = %v, want nil (проверка отключена)", err)
	}
	if err := checkTelegramChannel("interfaxonline", nil); err != nil {
		t.Errorf("checkTelegramChannel(пустой список) = %v, want nil", err)
	}
	if err := checkTelegramChannel("other", msgs); err == nil {
		t.Error("checkTelegramChannel(чужой канал) = nil, want ошибку")
	}
}
