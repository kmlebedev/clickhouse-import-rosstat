package util

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/text/encoding/charmap"
)

// Happy path: 200 + валидный XML → поля заполнены, ошибки нет. Заодно пиним
// User-Agent: хелпер обязан слать randomUA, как GetFile/GetCSV (конвенция проекта,
// правило 4 AGENTS.md). Проверка живёт в хендлере: он выполняет запрос, а t.Errorf
// безопасен из чужой горутины (t.Fatal там же уронил бы не тот фрейм).
func TestGetXMLDecodesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetFile/GetCSV")
		}
		_, _ = io.WriteString(w, `<root><value>42</value></root>`)
	}))
	defer server.Close()

	var out struct {
		Value int `xml:"value"`
	}
	if err := GetXML(server.URL, &out); err != nil {
		t.Fatalf("GetXML 200: %v", err)
	}
	if out.Value != 42 {
		t.Errorf("out.Value = %d, ожидалось 42", out.Value)
	}
}

// Не-200 обязан падать до разбора тела: HTML-заглушка WAF — не «пустой XML», и
// ошибка разбора скрыла бы настоящую причину отказа. В свою ошибку статуса попадают
// код и тело (по нему 403 WAF отличим от 403 «лента недоступна»), но не URL: ошибка
// уезжает в лог (урок eia) — контекст добавляет вызывающий. Проверка ниже покрывает
// только ветку статуса: транспортная ошибка приходит из net/http как *url.Error и
// URL содержит, её маскирует вызывающий.
func TestGetXMLNonOKStatusFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<html>blocked</html>")
	}))
	defer server.Close()

	var out struct {
		Value int `xml:"value"`
	}
	err := GetXML(server.URL, &out)
	if err == nil {
		t.Fatal("403 должен давать ошибку, а не тихий ноль с зелёным dagu")
	}
	for _, want := range []string{"403", "blocked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if strings.Contains(err.Error(), server.URL) {
		t.Errorf("ошибка содержит URL %q — ключ из query утёк бы в лог: %v", server.URL, err)
	}
}

// 200 с не-XML телом (HTML вместо данных) — ошибка разбора, а не молчаливый ноль:
// иначе импортёр счёл бы ленту пустой и «успешно» ничего не записал.
func TestGetXMLMalformedBodyFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "not xml")
	}))
	defer server.Close()

	var out struct {
		Value int `xml:"value"`
	}
	if err := GetXML(server.URL, &out); err == nil {
		t.Fatal("битый XML должен давать ошибку, а не тихий ноль")
	}
}

// Пустое тело со статусом 200 — ошибка (как в GetFile/GetJSON): пустой ответ
// неотличим от «в ленте нет новостей», и молчаливый ноль выглядел бы успешным прогоном.
func TestGetXMLEmptyBodyFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	var out struct {
		Value int `xml:"value"`
	}
	if err := GetXML(server.URL, &out); err == nil {
		t.Fatal("пустое тело должно давать ошибку, а не тихий ноль")
	}
}

// Дедлайн — не украшение: сбойная лента не должна вешать весь прогон, а зависший
// ответ сервера без таймаута ошибки не даёт. Подменяем xmlTimeout на короткий
// (переменная, а не константа — ровно ради этого), хендлер держит запрос дольше:
// ждём ошибку дедлайна, а не тишину.
func TestGetXMLTimeoutFails(t *testing.T) {
	const handlerDelay = 2 * time.Second
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// select, а не голый time.Sleep: на разрыв клиента хендлер завершается сам,
		// и server.Close() в defer не ждёт полные 2 s впустую.
		select {
		case <-r.Context().Done():
		case <-time.After(handlerDelay):
		}
	}))
	defer server.Close()

	old := xmlTimeout
	xmlTimeout = 50 * time.Millisecond
	t.Cleanup(func() { xmlTimeout = old })

	var out struct {
		Value int `xml:"value"`
	}
	err := GetXML(server.URL, &out)
	if err == nil {
		t.Fatal("зависший ответ должен падать по дедлайну, а не ждать вечно")
	}
	// Именно ошибка дедлайна, а не любая другая: на неё и рассчитывает обход
	// сбойных лент — иначе «получили ошибку» ничего не говорит о причине.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не является ошибкой дедлайна (context.DeadlineExceeded)", err)
	}
}

// Ленты встречаются с именованными HTML-сущностями (`&nbsp;` в заголовке), которых
// нет в XML-таблице: без HTMLEntity декодер падает, и одна такая сущность обнуляла
// бы ленту целиком. Проверяем, что разбор прошёл, а сущность стала неразрывным
// пробелом, а не мусором.
func TestGetXMLHTMLEntityDecodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `<root><title>a&nbsp;b</title></root>`)
	}))
	defer server.Close()

	var out struct {
		Title string `xml:"title"`
	}
	if err := GetXML(server.URL, &out); err != nil {
		t.Fatalf("GetXML с &nbsp;: %v", err)
	}
	if out.Title != "a\u00a0b" {
		t.Errorf("out.Title = %q, ожидалось %q", out.Title, "a\u00a0b")
	}
}

// Пиннит контракт использования: приёмник обязан пинить корень через XMLName
// (`xml:"rss"`), а не поведение хелпера без пина. Без пина 200 с well-formed
// HTML-страницей WAF (та же заглушка, что и в NonOKStatusFails, но с честным 200)
// разбирается в нулевую структуру без ошибки — encoding/xml не сверяет имя корня и
// пропускает неизвестные элементы, и вызывающий получил бы тихий ноль с зелёным
// прогоном. С пином чужой корень — ошибка декодирования, и в ней видно, что пришёл
// именно html.
func TestGetXMLRootMismatchFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><html><body>blocked</body></html>`)
	}))
	defer server.Close()

	var out struct {
		XMLName xml.Name `xml:"rss"`
	}
	err := GetXML(server.URL, &out)
	if err == nil {
		t.Fatal("чужой корень при пине XMLName должен давать ошибку, а не тихий ноль с зелёным прогоном")
	}
	if !strings.Contains(err.Error(), "html") {
		t.Errorf("ошибка %q не называет пришедший корень html", err)
	}
}

// ParseXML обязан понимать объявленную кодировку windows-1251: реальные XML-фиды ЦБ
// (XML_dynamic.asp…VAL_NM_RQ=R01235, курс USD) объявляют её, а без CharsetReader
// encoding/xml падает с «encoding "windows-1251" declared but Decoder.CharsetReader
// is nil» — тот самый живой сбой XML-импортёров курса. Фикстура повторяет живой ответ
// ЦБ: объявление windows-1251, запись с датой 08.10.2026 и значением 85,4811; кириллица
// в атрибуте нужна, чтобы декодирование было настоящим, а не ASCII-проходом. Байты
// собираем энкодером charmap.Windows1251: фикстура остаётся читаемой строкой, а в теле
// лежит валидный windows-1251 с верной декларацией.
func TestParseXMLWindows1251(t *testing.T) {
	const fixture = `<?xml version="1.0" encoding="windows-1251"?>
<ValCurs ID="R01235" DateRange1="08.10.2026" DateRange2="08.10.2026" name="Динамика официального курса заданной валюты">
	<Record Date="08.10.2026" Id="R01235">
		<Nominal>1</Nominal>
		<Value>85,4811</Value>
		<VunitRate>85,4811</VunitRate>
	</Record>
</ValCurs>`
	body, err := charmap.Windows1251.NewEncoder().Bytes([]byte(fixture))
	if err != nil {
		t.Fatalf("перекодирование фикстуры в windows-1251: %v", err)
	}

	var out struct {
		XMLName xml.Name `xml:"ValCurs"`
		Value   string   `xml:"Record>Value"`
	}
	if err := ParseXML(body, &out); err != nil {
		t.Fatalf("ParseXML с windows-1251: %v", err)
	}
	if out.Value != "85,4811" {
		t.Errorf("out.Value = %q, ожидалось %q", out.Value, "85,4811")
	}
}

// Неподдерживаемую кодировку нельзя молча отдать декодеру как есть: байты koi8-r в
// UTF-8-полях стали бы мусорными значениями в ClickHouse, и заметить это на дашборде
// невозможно. Поэтому такая кодировка — ошибка, и в её тексте обязана быть сама
// кодировка: без неё сбой ленты читается как «что-то пошло не так», а причина — что
// фид ЦБ перевели на другую кодировку.
func TestParseXMLUnknownCharset(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="koi8-r"?><ValCurs><Record><Value>85,4811</Value></Record></ValCurs>`)

	var out struct {
		XMLName xml.Name `xml:"ValCurs"`
		Value   string   `xml:"Record>Value"`
	}
	err := ParseXML(body, &out)
	if err == nil {
		t.Fatal("неподдерживаемая кодировка должна давать ошибку, а не мусор в полях")
	}
	if !strings.Contains(err.Error(), "koi8-r") {
		t.Errorf("ошибка %q не называет кодировку koi8-r", err)
	}
}

// Пин корня через XMLName — контракт приёмника, и перенос разбора в ParseXML его не
// отменяет: 200 с чужой страницей (HTML-заглушка WAF) при пине обязан давать ошибку
// декодирования, а не тихий ноль с зелёным прогоном. Проверка живёт здесь, на уровне
// разбора: GetXML сверху — лишь fetch, а регрессию пина ловим у самого декодера.
func TestParseXMLWrongRoot(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?><html><body>blocked</body></html>`)

	var out struct {
		XMLName xml.Name `xml:"ValCurs"`
	}
	err := ParseXML(body, &out)
	if err == nil {
		t.Fatal("чужой корень при пине XMLName должен давать ошибку, а не тихий ноль")
	}
	if !strings.Contains(err.Error(), "html") {
		t.Errorf("ошибка %q не называет пришедший корень html", err)
	}
}
