package util

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
